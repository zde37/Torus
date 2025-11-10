package pkg

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryConfig holds configuration for in-memory storage.
type MemoryConfig struct {
	// CleanupInterval determines how often expired entries are removed.
	// Default is 1 minute if not specified.
	CleanupInterval time.Duration
}

// MemoryStorage implements the Storage interface using an in-memory map.
// It provides thread-safe operations with automatic cleanup of expired entries.
type MemoryStorage struct {
	mu            sync.RWMutex
	data          map[string]*entry
	cleanupTicker *time.Ticker
	done          chan struct{}
	closed        atomic.Bool

	// Metrics for monitoring
	hits      atomic.Int64
	misses    atomic.Int64
	sets      atomic.Int64
	deletes   atomic.Int64
	evictions atomic.Int64
}

// entry represents a stored value with expiration.
type entry struct {
	value     []byte
	expiresAt time.Time
}

// NewMemoryStorage creates a new in-memory storage instance.
// If config is nil, default values are used.
func NewMemoryStorage(config *MemoryConfig) *MemoryStorage {
	cleanupInterval := time.Minute
	if config != nil && config.CleanupInterval > 0 {
		cleanupInterval = config.CleanupInterval
	}

	ms := &MemoryStorage{
		data:          make(map[string]*entry),
		cleanupTicker: time.NewTicker(cleanupInterval),
		done:          make(chan struct{}),
	}

	// Start cleanup goroutine
	go ms.cleanupExpired()

	return ms
}

// Get retrieves the value associated with the given key.
// Returns ErrKeyNotFound if the key doesn't exist or has expired.
func (ms *MemoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ErrContextCanceled
	default:
	}

	// Check if storage is closed
	if ms.closed.Load() {
		return nil, ErrStorageUnavailable
	}

	ms.mu.RLock()
	entry, exists := ms.data[key]
	ms.mu.RUnlock()

	if !exists {
		ms.misses.Add(1)
		return nil, ErrKeyNotFound
	}

	// Check if entry has expired
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		// Remove expired entry
		ms.mu.Lock()
		delete(ms.data, key)
		ms.mu.Unlock()

		ms.misses.Add(1)
		ms.evictions.Add(1)
		return nil, ErrKeyNotFound
	}

	ms.hits.Add(1)

	// Return a copy of the value to prevent external modification
	result := make([]byte, len(entry.value))
	copy(result, entry.value)

	return result, nil
}

// Set stores a value with the given key and TTL.
// If TTL is 0, the value will not expire.
func (ms *MemoryStorage) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return ErrContextCanceled
	default:
	}

	// Check if storage is closed
	if ms.closed.Load() {
		return ErrStorageUnavailable
	}

	// Calculate expiration time
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	// Create a copy of the value to prevent external modification
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Store the entry
	ms.mu.Lock()
	ms.data[key] = &entry{
		value:     valueCopy,
		expiresAt: expiresAt,
	}
	ms.mu.Unlock()

	ms.sets.Add(1)
	return nil
}

// Increment atomically increments a counter for the given key.
// If the key doesn't exist, it is created with value 1.
// The TTL is set for new keys or updated for existing keys.
func (ms *MemoryStorage) Increment(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return 0, ErrContextCanceled
	default:
	}

	// Check if storage is closed
	if ms.closed.Load() {
		return 0, ErrStorageUnavailable
	}

	// Calculate expiration time
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Check if key exists and hasn't expired
	if e, exists := ms.data[key]; exists {
		if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
			// Entry has expired, start fresh
			delete(ms.data, key)
			ms.evictions.Add(1)
		} else {
			// Decode current value
			if len(e.value) == 8 {
				currentValue := binary.BigEndian.Uint64(e.value)
				newValue := currentValue + 1

				// Update value and expiration
				binary.BigEndian.PutUint64(e.value, newValue)
				e.expiresAt = expiresAt

				ms.sets.Add(1)
				return int64(newValue), nil
			}
			// Invalid format, reset to 1
		}
	}

	// Create new entry with value 1
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, 1)

	ms.data[key] = &entry{
		value:     value,
		expiresAt: expiresAt,
	}

	ms.sets.Add(1)
	return 1, nil
}

// Delete removes the key and its associated value from storage.
// No error is returned if the key doesn't exist.
func (ms *MemoryStorage) Delete(ctx context.Context, key string) error {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return ErrContextCanceled
	default:
	}

	// Check if storage is closed
	if ms.closed.Load() {
		return ErrStorageUnavailable
	}

	ms.mu.Lock()
	delete(ms.data, key)
	ms.mu.Unlock()

	ms.deletes.Add(1)
	return nil
}

// Close gracefully shuts down the storage and releases resources.
func (ms *MemoryStorage) Close() error {
	// Check if already closed
	if !ms.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Stop cleanup goroutine
	ms.cleanupTicker.Stop()
	close(ms.done)

	// Clear all data
	ms.mu.Lock()
	ms.data = nil
	ms.mu.Unlock()

	return nil
}

// // TODO: Implement Shutdown with context support
// func (ms *MemoryStorage) Shutdown(ctx context.Context) error {
// 	// Drain in-flight operations with timeout
// 	// Then call Close()
// }

// cleanupExpired runs periodically to remove expired entries.
func (ms *MemoryStorage) cleanupExpired() {
	for {
		select {
		case <-ms.cleanupTicker.C:
			ms.removeExpiredEntries()
		case <-ms.done:
			return
		}
	}
}

// removeExpiredEntries removes all expired entries from the storage.
func (ms *MemoryStorage) removeExpiredEntries() {
	now := time.Now()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	for key, entry := range ms.data {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(ms.data, key)
			ms.evictions.Add(1)
		}
	}
}

// Stats returns current storage statistics.
type Stats struct {
	Entries   int
	Hits      int64
	Misses    int64
	Sets      int64
	Deletes   int64
	Evictions int64
}

// GetStats returns current storage statistics.
func (ms *MemoryStorage) GetStats() Stats {
	ms.mu.RLock()
	entries := len(ms.data)
	ms.mu.RUnlock()

	return Stats{
		Entries:   entries,
		Hits:      ms.hits.Load(),
		Misses:    ms.misses.Load(),
		Sets:      ms.sets.Load(),
		Deletes:   ms.deletes.Load(),
		Evictions: ms.evictions.Load(),
	}
}

// Clear removes all entries from storage but keeps it operational.
func (ms *MemoryStorage) Clear() error {
	if ms.closed.Load() {
		return ErrStorageUnavailable
	}

	ms.mu.Lock()
	ms.data = make(map[string]*entry)
	ms.mu.Unlock()

	return nil
}

// GetAll returns all key-value pairs in storage (excluding expired entries).
func (ms *MemoryStorage) GetAll(ctx context.Context) (map[string][]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ErrContextCanceled
	default:
	}

	if ms.closed.Load() {
		return nil, ErrStorageUnavailable
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make(map[string][]byte)
	now := time.Now()

	for key, entry := range ms.data {
		// Skip expired entries (zero time means no expiration)
		if !entry.expiresAt.IsZero() && entry.expiresAt.Before(now) {
			continue
		}

		// Copy the value
		value := make([]byte, len(entry.value))
		copy(value, entry.value)
		result[key] = value
	}

	return result, nil
}

// GetMultiple retrieves multiple values in a single operation.
// Returns a map of key-value pairs for existing keys.
func (ms *MemoryStorage) GetMultiple(ctx context.Context, keys []string) (map[string][]byte, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, ErrContextCanceled
	default:
	}

	// Check if storage is closed
	if ms.closed.Load() {
		return nil, ErrStorageUnavailable
	}

	now := time.Now()
	result := make(map[string][]byte)

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for _, key := range keys {
		if entry, exists := ms.data[key]; exists {
			// Check if entry has expired
			if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
				ms.misses.Add(1)
				continue
			}

			// Copy value to prevent external modification
			valueCopy := make([]byte, len(entry.value))
			copy(valueCopy, entry.value)
			result[key] = valueCopy
			ms.hits.Add(1)
		} else {
			ms.misses.Add(1)
		}
	}

	return result, nil
}

// SetMultiple stores multiple key-value pairs in a single operation.
func (ms *MemoryStorage) SetMultiple(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return ErrContextCanceled
	default:
	}

	// Check if storage is closed
	if ms.closed.Load() {
		return ErrStorageUnavailable
	}

	// Calculate expiration time
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	for key, value := range items {
		// Create a copy of the value
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		ms.data[key] = &entry{
			value:     valueCopy,
			expiresAt: expiresAt,
		}
		ms.sets.Add(1)
	}

	return nil
}