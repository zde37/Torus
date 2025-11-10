package pkg

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStorageGet tests the Get method.
func TestMemoryStorageGet(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	tests := []struct {
		name      string
		setup     func()
		key       string
		wantValue []byte
		wantErr   error
	}{
		{
			name:    "key not found",
			setup:   func() {},
			key:     "nonexistent",
			wantErr: ErrKeyNotFound,
		},
		{
			name: "valid key",
			setup: func() {
				storage.Set(ctx, "test-key", []byte("test-value"), time.Hour)
			},
			key:       "test-key",
			wantValue: []byte("test-value"),
			wantErr:   nil,
		},
		{
			name: "expired key",
			setup: func() {
				storage.Set(ctx, "expired-key", []byte("value"), time.Millisecond)
				time.Sleep(2 * time.Millisecond)
			},
			key:     "expired-key",
			wantErr: ErrKeyNotFound,
		},
		{
			name: "empty value",
			setup: func() {
				storage.Set(ctx, "empty", []byte{}, time.Hour)
			},
			key:       "empty",
			wantValue: []byte{},
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear storage before each test
			storage.Clear()

			// Run setup
			tt.setup()

			// Test Get
			value, err := storage.Get(ctx, tt.key)

			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr, err, "Expected specific error")
			} else {
				require.NoError(t, err, "Get() should not error")
				assert.Equal(t, tt.wantValue, value, "Value should match")
			}
		})
	}
}

// TestMemoryStorageSet tests the Set method.
func TestMemoryStorageSet(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	tests := []struct {
		name    string
		key     string
		value   []byte
		ttl     time.Duration
		wantErr error
	}{
		{
			name:    "basic set",
			key:     "key1",
			value:   []byte("value1"),
			ttl:     time.Hour,
			wantErr: nil,
		},
		{
			name:    "set with no TTL",
			key:     "key2",
			value:   []byte("value2"),
			ttl:     0,
			wantErr: nil,
		},
		{
			name:    "overwrite existing key",
			key:     "key1",
			value:   []byte("new-value"),
			ttl:     time.Hour,
			wantErr: nil,
		},
		{
			name:    "empty value",
			key:     "empty",
			value:   []byte{},
			ttl:     time.Hour,
			wantErr: nil,
		},
		{
			name:    "large value",
			key:     "large",
			value:   make([]byte, 1024*1024), // 1MB
			ttl:     time.Hour,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.Set(ctx, tt.key, tt.value, tt.ttl)

			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr, err, "Expected specific error")
			} else {
				require.NoError(t, err, "Set() should not error")

				// Verify the value was stored correctly
				got, err := storage.Get(ctx, tt.key)
				require.NoError(t, err, "Failed to get stored value")
				assert.Equal(t, tt.value, got, "Stored value should match")
			}
		})
	}
}

// TestMemoryStorageIncrement tests the Increment method.
func TestMemoryStorageIncrement(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	tests := []struct {
		name      string
		setup     func()
		key       string
		ttl       time.Duration
		wantValue int64
		wantErr   error
	}{
		{
			name:      "new key",
			setup:     func() {},
			key:       "counter1",
			ttl:       time.Hour,
			wantValue: 1,
			wantErr:   nil,
		},
		{
			name: "existing counter",
			setup: func() {
				storage.Increment(ctx, "counter2", time.Hour)
				storage.Increment(ctx, "counter2", time.Hour)
			},
			key:       "counter2",
			ttl:       time.Hour,
			wantValue: 3,
			wantErr:   nil,
		},
		{
			name: "expired counter resets to 1",
			setup: func() {
				storage.Increment(ctx, "expired", time.Millisecond)
				time.Sleep(2 * time.Millisecond)
			},
			key:       "expired",
			ttl:       time.Hour,
			wantValue: 1,
			wantErr:   nil,
		},
		{
			name: "non-counter value resets to 1",
			setup: func() {
				storage.Set(ctx, "non-counter", []byte("not-a-number"), time.Hour)
			},
			key:       "non-counter",
			ttl:       time.Hour,
			wantValue: 1,
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear storage before each test
			storage.Clear()

			// Run setup
			tt.setup()

			// Test Increment
			value, err := storage.Increment(ctx, tt.key, tt.ttl)

			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr, err, "Expected specific error")
			} else {
				require.NoError(t, err, "Increment() should not error")
				assert.Equal(t, tt.wantValue, value, "Increment value should match")
			}
		})
	}
}

// TestMemoryStorageDelete tests the Delete method.
func TestMemoryStorageDelete(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	// Set some test data
	storage.Set(ctx, "key1", []byte("value1"), time.Hour)
	storage.Set(ctx, "key2", []byte("value2"), time.Hour)

	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{
			name:    "delete existing key",
			key:     "key1",
			wantErr: nil,
		},
		{
			name:    "delete non-existent key",
			key:     "nonexistent",
			wantErr: nil, // Should not return error
		},
		{
			name:    "delete already deleted key",
			key:     "key1",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.Delete(ctx, tt.key)

			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr, err, "Expected specific error")
			} else {
				require.NoError(t, err, "Delete() should not error")

				// Verify key is deleted
				_, err = storage.Get(ctx, tt.key)
				assert.Equal(t, ErrKeyNotFound, err, "Key should be deleted")
			}
		})
	}
}

// TestMemoryStorageExpiration tests automatic expiration of entries.
func TestMemoryStorageExpiration(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: 50 * time.Millisecond}) // Fast cleanup for testing
	defer storage.Close()

	// Set entries with different TTLs
	err := storage.Set(ctx, "short", []byte("short-lived"), 100*time.Millisecond)
	require.NoError(t, err, "Set short entry should not error")

	err = storage.Set(ctx, "long", []byte("long-lived"), time.Hour)
	require.NoError(t, err, "Set long entry should not error")

	err = storage.Set(ctx, "no-ttl", []byte("no-expiry"), 0)
	require.NoError(t, err, "Set no-ttl entry should not error")

	// Initially all should exist
	for _, key := range []string{"short", "long", "no-ttl"} {
		_, err := storage.Get(ctx, key)
		require.NoError(t, err, "Key %s should exist initially", key)
	}

	// Wait for short-lived entry to expire
	time.Sleep(150 * time.Millisecond)

	// Short should be gone
	_, err = storage.Get(ctx, "short")
	assert.Equal(t, ErrKeyNotFound, err, "Short-lived entry should have expired")

	// Long and no-ttl should still exist
	_, err = storage.Get(ctx, "long")
	assert.NoError(t, err, "Long-lived entry should still exist")

	_, err = storage.Get(ctx, "no-ttl")
	assert.NoError(t, err, "No-TTL entry should still exist")

	// Check stats
	stats := storage.GetStats()
	assert.Greater(t, int(stats.Evictions), 0, "Should have at least one eviction")
}

// TestMemoryStorageConcurrentAccess tests concurrent access to storage.
func TestMemoryStorageConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	const (
		numGoroutines = 100
		numOperations = 1000
	)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOperations)

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				if err := storage.Set(ctx, key, value, time.Hour); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j/2) // Read some that might exist
				storage.Get(ctx, key)
			}
		}(i)
	}

	// Concurrent increments
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("counter-%d", id%10) // Share some counters
				storage.Increment(ctx, key, time.Hour)
			}
		}(i)
	}

	// Concurrent deletes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				storage.Delete(ctx, key)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		assert.NoError(t, err, "Concurrent operation error")
		errorCount++
	}
	assert.Equal(t, 0, errorCount, "Should have no errors from concurrent operations")

	// Verify counters
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("counter-%d", i)
		value, err := storage.Get(ctx, key)
		if err == nil {
			count := binary.BigEndian.Uint64(value)
			// Each counter should have been incremented multiple times
			assert.Greater(t, int(count), 0, "Counter %s should have non-zero value", key)
		}
	}
}

// TestMemoryStorageContextCancellation tests context cancellation handling.
func TestMemoryStorageContextCancellation(t *testing.T) {
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// All operations should return context cancelled error
	_, err := storage.Get(ctx, "key")
	assert.Equal(t, ErrContextCanceled, err, "Get with cancelled context should return ErrContextCanceled")

	err = storage.Set(ctx, "key", []byte("value"), time.Hour)
	assert.Equal(t, ErrContextCanceled, err, "Set with cancelled context should return ErrContextCanceled")

	_, err = storage.Increment(ctx, "key", time.Hour)
	assert.Equal(t, ErrContextCanceled, err, "Increment with cancelled context should return ErrContextCanceled")

	err = storage.Delete(ctx, "key")
	assert.Equal(t, ErrContextCanceled, err, "Delete with cancelled context should return ErrContextCanceled")
}

// TestMemoryStorageClose tests the Close method.
func TestMemoryStorageClose(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})

	// Add some data
	storage.Set(ctx, "key", []byte("value"), time.Hour)

	// Close storage
	err := storage.Close()
	assert.NoError(t, err, "Close() should not error")

	// Operations after close should fail
	_, err = storage.Get(ctx, "key")
	assert.Equal(t, ErrStorageUnavailable, err, "Get after Close should return ErrStorageUnavailable")

	err = storage.Set(ctx, "key", []byte("value"), time.Hour)
	assert.Equal(t, ErrStorageUnavailable, err, "Set after Close should return ErrStorageUnavailable")

	// Close again should not panic
	err = storage.Close()
	assert.NoError(t, err, "Second Close() should not return error")
}

// TestMemoryStorageStats tests the statistics functionality.
func TestMemoryStorageStats(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	// Initial stats should be zero
	stats := storage.GetStats()
	assert.Equal(t, 0, stats.Entries, "Initial entries should be zero")
	assert.Equal(t, int64(0), stats.Hits, "Initial hits should be zero")
	assert.Equal(t, int64(0), stats.Misses, "Initial misses should be zero")

	// Perform operations
	storage.Set(ctx, "key1", []byte("value1"), time.Hour)
	storage.Set(ctx, "key2", []byte("value2"), time.Hour)
	storage.Get(ctx, "key1")                     // Hit
	storage.Get(ctx, "nonexistent")              // Miss
	storage.Delete(ctx, "key2")                  // Delete
	storage.Increment(ctx, "counter", time.Hour) // Set

	// Check stats
	stats = storage.GetStats()
	assert.Equal(t, 2, stats.Entries, "Expected 2 entries") // key1 and counter
	assert.Equal(t, int64(1), stats.Hits, "Expected 1 hit")
	assert.Equal(t, int64(1), stats.Misses, "Expected 1 miss")
	assert.Equal(t, int64(3), stats.Sets, "Expected 3 sets") // 2 sets + 1 increment
	assert.Equal(t, int64(1), stats.Deletes, "Expected 1 delete")
}

// TestMemoryStorageMultipleOperations tests batch operations.
func TestMemoryStorageMultipleOperations(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: time.Second})
	defer storage.Close()

	// Test SetMultiple
	items := map[string][]byte{
		"multi1": []byte("value1"),
		"multi2": []byte("value2"),
		"multi3": []byte("value3"),
	}

	err := storage.SetMultiple(ctx, items, time.Hour)
	assert.NoError(t, err, "SetMultiple() should not error")

	// Test GetMultiple
	keys := []string{"multi1", "multi2", "multi3", "nonexistent"}
	results, err := storage.GetMultiple(ctx, keys)

	assert.NoError(t, err, "GetMultiple() should not error")
	assert.Equal(t, 3, len(results), "Expected 3 results")

	for key, expectedValue := range items {
		value, ok := results[key]
		assert.True(t, ok, "Missing key %s in results", key)
		if ok {
			assert.Equal(t, string(expectedValue), string(value), "Wrong value for key %s", key)
		}
	}

	_, ok := results["nonexistent"]
	assert.False(t, ok, "Non-existent key should not be in results")
}

// TestMemoryStorageMemoryLeak tests for memory leaks.
func TestMemoryStorageMemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	ctx := context.Background()
	storage := NewMemoryStorage(&MemoryConfig{CleanupInterval: 100 * time.Millisecond})
	defer storage.Close()

	// Get initial memory stats
	var initialMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&initialMem)

	// Perform many operations with TTL
	const iterations = 10000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("leak-test-%d", i)
		value := make([]byte, 1024) // 1KB per entry
		storage.Set(ctx, key, value, 200*time.Millisecond)

		// Also do some reads to trigger expiration checks
		if i%100 == 0 {
			storage.Get(ctx, fmt.Sprintf("leak-test-%d", i-50))
		}
	}

	// Wait for entries to expire and cleanup to run
	time.Sleep(500 * time.Millisecond)

	// Force GC and check memory
	runtime.GC()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// Memory usage should not grow significantly after cleanup
	memGrowth := int64(finalMem.Alloc) - int64(initialMem.Alloc)
	maxAllowedGrowth := int64(100 * 1024 * 1024) // 100MB

	assert.LessOrEqual(t, memGrowth, maxAllowedGrowth, "Memory growth should not exceed allowed limit")

	// Storage should have very few entries left
	stats := storage.GetStats()
	assert.LessOrEqual(t, stats.Entries, 100, "Too many entries remaining after expiration")
}

// BenchmarkMemoryStorageGet benchmarks the Get operation.
func BenchmarkMemoryStorageGet(b *testing.B) {
	ctx := context.Background()
	storage := NewMemoryStorage(nil)
	defer storage.Close()

	// Prepare test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		value := []byte(fmt.Sprintf("bench-value-%d", i))
		storage.Set(ctx, key, value, time.Hour)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-key-%d", i%1000)
			storage.Get(ctx, key)
			i++
		}
	})
}

// BenchmarkMemoryStorageSet benchmarks the Set operation.
func BenchmarkMemoryStorageSet(b *testing.B) {
	ctx := context.Background()
	storage := NewMemoryStorage(nil)
	defer storage.Close()

	value := []byte("benchmark-value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-key-%d", i)
			storage.Set(ctx, key, value, time.Hour)
			i++
		}
	})
}

// BenchmarkMemoryStorageIncrement benchmarks the Increment operation.
func BenchmarkMemoryStorageIncrement(b *testing.B) {
	ctx := context.Background()
	storage := NewMemoryStorage(nil)
	defer storage.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("counter-%d", i%100) // Share some counters
			storage.Increment(ctx, key, time.Hour)
			i++
		}
	})
}

// BenchmarkMemoryStorageDelete benchmarks the Delete operation.
func BenchmarkMemoryStorageDelete(b *testing.B) {
	ctx := context.Background()
	storage := NewMemoryStorage(nil)
	defer storage.Close()

	// Pre-populate with data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("delete-key-%d", i)
		storage.Set(ctx, key, []byte("value"), time.Hour)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("delete-key-%d", i)
		storage.Delete(ctx, key)
	}
}

// BenchmarkMemoryStorageConcurrent benchmarks concurrent operations.
func BenchmarkMemoryStorageConcurrent(b *testing.B) {
	ctx := context.Background()
	storage := NewMemoryStorage(nil)
	defer storage.Close()

	// Pre-populate some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("concurrent-key-%d", i)
		storage.Set(ctx, key, []byte("value"), time.Hour)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var ops int64
		for pb.Next() {
			n := atomic.AddInt64(&ops, 1)
			switch n % 4 {
			case 0:
				storage.Get(ctx, fmt.Sprintf("concurrent-key-%d", n%1000))
			case 1:
				storage.Set(ctx, fmt.Sprintf("new-key-%d", n), []byte("value"), time.Hour)
			case 2:
				storage.Increment(ctx, fmt.Sprintf("counter-%d", n%100), time.Hour)
			case 3:
				storage.Delete(ctx, fmt.Sprintf("concurrent-key-%d", n%1000))
			}
		}
	})
}
