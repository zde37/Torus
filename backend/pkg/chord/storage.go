package chord

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/zde37/torus/backend/pkg"
	"github.com/zde37/torus/backend/pkg/hash"
)

const (
	// Internal storage keys for Chord metadata
	keyPredecessor   = "__chord_predecessor__"
	keySuccessorList = "__chord_successor_list__"

	// Prefixes for internal storage
	chordKeyPrefix   = "__chord_"
	fingerKeyPrefix  = "__chord_finger_"
	replicaKeyPrefix = "__replica_"
)

// ChordStorage provides a Chord-specific wrapper around the generic MemoryStorage.
// It handles automatic key hashing and provides typed methods for Chord metadata.
type ChordStorage struct {
	storage *pkg.MemoryStorage
}

// NewChordStorage creates a new ChordStorage instance wrapping the provided MemoryStorage.
func NewChordStorage(storage *pkg.MemoryStorage) *ChordStorage {
	return &ChordStorage{
		storage: storage,
	}
}

// NewDefaultChordStorage creates a ChordStorage with default MemoryStorage configuration.
func NewDefaultChordStorage() *ChordStorage {
	memStorage := pkg.NewMemoryStorage(&pkg.MemoryConfig{
		CleanupInterval: 1 * time.Minute,
	})
	return NewChordStorage(memStorage)
}

// Get retrieves a value by key. The key is hashed to a Chord ID before lookup.
func (cs *ChordStorage) Get(ctx context.Context, key string) ([]byte, error) {
	hashedKey := cs.hashKey(key)
	return cs.storage.Get(ctx, hashedKey)
}

// Set stores a value with the given key and TTL. The key is hashed to a Chord ID.
// If TTL is 0, the value will not expire.
func (cs *ChordStorage) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	hashedKey := cs.hashKey(key)
	return cs.storage.Set(ctx, hashedKey, value, ttl)
}

// Delete removes a key-value pair. The key is hashed to a Chord ID.
func (cs *ChordStorage) Delete(ctx context.Context, key string) error {
	hashedKey := cs.hashKey(key)
	return cs.storage.Delete(ctx, hashedKey)
}

// SetReplica stores a replica of a key with the given value and TTL.
// Replicas are stored with a special prefix to distinguish them from primary keys.
func (cs *ChordStorage) SetReplica(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Store with original key name (not hashed) so we can identify it later
	replicaKey := replicaKeyPrefix + key
	return cs.storage.Set(ctx, replicaKey, value, ttl)
}

// GetReplica retrieves a replica value by key.
// Returns the value if found, nil if not found, and error on failures.
func (cs *ChordStorage) GetReplica(ctx context.Context, key string) ([]byte, error) {
	// Retrieve using original key name (not hashed)
	replicaKey := replicaKeyPrefix + key
	return cs.storage.Get(ctx, replicaKey)
}

// DeleteReplica removes a replica of a key.
func (cs *ChordStorage) DeleteReplica(ctx context.Context, key string) error {
	// Delete using original key name (not hashed)
	replicaKey := replicaKeyPrefix + key
	return cs.storage.Delete(ctx, replicaKey)
}

// GetAllReplicaKeys returns all replica keys stored in this node.
// These are the keys where this node is acting as a backup for other nodes.
func (cs *ChordStorage) GetAllReplicaKeys(ctx context.Context) ([]string, error) {
	// Get all keys with the replica prefix
	replicaKeys, err := cs.storage.ListKeysWithPrefix(ctx, replicaKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list replica keys: %w", err)
	}

	// Strip the prefix to get original key names
	var originalKeys []string
	for _, rKey := range replicaKeys {
		// Remove replica prefix to get the original key name
		if len(rKey) > len(replicaKeyPrefix) {
			originalKey := rKey[len(replicaKeyPrefix):]
			originalKeys = append(originalKeys, originalKey)
		}
	}

	return originalKeys, nil
}

// GetRaw retrieves a value by raw key (without hashing).
// This is useful for internal metadata storage.
func (cs *ChordStorage) GetRaw(ctx context.Context, key string) ([]byte, error) {
	return cs.storage.Get(ctx, key)
}

// SetRaw stores a value with raw key (without hashing).
// This is useful for internal metadata storage.
func (cs *ChordStorage) SetRaw(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return cs.storage.Set(ctx, key, value, ttl)
}

// DeleteRaw removes a key-value pair by raw key (without hashing).
func (cs *ChordStorage) DeleteRaw(ctx context.Context, key string) error {
	return cs.storage.Delete(ctx, key)
}

// GetPredecessor retrieves the predecessor node from storage.
// Returns nil if no predecessor is set.
func (cs *ChordStorage) GetPredecessor(ctx context.Context) (*NodeAddress, error) {
	data, err := cs.GetRaw(ctx, keyPredecessor)
	if err != nil {
		if err == pkg.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get predecessor: %w", err)
	}

	var node NodeAddress
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("failed to unmarshal predecessor: %w", err)
	}

	return &node, nil
}

// SetPredecessor stores the predecessor node.
func (cs *ChordStorage) SetPredecessor(ctx context.Context, pred *NodeAddress) error {
	if pred == nil {
		return cs.DeleteRaw(ctx, keyPredecessor)
	}

	data, err := json.Marshal(pred)
	if err != nil {
		return fmt.Errorf("failed to marshal predecessor: %w", err)
	}

	return cs.SetRaw(ctx, keyPredecessor, data, 0)
}

// GetSuccessorList retrieves the successor list from storage.
// Returns empty slice if no successor list is set.
func (cs *ChordStorage) GetSuccessorList(ctx context.Context) ([]*NodeAddress, error) {
	data, err := cs.GetRaw(ctx, keySuccessorList)
	if err != nil {
		if err == pkg.ErrKeyNotFound {
			return []*NodeAddress{}, nil
		}
		return nil, fmt.Errorf("failed to get successor list: %w", err)
	}

	var successors []*NodeAddress
	if err := json.Unmarshal(data, &successors); err != nil {
		return nil, fmt.Errorf("failed to unmarshal successor list: %w", err)
	}

	return successors, nil
}

// SetSuccessorList stores the successor list.
func (cs *ChordStorage) SetSuccessorList(ctx context.Context, successors []*NodeAddress) error {
	if successors == nil {
		successors = []*NodeAddress{}
	}

	data, err := json.Marshal(successors)
	if err != nil {
		return fmt.Errorf("failed to marshal successor list: %w", err)
	}

	return cs.SetRaw(ctx, keySuccessorList, data, 0)
}

// GetFingerEntry retrieves a specific finger table entry.
// Returns nil if the entry doesn't exist.
func (cs *ChordStorage) GetFingerEntry(ctx context.Context, index int) (*FingerEntry, error) {
	key := fmt.Sprintf("%s%d", fingerKeyPrefix, index)
	data, err := cs.GetRaw(ctx, key)
	if err != nil {
		if err == pkg.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get finger entry %d: %w", index, err)
	}

	var entry FingerEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal finger entry %d: %w", index, err)
	}

	return &entry, nil
}

// SetFingerEntry stores a finger table entry.
func (cs *ChordStorage) SetFingerEntry(ctx context.Context, index int, entry *FingerEntry) error {
	key := fmt.Sprintf("%s%d", fingerKeyPrefix, index)

	if entry == nil {
		return cs.DeleteRaw(ctx, key)
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal finger entry %d: %w", index, err)
	}

	return cs.SetRaw(ctx, key, data, 0)
}

// GetAllFingerEntries retrieves all finger table entries.
// Returns a map of index -> FingerEntry.
func (cs *ChordStorage) GetAllFingerEntries(ctx context.Context) (map[int]*FingerEntry, error) {
	entries := make(map[int]*FingerEntry)

	// Finger table has hash.M entries (0 to M-1)
	for i := 0; i < hash.M; i++ {
		entry, err := cs.GetFingerEntry(ctx, i)
		if err != nil {
			return nil, err
		}
		if entry != nil {
			entries[i] = entry
		}
	}

	return entries, nil
}

// SetAllFingerEntries stores multiple finger table entries at once.
func (cs *ChordStorage) SetAllFingerEntries(ctx context.Context, entries map[int]*FingerEntry) error {
	for index, entry := range entries {
		if err := cs.SetFingerEntry(ctx, index, entry); err != nil {
			return err
		}
	}
	return nil
}

// Increment atomically increments a counter. The key is hashed to a Chord ID.
func (cs *ChordStorage) Increment(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	hashedKey := cs.hashKey(key)
	return cs.storage.Increment(ctx, hashedKey, ttl)
}

// GetMultiple retrieves multiple values in a single operation.
// Keys are hashed to Chord IDs before lookup.
func (cs *ChordStorage) GetMultiple(ctx context.Context, keys []string) (map[string][]byte, error) {
	hashedKeys := make([]string, len(keys))
	for i, key := range keys {
		hashedKeys[i] = cs.hashKey(key)
	}

	results, err := cs.storage.GetMultiple(ctx, hashedKeys)
	if err != nil {
		return nil, err
	}

	// Map back to original keys
	output := make(map[string][]byte)
	for i, key := range keys {
		hashedKey := hashedKeys[i]
		if value, exists := results[hashedKey]; exists {
			output[key] = value
		}
	}

	return output, nil
}

// SetMultiple stores multiple key-value pairs in a single operation.
// Keys are hashed to Chord IDs before storage.
func (cs *ChordStorage) SetMultiple(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	hashedItems := make(map[string][]byte)
	for key, value := range items {
		hashedKey := cs.hashKey(key)
		hashedItems[hashedKey] = value
	}

	return cs.storage.SetMultiple(ctx, hashedItems, ttl)
}

// Clear removes all entries from storage.
func (cs *ChordStorage) Clear() error {
	return cs.storage.Clear()
}

// Close gracefully shuts down the storage.
func (cs *ChordStorage) Close() error {
	return cs.storage.Close()
}

// GetStats returns storage statistics.
func (cs *ChordStorage) GetStats() pkg.Stats {
	return cs.storage.GetStats()
}

// GetAll returns all key-value pairs in storage (excluding expired entries).
// This is used during node leave to transfer all data to other nodes.
func (cs *ChordStorage) GetAll(ctx context.Context) (map[string][]byte, error) {
	return cs.storage.GetAll(ctx)
}

// CountUserKeys returns the number of user keys (excluding Chord metadata and replicas).
// Chord metadata keys are prefixed with 'chordKeyPrefix' and replica keys with 'replicaKeyPrefix'.
func (cs *ChordStorage) CountUserKeys(ctx context.Context) (int, error) {
	allKeys, err := cs.storage.GetAll(ctx)
	if err != nil {
		return 0, err
	}

	userKeyCount := 0
	for key := range allKeys {
		// Skip Chord metadata keys
		if len(key) >= len(chordKeyPrefix) && key[:len(chordKeyPrefix)] == chordKeyPrefix {
			continue
		}
		// Skip replica keys
		if len(key) >= len(replicaKeyPrefix) && key[:len(replicaKeyPrefix)] == replicaKeyPrefix {
			continue
		}
		userKeyCount++
	}

	return userKeyCount, nil
}

// CountReplicaKeys returns the number of replica keys stored on this node.
// Replica keys are prefixed with 'replicaKeyPrefix'.
func (cs *ChordStorage) CountReplicaKeys(ctx context.Context) (int, error) {
	allKeys, err := cs.storage.GetAll(ctx)
	if err != nil {
		return 0, err
	}

	replicaKeyCount := 0
	for key := range allKeys {
		// Count replica keys
		if len(key) >= len(replicaKeyPrefix) && key[:len(replicaKeyPrefix)] == replicaKeyPrefix {
			replicaKeyCount++
		}
	}

	return replicaKeyCount, nil
}

// hashKey converts a string key to a hex string representation of its Chord ID.
// This ensures consistent hashing across the system.
func (cs *ChordStorage) hashKey(key string) string {
	id := hash.HashString(key)
	return id.Text(16) // Use hex encoding for string key
}

// HashKeyToID converts a string key to its Chord ID.
// This is useful for determining which node should store a key.
func (cs *ChordStorage) HashKeyToID(key string) *big.Int {
	return hash.HashString(key)
}

// IsResponsibleFor checks if a given node ID is responsible for storing a key.
// A node is responsible for keys in the range (predecessor, node].
func IsResponsibleFor(nodeID, predecessorID, keyID *big.Int) bool {
	if predecessorID == nil {
		// No predecessor means this is the only node, responsible for all keys
		return true
	}
	return hash.InRange(keyID, predecessorID, nodeID)
}

// GetKeysInRange returns all PRIMARY keys whose hash falls in the range (start, end].
// This is used for data migration when nodes join or leave.
// Excludes:
// - Chord metadata keys (those starting with 'chordKeyPrefix') to prevent corruption of node state
// - Replica keys (those starting with 'replicaKeyPrefix') as replicas should be recreated by new owners
// Only primary keys are transferred during node join/leave operations.
func (cs *ChordStorage) GetKeysInRange(ctx context.Context, startID, endID *big.Int) (map[string][]byte, error) {
	// Get all keys from storage
	allKeys, err := cs.storage.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]byte)

	// Filter keys based on the range
	for hashedKey, value := range allKeys {
		// Skip Chord metadata keys - they should not be migrated
		// Metadata keys use the 'chordKeyPrefix' (e.g., "__chord_predecessor__")
		if len(hashedKey) >= len(chordKeyPrefix) && hashedKey[:len(chordKeyPrefix)] == chordKeyPrefix {
			continue
		}

		// Skip replica keys - they should not be transferred
		// Replicas will be recreated by the new owner of the primary keys
		if len(hashedKey) >= len(replicaKeyPrefix) && hashedKey[:len(replicaKeyPrefix)] == replicaKeyPrefix {
			continue
		}

		// Convert hex string back to big.Int
		keyID := new(big.Int)
		_, success := keyID.SetString(hashedKey, 16)
		if !success {
			// Invalid hex string, skip it (likely metadata or corrupted data)
			continue
		}

		// Check if key is in range (start, end]
		if hash.InRange(keyID, startID, endID) {
			result[hashedKey] = value
		}
	}

	return result, nil
}

// DeleteKeysInRange deletes all keys whose hash falls in the range (start, end].
// Returns the number of keys deleted.
// Note: GetKeysInRange already excludes Chord metadata keys, so this is safe.
func (cs *ChordStorage) DeleteKeysInRange(ctx context.Context, startID, endID *big.Int) (int, error) {
	// Get keys in range (excludes metadata keys automatically)
	keys, err := cs.GetKeysInRange(ctx, startID, endID)
	if err != nil {
		return 0, err
	}

	// Delete each key
	for hashedKey := range keys {
		if err := cs.storage.Delete(ctx, hashedKey); err != nil {
			return 0, err
		}
	}

	return len(keys), nil
}
