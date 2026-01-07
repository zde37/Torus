package chord

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zde37/torus/backend/pkg"
	"github.com/zde37/torus/backend/pkg/hash"
)

func TestNewChordStorage(t *testing.T) {
	memStorage := pkg.NewMemoryStorage(&pkg.MemoryConfig{
		CleanupInterval: 1 * time.Minute,
	})
	cs := NewChordStorage(memStorage)
	require.NotNil(t, cs)
	assert.NotNil(t, cs.storage)
}

func TestNewDefaultChordStorage(t *testing.T) {
	cs := NewDefaultChordStorage()
	require.NotNil(t, cs)
	assert.NotNil(t, cs.storage)

	// Should be able to use it immediately
	ctx := context.Background()
	err := cs.Set(ctx, "test", []byte("value"), 0)
	assert.NoError(t, err)

	// Cleanup
	cs.Close()
}

func TestChordStorage_GetSet(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("set and get value", func(t *testing.T) {
		key := "test-key"
		value := []byte("test-value")

		err := cs.Set(ctx, key, value, 0)
		require.NoError(t, err)

		retrieved, err := cs.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		_, err := cs.Get(ctx, "non-existent")
		assert.ErrorIs(t, err, pkg.ErrKeyNotFound)
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		key := "overwrite-key"
		value1 := []byte("value1")
		value2 := []byte("value2")

		err := cs.Set(ctx, key, value1, 0)
		require.NoError(t, err)

		err = cs.Set(ctx, key, value2, 0)
		require.NoError(t, err)

		retrieved, err := cs.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value2, retrieved)
	})

	t.Run("set with TTL", func(t *testing.T) {
		key := "ttl-key"
		value := []byte("ttl-value")
		ttl := 50 * time.Millisecond

		err := cs.Set(ctx, key, value, ttl)
		require.NoError(t, err)

		// Should exist immediately
		_, err = cs.Get(ctx, key)
		assert.NoError(t, err)

		// Wait for expiration
		time.Sleep(100 * time.Millisecond)

		// Should be expired
		_, err = cs.Get(ctx, key)
		assert.ErrorIs(t, err, pkg.ErrKeyNotFound)
	})
}

func TestChordStorage_Delete(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("delete existing key", func(t *testing.T) {
		key := "delete-key"
		value := []byte("delete-value")

		err := cs.Set(ctx, key, value, 0)
		require.NoError(t, err)

		err = cs.Delete(ctx, key)
		require.NoError(t, err)

		_, err = cs.Get(ctx, key)
		assert.ErrorIs(t, err, pkg.ErrKeyNotFound)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err := cs.Delete(ctx, "non-existent")
		assert.NoError(t, err) // Should not error
	})
}

func TestChordStorage_RawOperations(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("raw set and get", func(t *testing.T) {
		key := "raw-key"
		value := []byte("raw-value")

		err := cs.SetRaw(ctx, key, value, 0)
		require.NoError(t, err)

		retrieved, err := cs.GetRaw(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	t.Run("raw vs hashed keys are different", func(t *testing.T) {
		key := "test"
		value1 := []byte("hashed")
		value2 := []byte("raw")

		// Set with hashing
		err := cs.Set(ctx, key, value1, 0)
		require.NoError(t, err)

		// Set raw without hashing
		err = cs.SetRaw(ctx, key, value2, 0)
		require.NoError(t, err)

		// Should get different values
		hashedValue, err := cs.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value1, hashedValue)

		rawValue, err := cs.GetRaw(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value2, rawValue)
	})

	t.Run("raw delete", func(t *testing.T) {
		key := "raw-delete-key"
		value := []byte("value")

		err := cs.SetRaw(ctx, key, value, 0)
		require.NoError(t, err)

		err = cs.DeleteRaw(ctx, key)
		require.NoError(t, err)

		_, err = cs.GetRaw(ctx, key)
		assert.ErrorIs(t, err, pkg.ErrKeyNotFound)
	})
}

func TestChordStorage_Predecessor(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("get predecessor when not set", func(t *testing.T) {
		pred, err := cs.GetPredecessor(ctx)
		require.NoError(t, err)
		assert.Nil(t, pred)
	})

	t.Run("set and get predecessor", func(t *testing.T) {
		expected := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080, 8081)

		err := cs.SetPredecessor(ctx, expected)
		require.NoError(t, err)

		pred, err := cs.GetPredecessor(ctx)
		require.NoError(t, err)
		require.NotNil(t, pred)
		assert.True(t, expected.Equals(pred))
	})

	t.Run("update predecessor", func(t *testing.T) {
		pred1 := NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080, 8082)
		pred2 := NewNodeAddress(big.NewInt(2), "127.0.0.2", 9000, 8083)

		err := cs.SetPredecessor(ctx, pred1)
		require.NoError(t, err)

		err = cs.SetPredecessor(ctx, pred2)
		require.NoError(t, err)

		pred, err := cs.GetPredecessor(ctx)
		require.NoError(t, err)
		assert.True(t, pred2.Equals(pred))
	})

	t.Run("set nil predecessor (clear)", func(t *testing.T) {
		pred := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080, 8085)

		err := cs.SetPredecessor(ctx, pred)
		require.NoError(t, err)

		err = cs.SetPredecessor(ctx, nil)
		require.NoError(t, err)

		retrieved, err := cs.GetPredecessor(ctx)
		require.NoError(t, err)
		assert.Nil(t, retrieved)
	})
}

func TestChordStorage_SuccessorList(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("get empty successor list", func(t *testing.T) {
		successors, err := cs.GetSuccessorList(ctx)
		require.NoError(t, err)
		assert.Empty(t, successors)
	})

	t.Run("set and get successor list", func(t *testing.T) {
		expected := []*NodeAddress{
			NewNodeAddress(big.NewInt(10), "127.0.0.1", 8080, 8081),
			NewNodeAddress(big.NewInt(20), "127.0.0.2", 8080, 8082),
			NewNodeAddress(big.NewInt(30), "127.0.0.3", 8080, 8083),
		}

		err := cs.SetSuccessorList(ctx, expected)
		require.NoError(t, err)

		successors, err := cs.GetSuccessorList(ctx)
		require.NoError(t, err)
		require.Len(t, successors, 3)

		for i, node := range expected {
			assert.True(t, node.Equals(successors[i]))
		}
	})

	t.Run("update successor list", func(t *testing.T) {
		list1 := []*NodeAddress{
			NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080, 8085),
		}
		list2 := []*NodeAddress{
			NewNodeAddress(big.NewInt(2), "127.0.0.2", 8080, 8086),
			NewNodeAddress(big.NewInt(3), "127.0.0.3", 8080, 8087),
		}

		err := cs.SetSuccessorList(ctx, list1)
		require.NoError(t, err)

		err = cs.SetSuccessorList(ctx, list2)
		require.NoError(t, err)

		successors, err := cs.GetSuccessorList(ctx)
		require.NoError(t, err)
		assert.Len(t, successors, 2)
	})

	t.Run("set nil successor list (empty)", func(t *testing.T) {
		err := cs.SetSuccessorList(ctx, nil)
		require.NoError(t, err)

		successors, err := cs.GetSuccessorList(ctx)
		require.NoError(t, err)
		assert.Empty(t, successors)
	})
}

func TestChordStorage_FingerEntry(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("get non-existent finger entry", func(t *testing.T) {
		entry, err := cs.GetFingerEntry(ctx, 0)
		require.NoError(t, err)
		assert.Nil(t, entry)
	})

	t.Run("set and get finger entry", func(t *testing.T) {
		expected := NewFingerEntry(
			big.NewInt(100),
			NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080, 8081),
		)

		err := cs.SetFingerEntry(ctx, 5, expected)
		require.NoError(t, err)

		entry, err := cs.GetFingerEntry(ctx, 5)
		require.NoError(t, err)
		require.NotNil(t, entry)
		assert.Equal(t, expected.Start, entry.Start)
		assert.True(t, expected.Node.Equals(entry.Node))
	})

	t.Run("update finger entry", func(t *testing.T) {
		entry1 := NewFingerEntry(
			big.NewInt(10),
			NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080, 8084),
		)
		entry2 := NewFingerEntry(
			big.NewInt(20),
			NewNodeAddress(big.NewInt(2), "127.0.0.2", 9000, 8085),
		)

		err := cs.SetFingerEntry(ctx, 10, entry1)
		require.NoError(t, err)

		err = cs.SetFingerEntry(ctx, 10, entry2)
		require.NoError(t, err)

		entry, err := cs.GetFingerEntry(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, entry2.Start, entry.Start)
	})

	t.Run("set nil finger entry (clear)", func(t *testing.T) {
		entry := NewFingerEntry(
			big.NewInt(10),
			NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080, 8087),
		)

		err := cs.SetFingerEntry(ctx, 15, entry)
		require.NoError(t, err)

		err = cs.SetFingerEntry(ctx, 15, nil)
		require.NoError(t, err)

		retrieved, err := cs.GetFingerEntry(ctx, 15)
		require.NoError(t, err)
		assert.Nil(t, retrieved)
	})
}

func TestChordStorage_AllFingerEntries(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("get all finger entries when empty", func(t *testing.T) {
		entries, err := cs.GetAllFingerEntries(ctx)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("set and get multiple finger entries", func(t *testing.T) {
		toSet := map[int]*FingerEntry{
			0: NewFingerEntry(
				big.NewInt(10),
				NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080, 8089),
			),
			5: NewFingerEntry(
				big.NewInt(50),
				NewNodeAddress(big.NewInt(5), "127.0.0.5", 8080, 8088),
			),
			10: NewFingerEntry(
				big.NewInt(100),
				NewNodeAddress(big.NewInt(10), "127.0.0.10", 8080, 8090),
			),
		}

		err := cs.SetAllFingerEntries(ctx, toSet)
		require.NoError(t, err)

		entries, err := cs.GetAllFingerEntries(ctx)
		require.NoError(t, err)
		assert.Len(t, entries, 3)

		for index, expected := range toSet {
			actual, exists := entries[index]
			require.True(t, exists, "entry %d should exist", index)
			assert.Equal(t, expected.Start, actual.Start)
			assert.True(t, expected.Node.Equals(actual.Node))
		}
	})
}

func TestChordStorage_Increment(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("increment new key", func(t *testing.T) {
		count, err := cs.Increment(ctx, "counter1", 0)
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("increment existing key", func(t *testing.T) {
		key := "counter2"
		count1, err := cs.Increment(ctx, key, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(1), count1)

		count2, err := cs.Increment(ctx, key, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count2)

		count3, err := cs.Increment(ctx, key, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(3), count3)
	})
}

func TestChordStorage_GetMultiple(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	// Set up test data
	err := cs.Set(ctx, "key1", []byte("value1"), 0)
	require.NoError(t, err)
	err = cs.Set(ctx, "key2", []byte("value2"), 0)
	require.NoError(t, err)

	t.Run("get multiple existing keys", func(t *testing.T) {
		keys := []string{"key1", "key2"}
		results, err := cs.GetMultiple(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, []byte("value1"), results["key1"])
		assert.Equal(t, []byte("value2"), results["key2"])
	})

	t.Run("get multiple with some missing", func(t *testing.T) {
		keys := []string{"key1", "missing", "key2"}
		results, err := cs.GetMultiple(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, []byte("value1"), results["key1"])
		assert.Equal(t, []byte("value2"), results["key2"])
		_, exists := results["missing"]
		assert.False(t, exists)
	})
}

func TestChordStorage_SetMultiple(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	t.Run("set multiple keys", func(t *testing.T) {
		items := map[string][]byte{
			"multi1": []byte("value1"),
			"multi2": []byte("value2"),
			"multi3": []byte("value3"),
		}

		err := cs.SetMultiple(ctx, items, 0)
		require.NoError(t, err)

		// Verify all were set
		for key, expectedValue := range items {
			value, err := cs.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)
		}
	})
}

func TestChordStorage_Clear(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	// Add some data
	err := cs.Set(ctx, "key1", []byte("value1"), 0)
	require.NoError(t, err)
	err = cs.SetPredecessor(ctx, NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080, 8081))
	require.NoError(t, err)

	// Clear
	err = cs.Clear()
	require.NoError(t, err)

	// Verify cleared
	_, err = cs.Get(ctx, "key1")
	assert.ErrorIs(t, err, pkg.ErrKeyNotFound)

	pred, err := cs.GetPredecessor(ctx)
	require.NoError(t, err)
	assert.Nil(t, pred)
}

func TestChordStorage_GetStats(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()

	// Initial stats
	stats := cs.GetStats()
	assert.Equal(t, 0, stats.Entries)

	// Add some data
	err := cs.Set(ctx, "key1", []byte("value1"), 0)
	require.NoError(t, err)

	stats = cs.GetStats()
	assert.Greater(t, stats.Entries, 0)
	assert.Greater(t, stats.Sets, int64(0))
}

func TestChordStorage_HashKeyToID(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	t.Run("hash key to ID", func(t *testing.T) {
		key := "test-key"
		id := cs.HashKeyToID(key)
		require.NotNil(t, id)
		assert.True(t, hash.IsValidID(id))
	})

	t.Run("same key produces same ID", func(t *testing.T) {
		key := "consistent-key"
		id1 := cs.HashKeyToID(key)
		id2 := cs.HashKeyToID(key)
		assert.Equal(t, id1, id2)
	})

	t.Run("different keys produce different IDs", func(t *testing.T) {
		id1 := cs.HashKeyToID("key1")
		id2 := cs.HashKeyToID("key2")
		assert.NotEqual(t, id1, id2)
	})
}

func TestIsResponsibleFor(t *testing.T) {
	tests := []struct {
		name          string
		nodeID        *big.Int
		predecessorID *big.Int
		keyID         *big.Int
		expected      bool
	}{
		{
			name:          "key in range",
			nodeID:        big.NewInt(100),
			predecessorID: big.NewInt(50),
			keyID:         big.NewInt(75),
			expected:      true,
		},
		{
			name:          "key equals node ID",
			nodeID:        big.NewInt(100),
			predecessorID: big.NewInt(50),
			keyID:         big.NewInt(100),
			expected:      true,
		},
		{
			name:          "key equals predecessor ID",
			nodeID:        big.NewInt(100),
			predecessorID: big.NewInt(50),
			keyID:         big.NewInt(50),
			expected:      false,
		},
		{
			name:          "key outside range",
			nodeID:        big.NewInt(100),
			predecessorID: big.NewInt(50),
			keyID:         big.NewInt(150),
			expected:      false,
		},
		{
			name:          "no predecessor (only node)",
			nodeID:        big.NewInt(100),
			predecessorID: nil,
			keyID:         big.NewInt(50),
			expected:      true,
		},
		{
			name:          "wraparound case",
			nodeID:        big.NewInt(10),
			predecessorID: big.NewInt(200),
			keyID:         big.NewInt(5),
			expected:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsResponsibleFor(tt.nodeID, tt.predecessorID, tt.keyID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestChordStorage_ContextCancellation(t *testing.T) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	t.Run("Get with canceled context", func(t *testing.T) {
		_, err := cs.Get(ctx, "key")
		assert.ErrorIs(t, err, pkg.ErrContextCanceled)
	})

	t.Run("Set with canceled context", func(t *testing.T) {
		err := cs.Set(ctx, "key", []byte("value"), 0)
		assert.ErrorIs(t, err, pkg.ErrContextCanceled)
	})
}

func BenchmarkChordStorage_Set(b *testing.B) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cs.Set(ctx, "bench-key", value, 0)
	}
}

func BenchmarkChordStorage_Get(b *testing.B) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	ctx := context.Background()
	cs.Set(ctx, "bench-key", []byte("value"), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cs.Get(ctx, "bench-key")
	}
}

func BenchmarkChordStorage_HashKeyToID(b *testing.B) {
	cs := NewDefaultChordStorage()
	defer cs.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cs.HashKeyToID("benchmark-key")
	}
}
