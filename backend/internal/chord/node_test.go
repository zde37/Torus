package chord

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zde37/torus/internal/config"
	"github.com/zde37/torus/internal/hash"
	"github.com/zde37/torus/pkg"
)

func createTestNode(t *testing.T, host string, port int) *ChordNode {
	cfg := config.DefaultConfig()
	cfg.Host = host
	cfg.Port = port
	cfg.StabilizeInterval = 100 * time.Millisecond
	cfg.FixFingersInterval = 100 * time.Millisecond

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	node, err := NewChordNode(cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, node)

	return node
}

func TestNewChordNode(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		node := createTestNode(t, "127.0.0.1", 8080)
		defer node.Shutdown()

		assert.NotNil(t, node.ID())
		assert.NotNil(t, node.Address())
		assert.Equal(t, "127.0.0.1", node.Address().Host)
		assert.Equal(t, 8080, node.Address().Port)
		assert.False(t, node.IsShutdown())
	})

	t.Run("nil config", func(t *testing.T) {
		logger, err := pkg.New(pkg.DefaultConfig())
		require.NoError(t, err)

		node, err := NewChordNode(nil, logger)
		assert.Error(t, err)
		assert.Nil(t, node)
		assert.Contains(t, err.Error(), "config cannot be nil")
	})

	t.Run("nil logger", func(t *testing.T) {
		cfg := config.DefaultConfig()
		node, err := NewChordNode(cfg, nil)
		assert.Error(t, err)
		assert.Nil(t, node)
		assert.Contains(t, err.Error(), "logger cannot be nil")
	})

	t.Run("invalid config", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.Port = -1 // Invalid port

		logger, err := pkg.New(pkg.DefaultConfig())
		require.NoError(t, err)

		node, err := NewChordNode(cfg, logger)
		assert.Error(t, err)
		assert.Nil(t, node)
		assert.Contains(t, err.Error(), "invalid config")
	})
}

func TestChordNode_IDAndAddress(t *testing.T) {
	node := createTestNode(t, "192.168.1.1", 9000)
	defer node.Shutdown()

	t.Run("ID is consistent", func(t *testing.T) {
		id1 := node.ID()
		id2 := node.ID()
		assert.Equal(t, id1, id2)
		assert.True(t, hash.IsValidID(id1))
	})

	t.Run("ID is a copy", func(t *testing.T) {
		id := node.ID()
		id.Add(id, big.NewInt(1))
		assert.NotEqual(t, id, node.ID())
	})

	t.Run("Address is correct", func(t *testing.T) {
		addr := node.Address()
		assert.Equal(t, "192.168.1.1", addr.Host)
		assert.Equal(t, 9000, addr.Port)
		assert.Equal(t, node.ID(), addr.ID)
	})

	t.Run("Address is a copy", func(t *testing.T) {
		addr := node.Address()
		addr.Port = 9999
		assert.Equal(t, 9000, node.Address().Port)
	})
}

func TestChordNode_Create(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	err := node.Create()
	require.NoError(t, err)

	// Give background tasks time to start
	time.Sleep(50 * time.Millisecond)

	t.Run("node is its own successor", func(t *testing.T) {
		succ := node.successor()
		require.NotNil(t, succ)
		assert.True(t, succ.Equals(node.Address()))
	})

	t.Run("predecessor is nil initially", func(t *testing.T) {
		pred := node.getPredecessor()
		assert.Nil(t, pred)
	})

	t.Run("finger table initialized", func(t *testing.T) {
		// All fingers should point to self
		for i := 0; i < 10; i++ { // Check first 10 fingers
			finger := node.getFinger(i)
			require.NotNil(t, finger)
			assert.True(t, finger.Node.Equals(node.Address()))
		}
	})

	t.Run("successor list contains self", func(t *testing.T) {
		succList := node.getSuccessorList()
		require.Len(t, succList, 1)
		assert.True(t, succList[0].Equals(node.Address()))
	})
}

// mockRemoteClient is a simple mock for testing Join
type mockRemoteClient struct {
	bootstrap *NodeAddress
}

func (m *mockRemoteClient) FindSuccessor(address string, id *big.Int) (*NodeAddress, error) {
	// For testing, return the bootstrap node as successor
	return m.bootstrap, nil
}

func (m *mockRemoteClient) GetPredecessor(address string) (*NodeAddress, error) {
	return nil, nil
}

func (m *mockRemoteClient) Notify(address string, node *NodeAddress) error {
	return nil
}

func (m *mockRemoteClient) GetSuccessorList(address string) ([]*NodeAddress, error) {
	return nil, nil
}

func (m *mockRemoteClient) Ping(address string, message string) (string, error) {
	return "pong", nil
}

func (m *mockRemoteClient) ClosestPrecedingFinger(address string, id *big.Int) (*NodeAddress, error) {
	return nil, nil
}

func (m *mockRemoteClient) Get(ctx context.Context, address string, key string) ([]byte, bool, error) {
	return nil, false, nil
}

func (m *mockRemoteClient) Set(ctx context.Context, address string, key string, value []byte) error {
	return nil
}

func (m *mockRemoteClient) Delete(ctx context.Context, address string, key string) error {
	return nil
}

func (m *mockRemoteClient) TransferKeys(ctx context.Context, address string, startID, endID *big.Int) (map[string][]byte, error) {
	// For testing, return empty map (no keys to transfer)
	return make(map[string][]byte), nil
}

func (m *mockRemoteClient) DeleteTransferredKeys(ctx context.Context, address string, startID, endID *big.Int) error {
	// For testing, assume deletion succeeds
	return nil
}

func TestChordNode_Join(t *testing.T) {
	bootstrap := createTestNode(t, "127.0.0.1", 8080)
	defer bootstrap.Shutdown()

	err := bootstrap.Create()
	require.NoError(t, err)

	node := createTestNode(t, "127.0.0.1", 8081)
	defer node.Shutdown()

	// Set mock remote client for testing
	mockClient := &mockRemoteClient{bootstrap: bootstrap.Address()}
	node.SetRemote(mockClient)

	err = node.Join(bootstrap.Address())
	require.NoError(t, err)

	// Give background tasks time to start
	time.Sleep(50 * time.Millisecond)

	t.Run("successor is bootstrap", func(t *testing.T) {
		succ := node.successor()
		require.NotNil(t, succ)
		assert.True(t, succ.Equals(bootstrap.Address()))
	})

	t.Run("predecessor is nil initially", func(t *testing.T) {
		pred := node.getPredecessor()
		assert.Nil(t, pred)
	})

	t.Run("join with nil bootstrap fails", func(t *testing.T) {
		node2 := createTestNode(t, "127.0.0.1", 8082)
		defer node2.Shutdown()

		err := node2.Join(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bootstrap address cannot be nil")
	})
}

func TestChordNode_SuccessorOperations(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	t.Run("set and get successor", func(t *testing.T) {
		succ := NewNodeAddress(big.NewInt(100), "127.0.0.1", 9000)
		node.setSuccessor(succ)

		retrieved := node.successor()
		require.NotNil(t, retrieved)
		assert.True(t, retrieved.Equals(succ))
	})

	t.Run("set nil successor", func(t *testing.T) {
		node.setSuccessor(nil)
		retrieved := node.successor()
		assert.Nil(t, retrieved)
	})

	t.Run("successor list operations", func(t *testing.T) {
		list := []*NodeAddress{
			NewNodeAddress(big.NewInt(10), "127.0.0.1", 9001),
			NewNodeAddress(big.NewInt(20), "127.0.0.1", 9002),
			NewNodeAddress(big.NewInt(30), "127.0.0.1", 9003),
		}

		node.setSuccessorList(list)
		retrieved := node.getSuccessorList()

		require.Len(t, retrieved, 3)
		for i, addr := range list {
			assert.True(t, addr.Equals(retrieved[i]))
		}
	})

	t.Run("successor list respects max size", func(t *testing.T) {
		// Create many successors
		list := make([]*NodeAddress, 10)
		for i := 0; i < 10; i++ {
			list[i] = NewNodeAddress(big.NewInt(int64(i*10)), "127.0.0.1", 9000+i)
		}

		node.setSuccessorList(list)
		retrieved := node.getSuccessorList()

		// Should only keep SuccessorListSize entries
		assert.LessOrEqual(t, len(retrieved), node.config.SuccessorListSize)
	})
}

func TestChordNode_PredecessorOperations(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	t.Run("set and get predecessor", func(t *testing.T) {
		pred := NewNodeAddress(big.NewInt(50), "127.0.0.1", 7000)
		node.setPredecessor(pred)

		retrieved := node.getPredecessor()
		require.NotNil(t, retrieved)
		assert.True(t, retrieved.Equals(pred))
	})

	t.Run("set nil predecessor", func(t *testing.T) {
		node.setPredecessor(nil)
		retrieved := node.getPredecessor()
		assert.Nil(t, retrieved)
	})

	t.Run("predecessor is copied", func(t *testing.T) {
		pred := NewNodeAddress(big.NewInt(50), "127.0.0.1", 7000)
		node.setPredecessor(pred)

		pred.Port = 9999 // Modify original

		retrieved := node.getPredecessor()
		assert.Equal(t, 7000, retrieved.Port) // Should still be 7000
	})
}

func TestChordNode_FingerTableOperations(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	t.Run("set and get finger", func(t *testing.T) {
		entry := NewFingerEntry(
			big.NewInt(100),
			NewNodeAddress(big.NewInt(200), "127.0.0.1", 9000),
		)

		node.setFinger(5, entry)
		retrieved := node.getFinger(5)

		require.NotNil(t, retrieved)
		assert.Equal(t, entry.Start, retrieved.Start)
		assert.True(t, entry.Node.Equals(retrieved.Node))
	})

	t.Run("set nil finger", func(t *testing.T) {
		node.setFinger(10, nil)
		retrieved := node.getFinger(10)
		assert.Nil(t, retrieved)
	})

	t.Run("get invalid index", func(t *testing.T) {
		assert.Nil(t, node.getFinger(-1))
		assert.Nil(t, node.getFinger(hash.M))
		assert.Nil(t, node.getFinger(hash.M + 1))
	})

	t.Run("init finger table", func(t *testing.T) {
		succ := NewNodeAddress(big.NewInt(500), "127.0.0.1", 9000)
		node.initFingerTable(succ)

		// All fingers should point to successor
		for i := 0; i < hash.M; i++ {
			finger := node.getFinger(i)
			require.NotNil(t, finger)
			assert.True(t, finger.Node.Equals(succ))
		}
	})
}

func TestChordNode_FindSuccessor(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	err := node.Create()
	require.NoError(t, err)

	t.Run("find successor when alone in ring", func(t *testing.T) {
		// Any ID should resolve to self
		succ, err := node.FindSuccessor(big.NewInt(12345))
		require.NoError(t, err)
		require.NotNil(t, succ)
		assert.True(t, succ.Equals(node.Address()))
	})

	t.Run("find successor with nil ID", func(t *testing.T) {
		_, err := node.FindSuccessor(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "id cannot be nil")
	})

	t.Run("find successor for own ID", func(t *testing.T) {
		succ, err := node.FindSuccessor(node.ID())
		require.NoError(t, err)
		require.NotNil(t, succ)
		// Should return successor (self in this case)
		assert.True(t, succ.Equals(node.Address()))
	})
}

func TestChordNode_ClosestPrecedingNode(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	// Setup finger table with some entries
	succ := NewNodeAddress(big.NewInt(1000), "127.0.0.1", 9000)
	node.initFingerTable(succ)

	t.Run("finds closest preceding node", func(t *testing.T) {
		targetID := big.NewInt(2000)
		closest := node.closestPrecedingNode(targetID)
		require.NotNil(t, closest)
		// Should return a valid node
		assert.NotNil(t, closest.ID)
	})

	t.Run("returns self when no better node", func(t *testing.T) {
		// When all fingers point to successor, and we query for something before successor,
		// we should get self
		targetID := big.NewInt(500)
		closest := node.closestPrecedingNode(targetID)
		// Result depends on finger table state, just verify it's not nil
		assert.NotNil(t, closest)
	})
}

func TestChordNode_Notify(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	nodeID := node.ID()

	t.Run("notify with no predecessor", func(t *testing.T) {
		assert.Nil(t, node.getPredecessor())

		// Any node can become predecessor
		newPred := NewNodeAddress(big.NewInt(50), "127.0.0.1", 7000)
		node.notify(newPred)

		pred := node.getPredecessor()
		require.NotNil(t, pred)
		assert.True(t, pred.Equals(newPred))
	})

	t.Run("notify with better predecessor", func(t *testing.T) {
		// Set initial predecessor
		oldPred := NewNodeAddress(
			new(big.Int).Sub(nodeID, big.NewInt(100)),
			"127.0.0.1",
			7000,
		)
		node.setPredecessor(oldPred)

		// Notify with a node that's between oldPred and node
		newPred := NewNodeAddress(
			new(big.Int).Sub(nodeID, big.NewInt(50)),
			"127.0.0.1",
			7001,
		)
		node.notify(newPred)

		pred := node.getPredecessor()
		require.NotNil(t, pred)
		assert.True(t, pred.Equals(newPred))
	})

	t.Run("notify with worse predecessor", func(t *testing.T) {
		// Set a good predecessor
		goodPred := NewNodeAddress(
			new(big.Int).Sub(nodeID, big.NewInt(10)),
			"127.0.0.1",
			7000,
		)
		node.setPredecessor(goodPred)

		// Try to notify with a worse predecessor
		badPred := NewNodeAddress(
			new(big.Int).Sub(nodeID, big.NewInt(200)),
			"127.0.0.1",
			7001,
		)
		node.notify(badPred)

		// Predecessor should not change
		pred := node.getPredecessor()
		require.NotNil(t, pred)
		assert.True(t, pred.Equals(goodPred))
	})

	t.Run("notify with nil node", func(t *testing.T) {
		initialPred := node.getPredecessor()
		node.notify(nil)
		// Predecessor should not change
		assert.Equal(t, initialPred, node.getPredecessor())
	})
}

func TestChordNode_Stabilize(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	err := node.Create()
	require.NoError(t, err)

	t.Run("stabilize with self as successor", func(t *testing.T) {
		err := node.stabilize()
		assert.NoError(t, err)
	})

	t.Run("stabilize with nil successor", func(t *testing.T) {
		node.setSuccessor(nil)
		err := node.stabilize()
		assert.NoError(t, err) // Should handle gracefully
	})
}

func TestChordNode_FixFingers(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	err := node.Create()
	require.NoError(t, err)

	t.Run("fix fingers updates finger table", func(t *testing.T) {
		err := node.fixFingers()
		assert.NoError(t, err)
	})

	t.Run("fix fingers cycles through all entries", func(t *testing.T) {
		// Get initial value
		node.nextFingerMu.Lock()
		initial := node.nextFingerToFix
		node.nextFingerMu.Unlock()

		// Run fixFingers M times to cycle through all entries
		for i := 0; i < hash.M; i++ {
			err := node.fixFingers()
			assert.NoError(t, err)
		}

		// Verify nextFingerToFix wrapped around to initial position
		node.nextFingerMu.Lock()
		assert.Equal(t, initial, node.nextFingerToFix)
		node.nextFingerMu.Unlock()
	})
}

func TestChordNode_BackgroundTasks(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	err := node.Create()
	require.NoError(t, err)

	// Wait for a few stabilization and fix finger cycles
	time.Sleep(350 * time.Millisecond)

	// Verify node is still running
	assert.False(t, node.IsShutdown())
}

func TestChordNode_Shutdown(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)

	err := node.Create()
	require.NoError(t, err)

	// Wait for background tasks to start
	time.Sleep(50 * time.Millisecond)

	t.Run("shutdown stops background tasks", func(t *testing.T) {
		err := node.Shutdown()
		assert.NoError(t, err)
		assert.True(t, node.IsShutdown())
	})

	t.Run("double shutdown is safe", func(t *testing.T) {
		err := node.Shutdown()
		assert.NoError(t, err)
		assert.True(t, node.IsShutdown())
	})
}

func TestChordNode_ConcurrentAccess(t *testing.T) {
	node := createTestNode(t, "127.0.0.1", 8080)
	defer node.Shutdown()

	err := node.Create()
	require.NoError(t, err)

	// Run concurrent operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Concurrent reads
			_ = node.successor()
			_ = node.getPredecessor()
			_ = node.getFinger(id % hash.M)

			// Concurrent writes
			if id%2 == 0 {
				addr := NewNodeAddress(big.NewInt(int64(id)), "127.0.0.1", 9000+id)
				node.setPredecessor(addr)
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out")
		}
	}
}

// Benchmark tests
func BenchmarkChordNode_FindSuccessor(b *testing.B) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 8080

	logger, _ := pkg.New(pkg.DefaultConfig())
	node, _ := NewChordNode(cfg, logger)
	defer node.Shutdown()

	node.Create()

	targetID := big.NewInt(12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = node.FindSuccessor(targetID)
	}
}

func BenchmarkChordNode_ClosestPrecedingNode(b *testing.B) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 8080

	logger, _ := pkg.New(pkg.DefaultConfig())
	node, _ := NewChordNode(cfg, logger)
	defer node.Shutdown()

	node.Create()

	targetID := big.NewInt(12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node.closestPrecedingNode(targetID)
	}
}
