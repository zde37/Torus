package integration

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zde37/torus/internal/chord"
	"github.com/zde37/torus/internal/config"
	"github.com/zde37/torus/internal/transport"
	"github.com/zde37/torus/pkg"
)

// testCluster represents a cluster of Chord nodes for testing.
type testCluster struct {
	nodes   []*chord.ChordNode
	servers []*transport.GRPCServer
	clients []*transport.GRPCClient
	logger  *pkg.Logger
}

// newTestCluster creates a new test cluster.
func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	return &testCluster{
		nodes:   make([]*chord.ChordNode, 0),
		servers: make([]*transport.GRPCServer, 0),
		clients: make([]*transport.GRPCClient, 0),
		logger:  logger,
	}
}

// addNode adds a new node to the cluster.
// If bootstrap is nil, the node creates a new ring.
// Otherwise, it joins via the bootstrap node.
func (tc *testCluster) addNode(t *testing.T, port int, bootstrap *chord.NodeAddress) *chord.ChordNode {
	t.Helper()

	cfg := &config.Config{
		Host:               "127.0.0.1",
		Port:               port,
		HTTPPort:           port + 1000,
		M:                  160,
		StabilizeInterval:  100 * time.Millisecond, // Fast for testing
		FixFingersInterval: 100 * time.Millisecond,
		SuccessorListSize:  3,
		RPCTimeout:         5 * time.Second,
		LogLevel:           "error", // Reduce noise in tests
		LogFormat:          "json",
	}

	// Create node
	node, err := chord.NewChordNode(cfg, tc.logger)
	require.NoError(t, err)

	// Create and start gRPC server
	serverAddr := fmt.Sprintf("127.0.0.1:%d", port)
	authToken := "" // No auth token for integration tests
	grpcServer, err := transport.NewGRPCServer(node, serverAddr, authToken, tc.logger)
	require.NoError(t, err)

	err = grpcServer.Start()
	require.NoError(t, err)

	// Create gRPC client for inter-node communication
	grpcClient := transport.NewGRPCClient(tc.logger, authToken, cfg.RPCTimeout)
	node.SetRemote(grpcClient)

	// Create or join ring
	if bootstrap == nil {
		err = node.Create()
		require.NoError(t, err)
	} else {
		err = node.Join(bootstrap)
		require.NoError(t, err)
	}

	// Add to cluster
	tc.nodes = append(tc.nodes, node)
	tc.servers = append(tc.servers, grpcServer)
	tc.clients = append(tc.clients, grpcClient)

	return node
}

// shutdown cleans up the cluster.
func (tc *testCluster) shutdown(t *testing.T) {
	t.Helper()

	// Stop servers
	for _, server := range tc.servers {
		err := server.Stop()
		if err != nil {
			t.Logf("Error stopping server: %v", err)
		}
	}

	// Shutdown nodes
	for _, node := range tc.nodes {
		err := node.Shutdown()
		if err != nil {
			t.Logf("Error shutting down node: %v", err)
		}
	}

	// Close clients
	for _, client := range tc.clients {
		err := client.Close()
		if err != nil {
			t.Logf("Error closing client: %v", err)
		}
	}
}

// waitForStabilization waits for nodes to stabilize.
func (tc *testCluster) waitForStabilization() {
	time.Sleep(500 * time.Millisecond)
}

// TestTwoNodeRing tests a basic two-node ring.
func TestTwoNodeRing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := newTestCluster(t)
	defer cluster.shutdown(t)

	// Create first node
	node1 := cluster.addNode(t, 9000, nil)

	// Join second node
	node2 := cluster.addNode(t, 9001, node1.Address())

	// Wait for stabilization
	cluster.waitForStabilization()

	// Verify both nodes are in the ring
	assert.NotNil(t, node1.Address())
	assert.NotNil(t, node2.Address())

	// Verify node1's successor is node2 or itself
	succ1 := node1.GetSuccessorList()
	assert.NotEmpty(t, succ1)

	// Verify node2's successor is node1 or itself
	succ2 := node2.GetSuccessorList()
	assert.NotEmpty(t, succ2)
}

// TestThreeNodeRing tests a three-node ring.
func TestThreeNodeRing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := newTestCluster(t)
	defer cluster.shutdown(t)

	// Create ring with 3 nodes
	node1 := cluster.addNode(t, 9010, nil)
	node2 := cluster.addNode(t, 9011, node1.Address())
	node3 := cluster.addNode(t, 9012, node1.Address())

	// Wait for stabilization
	cluster.waitForStabilization()

	// Verify all nodes have successors
	assert.NotEmpty(t, node1.GetSuccessorList())
	assert.NotEmpty(t, node2.GetSuccessorList())
	assert.NotEmpty(t, node3.GetSuccessorList())
}

// TestDHTOperations tests Set/Get/Delete operations across nodes.
func TestDHTOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := newTestCluster(t)
	defer cluster.shutdown(t)

	// Create 2-node ring
	node1 := cluster.addNode(t, 9020, nil)
	node2 := cluster.addNode(t, 9021, node1.Address())

	cluster.waitForStabilization()

	ctx := context.Background()

	t.Run("Set and Get on same node", func(t *testing.T) {
		key := "test:key1"
		value := []byte("value1")

		// Set on node1
		err := node1.Set(ctx, key, value, 0)
		require.NoError(t, err)

		// Get from node1
		retrieved, found, err := node1.Get(ctx, key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, value, retrieved)
	})

	t.Run("Set on one node, Get from another", func(t *testing.T) {
		key := "test:key2"
		value := []byte("value2")

		// Set on node1
		err := node1.Set(ctx, key, value, 0)
		require.NoError(t, err)

		// Get from node2 (should route correctly)
		retrieved, found, err := node2.Get(ctx, key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, value, retrieved)
	})

	t.Run("Delete key", func(t *testing.T) {
		key := "test:key3"
		value := []byte("value3")

		// Set
		err := node1.Set(ctx, key, value, 0)
		require.NoError(t, err)

		// Verify exists
		_, found, err := node1.Get(ctx, key)
		require.NoError(t, err)
		assert.True(t, found)

		// Delete
		err = node1.Delete(ctx, key)
		require.NoError(t, err)

		// Verify deleted
		_, found, err = node1.Get(ctx, key)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		_, found, err := node1.Get(ctx, "nonexistent")
		require.NoError(t, err)
		assert.False(t, found)
	})
}

// TestDataMigration tests automatic data migration when nodes join.
func TestDataMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := newTestCluster(t)
	defer cluster.shutdown(t)

	ctx := context.Background()

	// Create single-node ring
	node1 := cluster.addNode(t, 9030, nil)

	// Store multiple keys on node1
	keys := []string{"user:alice", "user:bob", "user:charlie", "user:diana"}
	values := [][]byte{
		[]byte("Alice Data"),
		[]byte("Bob Data"),
		[]byte("Charlie Data"),
		[]byte("Diana Data"),
	}

	for i, key := range keys {
		err := node1.Set(ctx, key, values[i], 0)
		require.NoError(t, err)
	}

	// Verify all keys exist on node1
	for i, key := range keys {
		retrieved, found, err := node1.Get(ctx, key)
		require.NoError(t, err)
		assert.True(t, found, "Key %s should exist", key)
		assert.Equal(t, values[i], retrieved)
	}

	// Add second node - this should trigger data migration
	node2 := cluster.addNode(t, 9031, node1.Address())

	// Wait for stabilization and migration
	// Need time for:
	// 1. TransferKeys RPC to complete (happens during Join)
	// 2. Stabilization rounds to update predecessor/successor
	// 3. Finger tables to be updated
	time.Sleep(3 * time.Second) // Multiple stabilization rounds at 100ms intervals

	// After migration, keys are distributed by hash across both nodes
	// Test that ALL keys are accessible via routing from both nodes
	for i, key := range keys {
		// Try from node1 (should route to responsible node)
		retrieved1, found1, err := node1.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, found1, "Key %s should be accessible from node1", key)
		assert.Equal(t, values[i], retrieved1, "Value mismatch for key %s via node1", key)

		// Also try from node2 (should also route correctly)
		retrieved2, found2, err := node2.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, found2, "Key %s should be accessible from node2", key)
		assert.Equal(t, values[i], retrieved2, "Value mismatch for key %s via node2", key)

		t.Logf("✓ Key %s accessible from both nodes", key)
	}

	t.Logf("✓ Data migration successful: all %d keys accessible after migration", len(keys))
}

// TestFindSuccessor tests the FindSuccessor operation.
func TestFindSuccessor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := newTestCluster(t)
	defer cluster.shutdown(t)

	// Create 3-node ring
	node1 := cluster.addNode(t, 9040, nil)
	node2 := cluster.addNode(t, 9041, node1.Address())
	_ = cluster.addNode(t, 9042, node1.Address())

	cluster.waitForStabilization()

	// Test finding successor for each node's ID
	t.Run("Find successor for node1", func(t *testing.T) {
		succ, err := node1.FindSuccessor(node1.ID())
		require.NoError(t, err)
		assert.NotNil(t, succ)
	})

	t.Run("Find successor for node2", func(t *testing.T) {
		succ, err := node2.FindSuccessor(node2.ID())
		require.NoError(t, err)
		assert.NotNil(t, succ)
	})

	t.Run("Find successor for arbitrary ID", func(t *testing.T) {
		testID := big.NewInt(12345)
		succ, err := node1.FindSuccessor(testID)
		require.NoError(t, err)
		assert.NotNil(t, succ)
	})
}

// TestTransferKeys tests the TransferKeys operation.
func TestTransferKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cluster := newTestCluster(t)
	defer cluster.shutdown(t)

	ctx := context.Background()

	// Create single node
	node1 := cluster.addNode(t, 9050, nil)

	// Store some keys
	err := node1.Set(ctx, "key1", []byte("value1"), 0)
	require.NoError(t, err)
	err = node1.Set(ctx, "key2", []byte("value2"), 0)
	require.NoError(t, err)

	// Test TransferKeys with a specific range
	startID := big.NewInt(1)
	endID := new(big.Int)
	endID.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	keys, err := node1.TransferKeys(ctx, startID, endID)
	require.NoError(t, err)

	// Should return some keys (at least the ones we stored)
	assert.NotNil(t, keys)
	t.Logf("Transferred %d keys", len(keys))
}
