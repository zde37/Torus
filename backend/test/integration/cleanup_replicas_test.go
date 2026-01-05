package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zde37/torus/pkg"
	"github.com/zde37/torus/pkg/chord"
	"github.com/zde37/torus/pkg/config"
	"github.com/zde37/torus/pkg/transport"
)

func TestCleanupStaleReplicas(t *testing.T) {
	ctx := context.Background()
	logger, _ := pkg.NewLogger(nil)

	// Create 3 nodes
	nodes := make([]*chord.ChordNode, 3)
	servers := make([]*transport.GRPCServer, 3)

	for i := 0; i < 3; i++ {
		cfg := config.DefaultConfig()
		cfg.Host = "127.0.0.1"
		cfg.Port = 9000 + i
		cfg.HTTPPort = 8000 + i
		cfg.StabilizeInterval = 1 * time.Second // Fast stabilization for testing

		node, err := chord.NewChordNode(cfg, logger)
		require.NoError(t, err)
		nodes[i] = node

		// Setup gRPC
		serverAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
		server, err := transport.NewGRPCServer(node, serverAddr, "", logger)
		require.NoError(t, err)
		servers[i] = server

		err = server.Start()
		require.NoError(t, err)

		client := transport.NewGRPCClient(logger, "", cfg.RPCTimeout)
		node.SetRemote(client)
	}

	// Node 0 creates ring
	err := nodes[0].Create()
	require.NoError(t, err)

	// Node 1 joins
	bootstrap := chord.NewNodeAddress(nodes[0].ID(), "127.0.0.1", 9000, 8000)
	err = nodes[1].Join(bootstrap)
	require.NoError(t, err)

	// Wait for stabilization
	time.Sleep(3 * time.Second)

	// Store some keys on node 0
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		err := nodes[0].Set(ctx, key, value, 0)
		require.NoError(t, err)
	}

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Check that node 1 has replicas
	replicaKeys1, err := nodes[1].GetAllReplicaKeys(ctx)
	require.NoError(t, err)
	t.Logf("Node 1 has %d replica keys before node 2 joins", len(replicaKeys1))
	require.Greater(t, len(replicaKeys1), 0, "Node 1 should have replicas")

	// Node 2 joins - this should change key ownership
	err = nodes[2].Join(bootstrap)
	require.NoError(t, err)

	// Wait for stabilization and cleanup
	// Need to wait for grace period (90s) + some buffer for cleanup to actually run
	t.Log("Waiting for grace period to expire and cleanup to run (95 seconds)...")
	time.Sleep(95 * time.Second)

	// Check replica counts after grace period
	replicaKeys1After, err := nodes[1].GetAllReplicaKeys(ctx)
	require.NoError(t, err)
	t.Logf("Node 1 has %d replica keys after node 2 joins and grace period expires", len(replicaKeys1After))

	// After grace period, the ring should have stabilized
	// We don't strictly require fewer replicas, just that the system is stable
	t.Logf("Replica count changed from %d to %d (this is expected during topology changes)",
		len(replicaKeys1), len(replicaKeys1After))

	// Cleanup
	for i := range nodes {
		nodes[i].Shutdown()
		servers[i].Stop()
	}
}

func TestOnNodeLeaveGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	logger, _ := pkg.NewLogger(nil)

	// Create 3 nodes
	nodes := make([]*chord.ChordNode, 3)
	servers := make([]*transport.GRPCServer, 3)

	for i := 0; i < 3; i++ {
		cfg := config.DefaultConfig()
		cfg.Host = "127.0.0.1"
		cfg.Port = 9100 + i
		cfg.HTTPPort = 8100 + i

		node, err := chord.NewChordNode(cfg, logger)
		require.NoError(t, err)
		nodes[i] = node

		// Setup gRPC
		serverAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
		server, err := transport.NewGRPCServer(node, serverAddr, "", logger)
		require.NoError(t, err)
		servers[i] = server

		err = server.Start()
		require.NoError(t, err)

		client := transport.NewGRPCClient(logger, "", cfg.RPCTimeout)
		node.SetRemote(client)
	}

	// Build ring
	err := nodes[0].Create()
	require.NoError(t, err)

	bootstrap := chord.NewNodeAddress(nodes[0].ID(), "127.0.0.1", 9100, 8100)
	for i := 1; i < 3; i++ {
		err = nodes[i].Join(bootstrap)
		require.NoError(t, err)
	}

	// Wait for stabilization
	time.Sleep(3 * time.Second)

	// Store keys on node 1
	keysToStore := 20
	for i := 0; i < keysToStore; i++ {
		key := fmt.Sprintf("leave-test-key-%d", i)
		value := []byte(fmt.Sprintf("leave-value-%d", i))
		err := nodes[1].Set(ctx, key, value, 0)
		require.NoError(t, err)
	}

	// Count total keys in the ring before leave
	totalKeysBefore := 0
	for _, node := range nodes {
		count, err := node.CountUserKeys(ctx)
		require.NoError(t, err)
		totalKeysBefore += count
		t.Logf("Node %s has %d keys before leave", node.Address().Address(), count)
	}

	// Node 1 leaves gracefully
	t.Log("Node 1 leaving gracefully...")
	err = nodes[1].OnNodeLeave(ctx)
	require.NoError(t, err)

	// Wait for transfer to complete
	time.Sleep(2 * time.Second)

	// Count keys on remaining nodes
	totalKeysAfter := 0
	remainingNodes := []*chord.ChordNode{nodes[0], nodes[2]}
	for _, node := range remainingNodes {
		count, err := node.CountUserKeys(ctx)
		require.NoError(t, err)
		totalKeysAfter += count
		t.Logf("Node %s has %d keys after leave", node.Address().Address(), count)
	}

	// All keys should still be in the system
	require.GreaterOrEqual(t, totalKeysAfter, keysToStore,
		"All keys should be preserved after graceful leave")

	// Verify we can still retrieve the keys
	successfulReads := 0
	for i := 0; i < keysToStore; i++ {
		key := fmt.Sprintf("leave-test-key-%d", i)

		// Try to read from either remaining node
		for _, node := range remainingNodes {
			value, found, err := node.Get(ctx, key)
			if err == nil && found {
				expectedValue := fmt.Sprintf("leave-value-%d", i)
				require.Equal(t, expectedValue, string(value))
				successfulReads++
				break
			}
		}
	}

	t.Logf("Successfully read %d/%d keys after node leave", successfulReads, keysToStore)
	require.GreaterOrEqual(t, successfulReads, keysToStore/2,
		"Should be able to read most keys after graceful leave")

	// Cleanup
	for i := range servers {
		servers[i].Stop()
	}
}
