package transport

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zde37/torus/internal/chord"
	"github.com/zde37/torus/pkg"
)

func TestNewGRPCClient(t *testing.T) {
	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	client := NewGRPCClient(logger, 5*time.Second)

	assert.NotNil(t, client)
	assert.NotNil(t, client.logger)
	assert.Equal(t, 5*time.Second, client.timeout)
	assert.NotNil(t, client.connections)
	assert.Empty(t, client.connections)
}

func TestNewGRPCClient_NilLogger(t *testing.T) {
	// Should create default logger if nil
	client := NewGRPCClient(nil, 5*time.Second)

	assert.NotNil(t, client)
	assert.NotNil(t, client.logger)
}

func TestGRPCClient_FindSuccessor(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8200)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9200", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Test successful call
	targetID := big.NewInt(100)
	successor, err := client.FindSuccessor("127.0.0.1:9200", targetID)

	assert.NoError(t, err)
	assert.NotNil(t, successor)
	// In single-node ring, successor is the node itself
	assert.Equal(t, node.ID(), successor.ID)
}

func TestGRPCClient_FindSuccessor_InvalidAddress(t *testing.T) {
	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	client := NewGRPCClient(logger, 1*time.Second)
	defer client.Close()

	// Test with invalid address
	targetID := big.NewInt(100)
	_, err = client.FindSuccessor("invalid-address:9999", targetID)

	assert.Error(t, err)
}

func TestGRPCClient_GetPredecessor(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8210)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9210", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Initially, predecessor should be nil
	pred, err := client.GetPredecessor("127.0.0.1:9210")
	assert.NoError(t, err)
	assert.Nil(t, pred)

	// Set a predecessor
	predAddr := chord.NewNodeAddress(big.NewInt(50), "127.0.0.1", 8211, 8080)
	node.Notify(predAddr)

	// Now should get the predecessor
	pred, err = client.GetPredecessor("127.0.0.1:9210")
	assert.NoError(t, err)
	assert.NotNil(t, pred)
	assert.Equal(t, big.NewInt(50), pred.ID)
}

func TestGRPCClient_Notify(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8220)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9220", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Send notification
	notifyNode := chord.NewNodeAddress(big.NewInt(75), "127.0.0.1", 8221, 8081)
	err = client.Notify("127.0.0.1:9220", notifyNode)

	assert.NoError(t, err)

	// Verify predecessor was updated
	pred := node.GetPredecessor()
	assert.NotNil(t, pred)
	assert.Equal(t, big.NewInt(75), pred.ID)
}

func TestGRPCClient_GetSuccessorList(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8230)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9230", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Get successor list
	successors, err := client.GetSuccessorList("127.0.0.1:9230")

	assert.NoError(t, err)
	assert.NotNil(t, successors)
	assert.Len(t, successors, 1) // Single-node ring
	assert.Equal(t, node.ID(), successors[0].ID)
}

func TestGRPCClient_Ping(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8240)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9240", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Send ping
	response, err := client.Ping("127.0.0.1:9240", "hello")

	assert.NoError(t, err)
	assert.Equal(t, "pong", response)
}

func TestGRPCClient_ClosestPrecedingFinger(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8250)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9250", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Get closest preceding finger
	targetID := big.NewInt(150)
	result, err := client.ClosestPrecedingFinger("127.0.0.1:9250", targetID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	// In single-node ring, should return the node itself
	assert.Equal(t, node.ID(), result.ID)
}

func TestGRPCClient_ConnectionPooling(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8260)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9260", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Make multiple calls to same address
	for i := 0; i < 3; i++ {
		_, err := client.Ping("127.0.0.1:9260", "test")
		assert.NoError(t, err)
	}

	// Verify connection was pooled
	client.connMu.RLock()
	assert.Len(t, client.connections, 1)
	client.connMu.RUnlock()
}

func TestGRPCClient_MultipleConnections(t *testing.T) {
	// Create two test servers
	node1 := createTestNode(t, 8270)
	defer node1.Shutdown()

	node2 := createTestNode(t, 8271)
	defer node2.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server1, err := NewGRPCServer(node1, "127.0.0.1:9270", logger)
	require.NoError(t, err)
	err = server1.Start()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := NewGRPCServer(node2, "127.0.0.1:9271", logger)
	require.NoError(t, err)
	err = server2.Start()
	require.NoError(t, err)
	defer server2.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Connect to both servers
	_, err = client.Ping("127.0.0.1:9270", "test1")
	assert.NoError(t, err)

	_, err = client.Ping("127.0.0.1:9271", "test2")
	assert.NoError(t, err)

	// Verify two connections exist
	client.connMu.RLock()
	assert.Len(t, client.connections, 2)
	client.connMu.RUnlock()
}

func TestGRPCClient_Close(t *testing.T) {
	// Create test servers
	node1 := createTestNode(t, 8280)
	defer node1.Shutdown()

	node2 := createTestNode(t, 8281)
	defer node2.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server1, err := NewGRPCServer(node1, "127.0.0.1:9280", logger)
	require.NoError(t, err)
	err = server1.Start()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := NewGRPCServer(node2, "127.0.0.1:9281", logger)
	require.NoError(t, err)
	err = server2.Start()
	require.NoError(t, err)
	defer server2.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client and establish connections
	client := NewGRPCClient(logger, 5*time.Second)

	_, err = client.Ping("127.0.0.1:9280", "test1")
	assert.NoError(t, err)

	_, err = client.Ping("127.0.0.1:9281", "test2")
	assert.NoError(t, err)

	// Verify connections exist
	client.connMu.RLock()
	assert.Len(t, client.connections, 2)
	client.connMu.RUnlock()

	// Close client
	err = client.Close()
	assert.NoError(t, err)

	// Verify connections were cleared
	client.connMu.RLock()
	assert.Empty(t, client.connections)
	client.connMu.RUnlock()
}

func TestGRPCClient_Timeout(t *testing.T) {
	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	// Create client with very short timeout
	client := NewGRPCClient(logger, 100*time.Millisecond)
	defer client.Close()

	// Try to connect to non-existent server
	// This should timeout during connection attempt
	targetID := big.NewInt(100)
	_, err = client.FindSuccessor("127.0.0.1:19999", targetID)

	assert.Error(t, err)
}

func TestGRPCClient_ConcurrentCalls(t *testing.T) {
	// Create a test server
	node := createTestNode(t, 8290)
	defer node.Shutdown()

	logger, err := pkg.New(pkg.DefaultConfig())
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9290", logger)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewGRPCClient(logger, 5*time.Second)
	defer client.Close()

	// Make concurrent calls
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, err := client.Ping("127.0.0.1:9290", "concurrent")
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify connection pooling worked correctly
	client.connMu.RLock()
	assert.Len(t, client.connections, 1)
	client.connMu.RUnlock()
}
