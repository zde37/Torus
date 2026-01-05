package transport

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zde37/torus/pkg"
	"github.com/zde37/torus/pkg/chord"
	"github.com/zde37/torus/pkg/config"
	pb "github.com/zde37/torus/protobuf/protogen"
)

var (
	TEST_AUTH_TOKEN = "auth_token"
)

// createTestNode creates a ChordNode for testing.
func createTestNode(t *testing.T, port int) *chord.ChordNode {
	t.Helper()

	cfg := &config.Config{
		Host:               "127.0.0.1",
		Port:               port,
		HTTPPort:           port + 1000, // Use port + 1000 for HTTP
		M:                  160,
		SuccessorListSize:  3,
		StabilizeInterval:  100 * time.Millisecond,
		FixFingersInterval: 100 * time.Millisecond,
	}

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	node, err := chord.NewChordNode(cfg, logger)
	require.NoError(t, err)

	// Create the ring
	err = node.Create()
	require.NoError(t, err)

	return node
}

func TestNewGRPCServer(t *testing.T) {
	tests := []struct {
		name        string
		node        *chord.ChordNode
		address     string
		logger      *pkg.Logger
		expectError bool
	}{
		{
			name:        "valid server creation",
			node:        createTestNode(t, 8001),
			address:     "127.0.0.1:9001",
			logger:      func() *pkg.Logger { l, _ := pkg.NewLogger(nil); return l }(),
			expectError: false,
		},
		{
			name:        "nil node",
			node:        nil,
			address:     "127.0.0.1:9002",
			logger:      func() *pkg.Logger { l, _ := pkg.NewLogger(nil); return l }(),
			expectError: true,
		},
		{
			name:        "nil logger",
			node:        createTestNode(t, 8002),
			address:     "127.0.0.1:9003",
			logger:      nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewGRPCServer(tt.node, tt.address, TEST_AUTH_TOKEN, tt.logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, server)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, server)
				assert.Equal(t, tt.address, server.address)
				assert.Equal(t, tt.node, server.node)

				// Cleanup
				if tt.node != nil {
					tt.node.Shutdown()
				}
			}
		})
	}
}

func TestGRPCServer_StartStop(t *testing.T) {
	node := createTestNode(t, 8010)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9010", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	// Start server
	err = server.Start()
	assert.NoError(t, err)
	assert.NotNil(t, server.server)
	assert.NotNil(t, server.listener)

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Stop server
	err = server.Stop()
	assert.NoError(t, err)
}

func TestGRPCServer_FindSuccessor(t *testing.T) {
	node := createTestNode(t, 8020)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9020", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	tests := []struct {
		name        string
		id          []byte
		expectError bool
	}{
		{
			name:        "valid id",
			id:          big.NewInt(100).Bytes(),
			expectError: false,
		},
		{
			name:        "nil id",
			id:          nil,
			expectError: true,
		},
		{
			name:        "empty id",
			id:          []byte{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.FindSuccessorRequest{Id: tt.id}
			resp, err := server.FindSuccessor(context.Background(), req)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Successor)
				assert.Equal(t, node.Address().ID.Bytes(), resp.Successor.Id)
			}
		})
	}
}

func TestGRPCServer_GetPredecessor(t *testing.T) {
	node := createTestNode(t, 8030)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9030", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	// Initially, predecessor should be nil (single node ring)
	req := &pb.GetPredecessorRequest{}
	resp, err := server.GetPredecessor(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Predecessor)

	// Set a predecessor
	predID := big.NewInt(50)
	predAddr := chord.NewNodeAddress(predID, "127.0.0.1", 8031, 8080)
	node.Notify(predAddr)

	// Now predecessor should be set
	resp, err = server.GetPredecessor(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Predecessor)
	assert.Equal(t, predID.Bytes(), resp.Predecessor.Id)
}

func TestGRPCServer_Notify(t *testing.T) {
	node := createTestNode(t, 8040)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9040", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	tests := []struct {
		name        string
		node        *pb.Node
		expectError bool
	}{
		{
			name: "valid notify",
			node: &pb.Node{
				Id:   big.NewInt(100).Bytes(),
				Host: "127.0.0.1",
				Port: 8041,
			},
			expectError: false,
		},
		{
			name:        "nil node",
			node:        nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.NotifyRequest{Node: tt.node}
			resp, err := server.Notify(context.Background(), req)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.True(t, resp.Success)

				// Verify predecessor was updated
				pred := node.GetPredecessor()
				assert.NotNil(t, pred)
				assert.Equal(t, tt.node.Id, pred.ID.Bytes())
			}
		})
	}
}

func TestGRPCServer_GetSuccessorList(t *testing.T) {
	node := createTestNode(t, 8050)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9050", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	req := &pb.GetSuccessorListRequest{}
	resp, err := server.GetSuccessorList(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Successors)

	// In a single-node ring, the node is its own successor
	assert.Len(t, resp.Successors, 1)
	assert.Equal(t, node.Address().ID.Bytes(), resp.Successors[0].Id)
}

func TestGRPCServer_Ping(t *testing.T) {
	node := createTestNode(t, 8060)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9060", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	tests := []struct {
		name    string
		message string
	}{
		{
			name:    "ping with message",
			message: "hello",
		},
		{
			name:    "ping with empty message",
			message: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.PingRequest{Message: tt.message}
			resp, err := server.Ping(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, "pong", resp.Message)
			assert.Greater(t, resp.Timestamp, int64(0))
		})
	}
}

func TestGRPCServer_ClosestPrecedingFinger(t *testing.T) {
	node := createTestNode(t, 8070)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9070", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	tests := []struct {
		name        string
		id          []byte
		expectError bool
	}{
		{
			name:        "valid id",
			id:          big.NewInt(200).Bytes(),
			expectError: false,
		},
		{
			name:        "nil id",
			id:          nil,
			expectError: true,
		},
		{
			name:        "empty id",
			id:          []byte{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.ClosestPrecedingFingerRequest{Id: tt.id}
			resp, err := server.ClosestPrecedingFinger(context.Background(), req)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Node)
			}
		})
	}
}

func TestGRPCServer_TransferKeys(t *testing.T) {
	node := createTestNode(t, 8080)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9080", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	// Create the ring
	err = node.Create()
	require.NoError(t, err)

	// Request keys in range - should return empty since no keys stored
	// Use non-zero values to avoid empty byte slice from big.Int(0).Bytes()
	startID := big.NewInt(1)
	endID := big.NewInt(1000)

	req := &pb.TransferKeysRequest{
		StartId: startID.Bytes(),
		EndId:   endID.Bytes(),
	}
	resp, err := server.TransferKeys(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Empty(t, resp.Keys)
	assert.Equal(t, int32(0), resp.Count)
}

func TestGRPCServer_Get(t *testing.T) {
	node := createTestNode(t, 8090)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9090", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	// For MVP, Get returns not found
	req := &pb.GetRequest{Key: "test-key"}
	resp, err := server.Get(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Value)
	assert.False(t, resp.Found)
}

func TestGRPCServer_Set(t *testing.T) {
	node := createTestNode(t, 8100)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9100", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	// For MVP, Set returns success
	req := &pb.SetRequest{
		Key:   "test-key",
		Value: []byte("test-value"),
	}
	resp, err := server.Set(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestGRPCServer_Delete(t *testing.T) {
	node := createTestNode(t, 8110)
	defer node.Shutdown()

	logger, err := pkg.NewLogger(nil)
	require.NoError(t, err)

	server, err := NewGRPCServer(node, "127.0.0.1:9110", TEST_AUTH_TOKEN, logger)
	require.NoError(t, err)

	// For MVP, Delete returns success
	req := &pb.DeleteRequest{Key: "test-key"}
	resp, err := server.Delete(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestNodeAddressToProto(t *testing.T) {
	tests := []struct {
		name     string
		addr     *chord.NodeAddress
		expected *pb.Node
	}{
		{
			name: "valid address",
			addr: chord.NewNodeAddress(big.NewInt(42), "192.168.1.1", 8000, 8080),
			expected: &pb.Node{
				Id:       big.NewInt(42).Bytes(),
				Host:     "192.168.1.1",
				Port:     8000,
				HttpPort: 8080,
			},
		},
		{
			name:     "nil address",
			addr:     nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nodeAddressToProto(tt.addr)

			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expected.Id, result.Id)
				assert.Equal(t, tt.expected.Host, result.Host)
				assert.Equal(t, tt.expected.Port, result.Port)
			}
		})
	}
}

func TestProtoToNodeAddress(t *testing.T) {
	tests := []struct {
		name     string
		node     *pb.Node
		expected *chord.NodeAddress
	}{
		{
			name: "valid node",
			node: &pb.Node{
				Id:       big.NewInt(42).Bytes(),
				Host:     "192.168.1.1",
				Port:     8000,
				HttpPort: 8080,
			},
			expected: chord.NewNodeAddress(big.NewInt(42), "192.168.1.1", 8000, 8080),
		},
		{
			name:     "nil node",
			node:     nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToNodeAddress(tt.node)

			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expected.ID, result.ID)
				assert.Equal(t, tt.expected.Host, result.Host)
				assert.Equal(t, tt.expected.Port, result.Port)
			}
		})
	}
}

func TestProtoConversionRoundTrip(t *testing.T) {
	// Test that converting to proto and back preserves data
	original := chord.NewNodeAddress(big.NewInt(12345), "10.0.0.1", 9000, 8080)

	// Convert to proto
	pbNode := nodeAddressToProto(original)
	assert.NotNil(t, pbNode)

	// Convert back
	result := protoToNodeAddress(pbNode)
	assert.NotNil(t, result)

	// Verify equality
	assert.Equal(t, original.ID, result.ID)
	assert.Equal(t, original.Host, result.Host)
	assert.Equal(t, original.Port, result.Port)
}
