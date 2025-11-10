package protogen

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNode_Creation(t *testing.T) {
	tests := []struct {
		name string
		node *Node
	}{
		{
			name: "valid node",
			node: &Node{
				Id:   []byte{1, 2, 3, 4, 5},
				Host: "127.0.0.1",
				Port: 8080,
			},
		},
		{
			name: "node with large ID",
			node: &Node{
				Id:   make([]byte, 20), // 160 bits
				Host: "192.168.1.1",
				Port: 9000,
			},
		},
		{
			name: "empty node",
			node: &Node{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.node)
			// Verify protobuf methods exist
			_ = tt.node.ProtoReflect()
		})
	}
}

func TestNode_MarshalUnmarshal(t *testing.T) {
	original := &Node{
		Id:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		Host: "127.0.0.1",
		Port: 8080,
	}

	// Marshal
	data, err := proto.Marshal(original)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal
	decoded := &Node{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Host, decoded.Host)
	assert.Equal(t, original.Port, decoded.Port)
}

func TestFindSuccessorRequest_MarshalUnmarshal(t *testing.T) {
	original := &FindSuccessorRequest{
		Id: []byte{1, 2, 3, 4, 5},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &FindSuccessorRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
}

func TestFindSuccessorResponse_MarshalUnmarshal(t *testing.T) {
	original := &FindSuccessorResponse{
		Successor: &Node{
			Id:   []byte{1, 2, 3},
			Host: "127.0.0.1",
			Port: 8080,
		},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &FindSuccessorResponse{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Successor.Id, decoded.Successor.Id)
	assert.Equal(t, original.Successor.Host, decoded.Successor.Host)
	assert.Equal(t, original.Successor.Port, decoded.Successor.Port)
}

func TestGetPredecessorResponse_NilPredecessor(t *testing.T) {
	t.Run("nil predecessor", func(t *testing.T) {
		resp := &GetPredecessorResponse{
			Predecessor: nil,
		}

		data, err := proto.Marshal(resp)
		require.NoError(t, err)

		decoded := &GetPredecessorResponse{}
		err = proto.Unmarshal(data, decoded)
		require.NoError(t, err)

		assert.Nil(t, decoded.Predecessor)
	})

	t.Run("non-nil predecessor", func(t *testing.T) {
		resp := &GetPredecessorResponse{
			Predecessor: &Node{
				Id:   []byte{1, 2, 3},
				Host: "127.0.0.1",
				Port: 8080,
			},
		}

		data, err := proto.Marshal(resp)
		require.NoError(t, err)

		decoded := &GetPredecessorResponse{}
		err = proto.Unmarshal(data, decoded)
		require.NoError(t, err)

		assert.NotNil(t, decoded.Predecessor)
		assert.Equal(t, resp.Predecessor.Id, decoded.Predecessor.Id)
	})
}

func TestNotifyRequest_MarshalUnmarshal(t *testing.T) {
	original := &NotifyRequest{
		Node: &Node{
			Id:   []byte{5, 6, 7, 8},
			Host: "192.168.1.1",
			Port: 9000,
		},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &NotifyRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Node.Id, decoded.Node.Id)
	assert.Equal(t, original.Node.Host, decoded.Node.Host)
	assert.Equal(t, original.Node.Port, decoded.Node.Port)
}

func TestNotifyResponse(t *testing.T) {
	tests := []struct {
		name    string
		success bool
	}{
		{name: "success true", success: true},
		{name: "success false", success: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &NotifyResponse{Success: tt.success}

			data, err := proto.Marshal(resp)
			require.NoError(t, err)

			decoded := &NotifyResponse{}
			err = proto.Unmarshal(data, decoded)
			require.NoError(t, err)

			assert.Equal(t, tt.success, decoded.Success)
		})
	}
}

func TestGetSuccessorListResponse_MarshalUnmarshal(t *testing.T) {
	original := &GetSuccessorListResponse{
		Successors: []*Node{
			{Id: []byte{1}, Host: "127.0.0.1", Port: 8080},
			{Id: []byte{2}, Host: "127.0.0.2", Port: 8081},
			{Id: []byte{3}, Host: "127.0.0.3", Port: 8082},
		},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &GetSuccessorListResponse{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	require.Len(t, decoded.Successors, 3)
	for i, node := range original.Successors {
		assert.Equal(t, node.Id, decoded.Successors[i].Id)
		assert.Equal(t, node.Host, decoded.Successors[i].Host)
		assert.Equal(t, node.Port, decoded.Successors[i].Port)
	}
}

func TestPingRequest_MarshalUnmarshal(t *testing.T) {
	original := &PingRequest{
		Message: "ping",
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &PingRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Message, decoded.Message)
}

func TestPingResponse_MarshalUnmarshal(t *testing.T) {
	now := time.Now().Unix()
	original := &PingResponse{
		Message:   "pong",
		Timestamp: now,
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &PingResponse{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Message, decoded.Message)
	assert.Equal(t, original.Timestamp, decoded.Timestamp)
}

func TestGetRequest_MarshalUnmarshal(t *testing.T) {
	original := &GetRequest{
		Key: "test-key",
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &GetRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Key, decoded.Key)
}

func TestGetResponse_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
		found bool
	}{
		{name: "found", value: []byte("test-value"), found: true},
		{name: "not found", value: nil, found: false},
		{name: "empty value", value: []byte{}, found: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &GetResponse{
				Value: tt.value,
				Found: tt.found,
			}

			data, err := proto.Marshal(original)
			require.NoError(t, err)

			decoded := &GetResponse{}
			err = proto.Unmarshal(data, decoded)
			require.NoError(t, err)

			assert.Equal(t, original.Found, decoded.Found)
			// Protobuf treats empty byte slices and nil the same way
			if tt.value == nil || len(tt.value) == 0 {
				assert.Empty(t, decoded.Value)
			} else {
				assert.Equal(t, original.Value, decoded.Value)
			}
		})
	}
}

func TestSetRequest_MarshalUnmarshal(t *testing.T) {
	original := &SetRequest{
		Key:        "test-key",
		Value:      []byte("test-value"),
		TtlSeconds: 3600,
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &SetRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Key, decoded.Key)
	assert.Equal(t, original.Value, decoded.Value)
	assert.Equal(t, original.TtlSeconds, decoded.TtlSeconds)
}

func TestDeleteRequest_MarshalUnmarshal(t *testing.T) {
	original := &DeleteRequest{
		Key: "delete-key",
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &DeleteRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Key, decoded.Key)
}

func TestClosestPrecedingFingerRequest_MarshalUnmarshal(t *testing.T) {
	original := &ClosestPrecedingFingerRequest{
		Id: []byte{10, 20, 30},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &ClosestPrecedingFingerRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
}

func TestTransferKeysRequest_MarshalUnmarshal(t *testing.T) {
	original := &TransferKeysRequest{
		StartId: []byte{1, 2, 3},
		EndId:   []byte{10, 20, 30},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &TransferKeysRequest{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.StartId, decoded.StartId)
	assert.Equal(t, original.EndId, decoded.EndId)
}

func TestKeyValuePair_MarshalUnmarshal(t *testing.T) {
	original := &KeyValuePair{
		Key:        "test-key",
		Value:      []byte("test-value"),
		TtlSeconds: 300,
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &KeyValuePair{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Key, decoded.Key)
	assert.Equal(t, original.Value, decoded.Value)
	assert.Equal(t, original.TtlSeconds, decoded.TtlSeconds)
}

func TestTransferKeysResponse_MarshalUnmarshal(t *testing.T) {
	original := &TransferKeysResponse{
		Keys: []*KeyValuePair{
			{Key: "key1", Value: []byte("value1"), TtlSeconds: 100},
			{Key: "key2", Value: []byte("value2"), TtlSeconds: 200},
		},
		Count: 2,
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)

	decoded := &TransferKeysResponse{}
	err = proto.Unmarshal(data, decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Count, decoded.Count)
	require.Len(t, decoded.Keys, 2)
	for i, kvp := range original.Keys {
		assert.Equal(t, kvp.Key, decoded.Keys[i].Key)
		assert.Equal(t, kvp.Value, decoded.Keys[i].Value)
		assert.Equal(t, kvp.TtlSeconds, decoded.Keys[i].TtlSeconds)
	}
}

func TestNode_BigIntConversion(t *testing.T) {
	t.Run("big.Int to bytes and back", func(t *testing.T) {
		// Create a large ID
		original := new(big.Int).Exp(big.NewInt(2), big.NewInt(159), nil)
		original.Add(original, big.NewInt(12345))

		// Convert to bytes
		idBytes := original.Bytes()

		// Create node
		node := &Node{
			Id:   idBytes,
			Host: "127.0.0.1",
			Port: 8080,
		}

		// Marshal and unmarshal
		data, err := proto.Marshal(node)
		require.NoError(t, err)

		decoded := &Node{}
		err = proto.Unmarshal(data, decoded)
		require.NoError(t, err)

		// Convert back to big.Int
		decodedID := new(big.Int).SetBytes(decoded.Id)

		// Verify
		assert.Equal(t, original, decodedID)
	})
}

func TestEmptyMessages(t *testing.T) {
	tests := []struct {
		name string
		msg  proto.Message
	}{
		{"GetPredecessorRequest", &GetPredecessorRequest{}},
		{"GetSuccessorListRequest", &GetSuccessorListRequest{}},
		{"NotifyResponse", &NotifyResponse{}},
		{"SetResponse", &SetResponse{}},
		{"DeleteResponse", &DeleteResponse{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := proto.Marshal(tt.msg)
			require.NoError(t, err)

			// Empty messages should still be marshalable
			assert.NotNil(t, data)
		})
	}
}

// Benchmark tests
func BenchmarkNode_Marshal(b *testing.B) {
	node := &Node{
		Id:   make([]byte, 20),
		Host: "127.0.0.1",
		Port: 8080,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(node)
	}
}

func BenchmarkNode_Unmarshal(b *testing.B) {
	node := &Node{
		Id:   make([]byte, 20),
		Host: "127.0.0.1",
		Port: 8080,
	}
	data, _ := proto.Marshal(node)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded := &Node{}
		_ = proto.Unmarshal(data, decoded)
	}
}

func BenchmarkFindSuccessorRequest_Marshal(b *testing.B) {
	req := &FindSuccessorRequest{
		Id: make([]byte, 20),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(req)
	}
}
