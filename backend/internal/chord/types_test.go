package chord

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNodeAddress(t *testing.T) {
	tests := []struct {
		name string
		id   *big.Int
		host string
		port int
	}{
		{
			name: "valid node",
			id:   big.NewInt(42),
			host: "127.0.0.1",
			port: 8080,
		},
		{
			name: "large ID",
			id:   new(big.Int).Exp(big.NewInt(2), big.NewInt(159), nil),
			host: "192.168.1.1",
			port: 9000,
		},
		{
			name: "nil ID",
			id:   nil,
			host: "localhost",
			port: 8440,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewNodeAddress(tt.id, tt.host, tt.port)
			require.NotNil(t, node)
			assert.Equal(t, tt.host, node.Host)
			assert.Equal(t, tt.port, node.Port)
			assert.NotNil(t, node.ID)

			if tt.id != nil {
				// Verify ID is copied, not referenced
				assert.Equal(t, tt.id, node.ID)
				assert.NotSame(t, tt.id, node.ID)
			}
		})
	}
}

func TestNodeAddress_String(t *testing.T) {
	tests := []struct {
		name     string
		node     *NodeAddress
		contains []string
	}{
		{
			name: "valid node",
			node: NewNodeAddress(big.NewInt(255), "127.0.0.1", 8080),
			contains: []string{
				"NodeAddress",
				"ff", // 255 in hex
				"127.0.0.1:8080",
			},
		},
		{
			name:     "nil node",
			node:     nil,
			contains: []string{"nil"},
		},
		{
			name: "zero ID",
			node: NewNodeAddress(big.NewInt(0), "localhost", 9000),
			contains: []string{
				"NodeAddress",
				"0",
				"localhost:9000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.node.String()
			for _, substr := range tt.contains {
				assert.Contains(t, str, substr)
			}
		})
	}
}

func TestNodeAddress_Address(t *testing.T) {
	tests := []struct {
		name     string
		node     *NodeAddress
		expected string
	}{
		{
			name:     "valid node",
			node:     NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080),
			expected: "127.0.0.1:8080",
		},
		{
			name:     "different port",
			node:     NewNodeAddress(big.NewInt(2), "192.168.1.1", 9000),
			expected: "192.168.1.1:9000",
		},
		{
			name:     "nil node",
			node:     nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.node.Address())
		})
	}
}

func TestNodeAddress_Equals(t *testing.T) {
	node1 := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080)
	node2 := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080)
	node3 := NewNodeAddress(big.NewInt(43), "127.0.0.1", 8080)
	node4 := NewNodeAddress(big.NewInt(42), "127.0.0.2", 8080)
	node5 := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8081)

	tests := []struct {
		name     string
		node1    *NodeAddress
		node2    *NodeAddress
		expected bool
	}{
		{
			name:     "identical nodes",
			node1:    node1,
			node2:    node2,
			expected: true,
		},
		{
			name:     "same reference",
			node1:    node1,
			node2:    node1,
			expected: true,
		},
		{
			name:     "different ID",
			node1:    node1,
			node2:    node3,
			expected: false,
		},
		{
			name:     "different host",
			node1:    node1,
			node2:    node4,
			expected: false,
		},
		{
			name:     "different port",
			node1:    node1,
			node2:    node5,
			expected: false,
		},
		{
			name:     "both nil",
			node1:    nil,
			node2:    nil,
			expected: true,
		},
		{
			name:     "first nil",
			node1:    nil,
			node2:    node1,
			expected: false,
		},
		{
			name:     "second nil",
			node1:    node1,
			node2:    nil,
			expected: false,
		},
		{
			name:     "nil IDs, same address",
			node1:    NewNodeAddress(nil, "127.0.0.1", 8080),
			node2:    NewNodeAddress(nil, "127.0.0.1", 8080),
			expected: true,
		},
		{
			name:     "nil IDs, different address",
			node1:    NewNodeAddress(nil, "127.0.0.1", 8080),
			node2:    NewNodeAddress(nil, "127.0.0.2", 8080),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.node1.Equals(tt.node2))
		})
	}
}

func TestNodeAddress_Copy(t *testing.T) {
	t.Run("copy valid node", func(t *testing.T) {
		original := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080)
		copy := original.Copy()

		require.NotNil(t, copy)
		assert.True(t, original.Equals(copy))
		assert.NotSame(t, original, copy)
		assert.NotSame(t, original.ID, copy.ID)

		// Modify copy and verify original unchanged
		copy.ID.Add(copy.ID, big.NewInt(1))
		assert.False(t, original.Equals(copy))
		assert.Equal(t, int64(42), original.ID.Int64())
	})

	t.Run("copy nil node", func(t *testing.T) {
		var node *NodeAddress
		copy := node.Copy()
		assert.Nil(t, copy)
	})
}

func TestNodeAddress_IsNil(t *testing.T) {
	tests := []struct {
		name     string
		node     *NodeAddress
		expected bool
	}{
		{
			name:     "valid node",
			node:     NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080),
			expected: false,
		},
		{
			name:     "nil node",
			node:     nil,
			expected: true,
		},
		{
			name: "node with nil ID",
			node: &NodeAddress{
				ID:   nil,
				Host: "127.0.0.1",
				Port: 8080,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.node.IsNil())
		})
	}
}

func TestNewFingerEntry(t *testing.T) {
	tests := []struct {
		name  string
		start *big.Int
		node  *NodeAddress
	}{
		{
			name:  "valid entry",
			start: big.NewInt(100),
			node:  NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080),
		},
		{
			name:  "nil start",
			start: nil,
			node:  NewNodeAddress(big.NewInt(1), "localhost", 9000),
		},
		{
			name:  "nil node",
			start: big.NewInt(50),
			node:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := NewFingerEntry(tt.start, tt.node)
			require.NotNil(t, entry)

			if tt.start != nil {
				assert.Equal(t, tt.start, entry.Start)
				assert.NotSame(t, tt.start, entry.Start)
			} else {
				assert.Nil(t, entry.Start)
			}

			if tt.node != nil {
				assert.True(t, tt.node.Equals(entry.Node))
				assert.NotSame(t, tt.node, entry.Node)
			} else {
				assert.Nil(t, entry.Node)
			}
		})
	}
}

func TestFingerEntry_String(t *testing.T) {
	tests := []struct {
		name     string
		entry    *FingerEntry
		contains []string
	}{
		{
			name: "valid entry",
			entry: NewFingerEntry(
				big.NewInt(255),
				NewNodeAddress(big.NewInt(300), "127.0.0.1", 8080),
			),
			contains: []string{
				"FingerEntry",
				"ff", // 255 in hex
			},
		},
		{
			name:     "nil entry",
			entry:    nil,
			contains: []string{"nil"},
		},
		{
			name: "nil start",
			entry: &FingerEntry{
				Start: nil,
				Node:  NewNodeAddress(big.NewInt(1), "localhost", 9000),
			},
			contains: []string{"FingerEntry", "<nil>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.entry.String()
			for _, substr := range tt.contains {
				assert.Contains(t, str, substr)
			}
		})
	}
}

func TestFingerEntry_Copy(t *testing.T) {
	t.Run("copy valid entry", func(t *testing.T) {
		original := NewFingerEntry(
			big.NewInt(100),
			NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080),
		)
		copy := original.Copy()

		require.NotNil(t, copy)
		assert.NotSame(t, original, copy)
		assert.NotSame(t, original.Start, copy.Start)
		assert.NotSame(t, original.Node, copy.Node)
		assert.Equal(t, original.Start, copy.Start)
		assert.True(t, original.Node.Equals(copy.Node))

		// Modify copy and verify original unchanged
		copy.Start.Add(copy.Start, big.NewInt(1))
		copy.Node.ID.Add(copy.Node.ID, big.NewInt(1))
		assert.Equal(t, int64(100), original.Start.Int64())
		assert.Equal(t, int64(42), original.Node.ID.Int64())
	})

	t.Run("copy nil entry", func(t *testing.T) {
		var entry *FingerEntry
		copy := entry.Copy()
		assert.Nil(t, copy)
	})
}

func TestFingerEntry_IsNil(t *testing.T) {
	tests := []struct {
		name     string
		entry    *FingerEntry
		expected bool
	}{
		{
			name: "valid entry",
			entry: NewFingerEntry(
				big.NewInt(1),
				NewNodeAddress(big.NewInt(2), "127.0.0.1", 8080),
			),
			expected: false,
		},
		{
			name:     "nil entry",
			entry:    nil,
			expected: true,
		},
		{
			name: "nil start",
			entry: &FingerEntry{
				Start: nil,
				Node:  NewNodeAddress(big.NewInt(1), "127.0.0.1", 8080),
			},
			expected: true,
		},
		{
			name: "nil node",
			entry: &FingerEntry{
				Start: big.NewInt(1),
				Node:  nil,
			},
			expected: true,
		},
		{
			name: "node with nil ID",
			entry: &FingerEntry{
				Start: big.NewInt(1),
				Node: &NodeAddress{
					ID:   nil,
					Host: "127.0.0.1",
					Port: 8080,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.entry.IsNil())
		})
	}
}

// Benchmark tests
func BenchmarkNodeAddress_Copy(b *testing.B) {
	node := NewNodeAddress(
		new(big.Int).Exp(big.NewInt(2), big.NewInt(159), nil),
		"127.0.0.1",
		8080,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node.Copy()
	}
}

func BenchmarkNodeAddress_Equals(b *testing.B) {
	node1 := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080)
	node2 := NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node1.Equals(node2)
	}
}

func BenchmarkFingerEntry_Copy(b *testing.B) {
	entry := NewFingerEntry(
		big.NewInt(100),
		NewNodeAddress(big.NewInt(42), "127.0.0.1", 8080),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.Copy()
	}
}
