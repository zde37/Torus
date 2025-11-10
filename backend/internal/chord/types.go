package chord

import (
	"fmt"
	"math/big"
)

// NodeAddress represents a node in the Chord ring with its identifier and network address.
type NodeAddress struct {
	ID   *big.Int // Node identifier in the Chord ring (0 to 2^M - 1)
	Host string   // Network host (IP address or hostname)
	Port int      // Network port
}

// NewNodeAddress creates a new NodeAddress with the given parameters.
// The ID is copied to prevent external modification.
func NewNodeAddress(id *big.Int, host string, port int) *NodeAddress {
	if id == nil {
		return &NodeAddress{
			ID:   new(big.Int),
			Host: host,
			Port: port,
		}
	}
	return &NodeAddress{
		ID:   new(big.Int).Set(id),
		Host: host,
		Port: port,
	}
}

// String returns a human-readable representation of the node address.
// Format: "NodeAddress{ID: <hex>, Addr: <host>:<port>}"
func (n *NodeAddress) String() string {
	if n == nil {
		return "NodeAddress{nil}"
	}
	return fmt.Sprintf("NodeAddress{ID: %s, Addr: %s:%d}",
		n.ID.Text(16), n.Host, n.Port)
}

// Address returns the network address in "host:port" format.
func (n *NodeAddress) Address() string {
	if n == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

// Equals checks if two NodeAddress instances are equal.
// Two nodes are equal if they have the same ID, host, and port.
func (n *NodeAddress) Equals(other *NodeAddress) bool {
	if n == nil && other == nil {
		return true
	}
	if n == nil || other == nil {
		return false
	}
	if n.ID == nil && other.ID == nil {
		return n.Host == other.Host && n.Port == other.Port
	}
	if n.ID == nil || other.ID == nil {
		return false
	}
	return n.ID.Cmp(other.ID) == 0 &&
		n.Host == other.Host &&
		n.Port == other.Port
}

// Copy creates a deep copy of the NodeAddress.
// This prevents external modifications from affecting the original.
func (n *NodeAddress) Copy() *NodeAddress {
	if n == nil {
		return nil
	}
	return NewNodeAddress(n.ID, n.Host, n.Port)
}

// IsNil checks if the NodeAddress is nil or has a nil ID.
func (n *NodeAddress) IsNil() bool {
	return n == nil || n.ID == nil
}

// FingerEntry represents an entry in the Chord finger table.
// Each entry tracks the successor of (n + 2^(i-1)) mod 2^M.
type FingerEntry struct {
	Start *big.Int     // Start of the interval: (n + 2^(i-1)) mod 2^M
	Node  *NodeAddress // First node that succeeds or equals start
}

// NewFingerEntry creates a new FingerEntry with the given parameters.
// The start ID is copied to prevent external modification.
func NewFingerEntry(start *big.Int, node *NodeAddress) *FingerEntry {
	var startCopy *big.Int
	if start != nil {
		startCopy = new(big.Int).Set(start)
	}

	return &FingerEntry{
		Start: startCopy,
		Node:  node.Copy(),
	}
}

// String returns a human-readable representation of the finger entry.
func (f *FingerEntry) String() string {
	if f == nil {
		return "FingerEntry{nil}"
	}
	startStr := "<nil>"
	if f.Start != nil {
		startStr = f.Start.Text(16)
	}
	nodeStr := "<nil>"
	if f.Node != nil {
		nodeStr = f.Node.String()
	}
	return fmt.Sprintf("FingerEntry{Start: %s, Node: %s}", startStr, nodeStr)
}

// Copy creates a deep copy of the FingerEntry.
func (f *FingerEntry) Copy() *FingerEntry {
	if f == nil {
		return nil
	}
	return NewFingerEntry(f.Start, f.Node)
}

// IsNil checks if the FingerEntry is nil or has nil fields.
func (f *FingerEntry) IsNil() bool {
	return f == nil || f.Start == nil || f.Node.IsNil()
}
