package chord

import (
	"context"
	"math/big"
	"time"
)

// RemoteClient defines the interface for making remote calls to other Chord nodes.
// This interface allows the ChordNode to make RPC calls without directly depending
// on the transport layer, avoiding circular dependencies.
type RemoteClient interface {
	// FindSuccessor calls the FindSuccessor RPC on a remote node.
	FindSuccessor(address string, id *big.Int) (*NodeAddress, error)

	// FindSuccessorWithPath calls the FindSuccessorWithPath RPC on a remote node.
	// Returns the successor and the path taken (list of nodes visited from that node onwards).
	FindSuccessorWithPath(address string, id *big.Int) (*NodeAddress, []*NodeAddress, error)

	// GetPredecessor calls the GetPredecessor RPC on a remote node.
	GetPredecessor(address string) (*NodeAddress, error)

	// Notify calls the Notify RPC on a remote node.
	Notify(address string, node *NodeAddress) error

	// GetSuccessorList calls the GetSuccessorList RPC on a remote node.
	GetSuccessorList(address string) ([]*NodeAddress, error)

	// Ping calls the Ping RPC on a remote node.
	Ping(address string, message string) (string, error)

	// ClosestPrecedingFinger calls the ClosestPrecedingFinger RPC on a remote node.
	ClosestPrecedingFinger(address string, id *big.Int) (*NodeAddress, error)

	// Get calls the Get RPC on a remote node to retrieve a value.
	Get(ctx context.Context, address string, key string) ([]byte, bool, error)

	// Set calls the Set RPC on a remote node to store a value.
	Set(ctx context.Context, address string, key string, value []byte) error

	// Delete calls the Delete RPC on a remote node to remove a value.
	Delete(ctx context.Context, address string, key string) error

	// TransferKeys calls the TransferKeys RPC on a remote node to retrieve keys in a range.
	// Returns a map of hashed keys to values for keys in range (startID, endID].
	TransferKeys(ctx context.Context, address string, startID, endID *big.Int) (map[string][]byte, error)

	// DeleteTransferredKeys calls the DeleteTransferredKeys RPC on a remote node to delete keys in a range.
	// Used after successful key transfer to clean up duplicates.
	DeleteTransferredKeys(ctx context.Context, address string, startID, endID *big.Int) error

	// SetReplica calls the SetReplica RPC on a remote node to store a replica.
	SetReplica(ctx context.Context, address string, key string, value []byte, ttl time.Duration) error

	// GetReplica calls the GetReplica RPC on a remote node to retrieve a replica.
	GetReplica(ctx context.Context, address string, key string) ([]byte, bool, error)

	// DeleteReplica calls the DeleteReplica RPC on a remote node to delete a replica.
	DeleteReplica(ctx context.Context, address string, key string) error

	// BulkStore calls the BulkStore RPC on a remote node to store multiple key-value pairs.
	// Used for efficient bulk transfer during node join/leave operations.
	BulkStore(ctx context.Context, address string, items map[string][]byte) error

	// StoreReplica stores a replica of a key-value pair on a remote node.
	// Unlike SetReplica, this stores with the original key name for replication purposes.
	StoreReplica(ctx context.Context, address string, key string, value []byte) error

	// NotifyPredecessorLeaving notifies a predecessor that we're leaving and it should update its successor.
	NotifyPredecessorLeaving(ctx context.Context, address string, newSuccessor *NodeAddress) error

	// NotifySuccessorLeaving notifies a successor that we're leaving and it should update its predecessor.
	NotifySuccessorLeaving(ctx context.Context, address string, newPredecessor *NodeAddress) error

	// NotifyNodeLeaving notifies a node in the successor list about our departure.
	NotifyNodeLeaving(ctx context.Context, address string, leavingNode *NodeAddress) error
}
