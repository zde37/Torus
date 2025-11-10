package chord

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/zde37/torus/internal/config"
	"github.com/zde37/torus/internal/hash"
	"github.com/zde37/torus/pkg"
)

// truncateHex safely truncates a hex string to the specified length.
func truncateHex(hexStr string, maxLen int) string {
	if len(hexStr) > maxLen {
		return hexStr[:maxLen]
	}
	return hexStr
}

// ChordNode represents a node in the Chord DHT ring.
type ChordNode struct {
	// Node identity
	id      *big.Int
	address *NodeAddress

	// Configuration
	config *config.Config

	// Storage
	storage *ChordStorage

	// Logger
	logger *pkg.Logger

	// Remote client for RPC calls to other nodes
	remote RemoteClient

	// Finger table (index 0 to M-1)
	// finger[i] points to successor of (n + 2^i) mod 2^M
	fingerTable []*FingerEntry
	fingerMu    sync.RWMutex

	// Successor list for fault tolerance
	successorList []*NodeAddress
	successorMu   sync.RWMutex

	// Predecessor
	predecessor   *NodeAddress
	predecessorMu sync.RWMutex

	// Next finger to fix (for periodic finger fixing)
	nextFingerToFix int
	nextFingerMu    sync.Mutex

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Shutdown flag
	shutdown bool
	shutdownMu sync.RWMutex
}

// NewChordNode creates a new Chord node with the given configuration.
func NewChordNode(cfg *config.Config, logger *pkg.Logger) (*ChordNode, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Compute node ID from address
	nodeID := hash.HashAddress(cfg.Host, cfg.Port)

	// Create node address
	address := NewNodeAddress(nodeID, cfg.Host, cfg.Port)

	// Create storage
	chordStorage := NewDefaultChordStorage()

	// Create context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background())

	node := &ChordNode{
		id:              nodeID,
		address:         address,
		config:          cfg,
		storage:         chordStorage,
		logger:          logger.WithFields(pkg.Fields{"node_id": truncateHex(nodeID.Text(16), 8)}),
		fingerTable:     make([]*FingerEntry, hash.M),
		successorList:   make([]*NodeAddress, 0, cfg.SuccessorListSize),
		predecessor:     nil,
		nextFingerToFix: 0,
		ctx:             ctx,
		cancel:          cancel,
		shutdown:        false,
	}

	node.logger.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Str("node_id", nodeID.Text(16)[:16]).
		Msg("ChordNode created")

	return node, nil
}

// ID returns the node's identifier.
func (n *ChordNode) ID() *big.Int {
	return new(big.Int).Set(n.id)
}

// Address returns the node's network address.
func (n *ChordNode) Address() *NodeAddress {
	return n.address.Copy()
}

// SetRemote sets the remote client for making RPC calls to other nodes.
func (n *ChordNode) SetRemote(remote RemoteClient) {
	n.remote = remote
}

// successor returns the first successor (immediate successor).
// This is equivalent to finger[0].node in standard Chord.
func (n *ChordNode) successor() *NodeAddress {
	n.successorMu.RLock()
	defer n.successorMu.RUnlock()

	if len(n.successorList) > 0 {
		return n.successorList[0].Copy()
	}
	return nil
}

// setSuccessor sets the immediate successor.
func (n *ChordNode) setSuccessor(node *NodeAddress) {
	n.successorMu.Lock()
	defer n.successorMu.Unlock()

	if node == nil {
		n.successorList = make([]*NodeAddress, 0, n.config.SuccessorListSize)
		return
	}

	// Update successor list with new successor at position 0
	newList := make([]*NodeAddress, 0, n.config.SuccessorListSize)
	newList = append(newList, node.Copy())

	// Keep remaining successors from old list (if any)
	for i := 0; i < len(n.successorList) && len(newList) < n.config.SuccessorListSize; i++ {
		if !n.successorList[i].Equals(node) {
			newList = append(newList, n.successorList[i].Copy())
		}
	}

	n.successorList = newList

	// Also update finger[0] to point to immediate successor
	n.fingerMu.Lock()
	if n.fingerTable[0] == nil {
		start := hash.AddPowerOfTwo(n.id, 0)
		n.fingerTable[0] = NewFingerEntry(start, node)
	} else {
		n.fingerTable[0].Node = node.Copy()
	}
	n.fingerMu.Unlock()
}

// getSuccessorList returns a copy of the successor list.
func (n *ChordNode) getSuccessorList() []*NodeAddress {
	n.successorMu.RLock()
	defer n.successorMu.RUnlock()

	list := make([]*NodeAddress, len(n.successorList))
	for i, node := range n.successorList {
		list[i] = node.Copy()
	}
	return list
}

// setSuccessorList updates the entire successor list.
func (n *ChordNode) setSuccessorList(list []*NodeAddress) {
	n.successorMu.Lock()
	defer n.successorMu.Unlock()

	n.successorList = make([]*NodeAddress, 0, n.config.SuccessorListSize)
	for i := 0; i < len(list) && i < n.config.SuccessorListSize; i++ {
		if list[i] != nil {
			n.successorList = append(n.successorList, list[i].Copy())
		}
	}
}

// getPredecessor returns a copy of the predecessor.
func (n *ChordNode) getPredecessor() *NodeAddress {
	n.predecessorMu.RLock()
	defer n.predecessorMu.RUnlock()

	if n.predecessor == nil {
		return nil
	}
	return n.predecessor.Copy()
}

// setPredecessor sets the predecessor.
func (n *ChordNode) setPredecessor(node *NodeAddress) {
	n.predecessorMu.Lock()
	defer n.predecessorMu.Unlock()

	if node == nil {
		n.predecessor = nil
	} else {
		n.predecessor = node.Copy()
	}

	n.logger.Debug().
		Str("predecessor_id", func() string {
			if node != nil {
				hexStr := node.ID.Text(16)
				if len(hexStr) > 8 {
					return hexStr[:8]
				}
				return hexStr
			}
			return "nil"
		}()).
		Msg("Predecessor updated")
}

// getFinger returns a copy of the finger table entry at the given index.
func (n *ChordNode) getFinger(index int) *FingerEntry {
	if index < 0 || index >= hash.M {
		return nil
	}

	n.fingerMu.RLock()
	defer n.fingerMu.RUnlock()

	if n.fingerTable[index] == nil {
		return nil
	}
	return n.fingerTable[index].Copy()
}

// setFinger sets the finger table entry at the given index.
func (n *ChordNode) setFinger(index int, entry *FingerEntry) {
	if index < 0 || index >= hash.M {
		return
	}

	n.fingerMu.Lock()
	defer n.fingerMu.Unlock()

	if entry == nil {
		n.fingerTable[index] = nil
	} else {
		n.fingerTable[index] = entry.Copy()
	}
}

// initFingerTable initializes the finger table with the node's successor.
func (n *ChordNode) initFingerTable(successor *NodeAddress) {
	n.fingerMu.Lock()
	defer n.fingerMu.Unlock()

	for i := 0; i < hash.M; i++ {
		start := hash.AddPowerOfTwo(n.id, i)
		n.fingerTable[i] = NewFingerEntry(start, successor)
	}

	n.logger.Debug().
		Int("entries", hash.M).
		Msg("Finger table initialized")
}

// Create creates a new Chord ring with this node as the only member.
func (n *ChordNode) Create() error {
	n.logger.Info().Msg("Creating new Chord ring")

	// In a new ring, this node is its own successor and has no predecessor
	n.setPredecessor(nil)
	n.setSuccessor(n.address)
	n.initFingerTable(n.address)

	// Start background tasks
	n.startBackgroundTasks()

	n.logger.Info().Msg("Chord ring created successfully")
	return nil
}

// Join joins an existing Chord ring using the given bootstrap node.
func (n *ChordNode) Join(bootstrapAddr *NodeAddress) error {
	if bootstrapAddr == nil {
		return fmt.Errorf("bootstrap address cannot be nil")
	}

	n.logger.Info().
		Str("bootstrap", bootstrapAddr.Address()).
		Msg("Joining Chord ring")

	// Check if we have a remote client for RPC calls
	if n.remote == nil {
		return fmt.Errorf("remote client not set - call SetRemote() before Join()")
	}

	// Ask the bootstrap node to find our successor
	n.logger.Debug().
		Str("node_id", truncateHex(n.id.Text(16), 8)).
		Msg("Asking bootstrap node for successor")

	successor, err := n.remote.FindSuccessor(bootstrapAddr.Address(), n.id)
	if err != nil {
		return fmt.Errorf("failed to find successor via bootstrap node: %w", err)
	}

	if successor == nil {
		return fmt.Errorf("bootstrap node returned nil successor")
	}

	n.logger.Info().
		Str("successor_id", truncateHex(successor.ID.Text(16), 8)).
		Str("successor_addr", successor.Address()).
		Msg("Found successor")

	// Set our successor (predecessor will be set via stabilization)
	n.setPredecessor(nil)
	n.setSuccessor(successor)

	// Initialize finger table with our successor
	n.initFingerTable(successor)

	// Migrate keys: Get keys from successor that are now our responsibility
	// Keys in range (successor.predecessor, n] should be transferred to us
	n.logger.Info().Msg("Requesting key transfer from successor")

	// Get the successor's predecessor to determine the range
	succPred, err := n.remote.GetPredecessor(successor.Address())
	if err != nil {
		n.logger.Warn().
			Err(err).
			Msg("Failed to get successor's predecessor for key transfer, skipping migration")
	} else {
		// Determine the start of the range
		var startID *big.Int
		if succPred != nil {
			startID = succPred.ID
		} else {
			// Successor has no predecessor (alone in ring), take all keys up to our ID
			// Use successor's ID as start to cover the full range in the ring
			// In Chord: keys in range (successor.ID, n.id] should be transferred to us
			startID = successor.ID
		}

		// Request keys in range (startID, n.id]
		ctx, cancel := context.WithTimeout(context.Background(), n.config.RPCTimeout)
		defer cancel()

		keys, err := n.remote.TransferKeys(ctx, successor.Address(), startID, n.id)
		if err != nil {
			n.logger.Error().
				Err(err).
				Msg("Failed to transfer keys from successor")
			return fmt.Errorf("key transfer failed: %w", err)
		}

		n.logger.Info().
			Int("key_count", len(keys)).
			Msg("Received keys from successor")

		// Store the transferred keys locally
		if len(keys) > 0 {
			for key, value := range keys {
				if err := n.storage.SetRaw(ctx, key, value, 0); err != nil {
					n.logger.Error().
						Err(err).
						Str("key", key).
						Msg("Failed to store transferred key")
					return fmt.Errorf("failed to store transferred key: %w", err)
				}
			}

			n.logger.Info().
				Int("key_count", len(keys)).
				Msg("Stored transferred keys locally")

			// Tell the successor to delete the transferred keys
			n.logger.Info().Msg("Requesting successor to delete transferred keys")
			if err := n.remote.DeleteTransferredKeys(ctx, successor.Address(), startID, n.id); err != nil {
				n.logger.Warn().
					Err(err).
					Msg("Failed to delete keys on successor (duplicates may exist temporarily)")
				// Don't fail the join - duplicates will be handled by stabilization
			} else {
				n.logger.Info().Msg("Key migration completed successfully")
			}
		}
	}

	// Notify our successor that we exist so it can update its predecessor
	// This is crucial for the ring to be properly connected
	n.logger.Info().Msg("Notifying successor of our presence")
	if err := n.remote.Notify(successor.Address(), n.address); err != nil {
		n.logger.Warn().
			Err(err).
			Msg("Failed to notify successor (will be fixed by stabilization)")
		// Don't fail the join - stabilization will eventually fix this
	}

	// Start background tasks (stabilization will update our position)
	n.startBackgroundTasks()

	n.logger.Info().Msg("Joined Chord ring successfully")
	return nil
}

// startBackgroundTasks starts the periodic maintenance tasks.
func (n *ChordNode) startBackgroundTasks() {
	// Start stabilization
	n.wg.Add(1)
	go n.stabilizeLoop()

	// Start fix fingers
	n.wg.Add(1)
	go n.fixFingersLoop()

	n.logger.Debug().Msg("Background tasks started")
}

// stabilizeLoop periodically runs the stabilization protocol.
func (n *ChordNode) stabilizeLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.config.StabilizeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug().Msg("Stabilize loop stopped")
			return
		case <-ticker.C:
			if err := n.stabilize(); err != nil {
				n.logger.Error().Err(err).Msg("Stabilization failed")
			}
		}
	}
}

// fixFingersLoop periodically fixes finger table entries.
func (n *ChordNode) fixFingersLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.config.FixFingersInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug().Msg("Fix fingers loop stopped")
			return
		case <-ticker.C:
			if err := n.fixFingers(); err != nil {
				n.logger.Error().Err(err).Msg("Fix fingers failed")
			}
		}
	}
}

// stabilize verifies the node's immediate successor and tells the successor about this node.
func (n *ChordNode) stabilize() error {
	succ := n.successor()
	if succ == nil {
		// No successor, nothing to stabilize
		return nil
	}

	// If successor is self, check if we have a predecessor
	if succ.Equals(n.address) {
		pred := n.getPredecessor()
		if pred == nil {
			// No predecessor either, we're truly alone in the ring
			return nil
		}
		// We have a predecessor but our successor is ourself
		// This happens when another node joins us. Update successor to predecessor.
		n.logger.Debug().
			Str("predecessor", truncateHex(pred.ID.Text(16), 8)).
			Msg("Updating successor from self to predecessor (forming ring)")
		n.setSuccessor(pred)
		succ = pred
	}

	// If we don't have a remote client, skip RPC calls
	if n.remote == nil {
		n.logger.Debug().
			Str("successor", truncateHex(succ.ID.Text(16), 8)).
			Msg("Stabilize completed (no remote client)")
		return nil
	}

	// Ask successor for its predecessor
	x, err := n.remote.GetPredecessor(succ.Address())
	if err != nil {
		n.logger.Debug().
			Err(err).
			Str("successor", succ.Address()).
			Msg("Failed to get predecessor from successor")
		return nil // Don't fail stabilization on RPC error
	}

	// If x is between (n, successor), then x should be our successor
	if x != nil && hash.Between(x.ID, n.id, succ.ID) {
		n.setSuccessor(x)
		succ = x
	}

	// Notify successor that we might be its predecessor
	if err := n.remote.Notify(succ.Address(), n.address); err != nil {
		n.logger.Debug().
			Err(err).
			Str("successor", succ.Address()).
			Msg("Failed to notify successor")
		return nil // Don't fail stabilization on RPC error
	}

	n.logger.Debug().
		Str("successor", truncateHex(succ.ID.Text(16), 8)).
		Msg("Stabilize completed")

	return nil
}

// notify handles notification from another node that it might be our predecessor.
func (n *ChordNode) notify(node *NodeAddress) {
	if node == nil {
		return
	}

	pred := n.getPredecessor()

	// If we have no predecessor, or node is between (predecessor, n),
	// then node becomes our new predecessor
	if pred == nil || hash.InRange(node.ID, pred.ID, n.id) {
		n.setPredecessor(node)

		n.logger.Debug().
			Str("new_predecessor", truncateHex(node.ID.Text(16), 8)).
			Msg("Predecessor updated via notify")
	}
}

// fixFingers refreshes a random finger table entry.
func (n *ChordNode) fixFingers() error {
	n.nextFingerMu.Lock()
	next := n.nextFingerToFix
	n.nextFingerToFix = (next + 1) % hash.M
	n.nextFingerMu.Unlock()

	// Calculate the ID we're looking for: (n + 2^next) mod 2^M
	targetID := hash.AddPowerOfTwo(n.id, next)

	// Find the successor for this finger
	finger, err := n.FindSuccessor(targetID)
	if err != nil {
		n.logger.Debug().
			Err(err).
			Int("finger_index", next).
			Msg("Failed to fix finger")
		return err
	}

	if finger != nil {
		n.setFinger(next, NewFingerEntry(targetID, finger))
	}

	return nil
}

// FindSuccessor finds the successor of a given ID.
// This is the core Chord lookup operation.
func (n *ChordNode) FindSuccessor(id *big.Int) (*NodeAddress, error) {
	if id == nil {
		return nil, fmt.Errorf("id cannot be nil")
	}

	// Normalize ID to ring
	id = hash.AddPowerOfTwo(id, 0) // This applies mod operation

	// If ID is between (n, successor], then successor is the answer
	succ := n.successor()
	if succ == nil {
		return n.address.Copy(), nil
	}

	if hash.InRange(id, n.id, succ.ID) {
		return succ.Copy(), nil
	}

	// Otherwise, forward the query to the closest preceding node
	closestNode := n.closestPrecedingNode(id)

	// If closest node is self, return successor
	if closestNode.Equals(n.address) {
		return succ.Copy(), nil
	}

	// If we have a remote client, forward the query via RPC
	if n.remote != nil {
		successor, err := n.remote.FindSuccessor(closestNode.Address(), id)
		if err != nil {
			n.logger.Debug().
				Err(err).
				Str("closest_node", closestNode.Address()).
				Msg("Failed to forward FindSuccessor via RPC")
			// Fall back to returning the closest node
			return closestNode.Copy(), nil
		}
		return successor, nil
	}

	// No remote client, return the closest node as best effort
	return closestNode.Copy(), nil
}

// closestPrecedingNode finds the closest node that precedes the given ID.
// This is used to optimize lookups by jumping closer to the target.
func (n *ChordNode) closestPrecedingNode(id *big.Int) *NodeAddress {
	n.fingerMu.RLock()
	defer n.fingerMu.RUnlock()

	// Search finger table from highest to lowest
	for i := hash.M - 1; i >= 0; i-- {
		finger := n.fingerTable[i]
		if finger == nil || finger.Node == nil {
			continue
		}

		// If finger.node is in (n, id), it's closer than us
		if hash.Between(finger.Node.ID, n.id, id) {
			return finger.Node.Copy()
		}
	}

	// No better node found, return self
	return n.address.Copy()
}

// Shutdown gracefully shuts down the node.
func (n *ChordNode) Shutdown() error {
	n.shutdownMu.Lock()
	if n.shutdown {
		n.shutdownMu.Unlock()
		return nil // Already shutdown
	}
	n.shutdown = true
	n.shutdownMu.Unlock()

	n.logger.Info().Msg("Shutting down ChordNode")

	// Cancel context to stop background tasks
	n.cancel()

	// Wait for background tasks to finish
	n.wg.Wait()

	// Close storage
	if err := n.storage.Close(); err != nil {
		n.logger.Error().Err(err).Msg("Failed to close storage")
	}

	n.logger.Info().Msg("ChordNode shutdown complete")
	return nil
}

// IsShutdown returns whether the node has been shutdown.
func (n *ChordNode) IsShutdown() bool {
	n.shutdownMu.RLock()
	defer n.shutdownMu.RUnlock()
	return n.shutdown
}

// Public methods for gRPC server

// GetPredecessor returns the node's predecessor (public version for RPC).
func (n *ChordNode) GetPredecessor() *NodeAddress {
	return n.getPredecessor()
}

// Notify handles notification from another node (public version for RPC).
func (n *ChordNode) Notify(node *NodeAddress) {
	n.notify(node)
}

// GetSuccessorList returns the successor list (public version for RPC).
func (n *ChordNode) GetSuccessorList() []*NodeAddress {
	return n.getSuccessorList()
}

// ClosestPrecedingNode finds the closest preceding node (public version for RPC).
func (n *ChordNode) ClosestPrecedingNode(id *big.Int) *NodeAddress {
	return n.closestPrecedingNode(id)
}

// TransferKeys returns all keys in the range (startID, endID] and optionally deletes them.
// This is used during node join to migrate keys to the new node.
func (n *ChordNode) TransferKeys(ctx context.Context, startID, endID *big.Int) (map[string][]byte, error) {
	if startID == nil || endID == nil {
		return nil, fmt.Errorf("start and end IDs cannot be nil")
	}

	n.logger.Info().
		Str("start_id", truncateHex(startID.Text(16), 8)).
		Str("end_id", truncateHex(endID.Text(16), 8)).
		Msg("Transferring keys in range")

	// Get keys in the specified range
	keys, err := n.storage.GetKeysInRange(ctx, startID, endID)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys in range: %w", err)
	}

	n.logger.Info().
		Int("key_count", len(keys)).
		Str("start_id", truncateHex(startID.Text(16), 8)).
		Str("end_id", truncateHex(endID.Text(16), 8)).
		Msg("Transferring keys - data will remain until explicitly deleted by caller")

	// Note: We return the keys but DON'T delete them automatically
	// The caller (joining node) should explicitly call DeleteKeysInRange
	// after successfully storing the transferred keys
	// This ensures data safety: delete only after confirming successful transfer

	return keys, nil
}

// DeleteTransferredKeys deletes keys in the range (startID, endID].
// This should be called after successfully transferring keys to avoid data duplication.
// Returns the number of keys deleted.
func (n *ChordNode) DeleteTransferredKeys(ctx context.Context, startID, endID *big.Int) (int, error) {
	if startID == nil || endID == nil {
		return 0, fmt.Errorf("start and end IDs cannot be nil")
	}

	n.logger.Info().
		Str("start_id", truncateHex(startID.Text(16), 8)).
		Str("end_id", truncateHex(endID.Text(16), 8)).
		Msg("Deleting transferred keys")

	count, err := n.storage.DeleteKeysInRange(ctx, startID, endID)
	if err != nil {
		return 0, fmt.Errorf("failed to delete keys in range: %w", err)
	}

	n.logger.Info().
		Int("key_count", count).
		Msg("Transferred keys deleted successfully")
	return count, nil
}

// Get retrieves a value from the DHT.
// It hashes the key to find the responsible node and retrieves the value.
func (n *ChordNode) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("key cannot be empty")
	}

	// Hash the key to find which node should store it
	keyID := hash.HashKey([]byte(key))

	// Find the node responsible for this key
	responsible, err := n.FindSuccessor(keyID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find successor for key: %w", err)
	}

	// If we are responsible, get from local storage
	if responsible.Equals(n.address) {
		value, err := n.storage.Get(ctx, key)
		if err != nil {
			if err.Error() == "key not found" {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("local storage get failed: %w", err)
		}
		return value, true, nil
	}

	// Remote node is responsible - forward request via RPC
	if n.remote == nil {
		n.logger.Debug().
			Str("key", key).
			Str("responsible_node", responsible.Address()).
			Msg("Remote client not available, cannot forward Get request")
		return nil, false, fmt.Errorf("remote client not set")
	}

	n.logger.Debug().
		Str("key", key).
		Str("responsible_node", responsible.Address()).
		Msg("Forwarding Get request to responsible node")

	value, found, err := n.remote.Get(ctx, responsible.Address(), key)
	if err != nil {
		return nil, false, fmt.Errorf("remote get failed: %w", err)
	}

	return value, found, nil
}

// Set stores a value in the DHT.
// It hashes the key to find the responsible node and stores the value there.
func (n *ChordNode) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Hash the key to find which node should store it
	keyID := hash.HashKey([]byte(key))

	// Find the node responsible for this key
	responsible, err := n.FindSuccessor(keyID)
	if err != nil {
		return fmt.Errorf("failed to find successor for key: %w", err)
	}

	// If we are responsible, store locally
	if responsible.Equals(n.address) {
		if err := n.storage.Set(ctx, key, value, ttl); err != nil {
			return fmt.Errorf("local storage set failed: %w", err)
		}

		n.logger.Debug().
			Str("key", key).
			Int("value_size", len(value)).
			Msg("Stored key locally")

		return nil
	}

	// Remote node is responsible - forward request via RPC
	if n.remote == nil {
		n.logger.Debug().
			Str("key", key).
			Str("responsible_node", responsible.Address()).
			Msg("Remote client not available, cannot forward Set request")
		return fmt.Errorf("remote client not set")
	}

	n.logger.Debug().
		Str("key", key).
		Str("responsible_node", responsible.Address()).
		Int("value_size", len(value)).
		Msg("Forwarding Set request to responsible node")

	if err := n.remote.Set(ctx, responsible.Address(), key, value); err != nil {
		return fmt.Errorf("remote set failed: %w", err)
	}

	return nil
}

// Delete removes a value from the DHT.
// It hashes the key to find the responsible node and deletes the value.
func (n *ChordNode) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Hash the key to find which node should store it
	keyID := hash.HashKey([]byte(key))

	// Find the node responsible for this key
	responsible, err := n.FindSuccessor(keyID)
	if err != nil {
		return fmt.Errorf("failed to find successor for key: %w", err)
	}

	// If we are responsible, delete locally
	if responsible.Equals(n.address) {
		if err := n.storage.Delete(ctx, key); err != nil {
			return fmt.Errorf("local storage delete failed: %w", err)
		}

		n.logger.Debug().
			Str("key", key).
			Msg("Deleted key locally")

		return nil
	}

	// Remote node is responsible - forward request via RPC
	if n.remote == nil {
		n.logger.Debug().
			Str("key", key).
			Str("responsible_node", responsible.Address()).
			Msg("Remote client not available, cannot forward Delete request")
		return fmt.Errorf("remote client not set")
	}

	n.logger.Debug().
		Str("key", key).
		Str("responsible_node", responsible.Address()).
		Msg("Forwarding Delete request to responsible node")

	if err := n.remote.Delete(ctx, responsible.Address(), key); err != nil {
		return fmt.Errorf("remote delete failed: %w", err)
	}

	return nil
}
