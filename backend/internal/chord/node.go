package chord

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zde37/torus/internal/config"
	"github.com/zde37/torus/internal/hash"
	"github.com/zde37/torus/pkg"
)

const (
	// replicationTimeout is the maximum time to wait for background replication operations
	replicationTimeout = 10 * time.Second

	// cleanupTimeout is the maximum time to wait for cleanup operations
	cleanupTimeout = 5 * time.Second

	// notificationTimeout is the maximum time to wait for node leave notifications
	notificationTimeout = 5 * time.Second

	// replicationThrottleDelay is the delay between replication batches to avoid network flooding
	replicationThrottleDelay = 10 * time.Millisecond

	// replicationBatchSize is the number of keys to replicate before applying throttle delay
	replicationBatchSize = 10

	// shortNotificationTimeout is used for less critical notifications
	shortNotificationTimeout = 2 * time.Second

	// newNodeGracePeriod is how long to wait before trusting a new node's successor list for cleanup decisions
	// Tuned to 90s to balance safety and aggressive cleanup
	newNodeGracePeriod = 90 * time.Second

	// nodeTrackingExpiry is when to remove old node tracking entries (grace period + 10min buffer)
	nodeTrackingExpiry = newNodeGracePeriod + 10*time.Minute

	// cleanupCooldownPeriod is how long to wait after successor list changes before running cleanup
	// This prevents cleanup from running before re-replication completes
	cleanupCooldownPeriod = 10 * time.Second

	// livenessCacheTTL is how long to cache node liveness results
	livenessCacheTTL = 5 * time.Second

	// livenessCacheExpiry is when to remove old liveness cache entries
	livenessCacheExpiry = 10 * time.Minute

	// cleanupRetryDelay is the delay before retrying a failed operation during cleanup
	cleanupRetryDelay = 100 * time.Millisecond

	// safetyNetBufferMultiplier is the multiplier for calculating max reasonable replica count
	// A 2x buffer accounts for transient states during stabilization
	safetyNetBufferMultiplier = 2

	// minReplicaCountThreshold is the minimum replica count before triggering safety net warnings
	minReplicaCountThreshold = 20

	// hexDisplayLength is the number of hex characters to display in logs for readability
	hexDisplayLength = 8

	// Ping message types
	pingMessageHealthCheck = "health_check"
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

	// Broadcaster for ring update events
	broadcaster RingUpdateBroadcaster

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
	shutdown   bool
	shutdownMu sync.RWMutex

	// Node tracking for grace period (to avoid cleaning up replicas from newly joined nodes)
	firstSeenNodes map[string]time.Time
	firstSeenMu    sync.Mutex

	// Replication synchronization to prevent race conditions
	replicationInProgress atomic.Bool  // tracks if replication is currently running
	lastSuccessorChange   time.Time    // timestamp of last successor list change
	lastSuccessorChangeMu sync.RWMutex // protects lastSuccessorChange

	// Old successor list for idempotent re-replication
	oldSuccessorList   []*NodeAddress // successor list before last change
	oldSuccessorListMu sync.RWMutex   // protects oldSuccessorList

	// Node liveness cache to reduce network overhead (5-second TTL)
	livenessCache   map[string]livenessCacheEntry
	livenessCacheMu sync.RWMutex
}

// livenessCacheEntry caches node liveness check results with timestamp
type livenessCacheEntry struct {
	alive     bool
	timestamp time.Time
}

// NewChordNode creates a new Chord node with the given configuration.
func NewChordNode(cfg *config.Config, logger *pkg.Logger) (*ChordNode, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	nodeID := hash.HashAddress(cfg.Host, cfg.Port)

	address := NewNodeAddress(nodeID, cfg.Host, cfg.Port, cfg.HTTPPort)

	chordStorage := NewDefaultChordStorage()

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
		firstSeenNodes:  make(map[string]time.Time),
		livenessCache:   make(map[string]livenessCacheEntry),
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

// SetBroadcaster sets the broadcaster for ring update events.
func (n *ChordNode) SetBroadcaster(broadcaster RingUpdateBroadcaster) {
	n.broadcaster = broadcaster
}

// broadcastRingUpdate broadcasts a ring update event if a broadcaster is set.
func (n *ChordNode) broadcastRingUpdate(eventType, message string) {
	if n.broadcaster == nil {
		return
	}

	event := RingUpdateEvent{
		Type:      eventType,
		NodeID:    truncateHex(n.id.Text(16), 16),
		Timestamp: time.Now().Unix(),
		Message:   message,
	}

	if err := n.broadcaster.BroadcastRingUpdate(event); err != nil {
		n.logger.Warn().Err(err).Msg("Failed to broadcast ring update")
	}
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

// updateSuccessorList updates the successor list by fetching the successor's successor list
// and merging it with our own. This is called during stabilization to maintain r successors.
func (n *ChordNode) updateSuccessorList() error {
	succ := n.successor()
	if succ == nil {
		return nil
	}

	// If successor is self, no need to update
	if succ.Equals(n.address) {
		return nil
	}

	// If we don't have a remote client, skip
	if n.remote == nil {
		return nil
	}

	// Get successor's successor list
	succList, err := n.remote.GetSuccessorList(succ.Address())
	if err != nil {
		n.logger.Debug().
			Err(err).
			Str("successor", succ.Address()).
			Msg("Failed to get successor list from successor")

		// If we can't get the list, at least verify our current list
		n.verifyAndCleanSuccessorList()
		return nil // Don't fail on RPC error
	}

	// Build new successor list:
	// [our_successor, successor's_successor_1, successor's_successor_2, ...]
	// up to SuccessorListSize entries
	newList := make([]*NodeAddress, 0, n.config.SuccessorListSize)

	// Add our immediate successor first
	newList = append(newList, succ.Copy())

	// Track when we first see our successor for grace period logic
	n.firstSeenMu.Lock()
	succAddr := succ.Address()
	if _, exists := n.firstSeenNodes[succAddr]; !exists {
		n.firstSeenNodes[succAddr] = time.Now()
		n.logger.Debug().
			Str("node", succAddr).
			Msg("Recording first seen timestamp for successor")
	}
	n.firstSeenMu.Unlock()

	// Add successors from our successor's list, avoiding duplicates and dead nodes
	for _, node := range succList {
		if len(newList) >= n.config.SuccessorListSize {
			break
		}
		// Skip if it's us or already in the list
		if node.Equals(n.address) {
			continue
		}
		isDuplicate := false
		for _, existing := range newList {
			if existing.Equals(node) {
				isDuplicate = true
				break
			}
		}
		if !isDuplicate {
			// Verify the node is alive before adding it
			if n.IsNodeAlive(node) {
				newList = append(newList, node.Copy())

				// Track when we first see this node for grace period logic
				n.firstSeenMu.Lock()
				nodeAddr := node.Address()
				if _, exists := n.firstSeenNodes[nodeAddr]; !exists {
					n.firstSeenNodes[nodeAddr] = time.Now()
					n.logger.Debug().
						Str("node", nodeAddr).
						Msg("Recording first seen timestamp for new node")
				}
				n.firstSeenMu.Unlock()

				n.logger.Debug().
					Str("node", node.Address()).
					Msg("Adding alive node to successor list")
			} else {
				n.logger.Debug().
					Str("node", node.Address()).
					Msg("Skipping dead node from successor's list")
			}
		}
	}

	// Update the successor list
	n.setSuccessorList(newList)

	n.logger.Debug().
		Int("list_size", len(newList)).
		Str("first_successor", truncateHex(succ.ID.Text(16), 8)).
		Msg("Successor list updated")

	return nil
}

// verifyAndCleanSuccessorList removes dead nodes from the successor list.
func (n *ChordNode) verifyAndCleanSuccessorList() {
	n.successorMu.Lock()
	defer n.successorMu.Unlock()

	if n.remote == nil {
		return
	}

	newList := make([]*NodeAddress, 0)
	for _, succ := range n.successorList {
		if succ == nil || succ.Equals(n.address) {
			continue
		}

		// Check if successor is alive
		if n.IsNodeAlive(succ) {
			newList = append(newList, succ)
		} else {
			n.logger.Debug().
				Str("node", succ.Address()).
				Msg("Removing dead node from successor list")
		}
	}

	// If all successors are dead, we're alone
	if len(newList) == 0 {
		newList = append(newList, n.address.Copy())
	}

	n.successorList = newList
}

// findFirstAliveSuccessor iterates through the successor list and returns the first alive successor.
// It uses Ping RPC to check if each successor is alive.
// Returns nil if no alive successor is found.
func (n *ChordNode) findFirstAliveSuccessor() *NodeAddress {
	n.successorMu.RLock()
	successors := make([]*NodeAddress, len(n.successorList))
	copy(successors, n.successorList)
	n.successorMu.RUnlock()

	// If we don't have a remote client, return the first successor
	if n.remote == nil {
		if len(successors) > 0 {
			return successors[0].Copy()
		}
		return nil
	}

	// Try each successor in order
	for _, succ := range successors {
		if succ == nil || succ.Equals(n.address) {
			continue
		}

		// Try to ping the successor
		if n.IsNodeAlive(succ) {
			n.logger.Debug().
				Str("successor", truncateHex(succ.ID.Text(16), 8)).
				Msg("Found alive successor")
			return succ.Copy()
		}

		n.logger.Debug().
			Str("successor", truncateHex(succ.ID.Text(16), 8)).
			Msg("Successor is not reachable, trying next")
	}

	n.logger.Warn().Msg("No alive successor found in successor list - setting self as successor")

	// Set ourselves as our own successor (we're alone in the ring)
	n.setSuccessor(n.address.Copy())
	n.setSuccessorList([]*NodeAddress{n.address.Copy()})
	n.setPredecessor(nil) // No predecessor when alone

	return n.address.Copy()
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
				return truncateHex(node.ID.Text(16), hexDisplayLength)
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

	n.startBackgroundTasks()

	n.logger.Info().Msg("Chord ring created successfully")

	n.broadcastRingUpdate(EventNodeJoin, "Node created new Chord ring")

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

	if n.remote == nil {
		return fmt.Errorf("remote client not set - call SetRemote() before Join()")
	}

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

				// Delete any existing REPLICA of this key after storing as PRIMARY
				// Storage keys are already in hex format, so pass directly to DeleteReplica
				// This prevents having both primary and replica of the same key
				if err := n.storage.DeleteReplica(ctx, key); err == nil {
					n.logger.Debug().
						Str("key", key).
						Msg("Deleted existing replica after storing as primary")
				}
			}

			n.logger.Info().
				Int("key_count", len(keys)).
				Msg("Stored transferred keys locally")

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

	n.broadcastRingUpdate(EventNodeJoin, "Node joined Chord ring")

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
		n.logger.Warn().
			Err(err).
			Str("successor", succ.Address()).
			Msg("Failed to contact successor, attempting to find alive successor")

		// Successor is unreachable - try to find an alive successor from successor list
		// findFirstAliveSuccessor() always returns something (alive successor or self)
		aliveSucc := n.findFirstAliveSuccessor()

		// If we're alone in the ring (successor is self), skip stabilization
		if aliveSucc.Equals(n.address) {
			n.logger.Info().Msg("We are alone in the ring, skipping stabilization")
			return nil
		}

		n.logger.Info().
			Str("old_successor", succ.Address()).
			Str("new_successor", aliveSucc.Address()).
			Msg("Replacing failed successor with alive successor from list")
		n.setSuccessor(aliveSucc)
		succ = aliveSucc

		// Try to get predecessor from new successor
		x, err = n.remote.GetPredecessor(succ.Address())
		if err != nil {
			n.logger.Debug().
				Err(err).
				Str("successor", succ.Address()).
				Msg("Failed to get predecessor from new successor")
			// Continue with stabilization anyway
		}
	}

	// If x is between (n, successor), then x should be our successor
	// But first verify that x is actually reachable
	if x != nil && hash.Between(x.ID, n.id, succ.ID) {
		if !n.IsNodeAlive(x) {
			n.logger.Warn().
				Str("potential_successor", x.Address()).
				Msg("Potential successor is unreachable, keeping current successor")
			// Don't update successor if the new one is dead
		} else {
			n.setSuccessor(x)
			succ = x
		}
	}

	// Notify successor that we might be its predecessor
	if err := n.remote.Notify(succ.Address(), n.address); err != nil {
		n.logger.Debug().
			Err(err).
			Str("successor", succ.Address()).
			Msg("Failed to notify successor")
		return nil
	}

	oldSuccessorList := n.getSuccessorList()

	// Update successor list for fault tolerance
	if err := n.updateSuccessorList(); err != nil {
		n.logger.Debug().
			Err(err).
			Msg("Failed to update successor list")
	}

	// Check if successor list has changed
	newSuccessorList := n.getSuccessorList()
	successorListChanged := n.successorListChanged(oldSuccessorList, newSuccessorList)

	if successorListChanged {
		// Save old successor list for idempotent re-replication
		n.oldSuccessorListMu.Lock()
		n.oldSuccessorList = make([]*NodeAddress, len(oldSuccessorList))
		copy(n.oldSuccessorList, oldSuccessorList)
		n.oldSuccessorListMu.Unlock()

		// Record the timestamp of this successor list change
		n.lastSuccessorChangeMu.Lock()
		n.lastSuccessorChange = time.Now()
		n.lastSuccessorChangeMu.Unlock()

		// Only trigger re-replication if we actually own primary keys
		// This prevents nodes without primary keys from creating unnecessary replicas
		keyCount, _ := n.GetKeyCount(context.Background())
		if keyCount > 0 {
			n.logger.Info().
				Int("primary_keys", int(keyCount)).
				Msg("Successor list changed and we own primary keys, triggering re-replication")

			// Re-replicate our primary keys to new successors and WAIT for completion
			// This prevents cleanup from running before replication is done
			done := make(chan struct{})
			go func() {
				n.replicateOwnedPrimaryKeys()
				close(done)
			}()

			// Wait for replication to complete with timeout
			select {
			case <-done:
				n.logger.Debug().Msg("Re-replication completed successfully")
			case <-time.After(replicationTimeout):
				n.logger.Warn().
					Dur("timeout", replicationTimeout).
					Msg("Re-replication timed out, proceeding with stabilization")
			}
		} else {
			n.logger.Info().Msg("Successor list changed but we have no primary keys to replicate")
		}
	}

	// Check if predecessor is still alive
	pred := n.getPredecessor()
	if pred != nil && !pred.Equals(n.address) {
		if !n.IsNodeAlive(pred) {
			n.logger.Warn().
				Str("predecessor", pred.Address()).
				Msg("Predecessor is unreachable, clearing it")
			n.setPredecessor(nil)
		}
	}

	n.cleanDeadNodesFromFingerTable()

	// Clean up stale replicas that we no longer should store
	// This happens when ring topology changes (nodes join/leave)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := n.CleanupStaleReplicas(cleanupCtx); err != nil {
		n.logger.Debug().
			Err(err).
			Msg("Failed to cleanup stale replicas during stabilization")
	}

	n.logger.Debug().
		Str("successor", truncateHex(succ.ID.Text(16), 8)).
		Msg("Stabilize completed")

	n.broadcastRingUpdate(EventStabilization, "Ring stabilization completed")

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
				Msg("Failed to forward FindSuccessor via RPC, trying fallback")

			// The target node might be dead. Try to find an alive successor from our list.
			// This provides better fault tolerance instead of returning a potentially dead node.
			successors := n.getSuccessorList()
			for _, s := range successors {
				if n.IsNodeAlive(s) {
					n.logger.Debug().
						Str("fallback_node", s.Address()).
						Msg("Using alive successor as fallback")
					return s.Copy(), nil
				}
			}

			// If no alive successors found, fall back to our immediate successor (better than returning a known-dead closestNode)
			return succ.Copy(), nil
		}
		return successor, nil
	}

	return closestNode.Copy(), nil
}

// FindSuccessorWithPath finds the successor of a given ID and tracks the path taken.
// This is used for visualization and debugging of the lookup process.
func (n *ChordNode) FindSuccessorWithPath(id *big.Int) (*NodeAddress, []*NodeAddress, error) {

	if id == nil {
		return nil, nil, fmt.Errorf("id cannot be nil")
	}

	// Initialize path with the current node
	path := []*NodeAddress{n.address.Copy()}

	// Normalize ID to ring
	id = hash.AddPowerOfTwo(id, 0) // This applies mod operation

	// If ID is between (n, successor], then successor is the answer
	succ := n.successor()
	if succ == nil {
		return n.address.Copy(), path, nil
	}

	if hash.InRange(id, n.id, succ.ID) {
		return succ.Copy(), path, nil
	}

	closestNode := n.closestPrecedingNode(id)

	// If closest node is self, return successor
	if closestNode.Equals(n.address) {
		return succ.Copy(), path, nil
	}

	// If we have a remote client, forward the query via RPC
	if n.remote != nil {
		// Forward to the next node and get its path
		// Note: Don't add closestNode here - it will add itself to its path
		successor, remotePath, err := n.remote.FindSuccessorWithPath(closestNode.Address(), id)
		if err != nil {
			n.logger.Debug().
				Err(err).
				Str("closest_node", closestNode.Address()).
				Msg("Failed to forward FindSuccessorWithPath via RPC, trying fallback")

			// The target node might be dead. Try to find an alive successor from our list.
			successors := n.getSuccessorList()
			for _, s := range successors {
				if n.IsNodeAlive(s) {
					n.logger.Debug().
						Str("fallback_node", s.Address()).
						Msg("Using alive successor as fallback for path visualization")
					path = append(path, s.Copy())
					return s.Copy(), path, nil
				}
			}

			// If no alive successors found, fall back to our immediate successor
			path = append(path, succ.Copy())
			return succ.Copy(), path, nil
		}

		path = append(path, remotePath...)
		return successor, path, nil
	}

	path = append(path, closestNode.Copy())
	return closestNode.Copy(), path, nil
}

// closestPrecedingNode finds the closest node that precedes the given ID.
// This is used to optimize lookups by jumping closer to the target.
// Only returns ALIVE nodes to prevent routing through dead nodes.
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
			// Verify the node is alive before returning it
			// This prevents routing through dead nodes after failures
			if n.IsNodeAlive(finger.Node) {
				return finger.Node.Copy()
			}
			// Node is dead, try the next finger entry
			n.logger.Debug().
				Str("dead_node", finger.Node.Address()).
				Msg("Skipping dead node in finger table")
		}
	}

	// No better alive node found, return self
	return n.address.Copy()
}

// OnNodeLeave performs graceful shutdown with key transfer to maintain data integrity.
// It transfers all primary and replica keys to appropriate nodes before leaving.
func (n *ChordNode) OnNodeLeave(ctx context.Context) error {
	n.shutdownMu.Lock()
	if n.shutdown {
		n.shutdownMu.Unlock()
		return nil
	}
	n.shutdownMu.Unlock()

	n.logger.Info().Msg("Starting graceful node leave with key transfer")

	// Get current ring state
	successor := n.successor()
	predecessor := n.getPredecessor()
	successorList := n.getSuccessorList()

	if successor == nil || successor.Equals(n.address) {
		// We're the only node in the ring, no transfer needed
		n.logger.Info().Msg("Single node in ring, no key transfer needed")
		return n.Shutdown()
	}

	n.logger.Info().
		Str("successor", successor.Address()).
		Msg("Transferring primary keys to successor")

	// Get all keys we're responsible for
	allData, err := n.storage.GetAll(ctx)
	if err != nil {
		n.logger.Error().Err(err).Msg("Failed to get all keys for transfer")
		// Continue with shutdown even if transfer fails
	} else {
		primaryKeys := make(map[string][]byte)
		replicaKeys := make(map[string][]byte)

		// Separate primary keys from replica keys
		for key, value := range allData {
			// Skip Chord metadata keys
			if len(key) >= len(chordKeyPrefix) && key[:len(chordKeyPrefix)] == chordKeyPrefix {
				continue
			}

			// Check if it's a replica key
			if len(key) >= len(replicaKeyPrefix) && key[:len(replicaKeyPrefix)] == replicaKeyPrefix {
				originalKey := key[len(replicaKeyPrefix):]
				replicaKeys[originalKey] = value
			} else {
				// It's a primary key (hashed)
				primaryKeys[key] = value
			}
		}

		// Transfer primary keys to successor
		if len(primaryKeys) > 0 {
			transferCtx, cancel := context.WithTimeout(ctx, replicationTimeout)
			defer cancel()

			err := n.remote.BulkStore(transferCtx, successor.Address(), primaryKeys)
			if err != nil {
				n.logger.Error().
					Err(err).
					Int("key_count", len(primaryKeys)).
					Msg("Failed to transfer primary keys to successor")
			} else {
				n.logger.Info().
					Int("key_count", len(primaryKeys)).
					Str("successor", successor.Address()).
					Msg("Successfully transferred primary keys to successor")
			}
		}

		// Transfer replica keys to appropriate nodes in successor list
		if len(replicaKeys) > 0 && len(successorList) > 0 {
			n.logger.Info().
				Int("replica_count", len(replicaKeys)).
				Msg("Transferring replica keys to successor list")

			for originalKey, value := range replicaKeys {
				// Find who should be responsible for this key
				keyID := hash.HashString(originalKey)
				responsible, err := n.FindSuccessor(keyID)
				if err != nil {
					n.logger.Debug().
						Err(err).
						Str("key", originalKey).
						Msg("Failed to find successor for replica key")
					continue
				}

				// The replica should go to one of the responsible node's successors
				// Since we're leaving, transfer to our successor if it's not the responsible node
				targetNode := successor
				if responsible.Equals(successor) && len(successorList) > 1 {
					// If successor is responsible, send replica to next in line
					targetNode = successorList[1]
				}

				transferCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
				err = n.remote.StoreReplica(transferCtx, targetNode.Address(), originalKey, value)
				cancel()

				if err != nil {
					n.logger.Debug().
						Err(err).
						Str("key", originalKey).
						Str("target", targetNode.Address()).
						Msg("Failed to transfer replica key")
				}
			}

			n.logger.Info().
				Int("replica_count", len(replicaKeys)).
				Msg("Completed replica key transfer")
		}
	}

	if predecessor != nil && n.remote != nil {
		n.logger.Info().
			Str("predecessor", predecessor.Address()).
			Str("new_successor", successor.Address()).
			Msg("Notifying predecessor to update successor")

		notifyCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
		err := n.remote.NotifyPredecessorLeaving(notifyCtx, predecessor.Address(), successor)
		cancel()

		if err != nil {
			n.logger.Warn().
				Err(err).
				Str("predecessor", predecessor.Address()).
				Msg("Failed to notify predecessor about leave")
		}
	}

	if predecessor != nil && n.remote != nil {
		n.logger.Info().
			Str("successor", successor.Address()).
			Str("new_predecessor", predecessor.Address()).
			Msg("Notifying successor to update predecessor")

		notifyCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
		err := n.remote.NotifySuccessorLeaving(notifyCtx, successor.Address(), predecessor)
		cancel()

		if err != nil {
			n.logger.Warn().
				Err(err).
				Str("successor", successor.Address()).
				Msg("Failed to notify successor about leave")
		}
	}

	for i, node := range successorList {
		if node.Equals(n.address) {
			continue
		}

		n.logger.Debug().
			Int("index", i).
			Str("node", node.Address()).
			Msg("Notifying successor list member about leave")

		notifyCtx, cancel := context.WithTimeout(ctx, shortNotificationTimeout)
		_ = n.remote.NotifyNodeLeaving(notifyCtx, node.Address(), n.address)
		cancel()
	}

	n.logger.Info().Msg("Graceful leave protocol completed, proceeding with shutdown")

	// Finally, perform the actual shutdown
	return n.Shutdown()
}

// Shutdown gracefully shuts down the node.
func (n *ChordNode) Shutdown() error {
	n.shutdownMu.Lock()
	if n.shutdown {
		n.shutdownMu.Unlock()
		return nil
	}
	n.shutdown = true
	n.shutdownMu.Unlock()

	n.logger.Info().Msg("Shutting down ChordNode")

	n.broadcastRingUpdate(EventNodeLeave, "Node leaving Chord ring")

	n.cancel()

	n.wg.Wait()

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
		// Primary node failed - try to get from replicas
		n.logger.Warn().
			Err(err).
			Str("key", key).
			Str("responsible_node", responsible.Address()).
			Msg("Primary node failed, trying replicas")

		// Replicas are stored with the hex hash key, not the original key string
		hashedKey := keyID.Text(16)

		// Get successor list to find replicas
		successors := n.getSuccessorList()
		for _, succ := range successors {
			if succ.Equals(responsible) || succ.Equals(n.address) {
				continue // Skip the failed primary and self
			}

			// Try to get replica from this successor
			replicaValue, replicaFound, replicaErr := n.remote.GetReplica(ctx, succ.Address(), hashedKey)
			if replicaErr == nil && replicaFound {
				n.logger.Info().
					Str("key", key).
					Str("replica_node", succ.Address()).
					Msg("Retrieved value from replica")
				return replicaValue, true, nil
			}
		}

		return nil, false, fmt.Errorf("remote get failed and no replicas found: %w", err)
	}

	return value, found, nil
}

// Set stores a value in the DHT.
// It hashes the key to find the responsible node and stores the value there.
func (n *ChordNode) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

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

		// Replicate to successors for fault tolerance
		// Use background context with timeout instead of request context
		// to avoid cancellation when HTTP request completes
		// IMPORTANT: Pass the hashed key (as hex string) to match how it's stored in the database
		hashedKey := keyID.Text(16)
		go func() {
			replicationCtx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
			defer cancel()
			n.replicateToSuccessors(replicationCtx, hashedKey, value, ttl)
		}()

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

		// Delete replicas from successors
		go func() {
			deletionCtx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
			defer cancel()
			n.deleteReplicasFromSuccessors(deletionCtx, key)
		}()

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

// HashKey hashes a key string to a big.Int for consistent hashing.
func (n *ChordNode) HashKey(key string) *big.Int {
	return hash.HashKey([]byte(key))
}

// SetSuccessor sets the node's immediate successor.
// Used during ring topology updates like node leave events.
func (n *ChordNode) SetSuccessor(node *NodeAddress) {
	n.setSuccessor(node)
}

// SetPredecessor sets the node's predecessor.
// Used during ring topology updates like node leave events.
func (n *ChordNode) SetPredecessor(node *NodeAddress) {
	n.setPredecessor(node)
}

// RemoveFromSuccessorList removes a node from the successor list.
// Used when a node leaves the ring gracefully.
func (n *ChordNode) RemoveFromSuccessorList(node *NodeAddress) {
	n.successorMu.Lock()
	defer n.successorMu.Unlock()

	// Filter out the leaving node from the successor list
	newList := make([]*NodeAddress, 0, len(n.successorList))
	for _, succ := range n.successorList {
		if !succ.Equals(node) {
			newList = append(newList, succ)
		}
	}
	n.successorList = newList

	n.logger.Debug().
		Str("removed_node", node.Address()).
		Int("new_list_size", len(newList)).
		Msg("Removed node from successor list")
}

// SetRaw stores a value with a raw key (without hashing).
// Used for bulk transfers during node leave operations.
func (n *ChordNode) SetRaw(ctx context.Context, key string, value []byte) error {
	return n.storage.SetRaw(ctx, key, value, 0)
}

// GetAllReplicaKeys returns all replica keys stored on this node.
// Used for testing and monitoring replica distribution.
func (n *ChordNode) GetAllReplicaKeys(ctx context.Context) ([]string, error) {
	return n.storage.GetAllReplicaKeys(ctx)
}

// CountUserKeys returns the number of user keys stored on this node.
// Excludes Chord metadata and system keys.
func (n *ChordNode) CountUserKeys(ctx context.Context) (int, error) {
	return n.storage.CountUserKeys(ctx)
}

// GetFingerTable returns a copy of the node's finger table.
func (n *ChordNode) GetFingerTable() []*FingerEntry {
	n.fingerMu.RLock()
	defer n.fingerMu.RUnlock()

	// Create a copy of the finger table
	entries := make([]*FingerEntry, len(n.fingerTable))
	for i, finger := range n.fingerTable {
		if finger != nil {
			entries[i] = finger.Copy()
		}
	}

	return entries
}

// GetKeyCount returns the number of user keys stored on this node.
// Excludes Chord metadata keys (predecessor, successors, finger table).
func (n *ChordNode) GetKeyCount(ctx context.Context) (int, error) {
	return n.storage.CountUserKeys(ctx)
}

// GetReplicaCount returns the number of replica keys stored on this node.
func (n *ChordNode) GetReplicaCount(ctx context.Context) (int, error) {
	return n.storage.CountReplicaKeys(ctx)
}

// IsNodeAlive checks if a given node is responsive.
// Returns true if the node responds to ping, false otherwise.
// Uses a 5-second cache to reduce redundant network calls.
func (n *ChordNode) IsNodeAlive(node *NodeAddress) bool {
	if node == nil {
		return false
	}

	// Self is always alive
	if node.Equals(n.address) {
		return true
	}

	// No remote client means we can't check
	if n.remote == nil {
		return false
	}

	nodeAddr := node.Address()

	// Check cache first (read lock)
	n.livenessCacheMu.RLock()
	if cached, found := n.livenessCache[nodeAddr]; found {
		age := time.Since(cached.timestamp)
		if age < livenessCacheTTL {
			// Cache hit - return cached result
			n.livenessCacheMu.RUnlock()
			return cached.alive
		}
	}
	n.livenessCacheMu.RUnlock()

	// Cache miss or expired - perform actual ping
	_, err := n.remote.Ping(nodeAddr, pingMessageHealthCheck)
	alive := err == nil

	// Update cache with on-access cleanup (write lock)
	n.livenessCacheMu.Lock()
	now := time.Now()

	// On-access cleanup: remove entries older than expiry threshold
	// This prevents unbounded cache growth in dynamic rings
	for addr, entry := range n.livenessCache {
		if now.Sub(entry.timestamp) > livenessCacheExpiry {
			delete(n.livenessCache, addr)
		}
	}

	// Add/update current entry
	n.livenessCache[nodeAddr] = livenessCacheEntry{
		alive:     alive,
		timestamp: now,
	}
	n.livenessCacheMu.Unlock()

	return alive
}

// SetReplica stores a replica of a key on this node.
// This is called by other nodes to store replicas for fault tolerance.
// Deduplication: If the replica already exists with the same value, skip storage to prevent duplicates.
func (n *ChordNode) SetReplica(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Check if replica already exists (deduplication)
	existingValue, err := n.storage.GetReplica(ctx, key)
	if err == nil && existingValue != nil {
		// Replica already exists - check if value is the same using bytes.Equal
		if bytes.Equal(existingValue, value) {
			// Exact same replica already exists, skip duplicate storage
			n.logger.Debug().
				Str("key", key).
				Msg("Replica already exists with same value, skipping duplicate")
			return nil
		}
		// Value is different, update it
		n.logger.Debug().
			Str("key", key).
			Msg("Replica exists with different value, updating")
	}

	if err := n.storage.SetReplica(ctx, key, value, ttl); err != nil {
		return fmt.Errorf("failed to set replica: %w", err)
	}

	n.logger.Debug().
		Str("key", key).
		Int("value_size", len(value)).
		Msg("Stored replica locally")

	return nil
}

// GetReplica retrieves a replica value by key.
func (n *ChordNode) GetReplica(ctx context.Context, key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("key cannot be empty")
	}

	value, err := n.storage.GetReplica(ctx, key)
	if err != nil {
		if err.Error() == "key not found" {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get replica: %w", err)
	}

	return value, true, nil
}

// DeleteReplica removes a replica of a key from this node.
func (n *ChordNode) DeleteReplica(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if err := n.storage.DeleteReplica(ctx, key); err != nil {
		// Don't error if key not found - it might have already been deleted
		if err.Error() != "key not found" {
			return fmt.Errorf("failed to delete replica: %w", err)
		}
	}

	n.logger.Debug().
		Str("key", key).
		Msg("Deleted replica locally")

	return nil
}

// replicateToSuccessors replicates a key-value pair to the first r successors.
// This provides fault tolerance - if the primary node fails, replicas can serve the data.
func (n *ChordNode) replicateToSuccessors(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if n.remote == nil {
		n.logger.Debug().Str("key", key).Msg("Remote client not available, skipping replication")
		return
	}

	successors := n.getSuccessorList()
	if len(successors) == 0 {
		n.logger.Debug().Str("key", key).Msg("No successors available for replication")
		return
	}

	// Replicate to up to r successors (skip self)
	// In a ring with N nodes, we can replicate to at most (N-1) other nodes
	maxReplicas := min(n.config.SuccessorListSize, len(successors))

	replicaCount := 0
	for _, succ := range successors {
		if succ.Equals(n.address) {
			continue
		}

		// Try to replicate to this successor
		if err := n.remote.SetReplica(ctx, succ.Address(), key, value, ttl); err != nil {
			n.logger.Warn().
				Err(err).
				Str("key", key).
				Str("successor", succ.Address()).
				Msg("Failed to replicate key to successor")
			// Continue trying other successors
		} else {
			replicaCount++
			n.logger.Debug().
				Str("key", key).
				Str("successor", succ.Address()).
				Msg("Replicated key to successor")
		}

		// Stop after replicating to maxReplicas successors
		if replicaCount >= maxReplicas {
			break
		}
	}

	if replicaCount > 0 {
		n.logger.Debug().
			Str("key", key).
			Int("replica_count", replicaCount).
			Msg("Key replication completed")
	}
}

// deleteReplicasFromSuccessors deletes replicas from successors.
func (n *ChordNode) deleteReplicasFromSuccessors(ctx context.Context, key string) {
	if n.remote == nil {
		n.logger.Debug().Str("key", key).Msg("Remote client not available, skipping replica deletion")
		return
	}

	successors := n.getSuccessorList()
	if len(successors) == 0 {
		n.logger.Debug().Str("key", key).Msg("No successors available for replica deletion")
		return
	}

	// Delete replicas from successors (skip self)
	for _, succ := range successors {
		if succ.Equals(n.address) {
			continue
		}

		// Try to delete replica from this successor
		if err := n.remote.DeleteReplica(ctx, succ.Address(), key); err != nil {
			n.logger.Warn().
				Err(err).
				Str("key", key).
				Str("successor", succ.Address()).
				Msg("Failed to delete replica from successor")
			// Continue trying other successors
		} else {
			n.logger.Debug().
				Str("key", key).
				Str("successor", succ.Address()).
				Msg("Deleted replica from successor")
		}
	}
}

// CleanupStaleReplicas removes replica keys that this node should no longer store.
// A node should only store replicas if it's in the successor list of the key's responsible node.
func (n *ChordNode) CleanupStaleReplicas(ctx context.Context) error {
	// Skip cleanup if replication is currently in progress
	if n.replicationInProgress.Load() {
		n.logger.Debug().Msg("Skipping cleanup while replication is in progress")
		return nil
	}

	// Skip cleanup if successor list changed recently (cooldown period)
	n.lastSuccessorChangeMu.RLock()
	timeSinceChange := time.Since(n.lastSuccessorChange)
	n.lastSuccessorChangeMu.RUnlock()

	if timeSinceChange < cleanupCooldownPeriod && !n.lastSuccessorChange.IsZero() {
		n.logger.Debug().
			Dur("time_since_change", timeSinceChange).
			Dur("cooldown_period", cleanupCooldownPeriod).
			Msg("Skipping cleanup during cooldown period after topology change")
		return nil
	}

	// Simple safety net: warn only if replica count is absurdly high
	replicaCount, _ := n.GetReplicaCount(context.Background())
	primaryCount, _ := n.GetKeyCount(context.Background())

	// Conservative heuristic: max expected replicas = primaryCount × replicationFactor × buffer
	// The buffer accounts for transient states during stabilization
	maxReasonable := primaryCount * n.config.SuccessorListSize * safetyNetBufferMultiplier

	if primaryCount > 0 && replicaCount > maxReasonable && replicaCount > minReplicaCountThreshold {
		n.logger.Warn().
			Int("replica_count", replicaCount).
			Int("primary_count", primaryCount).
			Int("max_reasonable", maxReasonable).
			Msg("Replica count unusually high, possible replication issue")
	}

	replicaKeys, err := n.storage.GetAllReplicaKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to get replica keys: %w", err)
	}

	if len(replicaKeys) == 0 {
		return nil // No replicas to clean up
	}

	n.logger.Debug().
		Int("replica_count", len(replicaKeys)).
		Msg("Starting cleanup of stale replicas")

	var cleanedCount int
	for _, key := range replicaKeys {
		keyID := new(big.Int)
		if _, ok := keyID.SetString(key, 16); !ok {
			n.logger.Warn().
				Str("key", key).
				Msg("Invalid hex key in replica storage, skipping")
			continue
		}

		// Find the node responsible for this key
		responsible, err := n.FindSuccessor(keyID)
		if err != nil {
			n.logger.Warn().
				Err(err).
				Str("key", key).
				Msg("Failed to find responsible node for replica, skipping")
			continue
		}

		// Check if we should be storing this replica
		shouldStore := false

		// IMPORTANT: Check grace period BEFORE self-owned replica check
		// During stabilization, FindSuccessor() might return incorrect results,
		// so we need to respect grace period even for self-owned replicas
		if n.remote != nil {
			// Track when we first see this node and check grace period
			nodeAddr := responsible.Address()
			n.firstSeenMu.Lock()
			firstSeen, exists := n.firstSeenNodes[nodeAddr]
			now := time.Now()
			if !exists {
				// First time seeing this node, record the timestamp
				firstSeen = now
				n.firstSeenNodes[nodeAddr] = firstSeen
			} else if now.Sub(firstSeen) > nodeTrackingExpiry {
				// On-access cleanup: entry is expired, remove and treat as new
				delete(n.firstSeenNodes, nodeAddr)
				firstSeen = now
				n.firstSeenNodes[nodeAddr] = firstSeen
			}
			n.firstSeenMu.Unlock()

			// Check if the node is within grace period
			nodeAge := now.Sub(firstSeen)
			if nodeAge < newNodeGracePeriod {
				// Node is too new, don't trust its successor list yet
				// Keep the replica (it will be cleaned up later if truly stale)
				n.logger.Debug().
					Str("key", key).
					Str("responsible", nodeAddr).
					Dur("node_age", nodeAge).
					Dur("grace_period", newNodeGracePeriod).
					Msg("Node within grace period, keeping replica")
				shouldStore = true
				continue // Move to next replica
			}
		}

		// Now check if we are the responsible node (after grace period check)
		// We should store a replica if:
		// We are the responsible node (shouldn't happen for replicas, but check anyway)
		if responsible.Equals(n.address) {
			// This shouldn't happen - replicas are stored on successors, not the primary
			n.logger.Warn().
				Str("key", key).
				Msg("Found replica for key we're responsible for, removing")
			shouldStore = false
		} else if n.remote != nil {
			nodeAddr := responsible.Address()

			// Node is stable (past grace period), verify replica using successor list
			// Try once with a single retry to handle transient failures
			successorList, err := n.remote.GetSuccessorList(nodeAddr)
			if err != nil {
				// Retry once after brief delay
				time.Sleep(cleanupRetryDelay)
				successorList, err = n.remote.GetSuccessorList(nodeAddr)
			}

			if err == nil {
				// Success - check if we're in the FIRST r successors
				// Replicas are only stored on the first r successors, not all successors
				replicationFactor := n.config.SuccessorListSize
				maxReplicas := min(len(successorList), replicationFactor)

				// Check if we're in positions [0, maxReplicas-1]
				myPosition := -1
				for i := 0; i < maxReplicas && i < len(successorList); i++ {
					if successorList[i].Equals(n.address) {
						shouldStore = true
						myPosition = i
						break
					}
				}

				n.logger.Debug().
					Str("key", key).
					Str("responsible", responsible.Address()).
					Int("successor_list_len", len(successorList)).
					Int("max_replicas", maxReplicas).
					Int("my_position", myPosition).
					Bool("should_store", shouldStore).
					Msg("Checked replica position in successor list")
			} else {
				// Both attempts failed - delete the replica (aggressive cleanup)
				// Coordinated replication will recreate it if truly needed
				n.logger.Debug().
					Err(err).
					Str("key", key).
					Str("responsible", responsible.Address()).
					Msg("Failed to verify replica after retry, deleting (will be recreated if needed)")
				shouldStore = false
			}
		}

		// Delete the replica if we shouldn't store it
		if !shouldStore {
			if err := n.storage.DeleteReplica(ctx, key); err != nil {
				n.logger.Warn().
					Err(err).
					Str("key", key).
					Msg("Failed to delete stale replica")
			} else {
				cleanedCount++
				n.logger.Debug().
					Str("key", key).
					Str("responsible", responsible.Address()).
					Msg("Deleted stale replica")
			}
		}
	}

	if cleanedCount > 0 {
		n.logger.Info().
			Int("cleaned_count", cleanedCount).
			Int("total_replicas", len(replicaKeys)).
			Msg("Cleaned up stale replicas")
	}

	return nil
}

// cleanDeadNodesFromFingerTable removes dead nodes from the finger table.
// This is called during stabilization to keep the finger table accurate.
// Uses deduplication to minimize network calls - checks each unique node only once.
func (n *ChordNode) cleanDeadNodesFromFingerTable() {
	n.fingerMu.Lock()
	defer n.fingerMu.Unlock()

	if n.remote == nil || n.fingerTable == nil {
		return
	}

	// Deduplicate - many finger entries point to the same node
	uniqueNodes := make(map[string]*NodeAddress)
	for i := 0; i < len(n.fingerTable); i++ {
		if n.fingerTable[i].Node == nil {
			continue
		}

		// Skip self references
		if n.fingerTable[i].Node.Equals(n.address) {
			continue
		}

		// Collect unique nodes (key is the address string)
		uniqueNodes[n.fingerTable[i].Node.Address()] = n.fingerTable[i].Node
	}

	if len(uniqueNodes) == 0 {
		return
	}

	n.logger.Debug().
		Int("total_entries", len(n.fingerTable)).
		Int("unique_nodes", len(uniqueNodes)).
		Msg("Checking finger table nodes for liveness")

	// Check only unique nodes for liveness (not all 160 entries!)
	deadNodes := make(map[string]bool)
	for addr, node := range uniqueNodes {
		if !n.IsNodeAlive(node) {
			deadNodes[addr] = true
			n.logger.Debug().
				Str("dead_node", addr).
				Msg("Found dead node in finger table")
		}
	}

	if len(deadNodes) == 0 {
		return
	}

	// Clean all finger entries pointing to dead nodes
	cleanedCount := 0
	for i := 0; i < len(n.fingerTable); i++ {
		if n.fingerTable[i].Node == nil {
			continue
		}

		// Check if this entry points to a dead node
		if deadNodes[n.fingerTable[i].Node.Address()] {
			n.logger.Debug().
				Int("finger_index", i).
				Str("dead_node", n.fingerTable[i].Node.Address()).
				Msg("Removing dead node from finger entry")

			n.fingerTable[i].Node = nil
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		n.logger.Info().
			Int("cleaned_entries", cleanedCount).
			Int("dead_nodes", len(deadNodes)).
			Int("unique_nodes_checked", len(uniqueNodes)).
			Msg("Cleaned dead nodes from finger table")

		// Trigger finger table update to find replacements
		go n.fixFingers()
	}
}

// successorListChanged compares two successor lists to detect changes.
func (n *ChordNode) successorListChanged(oldList, newList []*NodeAddress) bool {
	if len(oldList) != len(newList) {
		return true
	}

	// Check if all nodes are the same (order matters for replication)
	for i := 0; i < len(oldList); i++ {
		if oldList[i] == nil && newList[i] == nil {
			continue
		}
		if oldList[i] == nil || newList[i] == nil {
			return true
		}
		if !oldList[i].Equals(newList[i]) {
			return true
		}
	}

	return false
}

// deleteStaleReplicasFromSuccessors tells nodes that are no longer in our successor list
// to delete replicas of keys we own. This is part of idempotent re-replication.
func (n *ChordNode) deleteStaleReplicasFromSuccessors(oldSuccessors, newSuccessors []*NodeAddress) {
	if n.remote == nil {
		return
	}

	// Find successors that are in OLD list but NOT in NEW list
	staleSuccessors := make([]*NodeAddress, 0)
	for _, oldSucc := range oldSuccessors {
		found := false
		for _, newSucc := range newSuccessors {
			if oldSucc.Equals(newSucc) {
				found = true
				break
			}
		}
		if !found && !oldSucc.Equals(n.address) {
			staleSuccessors = append(staleSuccessors, oldSucc)
		}
	}

	oldAddrs := make([]string, len(oldSuccessors))
	for i, s := range oldSuccessors {
		oldAddrs[i] = s.Address()
	}
	newAddrs := make([]string, len(newSuccessors))
	for i, s := range newSuccessors {
		newAddrs[i] = s.Address()
	}
	staleAddrs := make([]string, len(staleSuccessors))
	for i, s := range staleSuccessors {
		staleAddrs[i] = s.Address()
	}
	n.logger.Info().
		Strs("old_successors", oldAddrs).
		Strs("new_successors", newAddrs).
		Strs("stale_successors", staleAddrs).
		Msg("Successor list comparison for stale deletion")

	if len(staleSuccessors) == 0 {
		n.logger.Info().Msg("No stale successors found - all old successors are still in new list")
		return
	}

	n.logger.Info().
		Int("stale_successor_count", len(staleSuccessors)).
		Msg("Deleting replicas from nodes no longer in successor list")

	// Get all primary keys we own
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	allKeys, err := n.storage.GetAll(ctx)
	if err != nil {
		n.logger.Error().Err(err).Msg("Failed to get keys for replica deletion")
		return
	}

	// For each stale successor, tell it to delete replicas of our keys
	for _, staleSucc := range staleSuccessors {
		deletedCount := 0
		for hashedKey := range allKeys {
			// Skip replica keys and chord metadata
			if len(hashedKey) >= len(replicaKeyPrefix) && hashedKey[:len(replicaKeyPrefix)] == replicaKeyPrefix {
				continue
			}
			if len(hashedKey) >= len(chordKeyPrefix) && hashedKey[:len(chordKeyPrefix)] == chordKeyPrefix {
				continue
			}

			// Convert hashed key back to key ID
			keyID := new(big.Int)
			_, success := keyID.SetString(hashedKey, 16)
			if !success {
				continue
			}

			// Verify we're still responsible for this key
			responsible, err := n.FindSuccessor(keyID)
			if err != nil || responsible == nil || !responsible.Equals(n.address) {
				continue // We're not the owner, skip
			}

			// Tell the stale successor to delete this replica
			if err := n.remote.DeleteReplica(ctx, staleSucc.Address(), hashedKey); err != nil {
				n.logger.Debug().
					Err(err).
					Str("key", truncateHex(hashedKey, hexDisplayLength)).
					Str("successor", staleSucc.Address()).
					Msg("Failed to delete replica from stale successor")
			} else {
				deletedCount++
			}
		}

		n.logger.Info().
			Int("deleted_count", deletedCount).
			Str("successor", staleSucc.Address()).
			Msg("Deleted replicas from stale successor")
	}
}

// replicateOwnedPrimaryKeys re-replicates ONLY the primary keys this node owns.
// This is more selective than replicateAllKeys and prevents duplicate replicas
// when multiple nodes' successor lists change simultaneously.
// Uses idempotent approach: deletes old replicas before creating new ones.
func (n *ChordNode) replicateOwnedPrimaryKeys() {
	// Mark replication as in progress
	n.replicationInProgress.Store(true)
	defer n.replicationInProgress.Store(false)

	ctx := context.Background()

	n.logger.Info().Msg("Starting idempotent replication of owned primary keys")

	// Delete stale replicas from old successors (idempotent approach)
	n.oldSuccessorListMu.RLock()
	oldSuccessors := make([]*NodeAddress, len(n.oldSuccessorList))
	copy(oldSuccessors, n.oldSuccessorList)
	n.oldSuccessorListMu.RUnlock()

	newSuccessors := n.getSuccessorList()

	n.logger.Info().
		Int("old_successor_count", len(oldSuccessors)).
		Int("new_successor_count", len(newSuccessors)).
		Msg("Idempotent replication: checking for stale successors")

	if len(oldSuccessors) > 0 {
		n.deleteStaleReplicasFromSuccessors(oldSuccessors, newSuccessors)
	} else {
		n.logger.Warn().Msg("No old successor list recorded - skipping stale deletion (this is expected on first re-replication)")
	}

	// Create fresh replicas on current successors
	n.logger.Debug().Msg("Creating fresh replicas on current successors")

	allKeys, err := n.storage.GetAll(ctx)
	if err != nil {
		n.logger.Error().Err(err).Msg("Failed to get all keys for re-replication")
		return
	}

	// Filter to only primary keys that we actually own
	ownedKeyCount := 0
	replicatedCount := 0

	for hashedKey, value := range allKeys {
		// Skip replica keys
		if len(hashedKey) >= len(replicaKeyPrefix) && hashedKey[:len(replicaKeyPrefix)] == replicaKeyPrefix {
			continue
		}

		// Skip Chord metadata keys
		if len(hashedKey) >= len(chordKeyPrefix) && hashedKey[:len(chordKeyPrefix)] == chordKeyPrefix {
			continue
		}

		keyID := new(big.Int)
		_, success := keyID.SetString(hashedKey, 16)
		if !success {
			continue
		}

		// Double-check that we're the responsible node for this key
		// This is crucial for coordinated replication - only the true owner replicates
		responsible, err := n.FindSuccessor(keyID)
		if err != nil || responsible == nil || !responsible.Equals(n.address) {
			// We're not the owner, skip replication
			continue
		}

		ownedKeyCount++

		// Replicate to our successors
		n.replicateToSuccessors(ctx, hashedKey, value, 0)
		replicatedCount++

		// Add small delay to avoid overwhelming the network
		if replicatedCount%replicationBatchSize == 0 {
			time.Sleep(replicationThrottleDelay)
		}
	}

	n.logger.Info().
		Int("owned_keys", ownedKeyCount).
		Int("replicated_keys", replicatedCount).
		Msg("Completed coordinated replication of owned primary keys")
}

// IsResponsibleForKey checks if this node is responsible for storing a key.
func (n *ChordNode) IsResponsibleForKey(keyID *big.Int) bool {
	pred := n.getPredecessor()
	if pred == nil {
		// We're the only node or don't know our predecessor
		return true
	}

	// We're responsible if key is in range (predecessor, us]
	return hash.InRange(keyID, pred.ID, n.id)
}
