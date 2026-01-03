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

	ErrKeyNotFound = "key not found"
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
		logger:          logger,
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

	node.logger.Info("ChordNode created", pkg.Fields{
		"host":    cfg.Host,
		"port":    cfg.Port,
		"node_id": nodeID.Text(16)[:16],
	})

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
		n.logger.Warn("Failed to broadcast ring update", pkg.Fields{"error": err.Error()})
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
		n.logger.Debug("Failed to get successor list from successor", pkg.Fields{
			"error":     err.Error(),
			"successor": succ.Address(),
		})

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
		n.logger.Debug("Recording first seen timestamp for successor", pkg.Fields{
			"node": succAddr,
		})
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
					n.logger.Debug("Recording first seen timestamp for new node", pkg.Fields{
						"node": nodeAddr,
					})
				}
				n.firstSeenMu.Unlock()

				n.logger.Debug("Adding alive node to successor list", pkg.Fields{
					"node": node.Address(),
				})
			} else {
				n.logger.Debug("Skipping dead node from successor's list", pkg.Fields{
					"node": node.Address(),
				})
			}
		}
	}

	// Update the successor list
	n.setSuccessorList(newList)

	n.logger.Debug("Successor list updated", pkg.Fields{
		"list_size":       len(newList),
		"first_successor": truncateHex(succ.ID.Text(16), 8),
	})

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
			n.logger.Debug("Removing dead node from successor list", pkg.Fields{
				"node": succ.Address(),
			})
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
			n.logger.Debug("Found alive successor", pkg.Fields{
				"successor": truncateHex(succ.ID.Text(16), 8),
			})
			return succ.Copy()
		}

		n.logger.Debug("Successor is not reachable, trying next", pkg.Fields{
			"successor": truncateHex(succ.ID.Text(16), 8),
		})
	}

	n.logger.Warn("No alive successor found in successor list - setting self as successor", nil)

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

	predecessorID := "nil"
	if node != nil {
		predecessorID = truncateHex(node.ID.Text(16), hexDisplayLength)
	}
	n.logger.Debug("Predecessor updated", pkg.Fields{
		"predecessor_id": predecessorID,
	})
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

	n.logger.Debug("Finger table initialized", pkg.Fields{
		"entries": hash.M,
	})
}

// Create creates a new Chord ring with this node as the only member.
func (n *ChordNode) Create() error {
	n.logger.Info("Creating new Chord ring", nil)

	// In a new ring, this node is its own successor and has no predecessor
	n.setPredecessor(nil)
	n.setSuccessor(n.address)
	n.initFingerTable(n.address)

	n.startBackgroundTasks()

	n.logger.Info("Chord ring created successfully", nil)

	n.broadcastRingUpdate(EventNodeJoin, "Node created new Chord ring")

	return nil
}

// Join joins an existing Chord ring using the given bootstrap node.
func (n *ChordNode) Join(bootstrapAddr *NodeAddress) error {
	if bootstrapAddr == nil {
		return fmt.Errorf("bootstrap address cannot be nil")
	}

	n.logger.Info("Joining Chord ring", pkg.Fields{
		"bootstrap": bootstrapAddr.Address(),
	})

	if n.remote == nil {
		return fmt.Errorf("remote client not set - call SetRemote() before Join()")
	}

	n.logger.Debug("Asking bootstrap node for successor", pkg.Fields{
		"node_id": truncateHex(n.id.Text(16), 8),
	})

	successor, err := n.remote.FindSuccessor(bootstrapAddr.Address(), n.id)
	if err != nil {
		return fmt.Errorf("failed to find successor via bootstrap node: %w", err)
	}

	if successor == nil {
		return fmt.Errorf("bootstrap node returned nil successor")
	}

	n.logger.Info("Found successor", pkg.Fields{
		"successor_id":   truncateHex(successor.ID.Text(16), 8),
		"successor_addr": successor.Address(),
	})

	// Set our successor (predecessor will be set via stabilization)
	n.setPredecessor(nil)
	n.setSuccessor(successor)

	// Initialize finger table with our successor
	n.initFingerTable(successor)

	// Migrate keys: Get keys from successor that are now our responsibility
	// Keys in range (successor.predecessor, n] should be transferred to us
	n.logger.Info("Requesting key transfer from successor", nil)

	// Get the successor's predecessor to determine the range
	succPred, err := n.remote.GetPredecessor(successor.Address())
	if err != nil {
		n.logger.Warn("Failed to get successor's predecessor for key transfer, skipping migration", pkg.Fields{
			"error": err.Error(),
		})
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
			n.logger.Error("Failed to transfer keys from successor", pkg.Fields{
				"error": err.Error(),
			})
			return fmt.Errorf("key transfer failed: %w", err)
		}

		n.logger.Info("Received keys from successor", pkg.Fields{
			"key_count": len(keys),
		})

		// Store the transferred keys locally
		if len(keys) > 0 {
			for key, value := range keys {
				if err := n.storage.SetRaw(ctx, key, value, 0); err != nil {
					n.logger.Error("Failed to store transferred key", pkg.Fields{
						"error": err.Error(),
						"key":   key,
					})
					return fmt.Errorf("failed to store transferred key: %w", err)
				}

				// Delete any existing REPLICA of this key after storing as PRIMARY
				// Storage keys are already in hex format, so pass directly to DeleteReplica
				// This prevents having both primary and replica of the same key
				if err := n.storage.DeleteReplica(ctx, key); err == nil {
					n.logger.Debug("Deleted existing replica after storing as primary", pkg.Fields{
						"key": key,
					})
				}
			}

			n.logger.Info("Stored transferred keys locally", pkg.Fields{
				"key_count": len(keys),
			})

			n.logger.Info("Requesting successor to delete transferred keys", nil)
			if err := n.remote.DeleteTransferredKeys(ctx, successor.Address(), startID, n.id); err != nil {
				n.logger.Warn("Failed to delete keys on successor (duplicates may exist temporarily)", pkg.Fields{
					"error": err.Error(),
				})
				// Don't fail the join - duplicates will be handled by stabilization
			} else {
				n.logger.Info("Key migration completed successfully", nil)
			}
		}
	}

	// Notify our successor that we exist so it can update its predecessor
	// This is crucial for the ring to be properly connected
	n.logger.Info("Notifying successor of our presence", nil)
	if err := n.remote.Notify(successor.Address(), n.address); err != nil {
		n.logger.Warn("Failed to notify successor (will be fixed by stabilization)", pkg.Fields{
			"error": err.Error(),
		})
		// Don't fail the join - stabilization will eventually fix this
	}

	// Start background tasks (stabilization will update our position)
	n.startBackgroundTasks()

	n.logger.Info("Joined Chord ring successfully", nil)

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

	n.logger.Debug("Background tasks started", nil)
}

// stabilizeLoop periodically runs the stabilization protocol.
func (n *ChordNode) stabilizeLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.config.StabilizeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			n.logger.Debug("Stabilize loop stopped", nil)
			return
		case <-ticker.C:
			if err := n.stabilize(); err != nil {
				n.logger.Error("Stabilization failed", pkg.Fields{
					"error": err.Error(),
				})
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
			n.logger.Debug("Fix fingers loop stopped", nil)
			return
		case <-ticker.C:
			if err := n.fixFingers(); err != nil {
				n.logger.Error("Fix fingers failed", pkg.Fields{
					"error": err.Error(),
				})
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
		n.logger.Debug("Updating successor from self to predecessor (forming ring)", pkg.Fields{
			"predecessor": truncateHex(pred.ID.Text(16), 8),
		})
		n.setSuccessor(pred)
		succ = pred
	}

	// If we don't have a remote client, skip RPC calls
	if n.remote == nil {
		n.logger.Debug("Stabilize completed (no remote client)", pkg.Fields{
			"successor": truncateHex(succ.ID.Text(16), 8),
		})
		return nil
	}

	// Ask successor for its predecessor
	x, err := n.remote.GetPredecessor(succ.Address())
	if err != nil {
		n.logger.Warn("Failed to contact successor, attempting to find alive successor", pkg.Fields{
			"error":     err.Error(),
			"successor": succ.Address(),
		})

		// Successor is unreachable - try to find an alive successor from successor list
		// findFirstAliveSuccessor() always returns something (alive successor or self)
		aliveSucc := n.findFirstAliveSuccessor()

		// If we're alone in the ring (successor is self), skip stabilization
		if aliveSucc.Equals(n.address) {
			n.logger.Info("We are alone in the ring, skipping stabilization", nil)
			return nil
		}

		n.logger.Info("Replacing failed successor with alive successor from list", pkg.Fields{
			"old_successor": succ.Address(),
			"new_successor": aliveSucc.Address(),
		})
		n.setSuccessor(aliveSucc)
		succ = aliveSucc

		// Try to get predecessor from new successor
		x, err = n.remote.GetPredecessor(succ.Address())
		if err != nil {
			n.logger.Debug("Failed to get predecessor from new successor", pkg.Fields{
				"error":     err.Error(),
				"successor": succ.Address(),
			})
			// Continue with stabilization anyway
		}
	}

	// If x is between (n, successor), then x should be our successor
	// But first verify that x is actually reachable
	if x != nil && hash.Between(x.ID, n.id, succ.ID) {
		if !n.IsNodeAlive(x) {
			n.logger.Warn("Potential successor is unreachable, keeping current successor", pkg.Fields{
				"potential_successor": x.Address(),
			})
			// Don't update successor if the new one is dead
		} else {
			n.setSuccessor(x)
			succ = x
		}
	}

	// Notify successor that we might be its predecessor
	if err := n.remote.Notify(succ.Address(), n.address); err != nil {
		n.logger.Debug("Failed to notify successor", pkg.Fields{
			"error":     err.Error(),
			"successor": succ.Address(),
		})
		return nil
	}

	oldSuccessorList := n.getSuccessorList()

	// Update successor list for fault tolerance
	if err := n.updateSuccessorList(); err != nil {
		n.logger.Debug("Failed to update successor list", pkg.Fields{
			"error": err.Error(),
		})
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
			n.logger.Info("Successor list changed and we own primary keys, triggering re-replication", pkg.Fields{
				"primary_keys": int(keyCount),
			})

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
				n.logger.Debug("Re-replication completed successfully", nil)
			case <-time.After(replicationTimeout):
				n.logger.Warn("Re-replication timed out, proceeding with stabilization", pkg.Fields{
					"timeout": replicationTimeout,
				})
			}
		} else {
			n.logger.Info("Successor list changed but we have no primary keys to replicate", nil)
		}
	}

	// Check if predecessor is still alive
	pred := n.getPredecessor()
	if pred != nil && !pred.Equals(n.address) {
		if !n.IsNodeAlive(pred) {
			n.logger.Warn("Predecessor is unreachable, clearing it", pkg.Fields{
				"predecessor": pred.Address(),
			})
			n.setPredecessor(nil)
		}
	}

	n.cleanDeadNodesFromFingerTable()

	// Clean up stale replicas that we no longer should store
	// This happens when ring topology changes (nodes join/leave)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := n.CleanupStaleReplicas(cleanupCtx); err != nil {
		n.logger.Debug("Failed to cleanup stale replicas during stabilization", pkg.Fields{
			"error": err.Error(),
		})
	}

	n.logger.Debug("Stabilize completed", pkg.Fields{
		"successor": truncateHex(succ.ID.Text(16), 8),
	})

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

		n.logger.Debug("Predecessor updated via notify", pkg.Fields{
			"new_predecessor": truncateHex(node.ID.Text(16), 8),
		})
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
		n.logger.Debug("Failed to fix finger", pkg.Fields{
			"error":        err.Error(),
			"finger_index": next,
		})
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
			n.logger.Debug("Failed to forward FindSuccessor via RPC, trying fallback", pkg.Fields{
				"error":        err.Error(),
				"closest_node": closestNode.Address(),
			})

			// The target node might be dead. Try to find an alive successor from our list.
			// This provides better fault tolerance instead of returning a potentially dead node.
			successors := n.getSuccessorList()
			for _, s := range successors {
				if n.IsNodeAlive(s) {
					n.logger.Debug("Using alive successor as fallback", pkg.Fields{
						"fallback_node": s.Address(),
					})
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
			n.logger.Debug("Failed to forward FindSuccessorWithPath via RPC, trying fallback", pkg.Fields{
				"error":        err.Error(),
				"closest_node": closestNode.Address(),
			})

			// The target node might be dead. Try to find an alive successor from our list.
			successors := n.getSuccessorList()
			for _, s := range successors {
				if n.IsNodeAlive(s) {
					n.logger.Debug("Using alive successor as fallback for path visualization", pkg.Fields{
						"fallback_node": s.Address(),
					})
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
			n.logger.Debug("Skipping dead node in finger table", pkg.Fields{
				"dead_node": finger.Node.Address(),
			})
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

	n.logger.Info("Starting graceful node leave with key transfer", nil)

	// Get current ring state
	successor := n.successor()
	predecessor := n.getPredecessor()
	successorList := n.getSuccessorList()

	if successor == nil || successor.Equals(n.address) {
		// We're the only node in the ring, no transfer needed
		n.logger.Info("Single node in ring, no key transfer needed", nil)
		return n.Shutdown()
	}

	n.logger.Info("Transferring primary keys to successor", pkg.Fields{
		"successor": successor.Address(),
	})

	// Get all keys we're responsible for
	allData, err := n.storage.GetAll(ctx)
	if err != nil {
		n.logger.Error("Failed to get all keys for transfer", pkg.Fields{
			"error": err.Error(),
		})
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
				n.logger.Error("Failed to transfer primary keys to successor", pkg.Fields{
					"error":     err.Error(),
					"key_count": len(primaryKeys),
				})
			} else {
				n.logger.Info("Successfully transferred primary keys to successor", pkg.Fields{
					"key_count": len(primaryKeys),
					"successor": successor.Address(),
				})
			}
		}

		// Transfer replica keys to appropriate nodes in successor list
		if len(replicaKeys) > 0 && len(successorList) > 0 {
			n.logger.Info("Transferring replica keys to successor list", pkg.Fields{
				"replica_count": len(replicaKeys),
			})

			for originalKey, value := range replicaKeys {
				// Find who should be responsible for this key
				keyID := hash.HashString(originalKey)
				responsible, err := n.FindSuccessor(keyID)
				if err != nil {
					n.logger.Debug("Failed to find successor for replica key", pkg.Fields{
						"error": err.Error(),
						"key":   originalKey,
					})
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
					n.logger.Debug("Failed to transfer replica key", pkg.Fields{
						"error":  err.Error(),
						"key":    originalKey,
						"target": targetNode.Address(),
					})
				}
			}

			n.logger.Info("Completed replica key transfer", pkg.Fields{
				"replica_count": len(replicaKeys),
			})
		}
	}

	if predecessor != nil && n.remote != nil {
		n.logger.Info("Notifying predecessor to update successor", pkg.Fields{
			"predecessor":   predecessor.Address(),
			"new_successor": successor.Address(),
		})

		notifyCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
		err := n.remote.NotifyPredecessorLeaving(notifyCtx, predecessor.Address(), successor)
		cancel()

		if err != nil {
			n.logger.Warn("Failed to notify predecessor about leave", pkg.Fields{
				"error":       err.Error(),
				"predecessor": predecessor.Address(),
			})
		}
	}

	if predecessor != nil && n.remote != nil {
		n.logger.Info("Notifying successor to update predecessor", pkg.Fields{
			"successor":       successor.Address(),
			"new_predecessor": predecessor.Address(),
		})

		notifyCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
		err := n.remote.NotifySuccessorLeaving(notifyCtx, successor.Address(), predecessor)
		cancel()

		if err != nil {
			n.logger.Warn("Failed to notify successor about leave", pkg.Fields{
				"error":     err.Error(),
				"successor": successor.Address(),
			})
		}
	}

	for i, node := range successorList {
		if node.Equals(n.address) {
			continue
		}

		n.logger.Debug("Notifying successor list member about leave", pkg.Fields{
			"index": i,
			"node":  node.Address(),
		})

		notifyCtx, cancel := context.WithTimeout(ctx, shortNotificationTimeout)
		_ = n.remote.NotifyNodeLeaving(notifyCtx, node.Address(), n.address)
		cancel()
	}

	n.logger.Info("Graceful leave protocol completed, proceeding with shutdown", nil)

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

	n.logger.Info("Shutting down ChordNode", nil)

	n.broadcastRingUpdate(EventNodeLeave, "Node leaving Chord ring")

	n.cancel()

	n.wg.Wait()

	if err := n.storage.Close(); err != nil {
		n.logger.Error("Failed to close storage", pkg.Fields{
			"error": err.Error(),
		})
	}

	n.logger.Info("ChordNode shutdown complete", nil)
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

	n.logger.Info("Transferring keys in range", pkg.Fields{
		"start_id": truncateHex(startID.Text(16), 8),
		"end_id":   truncateHex(endID.Text(16), 8),
	})

	keys, err := n.storage.GetKeysInRange(ctx, startID, endID)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys in range: %w", err)
	}

	n.logger.Info("Transferring keys - data will remain until explicitly deleted by caller", pkg.Fields{
		"key_count": len(keys),
		"start_id":  truncateHex(startID.Text(16), 8),
		"end_id":    truncateHex(endID.Text(16), 8),
	})

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

	n.logger.Info("Deleting transferred keys", pkg.Fields{
		"start_id": truncateHex(startID.Text(16), 8),
		"end_id":   truncateHex(endID.Text(16), 8),
	})

	count, err := n.storage.DeleteKeysInRange(ctx, startID, endID)
	if err != nil {
		return 0, fmt.Errorf("failed to delete keys in range: %w", err)
	}

	n.logger.Info("Transferred keys deleted successfully", pkg.Fields{
		"key_count": count,
	})
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
			if err.Error() == ErrKeyNotFound {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("local storage get failed: %w", err)
		}
		return value, true, nil
	}

	// Remote node is responsible - forward request via RPC
	if n.remote == nil {
		n.logger.Debug("Remote client not available, cannot forward Get request", pkg.Fields{
			"key":              key,
			"responsible_node": responsible.Address(),
		})
		return nil, false, fmt.Errorf("remote client not set")
	}

	n.logger.Debug("Forwarding Get request to responsible node", pkg.Fields{
		"key":              key,
		"responsible_node": responsible.Address(),
	})

	value, found, err := n.remote.Get(ctx, responsible.Address(), key)
	if err != nil {
		// Primary node failed - try to get from replicas
		n.logger.Warn("Primary node failed, trying replicas", pkg.Fields{
			"error":            err.Error(),
			"key":              key,
			"responsible_node": responsible.Address(),
		})

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
				n.logger.Info("Retrieved value from replica", pkg.Fields{
					"key":          key,
					"replica_node": succ.Address(),
				})
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

		n.logger.Debug("Stored key locally", pkg.Fields{
			"key":        key,
			"value_size": len(value),
		})

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
		n.logger.Debug("Remote client not available, cannot forward Set request", pkg.Fields{
			"key":              key,
			"responsible_node": responsible.Address(),
		})
		return fmt.Errorf("remote client not set")
	}

	n.logger.Debug("Forwarding Set request to responsible node", pkg.Fields{
		"key":              key,
		"responsible_node": responsible.Address(),
		"value_size":       len(value),
	})

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

		n.logger.Debug("Deleted key locally", pkg.Fields{
			"key": key,
		})

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
		n.logger.Debug("Remote client not available, cannot forward Delete request", pkg.Fields{
			"key":              key,
			"responsible_node": responsible.Address(),
		})
		return fmt.Errorf("remote client not set")
	}

	n.logger.Debug("Forwarding Delete request to responsible node", pkg.Fields{
		"key":              key,
		"responsible_node": responsible.Address(),
	})

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

	n.logger.Debug("Removed node from successor list", pkg.Fields{
		"removed_node":  node.Address(),
		"new_list_size": len(newList),
	})
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
			n.logger.Debug("Replica already exists with same value, skipping duplicate", pkg.Fields{
				"key": key,
			})
			return nil
		}
		// Value is different, update it
		n.logger.Debug("Replica exists with different value, updating", pkg.Fields{
			"key": key,
		})
	}

	if err := n.storage.SetReplica(ctx, key, value, ttl); err != nil {
		return fmt.Errorf("failed to set replica: %w", err)
	}

	n.logger.Debug("Stored replica locally", pkg.Fields{
		"key":        key,
		"value_size": len(value),
	})

	return nil
}

// GetReplica retrieves a replica value by key.
func (n *ChordNode) GetReplica(ctx context.Context, key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("key cannot be empty")
	}

	value, err := n.storage.GetReplica(ctx, key)
	if err != nil {
		if err.Error() == ErrKeyNotFound {
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
		if err.Error() != ErrKeyNotFound {
			return fmt.Errorf("failed to delete replica: %w", err)
		}
	}

	n.logger.Debug("Deleted replica locally", pkg.Fields{
		"key": key,
	})

	return nil
}

// replicateToSuccessors replicates a key-value pair to the first r successors.
// This provides fault tolerance - if the primary node fails, replicas can serve the data.
func (n *ChordNode) replicateToSuccessors(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if n.remote == nil {
		n.logger.Debug("Remote client not available, skipping replication", pkg.Fields{"key": key})
		return
	}

	successors := n.getSuccessorList()
	if len(successors) == 0 {
		n.logger.Debug("No successors available for replication", pkg.Fields{"key": key})
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
			n.logger.Warn("Failed to replicate key to successor", pkg.Fields{
				"error":     err.Error(),
				"key":       key,
				"successor": succ.Address(),
			})
			// Continue trying other successors
		} else {
			replicaCount++
			n.logger.Debug("Replicated key to successor", pkg.Fields{
				"key":       key,
				"successor": succ.Address(),
			})
		}

		// Stop after replicating to maxReplicas successors
		if replicaCount >= maxReplicas {
			break
		}
	}

	if replicaCount > 0 {
		n.logger.Debug("Key replication completed", pkg.Fields{
			"key":           key,
			"replica_count": replicaCount,
		})
	}
}

// deleteReplicasFromSuccessors deletes replicas from successors.
func (n *ChordNode) deleteReplicasFromSuccessors(ctx context.Context, key string) {
	if n.remote == nil {
		n.logger.Debug("Remote client not available, skipping replica deletion", pkg.Fields{"key": key})
		return
	}

	successors := n.getSuccessorList()
	if len(successors) == 0 {
		n.logger.Debug("No successors available for replica deletion", pkg.Fields{"key": key})
		return
	}

	// Delete replicas from successors (skip self)
	for _, succ := range successors {
		if succ.Equals(n.address) {
			continue
		}

		// Try to delete replica from this successor
		if err := n.remote.DeleteReplica(ctx, succ.Address(), key); err != nil {
			n.logger.Warn("Failed to delete replica from successor", pkg.Fields{
				"error":     err.Error(),
				"key":       key,
				"successor": succ.Address(),
			})
			// Continue trying other successors
		} else {
			n.logger.Debug("Deleted replica from successor", pkg.Fields{
				"key":       key,
				"successor": succ.Address(),
			})
		}
	}
}

// CleanupStaleReplicas removes replica keys that this node should no longer store.
// A node should only store replicas if it's in the successor list of the key's responsible node.
func (n *ChordNode) CleanupStaleReplicas(ctx context.Context) error {
	// Skip cleanup if replication is currently in progress
	if n.replicationInProgress.Load() {
		n.logger.Debug("Skipping cleanup while replication is in progress", nil)
		return nil
	}

	// Skip cleanup if successor list changed recently (cooldown period)
	n.lastSuccessorChangeMu.RLock()
	timeSinceChange := time.Since(n.lastSuccessorChange)
	n.lastSuccessorChangeMu.RUnlock()

	if timeSinceChange < cleanupCooldownPeriod && !n.lastSuccessorChange.IsZero() {
		n.logger.Debug("Skipping cleanup during cooldown period after topology change", pkg.Fields{
			"time_since_change": timeSinceChange,
			"cooldown_period":   cleanupCooldownPeriod,
		})
		return nil
	}

	// Simple safety net: warn only if replica count is absurdly high
	replicaCount, _ := n.GetReplicaCount(context.Background())
	primaryCount, _ := n.GetKeyCount(context.Background())

	// Conservative heuristic: max expected replicas = primaryCount × replicationFactor × buffer
	// The buffer accounts for transient states during stabilization
	maxReasonable := primaryCount * n.config.SuccessorListSize * safetyNetBufferMultiplier

	if primaryCount > 0 && replicaCount > maxReasonable && replicaCount > minReplicaCountThreshold {
		n.logger.Warn("Replica count unusually high, possible replication issue", pkg.Fields{
			"replica_count":  replicaCount,
			"primary_count":  primaryCount,
			"max_reasonable": maxReasonable,
		})
	}

	replicaKeys, err := n.storage.GetAllReplicaKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to get replica keys: %w", err)
	}

	if len(replicaKeys) == 0 {
		return nil // No replicas to clean up
	}

	n.logger.Debug("Starting cleanup of stale replicas", pkg.Fields{
		"replica_count": len(replicaKeys),
	})

	var cleanedCount int
	for _, key := range replicaKeys {
		keyID := new(big.Int)
		if _, ok := keyID.SetString(key, 16); !ok {
			n.logger.Warn("Invalid hex key in replica storage, skipping", pkg.Fields{
				"key": key,
			})
			continue
		}

		// Find the node responsible for this key
		responsible, err := n.FindSuccessor(keyID)
		if err != nil {
			n.logger.Warn("Failed to find responsible node for replica, skipping", pkg.Fields{
				"error": err.Error(),
				"key":   key,
			})
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
				n.logger.Debug("Node within grace period, keeping replica", pkg.Fields{
					"key":          key,
					"responsible":  nodeAddr,
					"node_age":     nodeAge,
					"grace_period": newNodeGracePeriod,
				})
				shouldStore = true
				continue // Move to next replica
			}
		}

		// Now check if we are the responsible node (after grace period check)
		// We should store a replica if:
		// We are the responsible node (shouldn't happen for replicas, but check anyway)
		if responsible.Equals(n.address) {
			// This shouldn't happen - replicas are stored on successors, not the primary
			n.logger.Warn("Found replica for key we're responsible for, removing", pkg.Fields{
				"key": key,
			})
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

				n.logger.Debug("Checked replica position in successor list", pkg.Fields{
					"key":                key,
					"responsible":        responsible.Address(),
					"successor_list_len": len(successorList),
					"max_replicas":       maxReplicas,
					"my_position":        myPosition,
					"should_store":       shouldStore,
				})
			} else {
				// Both attempts failed - delete the replica (aggressive cleanup)
				// Coordinated replication will recreate it if truly needed
				n.logger.Debug("Failed to verify replica after retry, deleting (will be recreated if needed)", pkg.Fields{
					"error":       err.Error(),
					"key":         key,
					"responsible": responsible.Address(),
				})
				shouldStore = false
			}
		}

		// Delete the replica if we shouldn't store it
		if !shouldStore {
			if err := n.storage.DeleteReplica(ctx, key); err != nil {
				n.logger.Warn("Failed to delete stale replica", pkg.Fields{
					"error": err.Error(),
					"key":   key,
				})
			} else {
				cleanedCount++
				n.logger.Debug("Deleted stale replica", pkg.Fields{
					"key":         key,
					"responsible": responsible.Address(),
				})
			}
		}
	}

	if cleanedCount > 0 {
		n.logger.Info("Cleaned up stale replicas", pkg.Fields{
			"cleaned_count":  cleanedCount,
			"total_replicas": len(replicaKeys),
		})
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

	n.logger.Debug("Checking finger table nodes for liveness", pkg.Fields{
		"total_entries": len(n.fingerTable),
		"unique_nodes":  len(uniqueNodes),
	})

	// Check only unique nodes for liveness (not all 160 entries!)
	deadNodes := make(map[string]bool)
	for addr, node := range uniqueNodes {
		if !n.IsNodeAlive(node) {
			deadNodes[addr] = true
			n.logger.Debug("Found dead node in finger table", pkg.Fields{
				"dead_node": addr,
			})
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
			n.logger.Debug("Removing dead node from finger entry", pkg.Fields{
				"finger_index": i,
				"dead_node":    n.fingerTable[i].Node.Address(),
			})

			n.fingerTable[i].Node = nil
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		n.logger.Info("Cleaned dead nodes from finger table", pkg.Fields{
			"cleaned_entries":      cleanedCount,
			"dead_nodes":           len(deadNodes),
			"unique_nodes_checked": len(uniqueNodes),
		})

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
	n.logger.Info("Successor list comparison for stale deletion", pkg.Fields{
		"old_successors":   oldAddrs,
		"new_successors":   newAddrs,
		"stale_successors": staleAddrs,
	})

	if len(staleSuccessors) == 0 {
		n.logger.Info("No stale successors found - all old successors are still in new list", nil)
		return
	}

	n.logger.Info("Deleting replicas from nodes no longer in successor list", pkg.Fields{
		"stale_successor_count": len(staleSuccessors),
	})

	// Get all primary keys we own
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	allKeys, err := n.storage.GetAll(ctx)
	if err != nil {
		n.logger.Error("Failed to get keys for replica deletion", pkg.Fields{"error": err.Error()})
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
				n.logger.Debug("Failed to delete replica from stale successor", pkg.Fields{
					"error":     err.Error(),
					"key":       truncateHex(hashedKey, hexDisplayLength),
					"successor": staleSucc.Address(),
				})
			} else {
				deletedCount++
			}
		}

		n.logger.Info("Deleted replicas from stale successor", pkg.Fields{
			"deleted_count": deletedCount,
			"successor":     staleSucc.Address(),
		})
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

	n.logger.Info("Starting idempotent replication of owned primary keys", nil)

	// Delete stale replicas from old successors (idempotent approach)
	n.oldSuccessorListMu.RLock()
	oldSuccessors := make([]*NodeAddress, len(n.oldSuccessorList))
	copy(oldSuccessors, n.oldSuccessorList)
	n.oldSuccessorListMu.RUnlock()

	newSuccessors := n.getSuccessorList()

	n.logger.Info("Idempotent replication: checking for stale successors", pkg.Fields{
		"old_successor_count": len(oldSuccessors),
		"new_successor_count": len(newSuccessors),
	})

	if len(oldSuccessors) > 0 {
		n.deleteStaleReplicasFromSuccessors(oldSuccessors, newSuccessors)
	} else {
		n.logger.Warn("No old successor list recorded - skipping stale deletion (this is expected on first re-replication)", nil)
	}

	// Create fresh replicas on current successors
	n.logger.Debug("Creating fresh replicas on current successors", nil)

	allKeys, err := n.storage.GetAll(ctx)
	if err != nil {
		n.logger.Error("Failed to get all keys for re-replication", pkg.Fields{"error": err.Error()})
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

	n.logger.Info("Completed coordinated replication of owned primary keys", pkg.Fields{
		"owned_keys":      ownedKeyCount,
		"replicated_keys": replicatedCount,
	})
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
