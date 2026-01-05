package chord

// Ring update event types
const (
	EventNodeJoin         = "node_join"
	EventNodeLeave        = "node_leave"
	EventStabilization    = "stabilization"
)

// RingUpdateBroadcaster is an interface for broadcasting ring updates.
// This allows the ChordNode to notify external systems (like WebSocket clients)
// when the ring topology changes without creating circular dependencies.
type RingUpdateBroadcaster interface {
	// BroadcastRingUpdate sends a ring update notification.
	// The update parameter can be any data structure that will be serialized and sent.
	BroadcastRingUpdate(update any) error
}

// RingUpdateEvent represents a ring topology change event.
type RingUpdateEvent struct {
	Type      string `json:"type"`       // "node_join", "node_leave", "stabilization"
	NodeID    string `json:"node_id"`    // ID of the node that triggered the event
	Timestamp int64  `json:"timestamp"`  // Unix timestamp
	Message   string `json:"message"`    // Human-readable message
}
