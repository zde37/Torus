// Ring update event types (must match backend constants)
export const EVENT_NODE_JOIN = 'node_join';
export const EVENT_NODE_LEAVE = 'node_leave';
export const EVENT_STABILIZATION = 'stabilization';

// WebSocket event types
export const WS_EVENT_CONNECTED = 'connected';
export const WS_EVENT_DISCONNECTED = 'disconnected';
export const WS_EVENT_ERROR = 'error';
export const WS_EVENT_RING_UPDATE = 'ring_update';
