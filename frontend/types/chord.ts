// Chord node representation
export interface ChordNode {
  id: string; // Base64 encoded node ID
  host: string;
  port: number; // gRPC port
  httpPort?: number; // HTTP API port
  successor?: ChordNode;
  successors?: ChordNode[]; // Full successor list (up to r successors)
  predecessor?: ChordNode;
  fingerTable?: FingerEntry[];
  keyCount?: number;
  replicaCount?: number;
}

// Finger table entry
export interface FingerEntry {
  start: string; // Base64 encoded
  node: ChordNode | null;
}

// Ring update message from WebSocket
export interface RingUpdate {
  type: 'ring_update' | 'node_joined' | 'node_left';
  nodes: ChordNode[];
  timestamp: number;
}

// Lookup path for visualization
export interface LookupPath {
  key: string;
  keyHash: string;
  path: ChordNode[];
  hops: number;
  finalNode: ChordNode;
}

// Statistics
export interface RingStatistics {
  totalNodes: number;
  averageHops: number;
  ringBalance: number;
  stabilizationEvents: number;
}
