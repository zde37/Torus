import { ChordNode } from '@/types/chord';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';

export class TorusAPI {
  private baseURL: string;

  constructor(baseURL: string = API_BASE_URL) {
    this.baseURL = baseURL;
  }

  async getNodeInfo(): Promise<ChordNode> {
    const response = await fetch(`${this.baseURL}/api/node/info`);
    if (!response.ok) {
      throw new Error(`Failed to fetch node info: ${response.statusText}`);
    }
    const data = await response.json();
    return data.node;
  }

  async getSuccessors(): Promise<ChordNode[]> {
    const response = await fetch(`${this.baseURL}/api/node/successors`);
    if (!response.ok) {
      throw new Error(`Failed to fetch successors: ${response.statusText}`);
    }
    const data = await response.json();
    return data.successors || [];
  }

  async ping(): Promise<{ message: string; timestamp: string }> {
    const response = await fetch(`${this.baseURL}/api/node/ping`);
    if (!response.ok) {
      throw new Error(`Failed to ping: ${response.statusText}`);
    }
    return await response.json();
  }

  async getKey(key: string): Promise<{ value: string; found: boolean }> {
    const response = await fetch(`${this.baseURL}/api/keys/${encodeURIComponent(key)}`);
    if (!response.ok) {
      throw new Error(`Failed to get key: ${response.statusText}`);
    }
    return await response.json();
  }

  async setKey(key: string, value: string, ttlSeconds: number = 0): Promise<{ success: boolean }> {
    const response = await fetch(`${this.baseURL}/api/keys`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        key,
        value: btoa(value), // Base64 encode
        ttl_seconds: ttlSeconds,
      }),
    });

    if (!response.ok) {
      throw new Error(`Failed to set key: ${response.statusText}`);
    }
    return await response.json();
  }

  async deleteKey(key: string): Promise<{ success: boolean }> {
    const response = await fetch(`${this.baseURL}/api/keys/${encodeURIComponent(key)}`, {
      method: 'DELETE',
    });

    if (!response.ok) {
      throw new Error(`Failed to delete key: ${response.statusText}`);
    }
    return await response.json();
  }

  async healthCheck(): Promise<{ status: string }> {
    const response = await fetch(`${this.baseURL}/health`);
    if (!response.ok) {
      throw new Error(`Health check failed: ${response.statusText}`);
    }
    return await response.json();
  }

  async discoverRing(): Promise<ChordNode[]> {
    const discovered = new Map<string, ChordNode>();
    const visited = new Set<string>();

    try {
      // Start with the bootstrap node
      const startNode = await this.getNodeInfo();
      const successors = await this.getSuccessors();

      // Add successor info to start node
      if (successors && successors.length > 0) {
        startNode.successor = successors[0];
      }

      discovered.set(startNode.id, startNode);
      visited.add(startNode.id);

      // Walk the ring by following successors
      let current = startNode;
      const maxNodes = 100; // Safety limit
      let count = 0;

      while (count < maxNodes) {
        if (!current.successor || visited.has(current.successor.id)) {
          break; // Completed the ring
        }

        count++;
        const nextId = current.successor.id;
        const nextHost = current.successor.host;
        const nextHttpPort = current.successor.httpPort; // Use HTTP port, not gRPC port

        if (!nextHttpPort) {
          console.error(`Node ${nextId} missing httpPort field`);
          break;
        }

        try {
          // Query the next node's HTTP API
          const nextBaseURL = `http://${nextHost}:${nextHttpPort}`;
          const nextAPI = new TorusAPI(nextBaseURL);

          const nextNode = await nextAPI.getNodeInfo();
          const nextSuccessors = await nextAPI.getSuccessors();

          if (nextSuccessors && nextSuccessors.length > 0) {
            nextNode.successor = nextSuccessors[0];
          }

          discovered.set(nextNode.id, nextNode);
          visited.add(nextNode.id);

          current = nextNode;
        } catch (error) {
          console.error(`Failed to query node at ${nextHost}:${nextHttpPort}:`, error);
          break;
        }
      }

      return Array.from(discovered.values());
    } catch (error) {
      console.error('Failed to discover ring:', error);
      return [];
    }
  }
}

// Singleton instance
let apiClient: TorusAPI | null = null;

export function getAPIClient(): TorusAPI {
  if (!apiClient) {
    apiClient = new TorusAPI();
  }
  return apiClient;
}
