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

  async getPredecessor(): Promise<ChordNode | null> {
    const response = await fetch(`${this.baseURL}/api/node/predecessor`);
    if (!response.ok) {
      throw new Error(`Failed to fetch predecessor: ${response.statusText}`);
    }
    const data = await response.json();
    return data.predecessor || null;
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

  async lookupPath(key: string): Promise<{
    key: string;
    keyHash: string;
    responsibleNode: ChordNode;
    path: ChordNode[];
    hops: number
  }> {
    const response = await fetch(`${this.baseURL}/api/lookup/${encodeURIComponent(key)}`);
    if (!response.ok) {
      throw new Error(`Failed to lookup path: ${response.statusText}`);
    }
    return await response.json();
  }

  async getFingerTable(): Promise<{ start: string; node: ChordNode; index?: number }[]> {
    const response = await fetch(`${this.baseURL}/api/node/fingers`);
    if (!response.ok) {
      throw new Error(`Failed to fetch finger table: ${response.statusText}`);
    }
    const data = await response.json();
    return data.entries || [];
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
      startNode.successors = successors || [];

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
          nextNode.successors = nextSuccessors || [];

          discovered.set(nextNode.id, nextNode);
          visited.add(nextNode.id);

          current = nextNode;
        } catch (error) {
          console.error(`Failed to query node at ${nextHost}:${nextHttpPort}:`, error);

          // Mark this failed node as visited to avoid retrying
          visited.add(nextId);

          // Try to find an alternate path using successor list
          const successors = current.successors;
          if (successors && successors.length > 1) {
            // Try the next successor in the list
            for (let i = 1; i < successors.length; i++) {
              const altSuccessor = successors[i];
              if (!visited.has(altSuccessor.id) && altSuccessor.httpPort) {
                try {
                  console.log(`Trying alternate successor: ${altSuccessor.host}:${altSuccessor.httpPort}`);
                  const altBaseURL = `http://${altSuccessor.host}:${altSuccessor.httpPort}`;
                  const altAPI = new TorusAPI(altBaseURL);

                  const altNode = await altAPI.getNodeInfo();
                  const altSuccessors = await altAPI.getSuccessors();

                  if (altSuccessors && altSuccessors.length > 0) {
                    altNode.successor = altSuccessors[0];
                  }
                  altNode.successors = altSuccessors || [];

                  discovered.set(altNode.id, altNode);
                  visited.add(altNode.id);
                  current = altNode;
                  break; // Successfully found alternate path
                } catch (altError) {
                  console.error(`Alternate successor also failed:`, altError);
                  continue; // Try next alternate
                }
              }
            }
          } else {
            // No alternates available, stop discovery
            break;
          }
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
