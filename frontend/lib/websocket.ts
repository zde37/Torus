import { ChordNode, RingUpdate } from '@/types/chord';

export class ChordWebSocketClient {
  private ws: WebSocket | null = null;
  private url: string;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private reconnectDelay = 3000;
  private listeners: Map<string, Set<(data: any) => void>> = new Map();

  constructor(url?: string) {
    // Derive WebSocket URL from API base URL if not provided
    if (!url) {
      const apiBaseUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';
      // Convert http://host:port to ws://host:port/api/ws
      const wsUrl = apiBaseUrl.replace(/^http/, 'ws') + '/api/ws';
      this.url = wsUrl;
    } else {
      this.url = url;
    }
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
          console.log('WebSocket connected');
          this.emit('connected', {});
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            this.emit('message', data);

            // Emit specific event based on message type
            if (data.type) {
              this.emit(data.type, data);
            }
          } catch (error) {
            console.error('Error parsing WebSocket message:', error);
          }
        };

        this.ws.onerror = (error) => {
          console.error('WebSocket error:', error);
          this.emit('error', error);
          reject(error);
        };

        this.ws.onclose = () => {
          console.log('WebSocket disconnected');
          this.emit('disconnected', {});
          this.scheduleReconnect();
        };
      } catch (error) {
        reject(error);
      }
    });
  }

  private scheduleReconnect() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }

    this.reconnectTimeout = setTimeout(() => {
      console.log('Attempting to reconnect...');
      this.connect().catch((error) => {
        console.error('Reconnection failed:', error);
      });
    }, this.reconnectDelay);
  }

  on(event: string, callback: (data: any) => void) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(callback);
  }

  off(event: string, callback: (data: any) => void) {
    const listeners = this.listeners.get(event);
    if (listeners) {
      listeners.delete(callback);
    }
  }

  private emit(event: string, data: any) {
    const listeners = this.listeners.get(event);
    if (listeners) {
      listeners.forEach((callback) => callback(data));
    }
  }

  disconnect() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }
}

// Singleton instance
let wsClient: ChordWebSocketClient | null = null;

export function getWebSocketClient(): ChordWebSocketClient {
  if (!wsClient) {
    wsClient = new ChordWebSocketClient();
  }
  return wsClient;
}
