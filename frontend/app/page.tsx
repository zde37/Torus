'use client';

import { useState, useEffect } from 'react';
import RingVisualization from '@/components/RingVisualization';
import NodeDetailsPanel from '@/components/NodeDetailsPanel';
import StatisticsDashboard from '@/components/StatisticsDashboard';
import DemoOperations from '@/components/DemoOperations';
import { ChordNode } from '@/types/chord';
import { getWebSocketClient } from '@/lib/websocket';
import { getAPIClient } from '@/lib/api';

export default function Home() {
  const [nodes, setNodes] = useState<ChordNode[]>([]);
  const [selectedNode, setSelectedNode] = useState<ChordNode | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string>('');

  useEffect(() => {
    const wsClient = getWebSocketClient();
    const apiClient = getAPIClient();

    // Fetch initial node data and discover full ring
    const fetchInitialData = async () => {
      try {
        // Discover all nodes in the ring
        const allNodes = await apiClient.discoverRing();

        if (allNodes.length > 0) {
          setNodes(allNodes);
          setSelectedNode(allNodes[0]); // Auto-select the first node
          console.log(`Discovered ${allNodes.length} nodes in the ring`);
        } else {
          setError('No nodes found in the ring');
        }
      } catch (error) {
        console.error('Failed to fetch initial data:', error);
        setError('Failed to connect to backend. Make sure the server is running on http://localhost:8080');
      }
    };

    fetchInitialData();

    // Connect WebSocket
    wsClient.connect().catch((error) => {
      console.error('WebSocket connection failed:', error);
    });

    // WebSocket event handlers
    wsClient.on('connected', () => {
      setIsConnected(true);
      setError('');
    });

    wsClient.on('disconnected', () => {
      setIsConnected(false);
    });

    wsClient.on('error', () => {
      setError('WebSocket connection error');
    });

    wsClient.on('ring_update', (data: any) => {
      if (data.nodes && Array.isArray(data.nodes)) {
        setNodes(data.nodes);
      }
    });

    // Cleanup
    return () => {
      wsClient.disconnect();
    };
  }, []);

  const handleNodeClick = (node: ChordNode) => {
    setSelectedNode(node);
  };

  const handleLookupSimulation = (key: string) => {
    // TODO: Implement lookup path visualization
    console.log('Simulating lookup for key:', key);
  };

  return (
    <main className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <header className="bg-gray-900 border-b border-gray-800 px-8 py-6">
        <div className="max-w-[1800px] mx-auto">
          <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-600 bg-clip-text text-transparent">
            Torus
          </h1>
          <p className="text-gray-400 mt-1">
            Interactive Chord Distributed Hash Table Visualization
          </p>
        </div>
      </header>

      {/* Error Message */}
      {error && (
        <div className="bg-red-900/30 border border-red-500/50 text-red-200 px-8 py-4 mx-8 mt-6 rounded-lg">
          <p className="font-semibold">Connection Error</p>
          <p className="text-sm mt-1">{error}</p>
        </div>
      )}

      {/* Main Content */}
      <div className="max-w-[1800px] mx-auto px-8 py-8 space-y-6">
        {/* Statistics */}
        <StatisticsDashboard nodes={nodes} isConnected={isConnected} />

        {/* Main Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Ring Visualization - Takes 2 columns */}
          <div className="lg:col-span-2 bg-gray-900 rounded-lg p-6 min-h-[800px]">
            <h2 className="text-xl font-bold mb-4">Chord Ring</h2>
            {nodes.length > 0 ? (
              <RingVisualization
                nodes={nodes}
                onNodeClick={handleNodeClick}
                selectedNode={selectedNode}
              />
            ) : (
              <div className="flex items-center justify-center h-[700px] text-gray-400">
                <p>Loading ring topology...</p>
              </div>
            )}
          </div>

          {/* Right Sidebar */}
          <div className="space-y-6">
            {/* Node Details */}
            <div className="min-h-[400px]">
              <NodeDetailsPanel node={selectedNode} />
            </div>

            {/* Demo Operations */}
            <DemoOperations
              nodes={nodes}
              onLookupSimulation={handleLookupSimulation}
            />
          </div>
        </div>
      </div>

      {/* Footer */}
      <footer className="bg-gray-900 border-t border-gray-800 px-8 py-6 mt-12">
        <div className="max-w-[1800px] mx-auto text-center text-gray-400 text-sm">
          <p>
            Built with Next.js, D3.js, and Go • View-only interactive dashboard
          </p>
          <p className="mt-1">
            Chord DHT implementation following the original MIT paper
          </p>
        </div>
      </footer>
    </main>
  );
}
