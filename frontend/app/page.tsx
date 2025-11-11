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
  const [lookupPath, setLookupPath] = useState<ChordNode[] | null>(null);
  const [lookupResponsibleNode, setLookupResponsibleNode] = useState<ChordNode | null>(null);

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
          // Fetch full details for the first node
          await handleNodeClick(allNodes[0]);
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

  const handleNodeClick = async (node: ChordNode) => {
    // Clear any ongoing lookup animation
    setLookupPath(null);
    setLookupResponsibleNode(null);

    // Fetch additional data for the selected node
    try {
      const nodeAPI = new (await import('@/lib/api')).TorusAPI(
        `http://${node.host}:${node.httpPort || node.port}`
      );

      // Fetch node info, successors, finger table and predecessor in parallel
      const [freshNodeInfo, successors, fingerTable, predecessor] = await Promise.all([
        nodeAPI.getNodeInfo(), // Get fresh node info with updated keyCount
        nodeAPI.getSuccessors(), // Get successor list
        nodeAPI.getFingerTable(),
        nodeAPI.getPredecessor().catch(() => null) // Handle failure gracefully
      ]);

      // Update the node with fresh data in a single setState
      const enrichedNode = {
        ...freshNodeInfo, // Use fresh node info (includes updated keyCount)
        successor: successors && successors.length > 0 ? successors[0] : undefined,
        predecessor: predecessor || undefined,
        fingerTable: fingerTable.map((entry) => ({
          start: entry.start,
          node: entry.node,
        })),
      };

      setSelectedNode(enrichedNode);
    } catch (error) {
      console.error('Failed to fetch node details:', error);
      // Set node even if fetch fails
      setSelectedNode(node);
    }
  };

  const handleLookupSimulation = async (key: string) => {
    try {
      const apiClient = getAPIClient();
      const result = await apiClient.lookupPath(key);

      console.log(`Lookup path for "${key}":`, result);
      console.log(`- Hops: ${result.hops ?? 0}`); // Default to 0 if undefined (found on first node)
      console.log(`- Responsible node: ${result.responsibleNode.host}:${result.responsibleNode.httpPort}`);

      // Set the path and responsible node for visualization
      setLookupPath(result.path);
      setLookupResponsibleNode(result.responsibleNode);

      // Clear the path after animation completes (5 seconds)
      setTimeout(() => {
        setLookupPath(null);
        setLookupResponsibleNode(null);
      }, 5000);
    } catch (error) {
      console.error('Failed to get lookup path:', error);
      setError('Failed to perform lookup path visualization');
      setTimeout(() => setError(''), 3000);
    }
  };

  const handleOperationComplete = async () => {
    // Refresh the selected node to get updated keyCount
    if (selectedNode) {
      await handleNodeClick(selectedNode);
    }
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
                lookupPath={lookupPath}
                lookupResponsibleNode={lookupResponsibleNode}
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
              onOperationComplete={handleOperationComplete}
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
