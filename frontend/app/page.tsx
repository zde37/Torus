'use client';

import { useState, useEffect } from 'react';
import RingVisualization from '@/components/RingVisualization';
import NodeDetailsPanel from '@/components/NodeDetailsPanel';
import StatisticsDashboard from '@/components/StatisticsDashboard';
import DemoOperations from '@/components/DemoOperations';
import { ChordNode } from '@/types/chord';
import { getWebSocketClient } from '@/lib/websocket';
import { getAPIClient } from '@/lib/api';
import {
  WS_EVENT_CONNECTED,
  WS_EVENT_DISCONNECTED,
  WS_EVENT_ERROR,
  EVENT_NODE_JOIN,
  EVENT_NODE_LEAVE,
  EVENT_STABILIZATION,
} from '@/lib/constants';

export default function Home() {
  const [nodes, setNodes] = useState<ChordNode[]>([]);
  const [selectedNode, setSelectedNode] = useState<ChordNode | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string>('');
  const [lookupPath, setLookupPath] = useState<ChordNode[] | null>(null);
  const [lookupResponsibleNode, setLookupResponsibleNode] = useState<ChordNode | null>(null);
  const [activeTab, setActiveTab] = useState<'details' | 'operations'>('details');

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
    wsClient.on(WS_EVENT_CONNECTED, () => {
      setIsConnected(true);
      setError('');
    });

    wsClient.on(WS_EVENT_DISCONNECTED, () => {
      setIsConnected(false);
    });

    wsClient.on(WS_EVENT_ERROR, () => {
      setError('WebSocket connection error');
    });

    // Handler for all ring update events
    const handleRingUpdate = async (data: any) => {
      console.log('Ring update received:', data);
      try {
        // Fetch updated node list from the ring
        const updatedNodes = await apiClient.discoverRing();
        setNodes(updatedNodes);

        // Re-validate selected node - if it no longer exists, clear selection
        if (selectedNode && !updatedNodes.find(n => n.id === selectedNode.id)) {
          setSelectedNode(null);
          console.log('Selected node no longer in ring, clearing selection');
        }
      } catch (error) {
        console.error('Failed to update nodes after ring_update:', error);
      }
    };

    // Listen for all ring update event types
    wsClient.on(EVENT_NODE_JOIN, handleRingUpdate);
    wsClient.on(EVENT_NODE_LEAVE, handleRingUpdate);
    wsClient.on(EVENT_STABILIZATION, handleRingUpdate);

    // Cleanup
    return () => {
      wsClient.disconnect();
    };
  }, []);

  // Polling fallback - only poll when WebSocket is disconnected
  useEffect(() => {
    if (!isConnected) {
      console.log('WebSocket disconnected, starting polling fallback');
      const apiClient = getAPIClient();

      const pollingInterval = setInterval(async () => {
        try {
          const updatedNodes = await apiClient.discoverRing();
          setNodes(updatedNodes);

          // Re-validate selected node
          if (selectedNode && !updatedNodes.find(n => n.id === selectedNode.id)) {
            setSelectedNode(null);
          }
        } catch (error) {
          console.error('Polling failed:', error);
        }
      }, 5000); // Poll every 5 seconds when disconnected

      return () => {
        console.log('WebSocket reconnected, stopping polling fallback');
        clearInterval(pollingInterval);
      };
    }
  }, [isConnected, selectedNode]);

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
        successors: successors || [], // Full successor list
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
    <main className="h-screen bg-gray-950 text-white flex flex-col">
      {/* Header with inline stats */}
      <header className="bg-gray-900/50 backdrop-blur-sm border-b border-gray-800 px-6 py-4 flex-shrink-0">
        <div className="max-w-[1900px] mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-purple-600 bg-clip-text text-transparent">
              Torus
            </h1>
            <p className="text-gray-500 text-xs mt-0.5">
              Interactive Chord DHT Visualization
            </p>
          </div>

          {/* Compact inline stats */}
          <StatisticsDashboard nodes={nodes} isConnected={isConnected} />
        </div>
      </header>

      {/* Error Message - Compact banner */}
      {error && (
        <div className="bg-red-900/20 border-l-4 border-red-500 text-red-200 px-6 py-3 text-sm flex-shrink-0 flex items-center gap-2">
          <span className="text-red-400">⚠</span>
          <span>{error}</span>
        </div>
      )}

      {/* Main Content - fills remaining space */}
      <div className="flex-1 overflow-hidden">
        <div className="h-full max-w-[1900px] mx-auto px-6 py-5">
          <div className="h-full grid grid-cols-1 lg:grid-cols-[1fr,420px] gap-5">
            {/* Ring Visualization */}
            <div className="bg-gray-900/50 backdrop-blur-sm rounded-xl border border-gray-800 p-5 h-full flex flex-col">
              <h2 className="text-lg font-semibold mb-4 text-gray-200">Chord Ring</h2>
              {nodes.length > 0 ? (
                <div className="flex-1 min-h-0">
                  <RingVisualization
                    nodes={nodes}
                    onNodeClick={handleNodeClick}
                    selectedNode={selectedNode}
                    lookupPath={lookupPath}
                    lookupResponsibleNode={lookupResponsibleNode}
                  />
                </div>
              ) : (
                <div className="flex-1 flex items-center justify-center text-gray-500">
                  <div className="text-center">
                    <div className="animate-pulse mb-2">⚡</div>
                    <p>Loading ring topology...</p>
                  </div>
                </div>
              )}
            </div>

            {/* Right Sidebar with Tabs */}
            <div className="bg-gray-900/50 backdrop-blur-sm rounded-xl border border-gray-800 h-full flex flex-col overflow-hidden">
              {/* Tab Navigation */}
              <div className="flex border-b border-gray-800 flex-shrink-0">
                <button
                  onClick={() => setActiveTab('details')}
                  className={`flex-1 py-3.5 px-4 text-sm font-medium transition-all duration-200 ${
                    activeTab === 'details'
                      ? 'text-blue-400 border-b-2 border-blue-400 bg-gray-800/50'
                      : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/30'
                  }`}
                >
                  📊 Node Details
                </button>
                <button
                  onClick={() => setActiveTab('operations')}
                  className={`flex-1 py-3.5 px-4 text-sm font-medium transition-all duration-200 ${
                    activeTab === 'operations'
                      ? 'text-green-400 border-b-2 border-green-400 bg-gray-800/50'
                      : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/30'
                  }`}
                >
                  ⚙️ Operations
                </button>
              </div>

              {/* Tab Content with smooth transition */}
              <div className="flex-1 overflow-hidden">
                <div className={`h-full transition-opacity duration-200 ${activeTab === 'details' ? 'opacity-100' : 'opacity-0 absolute'}`}>
                  {activeTab === 'details' && <NodeDetailsPanel node={selectedNode} />}
                </div>
                <div className={`h-full transition-opacity duration-200 ${activeTab === 'operations' ? 'opacity-100' : 'opacity-0 absolute'}`}>
                  {activeTab === 'operations' && (
                    <div className="h-full overflow-y-auto p-5">
                      <DemoOperations
                        nodes={nodes}
                        onLookupSimulation={handleLookupSimulation}
                        onOperationComplete={handleOperationComplete}
                      />
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Minimal Footer */}
      <footer className="bg-gray-900/30 border-t border-gray-800 px-6 py-2.5 text-center text-gray-500 text-xs flex-shrink-0">
        Built with Next.js, D3.js & Go • Chord DHT (MIT Paper)
      </footer>
    </main>
  );
}
