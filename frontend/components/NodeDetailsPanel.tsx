'use client';

import { ChordNode } from '@/types/chord';
import { Server, ArrowRight, ArrowLeft, Hash } from 'lucide-react';

interface NodeDetailsPanelProps {
  node: ChordNode | null;
}

export default function NodeDetailsPanel({ node }: NodeDetailsPanelProps) {
  if (!node) {
    return (
      <div className="bg-gray-800 rounded-lg p-6 h-full flex items-center justify-center">
        <p className="text-gray-400">Select a node to view details</p>
      </div>
    );
  }

  const formatId = (id: string) => {
    try {
      const hex = Array.from(atob(id))
        .map((char) => char.charCodeAt(0).toString(16).padStart(2, '0'))
        .join('');
      return hex.substring(0, 16) + '...';
    } catch {
      return id.substring(0, 16) + '...';
    }
  };

  return (
    <div className="bg-gray-800 rounded-lg p-6 space-y-6 h-full overflow-y-auto">
      <div>
        <h2 className="text-2xl font-bold text-white mb-4 flex items-center gap-2">
          <Server className="w-6 h-6" />
          Node Details
        </h2>
      </div>

      {/* Node Info */}
      <div className="space-y-3">
        <div>
          <label className="text-sm text-gray-400 flex items-center gap-2">
            <Hash className="w-4 h-4" />
            Node ID
          </label>
          <p className="mt-1 text-white font-mono text-sm bg-gray-900 p-2 rounded">
            {formatId(node.id)}
          </p>
        </div>

        <div>
          <label className="text-sm text-gray-400">Address</label>
          <p className="mt-1 text-white font-mono text-sm">
            {node.host}:{node.port}
          </p>
        </div>

        {node.keyCount !== undefined && (
          <div>
            <label className="text-sm text-gray-400">Stored Keys</label>
            <p className="mt-1 text-white text-2xl font-bold">{node.keyCount}</p>
          </div>
        )}
      </div>

      {/* Successor */}
      {node.successor && (
        <div className="border-t border-gray-700 pt-4">
          <label className="text-sm text-gray-400 flex items-center gap-2">
            <ArrowRight className="w-4 h-4 text-green-400" />
            Successor
          </label>
          <div className="mt-2 bg-gray-900 p-3 rounded">
            <p className="text-white font-mono text-xs">
              {formatId(node.successor.id)}
            </p>
            <p className="text-gray-400 text-xs mt-1">
              {node.successor.host}:{node.successor.port}
            </p>
          </div>
        </div>
      )}

      {/* Predecessor */}
      {node.predecessor && (
        <div className="border-t border-gray-700 pt-4">
          <label className="text-sm text-gray-400 flex items-center gap-2">
            <ArrowLeft className="w-4 h-4 text-blue-400" />
            Predecessor
          </label>
          <div className="mt-2 bg-gray-900 p-3 rounded">
            <p className="text-white font-mono text-xs">
              {formatId(node.predecessor.id)}
            </p>
            <p className="text-gray-400 text-xs mt-1">
              {node.predecessor.host}:{node.predecessor.port}
            </p>
          </div>
        </div>
      )}

      {/* Finger Table */}
      {node.fingerTable && node.fingerTable.length > 0 && (
        <div className="border-t border-gray-700 pt-4">
          <label className="text-sm text-gray-400 mb-2 block">
            Finger Table ({node.fingerTable.length} total, showing diverse samples)
          </label>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {/* Show entries at exponential intervals to see variety */}
            {[0, 1, 2, 3, 4, 5, 10, 20, 40, 80, 159].map((index) => {
              if (!node.fingerTable || index >= node.fingerTable.length) return null;
              const entry = node.fingerTable[index];
              if (!entry) return null;
              return (
                <div
                  key={index}
                  className="bg-gray-900 p-2 rounded text-xs"
                >
                  <div className="flex justify-between items-center">
                    <span className="text-gray-400">finger[{index}]</span>
                    {entry.node && (
                      <span className="text-white font-mono">
                        {formatId(entry.node.id)}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
