'use client';

import { useState } from 'react';
import { ChordNode } from '@/types/chord';
import { Key, Search, Trash2 } from 'lucide-react';
import { getAPIClient } from '@/lib/api';

interface DemoOperationsProps {
  nodes: ChordNode[];
  onLookupSimulation?: (key: string) => void;
  onOperationComplete?: () => void;
}

export default function DemoOperations({ nodes, onLookupSimulation, onOperationComplete }: DemoOperationsProps) {
  const [operation, setOperation] = useState<'get' | 'set' | 'delete'>('get');
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [result, setResult] = useState<string>('');
  const [loading, setLoading] = useState(false);

  const api = getAPIClient();

  const handleGet = async () => {
    if (!key) return;

    setLoading(true);
    setResult('');

    try {
      const response = await api.getKey(key);
      if (response.found) {
        // Decode base64 value
        const decodedValue = atob(response.value);
        setResult(`✓ Key found: "${decodedValue}"`);
      } else {
        setResult('✗ Key not found');
      }
    } catch (error) {
      setResult(`✗ Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSet = async () => {
    if (!key || !value) return;

    setLoading(true);
    setResult('');

    try {
      const response = await api.setKey(key, value);
      if (response.success) {
        setResult(`✓ Key stored successfully`);
        // Refresh node data after successful operation
        if (onOperationComplete) {
          onOperationComplete();
        }
      } else {
        setResult('✗ Failed to store key');
      }
    } catch (error) {
      setResult(`✗ Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async () => {
    if (!key) return;

    setLoading(true);
    setResult('');

    try {
      const response = await api.deleteKey(key);
      if (response.success) {
        setResult(`✓ Key deleted successfully`);
        // Refresh node data after successful operation
        if (onOperationComplete) {
          onOperationComplete();
        }
      } else {
        setResult('✗ Failed to delete key');
      }
    } catch (error) {
      setResult(`✗ Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSimulateLookup = () => {
    if (key && onLookupSimulation) {
      onLookupSimulation(key);
    }
  };

  const handleOperation = async () => {
    switch (operation) {
      case 'get':
        await handleGet();
        break;
      case 'set':
        await handleSet();
        break;
      case 'delete':
        await handleDelete();
        break;
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h3 className="text-base font-semibold text-gray-200 mb-1 flex items-center gap-2">
          <Key className="w-4 h-4 text-green-400" />
          DHT Operations
        </h3>
        <p className="text-xs text-gray-500">Execute live operations on the Chord DHT</p>
      </div>

      {/* Operation Selector */}
      <div className="flex gap-2">
        <button
          onClick={() => setOperation('get')}
          className={`flex-1 py-2 px-4 rounded-lg font-medium transition-colors ${
            operation === 'get'
              ? 'bg-blue-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
          }`}
        >
          Get
        </button>
        <button
          onClick={() => setOperation('set')}
          className={`flex-1 py-2 px-4 rounded-lg font-medium transition-colors ${
            operation === 'set'
              ? 'bg-green-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
          }`}
        >
          Set
        </button>
        <button
          onClick={() => setOperation('delete')}
          className={`flex-1 py-2 px-4 rounded-lg font-medium transition-colors ${
            operation === 'delete'
              ? 'bg-red-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
          }`}
        >
          Delete
        </button>
      </div>

      {/* Input Fields */}
      <div className="space-y-3">
        <div>
          <label className="text-xs text-gray-400 mb-1.5 block">Key</label>
          <input
            type="text"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder="Enter key..."
            className="w-full bg-gray-800/50 border border-gray-700 text-white px-3 py-2 text-sm rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-all"
          />
        </div>

        {operation === 'set' && (
          <div>
            <label className="text-xs text-gray-400 mb-1.5 block">Value</label>
            <input
              type="text"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder="Enter value..."
              className="w-full bg-gray-800/50 border border-gray-700 text-white px-3 py-2 text-sm rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-all"
            />
          </div>
        )}
      </div>

      {/* Action Buttons */}
      <div className="flex gap-3">
        <button
          onClick={handleOperation}
          disabled={loading || !key || (operation === 'set' && !value)}
          className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium py-2 px-4 rounded-lg transition-colors flex items-center justify-center gap-2"
        >
          {loading ? (
            'Processing...'
          ) : (
            <>
              {operation === 'get' && <Search className="w-4 h-4" />}
              {operation === 'set' && <Key className="w-4 h-4" />}
              {operation === 'delete' && <Trash2 className="w-4 h-4" />}
              Execute
            </>
          )}
        </button>

        {operation === 'get' && onLookupSimulation && (
          <button
            onClick={handleSimulateLookup}
            disabled={!key}
            className="bg-purple-600 hover:bg-purple-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium py-2 px-4 rounded-lg transition-colors"
          >
            Visualize Lookup
          </button>
        )}
      </div>

      {/* Result Display */}
      {result && (
        <div
          className={`p-4 rounded-lg font-mono text-sm ${
            result.startsWith('✓')
              ? 'bg-green-900/30 text-green-400 border border-green-500/30'
              : 'bg-red-900/30 text-red-400 border border-red-500/30'
          }`}
        >
          {result}
        </div>
      )}

      {/* Info */}
      <div className="text-xs text-gray-500 border-t border-gray-700/50 pt-3">
        <p className="flex items-center gap-1.5">
          <span className="text-green-400">●</span>
          Connected to <span className="text-gray-300 font-medium">{nodes.length}</span> node{nodes.length !== 1 ? 's' : ''}
        </p>
      </div>
    </div>
  );
}
