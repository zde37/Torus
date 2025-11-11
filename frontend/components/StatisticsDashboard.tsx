'use client';

import { Activity, Users, TrendingUp, Zap } from 'lucide-react';
import { ChordNode } from '@/types/chord';

interface StatisticsDashboardProps {
  nodes: ChordNode[];
  isConnected: boolean;
}

export default function StatisticsDashboard({ nodes, isConnected }: StatisticsDashboardProps) {
  // const totalKeys = nodes.reduce((sum, node) => sum + (node.keyCount || 0), 0);

  // Calculate average hops (simplified estimation)
  const averageHops = nodes.length > 0 ? Math.log2(nodes.length).toFixed(2) : '0';

  const stats = [
    {
      label: 'Total Nodes',
      value: nodes.length.toString(),
      icon: Users,
      color: 'text-blue-400',
      bgColor: 'bg-blue-400/10',
    },
    {
      label: 'Connection',
      value: isConnected ? 'Connected' : 'Disconnected',
      icon: Activity,
      color: isConnected ? 'text-green-400' : 'text-red-400',
      bgColor: isConnected ? 'bg-green-400/10' : 'bg-red-400/10',
    },
    {
      label: 'Avg Hops (O(log N))',
      value: averageHops,
      icon: TrendingUp,
      color: 'text-purple-400',
      bgColor: 'bg-purple-400/10',
    },
    // {
    //   label: 'Total Keys',
    //   value: totalKeys.toString(),
    //   icon: Zap,
    //   color: 'text-yellow-400',
    //   bgColor: 'bg-yellow-400/10',
    // },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {stats.map((stat, index) => (
        <div
          key={index}
          className="bg-gray-800 rounded-lg p-6 flex items-center gap-4"
        >
          <div className={`${stat.bgColor} ${stat.color} p-3 rounded-lg`}>
            <stat.icon className="w-6 h-6" />
          </div>
          <div>
            <p className="text-gray-400 text-sm">{stat.label}</p>
            <p className="text-white text-2xl font-bold mt-1">{stat.value}</p>
          </div>
        </div>
      ))}
    </div>
  );
}
