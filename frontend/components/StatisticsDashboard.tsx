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
    <div className="flex items-center gap-4">
      {stats.map((stat, index) => (
        <div
          key={index}
          className="flex items-center gap-2.5 bg-gray-800/60 backdrop-blur-sm rounded-lg px-3.5 py-2 border border-gray-700/50"
        >
          <div className={`${stat.bgColor} ${stat.color} p-2 rounded-md`}>
            <stat.icon className="w-4 h-4" />
          </div>
          <div>
            <p className="text-gray-400 text-[10px] uppercase tracking-wide">{stat.label}</p>
            <p className={`${stat.color} text-base font-bold`}>{stat.value}</p>
          </div>
        </div>
      ))}
    </div>
  );
}
