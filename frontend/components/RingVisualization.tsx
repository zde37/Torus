'use client';

import { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { ChordNode } from '@/types/chord';

interface RingVisualizationProps {
  nodes: ChordNode[];
  onNodeClick?: (node: ChordNode) => void;
  selectedNode?: ChordNode | null;
}

export default function RingVisualization({ nodes, onNodeClick, selectedNode }: RingVisualizationProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 800 });

  useEffect(() => {
    // Handle responsive sizing
    const updateDimensions = () => {
      if (svgRef.current) {
        const container = svgRef.current.parentElement;
        if (container) {
          const size = Math.min(container.clientWidth, container.clientHeight, 800);
          setDimensions({ width: size, height: size });
        }
      }
    };

    updateDimensions();
    window.addEventListener('resize', updateDimensions);
    return () => window.removeEventListener('resize', updateDimensions);
  }, []);

  useEffect(() => {
    if (!svgRef.current || nodes.length === 0) return;

    const { width, height } = dimensions;
    const radius = Math.min(width, height) / 2 - 80;
    const centerX = width / 2;
    const centerY = height / 2;

    // Clear existing SVG content
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    // Create main group
    const g = svg
      .append('g')
      .attr('transform', `translate(${centerX},${centerY})`);

    // Draw ring circle
    g.append('circle')
      .attr('r', radius)
      .attr('fill', 'none')
      .attr('stroke', '#333')
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', '5,5');

    // Calculate node positions on the ring
    // Nodes are positioned based on their ID hash value
    const maxId = BigInt('2') ** BigInt(160); // 2^160 for M=160

    const nodePositions = nodes.map((node) => {
      // Decode base64 ID to get the numeric value
      const idBytes = atob(node.id);
      let idValue = BigInt(0);
      for (let i = 0; i < idBytes.length; i++) {
        idValue = (idValue << BigInt(8)) + BigInt(idBytes.charCodeAt(i));
      }

      // Calculate angle (0 to 2π)
      const normalizedPosition = Number(idValue * BigInt(10000) / maxId) / 10000;
      const angle = normalizedPosition * 2 * Math.PI - Math.PI / 2; // Start from top

      return {
        node,
        angle,
        x: radius * Math.cos(angle),
        y: radius * Math.sin(angle),
      };
    });

    // Draw successor connections
    const connections = g.append('g').attr('class', 'connections');

    nodePositions.forEach((pos) => {
      if (pos.node.successor) {
        const successorPos = nodePositions.find((p) => p.node.id === pos.node.successor?.id);
        if (successorPos) {
          connections
            .append('line')
            .attr('x1', pos.x)
            .attr('y1', pos.y)
            .attr('x2', successorPos.x)
            .attr('y2', successorPos.y)
            .attr('stroke', '#4ade80')
            .attr('stroke-width', 2)
            .attr('opacity', 0.3)
            .attr('stroke-dasharray', '3,3');
        }
      }
    });

    // Draw nodes
    const nodeGroups = g
      .append('g')
      .attr('class', 'nodes')
      .selectAll('g')
      .data(nodePositions)
      .join('g')
      .attr('transform', (d) => `translate(${d.x},${d.y})`)
      .attr('cursor', 'pointer')
      .on('click', (event, d) => {
        if (onNodeClick) {
          onNodeClick(d.node);
        }
      });

    // Node circles
    nodeGroups
      .append('circle')
      .attr('r', (d) => (selectedNode?.id === d.node.id ? 12 : 8))
      .attr('fill', (d) => (selectedNode?.id === d.node.id ? '#3b82f6' : '#6366f1'))
      .attr('stroke', '#fff')
      .attr('stroke-width', 2)
      .style('transition', 'all 0.3s ease');

    // Node labels (shortened ID)
    nodeGroups
      .append('text')
      .attr('dy', -15)
      .attr('text-anchor', 'middle')
      .attr('fill', '#fff')
      .attr('font-size', '10px')
      .attr('font-family', 'monospace')
      .text((d) => {
        // Show first 8 chars of hex ID
        const hex = Array.from(atob(d.node.id))
          .map((char) => char.charCodeAt(0).toString(16).padStart(2, '0'))
          .join('')
          .substring(0, 8);
        return hex;
      });

    // Add hover effects
    nodeGroups
      .on('mouseenter', function () {
        d3.select(this).select('circle').attr('r', 12).attr('fill', '#3b82f6');
      })
      .on('mouseleave', function (event, d) {
        if (selectedNode?.id !== d.node.id) {
          d3.select(this).select('circle').attr('r', 8).attr('fill', '#6366f1');
        }
      });

  }, [nodes, dimensions, selectedNode, onNodeClick]);

  return (
    <div className="w-full h-full flex items-center justify-center bg-gray-900 rounded-lg">
      <svg
        ref={svgRef}
        width={dimensions.width}
        height={dimensions.height}
        className="ring-visualization"
      />
    </div>
  );
}
