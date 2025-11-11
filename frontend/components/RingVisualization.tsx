'use client';

import { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { ChordNode } from '@/types/chord';

interface RingVisualizationProps {
  nodes: ChordNode[];
  onNodeClick?: (node: ChordNode) => void;
  selectedNode?: ChordNode | null;
  lookupPath?: ChordNode[] | null;
  lookupResponsibleNode?: ChordNode | null;
}

export default function RingVisualization({ nodes, onNodeClick, selectedNode, lookupPath, lookupResponsibleNode }: RingVisualizationProps) {
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

    // Define arrowhead marker for successor connections
    svg
      .append('defs')
      .append('marker')
      .attr('id', 'successor-arrow')
      .attr('markerWidth', 10)
      .attr('markerHeight', 10)
      .attr('refX', 9)
      .attr('refY', 5)
      .attr('orient', 'auto')
      .append('polygon')
      .attr('points', '0 0, 8 4, 0 8')
      .attr('fill', '#4ade80');

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
            .attr('opacity', 0.6)
            .attr('stroke-dasharray', '3,3')
            .attr('marker-end', 'url(#successor-arrow)');
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
      .attr('r', (d) => (selectedNode?.id === d.node.id ? 14 : 8))
      .attr('fill', (d) => (selectedNode?.id === d.node.id ? '#10b981' : '#6366f1')) // Green for selected
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
        d3.select(this).select('circle').attr('r', 13).attr('fill', '#06b6d4'); // Cyan for hover
      })
      .on('mouseleave', function (event, d) {
        if (selectedNode?.id !== d.node.id) {
          d3.select(this).select('circle').attr('r', 8).attr('fill', '#6366f1');
        } else {
          d3.select(this).select('circle').attr('r', 14).attr('fill', '#10b981'); // Return to green if selected
        }
      });

    // Draw lookup path animation if provided
    if (lookupPath && lookupPath.length > 0) {
      const pathGroup = g.append('g').attr('class', 'lookup-path');

      // Find positions for nodes in the path
      const pathPositions = lookupPath.map((pathNode) => {
        const pos = nodePositions.find((p) => p.node.id === pathNode.id);
        if (!pos) {
          console.warn(`Node ${pathNode.id} from path not found in visualization`);
        }
        return pos;
      }).filter((p) => p !== undefined);

      console.log(`[Lookup Animation] Path has ${lookupPath.length} nodes, found ${pathPositions.length} positions`);

      if (pathPositions.length > 0) {
        // Draw animated arrows between consecutive nodes in the path
        for (let i = 0; i < pathPositions.length - 1; i++) {
          const start = pathPositions[i];
          const end = pathPositions[i + 1];

          if (start && end) {
            // Calculate arrow path
            const dx = end.x - start.x;
            const dy = end.y - start.y;
            const distance = Math.sqrt(dx * dx + dy * dy);
            const angle = Math.atan2(dy, dx);

            // Shorten the line to not overlap with node circles
            const startOffset = 15;
            const endOffset = 15;
            const x1 = start.x + (startOffset / distance) * dx;
            const y1 = start.y + (startOffset / distance) * dy;
            const x2 = end.x - (endOffset / distance) * dx;
            const y2 = end.y - (endOffset / distance) * dy;

            // Draw the line
            const line = pathGroup
              .append('line')
              .attr('x1', x1)
              .attr('y1', y1)
              .attr('x2', x1) // Start with zero length
              .attr('y2', y1)
              .attr('stroke', '#f59e0b') // Amber color
              .attr('stroke-width', 3)
              .attr('marker-end', 'url(#arrowhead)');

            // Animate the line
            line
              .transition()
              .delay(i * 600) // Stagger animation
              .duration(600)
              .attr('x2', x2)
              .attr('y2', y2);

            // Add hop number label
            pathGroup
              .append('text')
              .attr('x', (x1 + x2) / 2)
              .attr('y', (y1 + y2) / 2 - 10)
              .attr('text-anchor', 'middle')
              .attr('fill', '#f59e0b')
              .attr('font-size', '12px')
              .attr('font-weight', 'bold')
              .style('opacity', 0)
              .text(`Hop ${i + 1}`)
              .transition()
              .delay(i * 600 + 300)
              .duration(300)
              .style('opacity', 1);
          }
        }

        // Add arrowhead marker
        svg
          .append('defs')
          .append('marker')
          .attr('id', 'arrowhead')
          .attr('markerWidth', 10)
          .attr('markerHeight', 10)
          .attr('refX', 9)
          .attr('refY', 3)
          .attr('orient', 'auto')
          .append('polygon')
          .attr('points', '0 0, 10 3, 0 6')
          .attr('fill', '#f59e0b');

        // Check if responsible node is different from last path node
        if (lookupResponsibleNode) {
          const lastPathNode = lookupPath[lookupPath.length - 1];
          const responsiblePos = nodePositions.find((p) => p.node.id === lookupResponsibleNode.id);

          // If responsible node is not the last node in path, draw final arrow
          if (lastPathNode && responsiblePos && lastPathNode.id !== lookupResponsibleNode.id) {
            const lastPos = pathPositions[pathPositions.length - 1];

            if (lastPos) {
              const dx = responsiblePos.x - lastPos.x;
              const dy = responsiblePos.y - lastPos.y;
              const distance = Math.sqrt(dx * dx + dy * dy);

              const startOffset = 15;
              const endOffset = 15;
              const x1 = lastPos.x + (startOffset / distance) * dx;
              const y1 = lastPos.y + (startOffset / distance) * dy;
              const x2 = responsiblePos.x - (endOffset / distance) * dx;
              const y2 = responsiblePos.y - (endOffset / distance) * dy;

              const finalHop = pathPositions.length;

              // Draw final arrow to responsible node
              const finalLine = pathGroup
                .append('line')
                .attr('x1', x1)
                .attr('y1', y1)
                .attr('x2', x1)
                .attr('y2', y1)
                .attr('stroke', '#22c55e') // Green for final hop
                .attr('stroke-width', 3)
                .attr('marker-end', 'url(#arrowhead)');

              finalLine
                .transition()
                .delay((finalHop - 1) * 600)
                .duration(600)
                .attr('x2', x2)
                .attr('y2', y2);

              // Add "Result" label
              pathGroup
                .append('text')
                .attr('x', (x1 + x2) / 2)
                .attr('y', (y1 + y2) / 2 - 10)
                .attr('text-anchor', 'middle')
                .attr('fill', '#22c55e')
                .attr('font-size', '12px')
                .attr('font-weight', 'bold')
                .style('opacity', 0)
                .text('Result')
                .transition()
                .delay((finalHop - 1) * 600 + 300)
                .duration(300)
                .style('opacity', 1);

              // Highlight responsible node
              nodeGroups
                .filter((d: any) => d.node.id === lookupResponsibleNode.id)
                .select('circle')
                .transition()
                .delay((finalHop - 1) * 600)
                .duration(300)
                .attr('fill', '#22c55e') // Green for responsible node
                .attr('r', 16)
                .attr('stroke-width', 3);
            }
          }
        }

        // Highlight path nodes
        pathPositions.forEach((pos, index) => {
          if (pos) {
            nodeGroups
              .filter((d: any) => d.node.id === pos.node.id)
              .select('circle')
              .transition()
              .delay(index * 600)
              .duration(300)
              .attr('fill', '#f59e0b')
              .attr('r', 14)
              .attr('stroke-width', 3);
          }
        });
      }
    }

  }, [nodes, dimensions, selectedNode, onNodeClick, lookupPath, lookupResponsibleNode]);

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
