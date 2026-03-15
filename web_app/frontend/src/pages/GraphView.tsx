import React, { useEffect, useRef, useState } from 'react';
import { Graph } from '@antv/g6';
import axios from 'axios';
import { useSearchParams } from 'react-router-dom';

const API_BASE = 'http://localhost:8000'; // Make sure this matches FastAPI in dev mode. 

const GraphView = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<any>(null);
  const [searchParams] = useSearchParams();
  const companyId = searchParams.get('id');

  useEffect(() => {
    if (!containerRef.current) return;
    
    const width = containerRef.current.clientWidth;
    const height = 600;
    
    const graph = new Graph({
      container: containerRef.current,
      width,
      height,
      layout: { 
        type: 'force', 
        preventOverlap: true, 
        linkDistance: 150,
        nodeSize: 50 
      },
      modes: { 
        default: ['drag-canvas', 'zoom-canvas', 'drag-node'] 
      },
      defaultNode: { 
        size: 50, 
        style: { fill: '#C6E5FF', stroke: '#5B8FF9', lineWidth: 2 },
        labelCfg: { position: 'bottom', offset: 5 }
      },
      defaultEdge: { 
        style: { stroke: '#e2e2e2', lineWidth: 1, endArrow: true },
        labelCfg: { autoRotate: true }
      }
    });
    
    graphRef.current = graph;

    graph.on('node:dblclick', (e: any) => {
      // In G6 v5, the clicked target exposes id as e.target.id
      const clickedNodeId = e.target?.id || e.itemId || e.id;
      if (!clickedNodeId) return;

      axios.get(`${API_BASE}/api/graph/company/${clickedNodeId}/graph?hops=2`)
        .then(({ data }) => {
          const currentData = graphRef.current.getData();
          
          const newNodes = data.nodes.filter((n: any) => !currentData.nodes?.find((cn: any) => cn.id === n.id));
          const newEdges = data.edges.filter((e: any) => !currentData.edges?.find((ce: any) => ce.source === e.source && ce.target === e.target));
          
          graphRef.current.addData({
            nodes: newNodes,
            edges: newEdges
          });
          graphRef.current.render();
        })
        .catch(err => console.error("Error expanding node:", err));
    });

    if (companyId) {
      axios.get(`${API_BASE}/api/graph/company/${companyId}/graph`)
        .then((res) => {
          const graphData = res.data;
          graph.setData(graphData);
          graph.render();
        })
        .catch(err => console.error("Error fetching initial graph:", err));
    }

    return () => graph.destroy();
  }, [companyId]);

  return (
    <div style={{ padding: '20px' }}>
      <h2>公司关系图谱 (ID: {companyId})</h2>
      <p style={{ color: '#666' }}>提示：双击节点展开2层关系</p>
      <div 
        ref={containerRef} 
        style={{ width: '100%', height: '600px', border: '1px solid #eee', background: '#fafafa' }} 
      />
    </div>
  );
};

export default GraphView;
