import React, { useEffect, useRef, useState } from 'react';
import { Graph } from '@antv/g6';
import axios from 'axios';
import { useSearchParams } from 'react-router-dom';
import styles from './GraphView.module.css';

type RawGraphNode = {
  id: string;
  label?: string;
  type?: string;
  properties?: Record<string, unknown>;
};

type RawGraphEdge = {
  source: string;
  target: string;
  label?: string;
  properties?: Record<string, unknown>;
};

type RawGraphData = {
  nodes?: RawGraphNode[];
  edges?: RawGraphEdge[];
};

type GraphNode = {
  id: string;
  data: Record<string, unknown>;
  style: Record<string, unknown>;
};

type GraphEdge = {
  id: string;
  source: string;
  target: string;
  data: Record<string, unknown>;
  style: Record<string, unknown>;
};

type GraphDataset = {
  nodes: GraphNode[];
  edges: GraphEdge[];
};

const getNodePalette = (entityType?: string) => {
  switch (entityType) {
    case 'Company':
      return { fill: '#C6E5FF', stroke: '#5B8FF9' };
    case 'Person':
      return { fill: '#FCE8C3', stroke: '#D9902F' };
    default:
      return { fill: '#E8EEF5', stroke: '#718096' };
  }
};

const buildEdgeId = (edge: RawGraphEdge) => {
  const relation = edge.label || 'RELATION';
  const percent = String(edge.properties?.percent || '');
  return `${String(edge.source)}-${relation}-${String(edge.target)}-${percent}`;
};

const normalizeGraphData = (rawData: RawGraphData | null): GraphDataset => {
  const rawNodes = rawData?.nodes || [];
  const rawEdges = rawData?.edges || [];

  return {
    nodes: rawNodes.map((node) => {
      const id = String(node.id);
      const label = String(node.label || node.properties?.name || id);
      const entityType = String(node.type || 'Unknown');
      const palette = getNodePalette(entityType);

      return {
        id,
        data: {
          ...(node.properties || {}),
          entityType,
          displayLabel: label,
        },
        style: {
          size: 48,
          fill: palette.fill,
          stroke: palette.stroke,
          lineWidth: 2,
          label: true,
          labelText: label,
          labelPlacement: 'bottom',
          labelFill: '#1F2D3D',
          labelFontSize: 12,
          labelMaxWidth: 180,
          labelWordWrap: true,
          labelOffsetY: 10,
          cursor: 'pointer',
        },
      };
    }),
    edges: rawEdges.map((edge) => {
      const percent = String(edge.properties?.percent || '').trim();
      const relationLabel = String(edge.label || 'RELATION');
      const edgeLabel = percent && percent !== '-' ? `${relationLabel} ${percent}` : relationLabel;

      return {
        id: buildEdgeId(edge),
        source: String(edge.source),
        target: String(edge.target),
        data: {
          ...(edge.properties || {}),
          relationLabel,
        },
        style: {
          stroke: '#AAB7C4',
          lineWidth: 1.3,
          endArrow: true,
          label: true,
          labelText: edgeLabel,
          labelFontSize: 10,
          labelFill: '#5D6B7A',
          labelBackground: true,
          labelBackgroundFill: 'rgba(255, 255, 255, 0.88)',
          labelPadding: [2, 4, 2, 4],
        },
      };
    }),
  };
};

const getEventNodeId = (event: any) => {
  const candidate =
    event?.target?.id ||
    event?.target?.config?.id ||
    event?.target?.__data__?.id ||
    event?.itemId ||
    event?.id ||
    event?.data?.id;

  return candidate ? String(candidate) : '';
};

const GraphView = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<Graph | null>(null);
  const [searchParams] = useSearchParams();
  const companyId = searchParams.get('id');
  const queryType = searchParams.get('queryType');
  const keyword = searchParams.get('keyword');
  const [title, setTitle] = useState('公司关系图谱');
  const [subtitle, setSubtitle] = useState('');
  const [message, setMessage] = useState('正在加载图谱...');
  const [status, setStatus] = useState<'loading' | 'ready' | 'empty' | 'error'>('loading');
  const [graphData, setGraphData] = useState<GraphDataset | null>(null);

  const statusLabel = status === 'loading'
    ? '加载中'
    : status === 'ready'
      ? '图谱可用'
      : status === 'empty'
        ? '暂无数据'
        : '加载失败';

  const statusBadgeClass = status === 'loading'
    ? `${styles.badge} ${styles.badgeLoading}`
    : status === 'ready'
      ? `${styles.badge} ${styles.badgeReady}`
      : status === 'empty'
        ? `${styles.badge} ${styles.badgeEmpty}`
        : `${styles.badge} ${styles.badgeError}`;

  useEffect(() => {
    let cancelled = false;

    const fetchGraph = async () => {
      setStatus('loading');
      setMessage('正在加载图谱...');
      setGraphData(null);

      try {
        if (companyId) {
          setTitle('公司关系图谱');
          setSubtitle(`公司 ID: ${companyId}`);
          const { data } = await axios.get(`/api/graph/company/${companyId}/graph`);
          if (cancelled) return;

          if (!data.nodes?.length) {
            setStatus('empty');
            setMessage('还没有数据');
            return;
          }

          setGraphData(normalizeGraphData(data));
          setStatus('ready');
          setMessage('');
          return;
        }

        if (queryType && keyword) {
          const { data } = await axios.get(`/api/stock/graph`, {
            params: { query_type: queryType, keyword },
          });
          if (cancelled) return;

          const stockLabel = data.stock
            ? `${data.stock.name} (${data.stock.code}.${data.stock.exchange})`
            : String(keyword);

          setTitle('A股公司关系图谱');
          setSubtitle(stockLabel);

          if (!data.has_data) {
            setStatus('empty');
            setMessage(data.message || '还没有数据');
            return;
          }

          setGraphData(normalizeGraphData(data.graph));
          setStatus('ready');
          setMessage('');
          return;
        }

        setStatus('error');
        setMessage('缺少查询参数');
      } catch (err) {
        console.error('Error fetching initial graph:', err);
        if (cancelled) return;
        setStatus('error');
        setMessage('加载图谱失败');
      }
    };

    fetchGraph();

    return () => {
      cancelled = true;
    };
  }, [companyId, keyword, queryType]);

  useEffect(() => {
    if (status !== 'ready' || !graphData || !containerRef.current) {
      return;
    }

    const container = containerRef.current;
    const height = 600;
    const width = container.clientWidth || container.offsetWidth || 800;
    const nodeCount = graphData.nodes.length;
    const edgeCount = graphData.edges.length;
    const isLargeGraph = nodeCount > 300;

    // 节点多时关闭标签，减少渲染开销
    const showNodeLabel = nodeCount <= 300;
    const showEdgeLabel = edgeCount <= 400;

    // 根据数据量调整布局参数，节点越多迭代越少，保证及时出图
    const forceIterations = nodeCount <= 200 ? 300
      : nodeCount <= 500 ? 150
      : nodeCount <= 1000 ? 80
      : 40;
    const forceAlphaDecay = nodeCount <= 200 ? 0.01
      : nodeCount <= 500 ? 0.03
      : nodeCount <= 1000 ? 0.06
      : 0.12;

    let graph: Graph;
    try {
      graph = new Graph({
        container,
        data: graphData,
        width,
        height,
        autoFit: 'view',
        padding: 32,
        zoomRange: [0.2, 4],
        animation: !isLargeGraph,
        layout: {
          type: 'force',
          preventOverlap: true,
          linkDistance: isLargeGraph ? 100 : 150,
          nodeSize: isLargeGraph ? 32 : 48,
          iterations: forceIterations,
          alphaDecay: forceAlphaDecay,
          animate: !isLargeGraph,
        },
        behaviors: ['drag-canvas', 'zoom-canvas', 'drag-element'],
        node: {
          type: 'circle',
          style: {
            size: isLargeGraph ? 32 : 48,
            fill: '#C6E5FF',
            stroke: '#5B8FF9',
            lineWidth: isLargeGraph ? 1.2 : 2,
            label: showNodeLabel,
            labelPlacement: 'bottom',
            labelFill: '#1F2D3D',
            labelFontSize: 12,
            labelMaxWidth: 180,
            labelWordWrap: true,
            labelOffsetY: 10,
          },
        },
        edge: {
          type: 'line',
          style: {
            stroke: '#AAB7C4',
            lineWidth: isLargeGraph ? 0.8 : 1.3,
            endArrow: !isLargeGraph,
            label: showEdgeLabel,
            labelFontSize: 10,
            labelFill: '#5D6B7A',
            labelBackground: true,
            labelBackgroundFill: 'rgba(255, 255, 255, 0.88)',
            labelPadding: [2, 4, 2, 4],
            labelAutoRotate: true,
          },
        },
      });
    } catch (err) {
      console.error('Error creating graph:', err);
      setStatus('error');
      setMessage('图谱初始化失败');
      return;
    }

    graphRef.current = graph;
    void graph.render().then(() => {
      // render succeeded
    }).catch((err) => {
      console.error('Error rendering graph:', err);
      if (graphRef.current === graph) {
        graphRef.current = null;
      }
      setStatus('error');
      setMessage('图谱渲染失败');
      graph.destroy();
    });

    graph.on('node:dblclick', (e: any) => {
      const clickedNodeId = getEventNodeId(e);
      if (!clickedNodeId || !graphRef.current) {
        return;
      }

      axios.get(`/api/graph/company/${clickedNodeId}/graph?hops=2`)
        .then(({ data }) => {
          if (!graphRef.current) {
            return;
          }

          const nextData = normalizeGraphData(data);
          const currentData = graphRef.current.getData();
          const newNodes = nextData.nodes.filter((node) => !currentData.nodes?.find((currentNode: any) => currentNode.id === node.id));
          const newEdges = nextData.edges.filter((edge) => !currentData.edges?.find((currentEdge: any) => currentEdge.id === edge.id));

          graphRef.current.addData({
            nodes: newNodes,
            edges: newEdges,
          });
          void graphRef.current.render();
        })
        .catch(err => console.error('Error expanding node:', err));
    });

    const handleResize = () => {
      if (!containerRef.current || !graphRef.current) {
        return;
      }

      const nextWidth = containerRef.current.clientWidth || containerRef.current.offsetWidth;
      if (nextWidth > 0 && graphRef.current.setSize) {
        graphRef.current.setSize(nextWidth, height);
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      if (graphRef.current === graph) {
        graphRef.current = null;
      }
      graph.destroy();
    };
  }, [graphData, status]);

  const renderHeader = () => (
    <div className={styles.header}>
      <div>
        <h2 className={styles.headerTitle}>{title}</h2>
        <p className={styles.headerSubtitle}>当前页会直接展示二跳关系图。图库中存在数据时可继续双击节点扩展；没有数据时会保留明确提示，不再显示空白画布。</p>
      </div>
      <div className={styles.metaCard}>
        <span className={styles.metaLabel}>当前查询</span>
        <strong className={styles.metaValue}>{subtitle || '未提供查询条件'}</strong>
      </div>
    </div>
  );

  const renderNotice = () => (
    <div className={styles.noticeRow}>
      <p className={styles.noticeText}>提示：首次打开即展示二跳关系，双击任意节点会继续拉取该节点的二跳结构并合并进当前图谱。</p>
      <span className={statusBadgeClass}>{statusLabel}</span>
    </div>
  );

  const renderGraphCanvas = () => (
    <div
      ref={containerRef}
      className={styles.canvas}
    />
  );

  const renderStatusPanel = () => (
    <div className={styles.statusPanel}>
      <div className={`${styles.statusCard} ${status === 'error' ? styles.statusError : ''}`}>
        <h3 className={styles.statusTitle}>{status === 'empty' ? '还没有数据' : status === 'error' ? '加载图谱失败' : '正在准备图谱'}</h3>
        <p className={styles.statusMessage}>{message}</p>
      </div>
    </div>
  );

  return (
    <div className={styles.page}>
      {renderHeader()}
      {renderNotice()}

      <div className={styles.canvasShell}>
        {status === 'ready' ? renderGraphCanvas() : renderStatusPanel()}
      </div>
    </div>
  );
};

export default GraphView;
