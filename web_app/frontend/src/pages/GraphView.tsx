import React, { useEffect, useRef, useState } from 'react';
import { Graph } from '@antv/g6';
import axios from 'axios';
import { useSearchParams } from 'react-router-dom';
import styles from './GraphView.module.css';

const API_BASE = 'http://localhost:8000'; // Make sure this matches FastAPI in dev mode. 

const GraphView = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<any>(null);
  const [searchParams] = useSearchParams();
  const companyId = searchParams.get('id');
  const queryType = searchParams.get('queryType');
  const keyword = searchParams.get('keyword');
  const [title, setTitle] = useState('公司关系图谱');
  const [subtitle, setSubtitle] = useState('');
  const [message, setMessage] = useState('正在加载图谱...');
  const [status, setStatus] = useState<'loading' | 'ready' | 'empty' | 'error'>('loading');

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
    let graph: any = null;
    let cancelled = false;

    const mountGraph = (graphData: any) => {
      if (!containerRef.current) {
        return;
      }

      const width = containerRef.current.clientWidth;
      const height = 600;

      graph = new Graph({
        container: containerRef.current,
        width,
        height,
        layout: {
          type: 'force',
          preventOverlap: true,
          linkDistance: 150,
          nodeSize: 50,
        },
        modes: {
          default: ['drag-canvas', 'zoom-canvas', 'drag-node'],
        },
        defaultNode: {
          size: 50,
          style: { fill: '#C6E5FF', stroke: '#5B8FF9', lineWidth: 2 },
          labelCfg: { position: 'bottom', offset: 5 },
        },
        defaultEdge: {
          style: { stroke: '#e2e2e2', lineWidth: 1, endArrow: true },
          labelCfg: { autoRotate: true },
        },
      });

      graphRef.current = graph;
      graph.setData(graphData);
      graph.render();

      graph.on('node:dblclick', (e: any) => {
        const clickedNodeId = e.target?.id || e.itemId || e.id;
        if (!clickedNodeId) return;

        axios.get(`${API_BASE}/api/graph/company/${clickedNodeId}/graph?hops=2`)
          .then(({ data }) => {
            const currentData = graphRef.current.getData();

            const newNodes = data.nodes.filter((n: any) => !currentData.nodes?.find((cn: any) => cn.id === n.id));
            const newEdges = data.edges.filter((edge: any) => !currentData.edges?.find((currentEdge: any) => currentEdge.source === edge.source && currentEdge.target === edge.target && currentEdge.label === edge.label));

            graphRef.current.addData({
              nodes: newNodes,
              edges: newEdges,
            });
            graphRef.current.render();
          })
          .catch(err => console.error('Error expanding node:', err));
      });
    };

    const fetchGraph = async () => {
      setStatus('loading');
      setMessage('正在加载图谱...');

      try {
        if (companyId) {
          setTitle('公司关系图谱');
          setSubtitle(`公司 ID: ${companyId}`);
          const { data } = await axios.get(`${API_BASE}/api/graph/company/${companyId}/graph`);
          if (cancelled) return;

          if (!data.nodes?.length) {
            setStatus('empty');
            setMessage('还没有数据');
            return;
          }

          setStatus('ready');
          setMessage('');
          mountGraph(data);
          return;
        }

        if (queryType && keyword) {
          const { data } = await axios.get(`${API_BASE}/api/graph/stock/graph`, {
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

          setStatus('ready');
          setMessage('');
          mountGraph(data.graph);
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
      if (graph) {
        graph.destroy();
      }
    };
  }, [companyId, keyword, queryType]);

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
