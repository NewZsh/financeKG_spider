import React, { useEffect, useRef, useState } from 'react';
import { Graph } from '@antv/g6';
import axios from 'axios';
import { useNavigate, useSearchParams } from 'react-router-dom';
import styles from './StockGraphView.module.css';

const KLINE_MIN_DATE = '2021-01-01';
const KLINE_INITIAL_VISIBLE_DAYS = 60;
const KLINE_CANDLE_STEP = 14;

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

type StockInfo = {
  code: string;
  name: string;
  full_name?: string | null;
  exchange: string;
};

type CompanyInfo = {
  id: string;
  name: string;
};

type DailySeriesItem = {
  date: string;
  open: number | null;
  close: number | null;
  high: number | null;
  low: number | null;
  volume: number | null;
  amount: number | null;
  amplitude: number | null;
  change_pct: number | null;
  change_amount: number | null;
  turnover_rate: number | null;
};

type MinuteSeriesItem = {
  timestamp: string;
  date: string;
  time: string;
  open: number | null;
  close: number | null;
  high: number | null;
  low: number | null;
  avg_price: number | null;
  volume: number | null;
  amount: number | null;
  change_pct: number | null;
  change_amount: number | null;
};

type DistributionBin = {
  label: string;
  lower_price: number;
  upper_price: number;
  buy_volume?: number;
  sell_volume?: number;
  neutral_volume?: number;
  total_volume?: number;
  count?: number;
  volume?: number;
};

type DailyDistribution = {
  date: string;
  summary: {
    open: number | null;
    close: number | null;
    high: number | null;
    low: number | null;
    total_volume: number | null;
    total_amount: number | null;
  };
  buy_sell_bins: DistributionBin[];
  price_histogram: DistributionBin[];
};

type MarketPayload = {
  daily_series: DailySeriesItem[];
  candle_windows: {
    day: DailySeriesItem[];
    five_day: DailySeriesItem[];
    twenty_day: DailySeriesItem[];
  };
  intraday_series: MinuteSeriesItem[];
  daily_distributions: DailyDistribution[];
  warnings: string[];
};

type StockDetailResponse = {
  matched: boolean;
  message: string | null;
  stock: StockInfo | null;
  company: CompanyInfo | null;
  has_data: boolean;
  graph: RawGraphData;
  market: MarketPayload;
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

const formatNumber = (value: number | null | undefined, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--';
  }
  return value.toFixed(digits);
};

const formatLargeNumber = (value: number | null | undefined) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--';
  }
  if (Math.abs(value) >= 100000000) {
    return `${(value / 100000000).toFixed(2)}亿`;
  }
  if (Math.abs(value) >= 10000) {
    return `${(value / 10000).toFixed(2)}万`;
  }
  return value.toFixed(0);
};

const buildMovingAverageSeries = (points: DailySeriesItem[], period: number) => {
  return points.map((_, index) => {
    if (index + 1 < period) {
      return null;
    }

    // Moving averages are computed from the daily close field.
    // The closing price is the most stable end-of-day price anchor for MA5/10/20.
    const window = points.slice(index + 1 - period, index + 1);
    const closeValues = window
      .map((item) => item.close)
      .filter((value): value is number => value !== null && value !== undefined);

    if (closeValues.length !== period) {
      return null;
    }

    const average = closeValues.reduce((sum, value) => sum + value, 0) / period;
    return Number(average.toFixed(4));
  });
};

const buildPolylinePath = (
  values: Array<number | null>,
  getX: (index: number) => number,
  getY: (value: number) => number,
) => {
  return values
    .map((value, index) => {
      if (value === null || value === undefined) {
        return '';
      }
      return `${index === 0 || values[index - 1] === null ? 'M' : 'L'} ${getX(index)} ${getY(value)}`;
    })
    .filter(Boolean)
    .join(' ');
};

const RollingCandleChart = ({ points }: { points: DailySeriesItem[] }) => {
  const viewportRef = useRef<HTMLDivElement>(null);
  const dragStateRef = useRef<{ active: boolean; startX: number; startScrollLeft: number }>({
    active: false,
    startX: 0,
    startScrollLeft: 0,
  });

  const visiblePoints = points.filter((point) => point.date >= KLINE_MIN_DATE);

  useEffect(() => {
    if (!viewportRef.current || !visiblePoints.length) {
      return;
    }

    const viewport = viewportRef.current;
    viewport.scrollLeft = Math.max(viewport.scrollWidth - viewport.clientWidth, 0);
  }, [visiblePoints.length]);

  if (!visiblePoints.length) {
    return <div className={styles.emptyBlock}>暂无可用于绘制长周期 K 线的数据</div>;
  }

  const height = 420;
  const paddingTop = 24;
  const paddingRight = 40;
  const paddingBottom = 38;
  const paddingLeft = 56;
  const width = Math.max(
    paddingLeft + paddingRight + visiblePoints.length * KLINE_CANDLE_STEP,
    paddingLeft + paddingRight + KLINE_INITIAL_VISIBLE_DAYS * KLINE_CANDLE_STEP,
  );
  const candleBodyWidth = 8;
  const usableHeight = height - paddingTop - paddingBottom;
  const prices = visiblePoints.flatMap((point) => [point.low ?? point.close ?? 0, point.high ?? point.close ?? 0]);
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const priceSpan = Math.max(maxPrice - minPrice, 0.01);
  const ma5 = buildMovingAverageSeries(visiblePoints, 5);
  const ma10 = buildMovingAverageSeries(visiblePoints, 10);
  const ma20 = buildMovingAverageSeries(visiblePoints, 20);

  const getX = (index: number) => paddingLeft + index * KLINE_CANDLE_STEP;
  const getY = (price: number) => paddingTop + (maxPrice - price) / priceSpan * usableHeight;

  const yAxisTicks = Array.from({ length: 5 }, (_, index) => {
    const price = maxPrice - (priceSpan / 4) * index;
    return { label: price.toFixed(2), y: getY(price) };
  });

  const dateTicks = visiblePoints.reduce<Array<{ x: number; label: string }>>((accumulator, point, index) => {
    const isFirstPoint = index === 0;
    const isMonthStart = point.date.endsWith('-01');
    const isLastPoint = index === visiblePoints.length - 1;
    if (isFirstPoint || isMonthStart || isLastPoint) {
      accumulator.push({ x: getX(index), label: point.date.slice(0, 7) });
    }
    return accumulator;
  }, []);

  const handleMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!viewportRef.current) {
      return;
    }

    dragStateRef.current = {
      active: true,
      startX: event.clientX,
      startScrollLeft: viewportRef.current.scrollLeft,
    };
  };

  const handleMouseMove = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!dragStateRef.current.active || !viewportRef.current) {
      return;
    }

    const delta = event.clientX - dragStateRef.current.startX;
    viewportRef.current.scrollLeft = dragStateRef.current.startScrollLeft - delta;
  };

  const stopDrag = () => {
    dragStateRef.current.active = false;
  };

  return (
    <div className={styles.klineCard}>
      <div className={styles.chartCardHeader}>
        <h3>长周期日线图</h3>
        <span>{visiblePoints[0].date} 至 {visiblePoints[visiblePoints.length - 1].date}</span>
      </div>
      <div className={styles.klineMetaRow}>
        <div className={styles.legendList}>
          <span className={styles.legendItem}><i className={`${styles.legendSwatch} ${styles.legendMa5}`} />MA5</span>
          <span className={styles.legendItem}><i className={`${styles.legendSwatch} ${styles.legendMa10}`} />MA10</span>
          <span className={styles.legendItem}><i className={`${styles.legendSwatch} ${styles.legendMa20}`} />MA20</span>
        </div>
        <div className={styles.klineHint}>默认显示最近 60 个交易日，可横向拖动回看至 2021-01-01。</div>
      </div>
      <div className={styles.klineNote}>
        均线说明：MA5、MA10、MA20 均基于日线 <strong>收盘价 close</strong> 字段计算。
      </div>
      <div
        ref={viewportRef}
        className={styles.klineViewport}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={stopDrag}
        onMouseLeave={stopDrag}
      >
        <svg viewBox={`0 0 ${width} ${height}`} className={styles.klineSvg} role="img" aria-label="长周期日线图和均线图">
          {yAxisTicks.map((tick) => (
            <g key={`tick-${tick.label}`}>
              <line x1={paddingLeft} y1={tick.y} x2={width - paddingRight} y2={tick.y} className={styles.gridLine} />
              <text x={paddingLeft - 8} y={tick.y + 4} textAnchor="end" className={styles.axisText}>{tick.label}</text>
            </g>
          ))}
          <line x1={paddingLeft} y1={paddingTop} x2={paddingLeft} y2={height - paddingBottom} className={styles.axisLine} />
          <line x1={paddingLeft} y1={height - paddingBottom} x2={width - paddingRight} y2={height - paddingBottom} className={styles.axisLine} />
          <path d={buildPolylinePath(ma5, getX, getY)} className={styles.ma5Line} />
          <path d={buildPolylinePath(ma10, getX, getY)} className={styles.ma10Line} />
          <path d={buildPolylinePath(ma20, getX, getY)} className={styles.ma20Line} />
          {visiblePoints.map((point, index) => {
            const x = getX(index);
            const open = point.open ?? point.close ?? 0;
            const close = point.close ?? point.open ?? 0;
            const high = point.high ?? Math.max(open, close);
            const low = point.low ?? Math.min(open, close);
            const isRise = close >= open;
            const color = isRise ? '#0f8f6f' : '#c24d46';
            const bodyTop = Math.min(getY(open), getY(close));
            const bodyHeight = Math.max(Math.abs(getY(open) - getY(close)), 2.5);

            return (
              <g key={`day-${point.date}`}>
                <line x1={x} y1={getY(high)} x2={x} y2={getY(low)} stroke={color} strokeWidth="1.5" />
                <rect
                  x={x - candleBodyWidth / 2}
                  y={bodyTop}
                  width={candleBodyWidth}
                  height={bodyHeight}
                  fill={isRise ? 'rgba(15, 143, 111, 0.18)' : color}
                  stroke={color}
                  strokeWidth="1.2"
                  rx="1.5"
                />
              </g>
            );
          })}
          {dateTicks.map((tick) => (
            <g key={`date-${tick.label}-${tick.x}`}>
              <line x1={tick.x} y1={height - paddingBottom} x2={tick.x} y2={height - paddingBottom + 6} className={styles.axisLine} />
              <text x={tick.x} y={height - 10} textAnchor="middle" className={styles.axisText}>{tick.label}</text>
            </g>
          ))}
        </svg>
      </div>
    </div>
  );
};

const buildLinePath = (points: MinuteSeriesItem[], getY: (value: number) => number, valueKey: 'close' | 'avg_price', width: number, padding: number) => {
  const usableWidth = width - padding * 2;
  return points
    .map((point, index) => {
      const rawValue = point[valueKey];
      if (rawValue === null || rawValue === undefined) {
        return '';
      }
      const x = padding + (points.length === 1 ? usableWidth / 2 : usableWidth * index / (points.length - 1));
      const y = getY(rawValue);
      return `${index === 0 ? 'M' : 'L'} ${x} ${y}`;
    })
    .filter(Boolean)
    .join(' ');
};

const IntradayChart = ({ points }: { points: MinuteSeriesItem[] }) => {
  if (!points.length) {
    return <div className={styles.emptyBlock}>暂无 5 日量价数据</div>;
  }

  const width = 1040;
  const height = 340;
  const padding = 36;
  const volumeHeight = 90;
  const chartHeight = height - padding * 2 - volumeHeight;
  const priceValues = points.flatMap((point) => [point.low ?? point.close ?? 0, point.high ?? point.close ?? 0, point.avg_price ?? point.close ?? 0]);
  const minPrice = Math.min(...priceValues);
  const maxPrice = Math.max(...priceValues);
  const maxVolume = Math.max(...points.map((point) => point.volume ?? 0), 1);
  const priceSpan = Math.max(maxPrice - minPrice, 0.01);
  const usableWidth = width - padding * 2;

  const getPriceY = (value: number) => padding + (maxPrice - value) / priceSpan * chartHeight;
  const closePath = buildLinePath(points, getPriceY, 'close', width, padding);
  const avgPath = buildLinePath(points, getPriceY, 'avg_price', width, padding);
  const uniqueDates = Array.from(new Set(points.map((point) => point.date)));

  return (
    <div className={styles.largeChartCard}>
      <div className={styles.chartCardHeader}>
        <h3>5日量价曲线</h3>
        <span>{uniqueDates.length} 个交易日 / {points.length} 个时间点</span>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className={styles.svgChartWide} role="img" aria-label="5日量价曲线">
        <line x1={padding} y1={padding} x2={padding} y2={padding + chartHeight} className={styles.axisLine} />
        <line x1={padding} y1={padding + chartHeight} x2={width - padding} y2={padding + chartHeight} className={styles.axisLine} />
        <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} className={styles.axisLine} />
        <path d={avgPath} className={styles.avgLine} />
        <path d={closePath} className={styles.priceLine} />
        {points.map((point, index) => {
          const x = padding + (points.length === 1 ? usableWidth / 2 : usableWidth * index / (points.length - 1));
          const nextX = padding + (points.length === 1 ? usableWidth / 2 : usableWidth * Math.min(index + 1, points.length - 1) / Math.max(points.length - 1, 1));
          const barWidth = Math.max(Math.min(nextX - x, 6), 2);
          const volume = point.volume ?? 0;
          const barHeight = volume / maxVolume * volumeHeight;
          const isRise = (point.close ?? 0) >= (point.open ?? point.close ?? 0);

          return (
            <g key={point.timestamp}>
              <rect
                x={x - barWidth / 2}
                y={height - padding - barHeight}
                width={barWidth}
                height={barHeight}
                fill={isRise ? '#1b8d72' : '#cc5a54'}
                opacity="0.42"
                rx="1"
              />
              {index === 0 || point.date !== points[index - 1].date ? (
                <>
                  <line x1={x} y1={padding} x2={x} y2={height - padding} className={styles.dayDivider} />
                  <text x={x + 4} y={padding + 14} className={styles.axisText}>{point.date.slice(5)}</text>
                </>
              ) : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
};

const DistributionChart = ({ bins, mode }: { bins: DistributionBin[]; mode: 'buy-sell' | 'price' }) => {
  if (!bins.length) {
    return <div className={styles.emptyBlock}>暂无分布数据</div>;
  }

  const maxValue = Math.max(
    ...bins.map((bin) => mode === 'buy-sell'
      ? (bin.buy_volume ?? 0) + (bin.sell_volume ?? 0) + (bin.neutral_volume ?? 0)
      : bin.volume ?? bin.count ?? 0),
    1,
  );

  return (
    <div className={styles.distributionList}>
      {bins.map((bin) => {
        const buy = bin.buy_volume ?? 0;
        const sell = bin.sell_volume ?? 0;
        const neutral = bin.neutral_volume ?? 0;
        const total = buy + sell + neutral;
        const priceValue = bin.volume ?? bin.count ?? 0;
        return (
          <div key={`${mode}-${bin.label}`} className={styles.distributionRow}>
            <div className={styles.distributionLabel}>{bin.label}</div>
            <div className={styles.distributionBarTrack}>
              {mode === 'buy-sell' ? (
                <>
                  <div className={styles.buyBar} style={{ width: `${buy / maxValue * 100}%` }} />
                  <div className={styles.neutralBar} style={{ width: `${neutral / maxValue * 100}%` }} />
                  <div className={styles.sellBar} style={{ width: `${sell / maxValue * 100}%` }} />
                </>
              ) : (
                <div className={styles.priceBar} style={{ width: `${priceValue / maxValue * 100}%` }} />
              )}
            </div>
            <div className={styles.distributionValue}>
              {mode === 'buy-sell' ? formatLargeNumber(total) : formatLargeNumber(priceValue)}
            </div>
          </div>
        );
      })}
    </div>
  );
};

const StockGraphView = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<Graph | null>(null);
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const queryType = searchParams.get('queryType');
  const keyword = searchParams.get('keyword');
  const [detail, setDetail] = useState<StockDetailResponse | null>(null);
  const [graphData, setGraphData] = useState<GraphDataset | null>(null);
  const [status, setStatus] = useState<'loading' | 'ready' | 'empty' | 'error'>('loading');
  const [message, setMessage] = useState('正在加载上市公司详情...');
  const [selectedTradeDate, setSelectedTradeDate] = useState('');

  useEffect(() => {
    let cancelled = false;

    const fetchDetail = async () => {
      if (!queryType || !keyword) {
        setStatus('error');
        setMessage('缺少查询参数');
        return;
      }

      setStatus('loading');
      setMessage('正在加载上市公司详情...');
      setGraphData(null);

      try {
        const { data } = await axios.get<StockDetailResponse>('/api/stock/detail', {
          params: { query_type: queryType, keyword },
        });

        if (cancelled) {
          return;
        }

        if (!data.matched) {
          navigate(`/graph?queryType=${queryType}&keyword=${encodeURIComponent(keyword)}`, { replace: true });
          return;
        }

        setDetail(data);
        setSelectedTradeDate(data.market.daily_distributions[data.market.daily_distributions.length - 1]?.date || '');

        if (data.has_data && data.graph.nodes?.length) {
          setGraphData(normalizeGraphData(data.graph));
          setStatus('ready');
          setMessage('');
          return;
        }

        setStatus('empty');
        setMessage(data.message || '还没有图谱数据');
      } catch (error) {
        console.error('Error fetching stock detail:', error);
        if (cancelled) {
          return;
        }
        setStatus('error');
        setMessage('加载上市公司详情失败');
      }
    };

    void fetchDetail();

    return () => {
      cancelled = true;
    };
  }, [keyword, navigate, queryType]);

  useEffect(() => {
    if (status !== 'ready' || !graphData || !containerRef.current) {
      return;
    }

    const container = containerRef.current;
    const height = 620;
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
    }).catch((error) => {
      console.error('Error rendering graph:', error);
      if (graphRef.current === graph) {
        graphRef.current = null;
      }
      setStatus('error');
      setMessage('图谱渲染失败');
      graph.destroy();
    });

    graph.on('node:dblclick', (event: any) => {
      const clickedNodeId = getEventNodeId(event);
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

          graphRef.current.addData({ nodes: newNodes, edges: newEdges });
          void graphRef.current.render();
        })
        .catch(error => console.error('Error expanding node:', error));
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

  const dailySeries = detail?.market?.daily_series;
  const latestDaily = dailySeries?.[dailySeries.length - 1] ?? null;
  const distributions = detail?.market.daily_distributions || [];
  const selectedDistribution = distributions.find((item) => item.date === selectedTradeDate) || distributions[distributions.length - 1] || null;
  const badgeClass = status === 'loading'
    ? `${styles.badge} ${styles.badgeLoading}`
    : status === 'ready'
      ? `${styles.badge} ${styles.badgeReady}`
      : status === 'empty'
        ? `${styles.badge} ${styles.badgeEmpty}`
        : `${styles.badge} ${styles.badgeError}`;
  const statusLabel = status === 'loading' ? '加载中' : status === 'ready' ? '图谱可用' : status === 'empty' ? '暂无图谱' : '加载失败';

  return (
    <div className={styles.page}>
      <div className={styles.hero}>
        <div className={styles.heroCopy}>
          <span className={styles.eyebrow}>Listed Company Detail</span>
          <h1 className={styles.title}>{detail?.stock?.name || '上市公司详情'}</h1>
          <p className={styles.subtitle}>
            {detail?.stock?.full_name || '命中上市公司目录后，当前页会在关系图谱之外补充 K 线、5 日量价曲线和交易分布。'}
          </p>
        </div>
        <div className={styles.heroStats}>
          <div className={styles.heroStatCard}>
            <span>股票代码</span>
            <strong>{detail?.stock ? `${detail.stock.code}.${detail.stock.exchange}` : '--'}</strong>
          </div>
          <div className={styles.heroStatCard}>
            <span>知识图谱节点</span>
            <strong>{detail?.graph?.nodes?.length || 0}</strong>
          </div>
          <div className={styles.heroStatCard}>
            <span>最近收盘</span>
            <strong>{formatNumber(latestDaily?.close)}</strong>
          </div>
        </div>
      </div>

      <div className={styles.noticeRow}>
        <p className={styles.noticeText}>提示：上市公司进入当前增强页；如果 Neo4j 暂无图谱数据，行情模块仍会保留并继续展示。</p>
        <span className={badgeClass}>{statusLabel}</span>
      </div>

      {detail?.market.warnings?.length ? (
        <div className={styles.warningPanel}>
          {detail.market.warnings.map((warning) => (
            <div key={warning} className={styles.warningItem}>{warning}</div>
          ))}
        </div>
      ) : null}

      <div className={styles.metricGrid}>
        <div className={styles.metricCard}>
          <span className={styles.metricLabel}>最近交易日</span>
          <strong className={styles.metricValue}>{latestDaily?.date || '--'}</strong>
          <p className={styles.metricHint}>最新一条日线时间</p>
        </div>
        <div className={styles.metricCard}>
          <span className={styles.metricLabel}>涨跌幅</span>
          <strong className={styles.metricValue}>{latestDaily?.change_pct !== null && latestDaily?.change_pct !== undefined ? `${formatNumber(latestDaily.change_pct)}%` : '--'}</strong>
          <p className={styles.metricHint}>最近收盘相对前一日</p>
        </div>
        <div className={styles.metricCard}>
          <span className={styles.metricLabel}>成交量</span>
          <strong className={styles.metricValue}>{formatLargeNumber(latestDaily?.volume)}</strong>
          <p className={styles.metricHint}>最近一日累计成交量</p>
        </div>
        <div className={styles.metricCard}>
          <span className={styles.metricLabel}>成交额</span>
          <strong className={styles.metricValue}>{formatLargeNumber(latestDaily?.amount)}</strong>
          <p className={styles.metricHint}>最近一日累计成交额</p>
        </div>
      </div>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <span className={styles.sectionEyebrow}>K Line</span>
            <h2 className={styles.sectionTitle}>长周期 K线与均线</h2>
          </div>
          <p className={styles.sectionDesc}>同一张图中展示 2021-01-01 以来的日线价格图，并叠加 5 日、10 日、20 日均线；默认定位到最近 60 个交易日。</p>
        </div>
        <RollingCandleChart points={detail?.market.daily_series || []} />
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <span className={styles.sectionEyebrow}>Intraday</span>
            <h2 className={styles.sectionTitle}>5日量价曲线图</h2>
          </div>
          <p className={styles.sectionDesc}>按时间顺序串联最近 5 个交易日的 1 分钟数据，折线展示价格与均价，柱体展示成交量。</p>
        </div>
        <IntradayChart points={detail?.market.intraday_series || []} />
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <span className={styles.sectionEyebrow}>Distribution</span>
            <h2 className={styles.sectionTitle}>最近5日每日交易分布</h2>
          </div>
          <p className={styles.sectionDesc}>基于分钟级量价序列，按价格区间聚合买入、卖出与中性成交量，并同步展示价格统计分布。</p>
        </div>

        {distributions.length ? (
          <>
            <div className={styles.tradeDateTabs}>
              {distributions.map((item) => (
                <button
                  key={item.date}
                  type="button"
                  className={item.date === selectedTradeDate ? styles.tradeDateTabActive : styles.tradeDateTab}
                  onClick={() => setSelectedTradeDate(item.date)}
                >
                  {item.date}
                </button>
              ))}
            </div>
            <div className={styles.distributionSummary}>
              <div className={styles.summaryChip}>开盘 {formatNumber(selectedDistribution?.summary?.open)}</div>
              <div className={styles.summaryChip}>收盘 {formatNumber(selectedDistribution?.summary?.close)}</div>
              <div className={styles.summaryChip}>高点 {formatNumber(selectedDistribution?.summary?.high)}</div>
              <div className={styles.summaryChip}>低点 {formatNumber(selectedDistribution?.summary?.low)}</div>
              <div className={styles.summaryChip}>总量 {formatLargeNumber(selectedDistribution?.summary?.total_volume)}</div>
            </div>
            <div className={styles.distributionGrid}>
              <div className={styles.distributionCard}>
                <div className={styles.chartCardHeader}>
                  <h3>买入 / 卖出量分布</h3>
                  <span>按价格区间聚合</span>
                </div>
                <DistributionChart bins={selectedDistribution?.buy_sell_bins || []} mode="buy-sell" />
              </div>
              <div className={styles.distributionCard}>
                <div className={styles.chartCardHeader}>
                  <h3>价格统计分布</h3>
                  <span>以量能权重展示</span>
                </div>
                <DistributionChart bins={selectedDistribution?.price_histogram || []} mode="price" />
              </div>
            </div>
          </>
        ) : (
          <div className={styles.emptyBlock}>最近 5 日交易分布暂不可用</div>
        )}
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <span className={styles.sectionEyebrow}>Graph</span>
            <h2 className={styles.sectionTitle}>公司关系图谱</h2>
          </div>
          <p className={styles.sectionDesc}>
            二跳关系图谱（{graphData?.nodes.length ?? 0} 节点 / {graphData?.edges.length ?? 0} 边）
            {graphData && graphData.nodes.length > 300 && ' — 节点较多，已关闭标签以提升性能，滚轮放大可查看细节'}
          </p>
        </div>
        <div className={styles.graphShell}>
          {status === 'ready' ? (
            <div ref={containerRef} className={styles.canvas} />
          ) : (
            <div className={styles.statusPanel}>
              <div className={`${styles.statusCard} ${status === 'error' ? styles.statusError : ''}`}>
                <h3 className={styles.statusTitle}>{status === 'empty' ? '还没有图谱数据' : status === 'error' ? '加载图谱失败' : '正在准备图谱'}</h3>
                <p className={styles.statusMessage}>{message}</p>
              </div>
            </div>
          )}
        </div>
      </section>
    </div>
  );
};

export default StockGraphView;