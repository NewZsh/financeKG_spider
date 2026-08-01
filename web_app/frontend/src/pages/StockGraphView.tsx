import React, { useEffect, useRef, useState } from 'react';
import * as echarts from 'echarts';
import axios from 'axios';
import { useSearchParams, useNavigate, Link } from 'react-router-dom';
import styles from './StockGraphView.module.css';

type Period = 'm1' | 'm5' | 'm15' | 'm30' | 'm60' | 'day' | 'week' | 'month';

export default function StockGraphView() {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const queryType = params.get('queryType') || 'code';
  const keyword = params.get('keyword') || '';

  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<echarts.ECharts | null>(null);
  const [period, setPeriod] = useState<Period>('day');
  const [loading, setLoading] = useState(false);
  const [stockInfo, setStockInfo] = useState<{name: string, code: string} | null>(null);

  useEffect(() => {
    if (!keyword) return;
    axios.get('/api/stock/detail', { params: { query_type: queryType, keyword } })
      .then(res => {
        if (!res.data.matched) {
          navigate(/);
        } else {
          setStockInfo(res.data.stock);
        }
      });
  }, [queryType, keyword, navigate]);

  useEffect(() => {
    if (!stockInfo) return;
    setLoading(true);
    axios.get(/api/ta/\?period=\)
      .then(res => {
        if (res.data.error) {
          alert('暂无数据');
          return;
        }
        renderChart(res.data);
      })
      .finally(() => setLoading(false));
  }, [stockInfo, period]);

  const renderChart = (data: any) => {
    if (!chartRef.current) return;
    if (!chartInstance.current) {
      chartInstance.current = echarts.init(chartRef.current);
    }

    const { ohlcv, indicators, chanlun } = data;

    const dates = ohlcv.map((d: any) => d.datetime);
    const kData = ohlcv.map((d: any) => [d.open, d.close, d.low, d.high]);
    
    // volume colored based on daily return
    const volumeData = ohlcv.map((d: any, i: number) => {
      const isUp = d.close >= d.open;
      return {
        value: [i, d.volume, isUp ? 1 : -1],
        itemStyle: {
          color: isUp ? '#ef232a' : '#14b143'
        }
      };
    });

    const markPointData = chanlun.points.map((pt: any) => {
      const idx = pt[0];
      const type = pt[1];
      const direction = pt[2];
      const item = ohlcv[idx];
      if (!item) return null;
      const isBuy = direction === "buy";
      return {
        name: \\\\,
        coord: [item.datetime, isBuy ? item.low : item.high],
        value: \\\\,
        itemStyle: { color: isBuy ? "#c23531" : "#14b143" },
        symbol: "path://M0,0 L10,0 L5,10 Z",  // Triangle
        symbolRotate: isBuy ? 180 : 0, // buy pointer points up (inverted triangle), sell points down
        symbolSize: 12,
        label: {
          show: true,
          position: isBuy ? "bottom" : "top",
          formatter: "{c}",
          fontSize: 10,
          color: isBuy ? "#c23531" : "#14b143"
        }
      };
    }).filter(Boolean);

    // MACD data
    const difData = indicators.map((d: any) => d.dif);
    const deaData = indicators.map((d: any) => d.dea);
    const macdData = indicators.map((d: any, i: number) => {
      const val = d.macd;
      return {
        value: val,
        itemStyle: { color: val > 0 ? '#ef232a' : '#14b143' }
      };
    });

    const option = {
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        borderWidth: 1,
        borderColor: '#ccc',
        padding: 10,
        textStyle: { color: '#000' }
      },
      axisPointer: {
        link: [{ xAxisIndex: 'all' }],
        label: { backgroundColor: '#777' }
      },
      toolbox: {
        feature: {
          dataZoom: { yAxisIndex: false },
          brush: { type: ['lineX', 'clear'] }
        }
      },
      legend: {
        data: ['K线', 'MA5', 'MA20', 'MA60', 'BOLL上轨', 'BOLL下轨'],
        top: 0
      },
      grid: [
        { left: '5%', right: '3%', top: '8%', height: '45%' },     // K-line
        { left: '5%', right: '3%', top: '56%', height: '12%' },    // Volume
        { left: '5%', right: '3%', top: '71%', height: '12%' },    // MACD
        { left: '5%', right: '3%', top: '86%', height: '12%' }     // RSI
      ],
      xAxis: [
        { type: 'category', data: dates, boundaryGap: false, axisLine: { onZero: false }, splitLine: { show: false }, min: 'dataMin', max: 'dataMax' },
        { type: 'category', gridIndex: 1, data: dates, boundaryGap: false, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { lineStyle: { color: '#777' } }, min: 'dataMin', max: 'dataMax', axisPointer: { type: 'shadow' } },
        { type: 'category', gridIndex: 2, data: dates, boundaryGap: false, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { lineStyle: { color: '#777' } }, min: 'dataMin', max: 'dataMax' },
        { type: 'category', gridIndex: 3, data: dates, boundaryGap: false, axisLabel: { show: false }, axisTick: { show: false }, axisLine: { lineStyle: { color: '#777' } }, min: 'dataMin', max: 'dataMax' }
      ],
      yAxis: [
        { scale: true, splitArea: { show: true } },
        { scale: true, gridIndex: 1, splitNumber: 2, axisLabel: { show: false }, axisLine: { show: false }, axisTick: { show: false }, splitLine: { show: false } },
        { scale: true, gridIndex: 2, splitNumber: 2, axisLabel: { show: false }, axisLine: { show: false }, axisTick: { show: false }, splitLine: { show: false } },
        { scale: true, gridIndex: 3, splitNumber: 2, axisLabel: { show: false }, axisLine: { show: false }, axisTick: { show: false }, splitLine: { show: false } }
      ],
      dataZoom: [
        { type: 'inside', xAxisIndex: [0, 1, 2, 3], start: 50, end: 100 },
        { show: true, xAxisIndex: [0, 1, 2, 3], type: 'slider', bottom: '0%', start: 50, end: 100 }
      ],
      series: [
        {
          name: 'K线', type: 'candlestick', data: kData,
          itemStyle: { color: '#ef232a', color0: '#14b143', borderColor: '#ef232a', borderColor0: '#14b143' },
          markPoint: {
            data: markPointData,
            tooltip: { formatter: '{b}' }
          }
        },
        { name: 'MA5', type: 'line', data: indicators.map((d: any) => d.ma5), smooth: true, showSymbol: false, lineStyle: { width: 1 } },
        { name: 'MA20', type: 'line', data: indicators.map((d: any) => d.ma20), smooth: true, showSymbol: false, lineStyle: { width: 1 } },
        { name: 'MA60', type: 'line', data: indicators.map((d: any) => d.ma60), smooth: true, showSymbol: false, lineStyle: { width: 1 } },
        { name: 'BOLL上轨', type: 'line', data: indicators.map((d: any) => d.boll_up), smooth: true, showSymbol: false, lineStyle: { width: 1, type: 'dashed' } },
        { name: 'BOLL下轨', type: 'line', data: indicators.map((d: any) => d.boll_dn), smooth: true, showSymbol: false, lineStyle: { width: 1, type: 'dashed' } },
        
        { name: 'Volume', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: volumeData },
        
        { name: 'MACD', type: 'bar', xAxisIndex: 2, yAxisIndex: 2, data: macdData },
        { name: 'DIF', type: 'line', xAxisIndex: 2, yAxisIndex: 2, data: difData, showSymbol: false, lineStyle: { width: 1 } },
        { name: 'DEA', type: 'line', xAxisIndex: 2, yAxisIndex: 2, data: deaData, showSymbol: false, lineStyle: { width: 1 } },

        { name: 'RSI14', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: indicators.map((d: any) => d.rsi14), showSymbol: false, lineStyle: { width: 1 } },
      ]
    };

    chartInstance.current.setOption(option, true);
  };

  useEffect(() => {
    const handleResize = () => chartInstance.current?.resize();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  const periodOptions: { value: Period; label: string }[] = [
    { value: 'm1', label: '1分钟' },
    { value: 'm5', label: '5分钟' },
    { value: 'm15', label: '15分钟' },
    { value: 'm30', label: '30分钟' },
    { value: 'm60', label: '60分钟' },
    { value: 'day', label: '日线' },
    { value: 'week', label: '周线' },
    { value: 'month', label: '月线' },
  ];

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <div className={styles.logoGroup}>
          <Link to="/" className={styles.backLink}>← 返回</Link>
          <h2>{stockInfo ? \\ (\)\ : '加载中...'}</h2>
        </div>
        <div className={styles.periodSelector}>
          {periodOptions.map(p => (
            <button
              key={p.value}
              className={\\ \\}
              onClick={() => setPeriod(p.value)}
            >
              {p.label}
            </button>
          ))}
        </div>
      </header>

      <main className={styles.main}>
        {loading && <div className={styles.loading}>数据加载/计算中...</div>}
        <div ref={chartRef} className={styles.chartArea} style={{ opacity: loading ? 0.5 : 1 }}></div>
      </main>
    </div>
  );
}
