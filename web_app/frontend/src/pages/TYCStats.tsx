import React, { useEffect, useState } from 'react';
import axios from 'axios';
import styles from './TYCStats.module.css';

interface StatRecord {
  total_records: number;
  src_stats: [string, number, number][]; // [source, count, visits]
  recent_records: [string, string, string, string, number][]; // [source, id, type, time, visits]
  error?: string;
}

export default function TYCStats(){
  const [stats, setStats] = useState<StatRecord | null>(null);
  const [errorHeader, setErrorHeader] = useState<string | null>(null);

  useEffect(()=>{
    const fetchStats = async ()=>{
      try{
        const resp = await axios.get('/api/tyc/stats');
        setStats(resp.data);
      }catch(e: any){
        const msg = e.response?.data?.detail || e.message || '获取失败';
        setErrorHeader(msg);
        setStats({ error: msg } as any);
      }
    }
    fetchStats();
  },[])

  if(!stats && !errorHeader) return <div className={styles.loading}>加载中...</div>
  if(stats?.error || errorHeader) return <div className={styles.error}>错误: {stats?.error || errorHeader}</div>

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h2 className={styles.title}>📊 爬取统计</h2>
      </div>

      <div className={styles.statGrid}>
        <div className={styles.statCard}>
          <span className={styles.statLabel}>总记录数</span>
          <div className={styles.statValue}>{stats.total_records.toLocaleString()}</div>
        </div>

        {stats.src_stats.map((s,i)=>(
           <div key={i} className={styles.statCard}>
             <span className={styles.statLabel}>{s[0] || 'Default'}</span>
             <div className={styles.statValue}>{s[1].toLocaleString()} <small style={{fontSize:12, color:'#999'}}>条</small></div>
             <div style={{fontSize:12, color:'#888', marginTop: 4}}>总访问次数: {s[2]}</div>
           </div>
        ))}
      </div>

      <h3>最近爬取记录 (Top 20)</h3>
      <div className={styles.tableContainer}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>来源</th>
              <th>ID</th>
              <th>类型</th>
              <th>时间</th>
              <th>访问次数</th>
            </tr>
          </thead>
          <tbody>
            {stats.recent_records.length > 0 ? (
                stats.recent_records.map((r,i)=>(
                  <tr key={i}>
                    <td>{r[0]}</td>
                    <td style={{fontFamily:'monospace', color:'#0366d6'}}>{r[1]}</td>
                    <td>{r[2]}</td>
                    <td>{r[3]}</td>
                    <td>{r[4]}</td>
                  </tr>
                ))
            ) : (
                <tr><td colSpan={5} style={{textAlign:'center', color: '#999', padding: 20}}>暂无数据</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
