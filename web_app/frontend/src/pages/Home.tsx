import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import axios from 'axios';
import styles from './Home.module.css';

const API_BASE = 'http://localhost:8000';

export default function Home() {
  const [keyword, setKeyword] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [examples, setExamples] = useState<any[]>([]);
  const navigate = useNavigate();

  useEffect(() => {
    // 获取 10 个展示例子
    axios.get(`${API_BASE}/api/graph/examples`)
      .then(res => setExamples(res.data.examples))
      .catch(err => console.error(err));
  }, []);

  const handleSearch = async () => {
    if (!keyword) return;
    try {
      const { data } = await axios.get(`${API_BASE}/api/graph/search?keyword=${keyword}`);
      setSearchResults(data.results);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className={styles.dashboard}>
      <div className={styles.center}>
        <h1 className={styles.title}>🕷️ FinanceKG Spider Dashboard</h1>
        <p className={styles.subtitle}>全新 Web 仪表盘，前后端分离，数据可视化与管理</p>
      </div>

      {/* 查询图谱区 */}
      <div style={{ marginBottom: '30px', background: '#fff', padding: '20px', borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)' }}>
        <h2>知识图谱查询</h2>
        <div style={{ display: 'flex', gap: '10px', marginBottom: '15px' }}>
          <input 
            value={keyword} 
            onChange={e => setKeyword(e.target.value)} 
            placeholder="输入公司名称进行模糊查询..." 
            style={{ flex: 1, padding: '8px', fontSize: '16px' }}
            onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
          />
          <button onClick={handleSearch} className={styles.btn}>检索 Top 10</button>
        </div>
        
        {searchResults.length > 0 && (
          <ul style={{ listStyle: 'none', padding: 0 }}>
            {searchResults.map((company) => (
              <li 
                key={company.id} 
                onClick={() => navigate(`/graph?id=${company.id}`)}
                style={{ padding: '8px 12px', background: '#f5f5f5', marginBottom: '8px', cursor: 'pointer', borderRadius: '4px', borderLeft: '4px solid #5B8FF9' }}
              >
                {company.name} <span style={{ color: '#999', fontSize: '0.8em' }}>({company.id})</span>
              </li>
            ))}
          </ul>
        )}

        <h3 style={{ marginTop: '20px' }}>演示样例（点击可直接查看关系图）：</h3>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px' }}>
          {examples.map((ex) => (
            <button 
              key={ex.id} 
              onClick={() => navigate(`/graph?id=${ex.id}`)}
              style={{ background: '#e6f7ff', border: '1px solid #91d5ff', padding: '6px 12px', borderRadius: '16px', cursor: 'pointer', color: '#096dd9' }}
            >
              🏢 {ex.name}
            </button>
          ))}
          {examples.length === 0 && <span style={{color: '#999'}}>暂无数据 / Backend Not Ready</span>}
        </div>
      </div>

      <div className={styles.cards}>
        <div className={styles.card}>
          <h2 className={styles.cardTitle}>配置管理</h2>
          <p className={styles.cardDesc}>查看和管理爬虫配置项</p>
          <Link to="/config"><button className={styles.btn}>进入配置</button></Link>
        </div>
        <div className={styles.card}>
          <h2 className={styles.cardTitle}>爬虫管理</h2>
          <p className={styles.cardDesc}>QXB Spider 状态监控</p>
          <Link to="/qxb_spider"><button className={styles.btn}>查看状态</button></Link>
        </div>
        <div className={styles.card}>
          <h2 className={styles.cardTitle}>天眼查爬虫</h2>
          <p className={styles.cardDesc}>关键词管理与爬取统计</p>
          <Link to="/tyc/keywords"><button className={styles.btn}>关键词管理</button></Link>
          <Link to="/tyc/stats"><button className={`${styles.btn} ${styles.btnGreen}`}>爬取统计</button></Link>
        </div>
      </div>
    </div>
  );
}
