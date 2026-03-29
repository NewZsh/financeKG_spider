import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import axios from 'axios';
import styles from './Home.module.css';

const API_BASE = 'http://localhost:8000';

type QueryType = 'code' | 'name';

type ExampleCompany = {
  id: string;
  name: string;
};

type DashboardLink = {
  to: string;
  label: string;
  className?: string;
};

type DashboardCard = {
  title: string;
  description: string;
  links: DashboardLink[];
};

const dashboardCards: DashboardCard[] = [
  {
    title: '配置管理',
    description: '查看和管理爬虫配置项',
    links: [{ to: '/config', label: '进入配置' }],
  },
  {
    title: '爬虫管理',
    description: 'QXB Spider 状态监控',
    links: [{ to: '/qxb_spider', label: '查看状态' }],
  },
  {
    title: '天眼查爬虫',
    description: '关键词管理与爬取统计',
    links: [
      { to: '/tyc/keywords', label: '关键词管理' },
      { to: '/tyc/stats', label: '爬取统计', className: styles.btnGreen },
    ],
  },
];

export default function Home() {
  const [keyword, setKeyword] = useState('');
  const [queryType, setQueryType] = useState<QueryType>('code');
  const [examples, setExamples] = useState<ExampleCompany[]>([]);
  const [errorMessage, setErrorMessage] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    axios.get(`${API_BASE}/api/graph/examples`)
      .then(res => setExamples(res.data.examples))
      .catch(err => console.error(err));
  }, []);

  const handleSearch = () => {
    const trimmedKeyword = keyword.trim();
    if (!trimmedKeyword) {
      setErrorMessage(queryType === 'code' ? '请输入 6 位股票代码' : '请输入公司名称');
      return;
    }

    if (queryType === 'code' && !/^\d{6}$/.test(trimmedKeyword)) {
      setErrorMessage('股票代码必须是 6 位数字');
      return;
    }

    setErrorMessage('');
    navigate(`/graph?queryType=${queryType}&keyword=${encodeURIComponent(trimmedKeyword)}`);
  };

  const handleQueryTypeChange = (value: QueryType) => {
    setQueryType(value);
    setErrorMessage('');
  };

  const handleKeywordChange = (value: string) => {
    setKeyword(value);
    if (errorMessage) {
      setErrorMessage('');
    }
  };

  const renderHero = () => (
    <div className={styles.hero}>
      <div className={styles.heroCopy}>
        <span className={styles.eyebrow}>FinanceKG Web App</span>
        <h1 className={styles.title}>A股公司关系图谱查询</h1>
        <p className={styles.subtitle}>输入股票代码或公司名称，直接展开对应公司的二跳关系结构。</p>
      </div>
      <div className={styles.heroStat}>
        <span className={styles.heroStatLabel}>覆盖范围</span>
        <strong className={styles.heroStatValue}>上证 / 深证 / 北交所</strong>
      </div>
    </div>
  );

  const renderSearchPanel = () => (
    <div className={styles.searchPanel}>
      <div className={styles.sectionHeader}>
        <h2 className={styles.sectionTitle}>知识图谱查询</h2>
        <p className={styles.sectionDesc}>支持沪深北 A 股精确匹配。命中上市公司后会自动跳转到二跳图谱；如果数据库中还没有该公司数据，会在图谱页直接提示。</p>
      </div>
      <div className={styles.segmented}>
        <button
          type="button"
          className={queryType === 'code' ? styles.segmentActive : styles.segment}
          onClick={() => handleQueryTypeChange('code')}
        >
          股票代码
        </button>
        <button
          type="button"
          className={queryType === 'name' ? styles.segmentActive : styles.segment}
          onClick={() => handleQueryTypeChange('name')}
        >
          公司名称
        </button>
      </div>
      <div className={styles.searchRow}>
        <input
          value={keyword}
          onChange={(e) => handleKeywordChange(e.target.value)}
          placeholder={queryType === 'code' ? '输入 6 位股票代码，例如 000001' : '输入公司名称，例如 平安银行'}
          className={styles.searchInput}
          onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
        />
        <button type="button" onClick={handleSearch} className={styles.searchButton}>打开图谱</button>
      </div>
      <div className={styles.helperText}>
        {queryType === 'code' ? '代码查询要求 6 位数字，例如 000001、600000、920000。' : '公司名查询为精确匹配，会优先匹配上市公司名称。'}
      </div>
      {errorMessage && <div className={styles.errorText}>{errorMessage}</div>}
      <div className={styles.examplesBlock}>
        <h3 className={styles.examplesTitle}>演示样例</h3>
        <p className={styles.examplesHint}>点击后会直接进入图谱页，适合快速验证现有图库数据。</p>
      </div>
      <div className={styles.examplesWrap}>
        {examples.map((example) => (
          <button
            type="button"
            key={example.id}
            onClick={() => navigate(`/graph?id=${example.id}`)}
            className={styles.exampleChip}
          >
            {example.name}
          </button>
        ))}
        {examples.length === 0 && <span className={styles.emptyHint}>暂无数据 / Backend Not Ready</span>}
      </div>
    </div>
  );

  const renderCards = () => (
    <div className={styles.cards}>
      {dashboardCards.map((card) => (
        <div key={card.title} className={styles.card}>
          <h2 className={styles.cardTitle}>{card.title}</h2>
          <p className={styles.cardDesc}>{card.description}</p>
          {card.links.map((link) => (
            <Link key={link.to} to={link.to}>
              <button className={`${styles.btn}${link.className ? ` ${link.className}` : ''}`}>{link.label}</button>
            </Link>
          ))}
        </div>
      ))}
    </div>
  );

  return (
    <div className={styles.dashboard}>
      {renderHero()}
      {renderSearchPanel()}
      {renderCards()}
    </div>
  );
}
