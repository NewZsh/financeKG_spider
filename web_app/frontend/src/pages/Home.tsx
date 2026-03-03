import React from 'react';
import { Link } from 'react-router-dom';
import styles from './Home.module.css';

export default function Home() {
  return (
    <div className={styles.dashboard}>
      <div className={styles.center}>
        <h1 className={styles.title}>🕷️ FinanceKG Spider Dashboard</h1>
        <p className={styles.subtitle}>全新 Web 仪表盘，前后端分离，数据可视化与管理</p>
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
