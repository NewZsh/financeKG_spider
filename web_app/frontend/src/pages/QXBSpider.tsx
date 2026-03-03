import React, { useEffect, useState } from 'react';
import axios from 'axios';
import styles from './QXBSpider.module.css';

export default function QXBSpider() {
  const [status, setStatus] = useState<any>(null);
  useEffect(() => {
    axios.get('/api/qxb_spider/').then(r => setStatus(r.data));
  }, []);

  if (!status) return <div className={styles.loading}>加载中...</div>;

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <h2 className={styles.title}>🕷️ QXB Spider 状态</h2>
      </header>
      
      <div className={styles.statusCard}>
        <div className={styles.statusRow}>
          <span className={styles.label}>运行状态</span>
          <span className={styles.value}>
            <span className={`${styles.badge} ${status.status === 'running' ? styles.running : styles.stopped}`}>
              {status.status}
            </span>
          </span>
        </div>
        <div className={styles.statusRow}>
          <span className={styles.label}>说明信息</span>
          <span className={styles.value}>{status.message}</span>
        </div>
      </div>
    </div>
  );
}
