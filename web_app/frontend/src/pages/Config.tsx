import React, { useEffect, useState } from 'react';
import axios from 'axios';
import styles from './Config.module.css';

export default function Config() {
  const [cfg, setCfg] = useState<any>(null);
  const [editMode, setEditMode] = useState(false);
  const [cfgText, setCfgText] = useState('');
  const [status, setStatus] = useState('');

  useEffect(() => {
    axios.get('/api/config/').then(r => {
      setCfg(r.data);
      setCfgText(JSON.stringify(r.data, null, 2));
    });
  }, []);

  const handleSave = async () => {
    setStatus('保存中...');
    try {
      // 校验 JSON
      JSON.parse(cfgText);
      const formData = new FormData();
      formData.append('config', cfgText);
      const res = await axios.post('/api/config/', formData);
      if (res.data.success) {
        setStatus('✅ 配置已保存');
        setEditMode(false);
        setCfg(JSON.parse(cfgText));
        setTimeout(() => setStatus(''), 2000);
      } else {
        setStatus('❌ ' + res.data.error);
      }
    } catch (e: any) {
      setStatus('❌ JSON 格式错误: ' + e.message);
    }
  };

  if (!cfg) return <div className={styles.loading}>加载中...</div>;

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <h2 className={styles.title}>系统配置</h2>
        <button className={styles.editBtn} onClick={() => setEditMode(!editMode)}>
          {editMode ? '取消编辑' : '编辑配置'}
        </button>
      </header>
      <div className={styles.configCard}>
        {editMode ? (
          <>
            <textarea
              className={styles.textarea}
              value={cfgText}
              onChange={e => setCfgText(e.target.value)}
              rows={20}
              spellCheck={false}
            />
            <div className={styles.actions}>
              <button className={styles.saveBtn} onClick={handleSave}>保存</button>
              <span className={status.startsWith('✅') ? styles.success : styles.error}>{status}</span>
            </div>
          </>
        ) : (
          <table className={styles.table}>
            <tbody>
              {Object.entries(cfg).map(([k,v]) => (
                <tr key={k}>
                  <td className={styles.keyCell}>{k}</td>
                  <td className={styles.valueCell}>{typeof v === 'object' ? JSON.stringify(v, null, 2) : String(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
