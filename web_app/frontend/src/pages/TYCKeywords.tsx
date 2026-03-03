import React, { useState, useEffect } from 'react';
import axios from 'axios';
import styles from './TYCKeywords.module.css';

export default function TYCKeywords(){
  const [file, setFile] = useState<File | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [files, setFiles] = useState<string[]>([]);
  const [kwStats, setKwStats] = useState<{total:number,finished:number} | null>(null);

  const refresh = async ()=>{
    try{
      const resp = await axios.get('/api/tyc/files');
      setFiles(resp.data);
      const statsResp = await axios.get('/api/tyc/keywords/stats');
      setKwStats(statsResp.data);
    }catch(e){
      console.error(e);
    }
  }

  const handleUpload = async ()=>{
    if(!file) return;
    const fd = new FormData();
    fd.append('file', file);
    try{
      const resp = await axios.post('/api/tasks/tyc/upload', fd, {
        headers: {'Content-Type': 'multipart/form-data'}
      });
      setMessage(resp.data.message || '上传成功');
      await refresh();
      setFile(null); // Reset input
    }catch(e:any){
      setMessage(e?.response?.data?.detail || e.message || '上传失败');
    }
  }

  const handleRemove = async (name:string)=>{
    if(!window.confirm('删除 ' + name + ' ?')) return;
    try{
      await axios.delete(`/api/tyc/files/${encodeURIComponent(name)}`);
      setMessage('已删除 '+name);
      await refresh();
    }catch(e:any){
      setMessage(e?.response?.data?.detail || e.message || '删除失败');
    }
  }

  useEffect(()=>{refresh();},[]);

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <h2 className={styles.title}>天眼查 - 关键词管理</h2>
      </header>

      {kwStats && (
        <div className={styles.statsSection}>
          <div className={styles.statCard}>
            <span className={styles.statLabel}>关键词总数</span>
            <div className={styles.statValue}>{kwStats.total.toLocaleString()}</div>
          </div>
          <div className={styles.statCard}>
            <span className={styles.statLabel}>已完成关键词数</span>
            <div className={styles.statValue}>{kwStats.finished.toLocaleString()}</div>
          </div>
          <div className={styles.statCard}>
            <span className={styles.statLabel}>完成进度</span>
            <div className={styles.statValue}>
              {kwStats.total > 0 ? ((kwStats.finished / kwStats.total) * 100).toFixed(1) : 0}%
            </div>
          </div>
        </div>
      )}

      <div className={styles.uploadSection}>
        <h3 className={styles.uploadTitle}>上传新关键词文件</h3>
        <input 
          type="file" 
          accept=".txt" 
          onChange={e=>setFile(e.target.files?e.target.files[0]:null)} 
          className={styles.fileInput}
        />
        <button 
          onClick={handleUpload} 
          className={styles.uploadBtn}
          disabled={!file}
        >
          上传
        </button>
        {message && <div className={styles.message}>{message}</div>}
      </div>

      <div className={styles.fileListSection}>
        <h3 className={styles.fileListTitle}>已有文件列表</h3>
        {files.length === 0 ? (
          <div style={{padding: '24px', textAlign: 'center', color: '#666'}}>暂无文件</div>
        ) : (
          <ul className={styles.fileList}>
            {files.map(f => (
              <li key={f} className={styles.fileItem}>
                <span className={styles.fileName}>{f}</span>
                <button onClick={() => handleRemove(f)} className={styles.deleteBtn}>
                  删除
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
