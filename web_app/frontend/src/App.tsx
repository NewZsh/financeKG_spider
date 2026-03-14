import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import Home from './pages/Home';
import TYCKeywords from './pages/TYCKeywords';
import TYcStats from './pages/TYCStats';
import Config from './pages/Config';
import QXBSpider from './pages/QXBSpider';
import GraphView from './pages/GraphView';

export default function App(){
  return (
    <BrowserRouter>
      <div style={{padding:20}}>
        <h1>FinanceKG Dashboard (Web App)</h1>
        <nav style={{marginBottom:10}}>
          <Link to="/">Home</Link> | <Link to="/tyc/keywords">关键词管理</Link> | <Link to="/tyc/stats">爬取统计</Link> | <Link to="/graph">关系图谱</Link>
        </nav>
        <Routes>
          <Route path='/' element={<Home/>} />
          <Route path='/config' element={<Config/>} />
          <Route path='/qxb_spider' element={<QXBSpider/>} />
          <Route path='/tyc/keywords' element={<TYCKeywords/>} />
          <Route path='/tyc/stats' element={<TYcStats/>} />
          <Route path='/graph' element={<GraphView/>} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}
