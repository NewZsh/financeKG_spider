import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import Home from './pages/Home';
import TYCKeywords from './pages/TYCKeywords';
import TYcStats from './pages/TYCStats';
import Config from './pages/Config';
import GraphView from './pages/GraphView';

export default function App(){
  return (
    <BrowserRouter>
      <div style={{padding:20}}>
        <Routes>
          <Route path='/' element={<Home/>} />
          <Route path='/config' element={<Config/>} />
          <Route path='/tyc/keywords' element={<TYCKeywords/>} />
          <Route path='/tyc/stats' element={<TYcStats/>} />
          <Route path='/graph' element={<GraphView/>} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

// import QXBSpider from './pages/QXBSpider';
//         <Route path='/qxb_spider' element={<QXBSpider/>} />
