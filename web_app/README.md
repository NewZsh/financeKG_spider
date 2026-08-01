# FinanceKG Web App

这是一个基于 FastAPI + React 构建的一体化 Web 应用，用于替代旧版的 Flask 仪表盘。它提供了爬虫任务配置、关键词管理、爬虫进度统计、Neo4j A股知识图谱查询，以及最新加入的**分时多周期缠论形态看盘**功能。

## 启动方式

### 后端启动
在 inanceKG_spider 根目录执行：
`ash
uvicorn web_app.backend.main:app --reload --port 8000
`
- 支持热重载，默认端口 8000。
- 接口包含知识图谱 (/api/graph)、股票信息 (/api/stock) 和 缠论技术分析 (/api/ta)。

### 前端启动
进入 web_app/frontend 运行：
`ash
npm install
npm run dev
`
- 前端基于 React + Vite + TypeScript 开发，默认端口 5173。
- 生产构建产物 (/dist) 可直接交由 FastAPI 后端一并挂载服务。

---

## 核心功能

### 1. 📈 缠论多周期分时看盘 (最新功能)
基于原生本地 SQLite 数据 (stock.db) 和自主集成的 	a_calc.py 算法模块，实现了从 **分钟级别(1m/5m/15m/30m/60m) 到 日/周/月级别** 的技术形态全覆盖。
- **全自动本地复权对齐**：突破了公网(如腾讯接口)分时级别不复权的痛点。系统内部会在请求分时数据时自动关联**日线前复权数据**并对位折算因子，算出精准复权的分时形态。
- **本地算法渲染**：集成均线(MA)、MACD、布林带(BOLL)、RSI 等基础指标，并原生嵌入**缠论分型、笔、线段、中枢与背驰判定**，生成一、二、三类买卖点。
- **高性能前端交互**：抛弃繁重的静态出图，采用 ECharts 构建联动式画板系统。支持买卖点 markPoint 高光标记（三角形置于波峰/波谷），支持鼠标滚轮自如推拉平移时间轴轴，且全盘渲染在浏览器本地进行极速重绘。

### 2. 🕸️ A股知识图谱查询 (Neo4j)
- **图谱查询**：支持通过股票代码 (如000001) 或 公司名称 (如平安银行) 进行精确检索。
- **二跳脉络展开**：使用 @antv/g6 渲染可视化知识图谱，默认展示关联节点的二跳关系网络，支持双击节点动态拓展关系层级。

### 3. ⚙️ 系统配置 & 爬虫管理
- **在线配置编辑**：前端支持在线以 JSON 形式检视和修改爬虫与系统配置 (config.yaml)，自动保存并热更新。
- **关键词管理上传**：通过 /api/tasks/tyc/upload 接口可视化上传关键词名单文件。
- **爬虫进度统计**：图形化展示天眼查/企查查等企信爬虫的进度明细和队列长度。

---

## 后台 API 设计详解

### /api/ta/{code} - 缠论技术面行情接口
- 请求示例: GET /api/ta/000001?period=m30
- 支持参数: period = m1|m5|m15|m30|m60|day|week|month
- 响应结构:
`json
{
  "code": "000001",
  "period": "m30",
  "ohlcv": [
    {"datetime": "2026-07-24 10:00:00", "open": 10.1, "high": 10.5, "low": 10.0, "close": 10.4, "volume": 50000}
  ],
  "indicators": [
    { /* 包含 MACD, DIF, DEA, MA, BOLL_*/ }
  ],
  "chanlun": {
    "bi": [],
    "duan": [],
    "zhongshu": [],
    "beichi": [],
    "points": []  // 一二三类买卖点数据
  }
}
`

## 其他说明
- **一键启动**：提供顶层 start.sh (Mac/Linux) 与 start.ps1 (Windows) 脚本一键拉起前后端。
- **如何关停**：在对应的终端执行 Ctrl+C 中断服务。若因异常无法停止，请使用 Stop-Process -Name python / killall python 强行终止。