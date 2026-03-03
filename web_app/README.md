该目录包含一个最小的 Web 应用（FastAPI 后端 + React 前端），用于替换原来的 Flask 仪表板。它与遗留的 `spider_dashboard.py` 并存，通过新的架构提供相同功能，并支持在线配置修改、关键词管理、爬虫进度统计等。

## 启动方式

### 后端启动
在 `financeKG_spider` 根目录执行：
```
uvicorn web_app.backend.main:app --reload --port 8000
```
- 支持热重载，默认端口 8000。
- 所有 API 均以 `/api/` 前缀暴露。
- 跨域已配置，前端可直接访问。

### 前端启动
进入 `web_app/frontend` 后运行：
```
npm install
npm run dev
```
- React + TypeScript 实现，开发模式端口默认 5173。
- 生产构建产物在 `frontend/dist`，可由后端直接服务。

## 主要功能
- **配置管理**：前端支持在线查看和编辑 JSON 配置，后端 `/api/config/` 支持 GET/POST，自动保存并热更新。
- **关键词管理**：上传、列出、删除关键词文件，接口见 `/api/tasks/tyc/upload`、`/api/tyc/files` 等，文件保存路径与旧系统一致。
- **爬虫统计**：接口 `/api/tyc/stats` 返回 JSON 格式统计数据，前端页面展示进度和明细。
- **兼容原有数据结构**，可与 `spider_dashboard.py` 并行运行。

## 其他说明
- 顶层 `start.sh` 脚本可一键检查环境并启动前后端。（windows11 用 start.ps1）
- 提供 `Dockerfile`，自动安装依赖、构建前端并通过 uvicorn 提供服务。
- 新增接口详见 `web_app/backend/api/tyc.py`，包括关键词文件管理、统计等。
- 后端已配置静态文件服务，可直接访问前端构建产物。

## 停止服务
- 前端：`Ctrl+C` 终止开发服务器。
- 后端：`Ctrl+C` 终止 uvicorn 进程，或用 `Stop-Process -Name python` 停止。

---
如需迁移或扩展功能，请参考各 API 路由及前端页面实现。
