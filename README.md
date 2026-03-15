# FinanceKG Spider & Dashboard

一个功能强大的企业信息爬虫 + 现代前后端相分离的知识图谱可视化系统。整合了公司搜索和投资信息爬取，通过 Web 仪表板（React + FastAPI）实现关键词管理、可视化状态监控，并内置基于 AntV G6 的交互式知识图谱展示。


## ✨ 核心功能与特性

| 功能 | 描述 |
| ------|------|
| **增量抓取** | 按关键词搜索公司信息，自动分页爬取基础信息、股东、对外投资三类信息 |
| **图库同步** | 基于图数据库 Neo4j （使用图数据库主要是为了减少多跳时候的数据冗余和提高查询效率） |
| **爬虫控制台界面** | 基于vite + React，支持关键词在线上传、爬虫配置热更、监控爬虫进度与健康状况。<br>爬虫进度包括：公司数量、人物数量、数据时效性等 <br>(**sql**: src - ID - entity_type - time) |
| **图谱** | 支持模糊搜索 Top 10 公司，支持节点为中心的2层关系展示，支持双击跳转 |


## 🚀 快速开始

本项目已迁移为现代前后端分离架构（位于 web_app/），请按以下步骤快速启动：

### 1. 安装和启动 Neo4j 服务

windows 用户推荐从 [Neo4j-windows官方版本](https://neo4j.com/download/?utm_source=GSearch&utm_medium=PaidSearch&utm_campaign=Evergreen&utm_content=AMS-Search-SEMBrand-Evergreen-None-SEM-SEM-NonABM&utm_term=download%20neo4j&utm_adgroup=download&gad_source=1&gad_campaignid=20973570619&gbraid=0AAAAADk9OYp-tKZTbw7jUTry-bU_XS6aa&gclid=Cj0KCQjwsdnNBhC4ARIsAA_3hej0u11m6sD6fduWuNOHdg6d7k9n5rN459dfPLv8eyfp9DmMaE9XShcaAvFREALw_wcB) 下载并安装桌面版，相当于把 Neo4j 作为一个本地服务来使用，安装完成后直接启动即可。

Mac 用户可以通过 Homebrew 安装：

```bash
brew install neo4j
neo4j start
```

Linux 用户可以使用 Docker 启动 Neo4j：

```bash
docker run -d --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/your_password \
  -v neo4j_data:/data \
  neo4j:latest
```

或者直接安装 Neo4j：

```bash
wget https://neo4j.com/artifact.php?name=neo4j-community-4.4.0-unix.tar.gz -O neo4j.tar.gz
tar -xzf neo4j.tar.gz
cd neo4j-community-4.4.0
bin/neo4j start
```

无论哪种方式，最后都需要确保 Neo4j 服务已成功启动

```bash
neo4j status
```

本项目硬编码了默认连接参数：

```python
uri="neo4j://localhost:7687", user="neo4j", password="83939190ys"
```
如果你的 Neo4j 服务使用了不同的地址、端口或密码，请修改 `neo4j_utils.py` 中 `Neo4jManager` 的实例化参数。

- `7474`：Neo4j Browser（Web 管理界面）
- `7687`：Bolt 协议端口（代码连接用）

### 2. 安装依赖

推荐使用 `uv` 方式创建虚拟环境

```bash
uv venv
uv pip install -r requirements.txt
```

前端依赖在 `web_app/frontend` 目录下，使用 npm 安装：

```bash
cd web_app/frontend
npm install
```

### 3. 爬虫数据导入（若有历史数据）
如果之前已经爬取过数据并保存在 `data/tyc_data/`，可以直接导入到 Neo4j：

```bash
python neo4j_utils.py
```

该脚本会自动读取 `data/tyc_data/` 下的数据文件并批量导入：

| 文件类型 | 导入内容 |
|---------|---------|
| `base_info_{id}.json` | 公司节点（Company） |
| `investments_{id}.json` | 被投公司节点 + 投资关系（INVEST） |
| `shareholders_{id}.json` | 股东节点（Company/Person） + 股东关系（SHAREHOLDER） |

> **注意**：脚本使用 `MERGE` 语句导入，重复执行不会产生重复数据。

#### 3.1 查看图谱

1. 浏览器访问 `http://localhost:7474/`
2. 默认用户名 `neo4j`，密码为你设置的密码（首次登录需修改）
3. 在 Cypher 控制台执行查询语句, 常用 Cypher 示例：

```cypher
-- 查询所有公司节点
MATCH (c:Company) RETURN c LIMIT 20;

-- 查询所有人物节点
MATCH (p:Person) RETURN p LIMIT 20;

-- 查询公司之间的投资关系
MATCH (a:Company)-[r:INVEST]->(b:Company) RETURN a, r, b LIMIT 20;

-- 查询某个公司的所有股东
MATCH (s)-[r:SHAREHOLDER]->(c:Company {id: "公司ID"}) RETURN s, r, c;

-- 查询某个人/公司作为股东投资的所有公司
MATCH (s {id: "股东ID"})-[r:SHAREHOLDER]->(c:Company) RETURN s, r, c;

-- 查看全部图谱（限制 50 条）
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 50;

查询某个公司的多层关系：
    MATCH path=(c:Company {id: 'company_id'})-[:INVEST*1..N]->(related) RETURN path
```


### 4. 一键启动📊看板 

在 Windows 平台，直接通过脚本自动挂载环境双开运行：
```powershell
.\start.ps1
```

在 Linux/macOS 下运行：
```bash
bash start.sh
```

注意，以上两个脚本会自动检查环境、安装依赖、构建前端并同时启动服务前后端，如果启动不了，请确保已正确安装 Python 包、Node.js 和 Neo4j，并确保 Neo4j 服务已运行。

启动成功后：
* 📱 **Web 页面**: http://localhost:5173
* 🔌 **API 服务**: http://localhost:8000


#### 4.1 (deprecated) 启动仪表板
```bash
python spider_dashboard.py
```
这是一个老版的

#### 4.2 上传关键词
- 访问 `/tyc/keywords`
- 上传 `.txt` 文件
- 支持拖拽上传，可下载示例文件

```
CVTE
百度
阿里
腾讯
小米
```

关键词文件要求
- **格式**：`.txt` 纯文本
- **编码**：UTF-8
- **内容**：每行一个关键词
- **大小**：1-10,000 个关键词，单个文件 ≤16MB
- **校验**：自动验证格式、编码、非空、数量限制

#### 4.2 ⚙️ 配置

编辑 `spider.cfg` 中的 `TYCSpider` 部分：

```json
{
    "TYCSpider": {
        "api_base_url": "https://capi.tianyancha.com/...",
        "headers": {
            "X-Auth-Token": "YOUR_TOKEN",      // 需要更新
            "X-Tycid": "YOUR_ID"               // 需要更新
        },
        "data_direc": "../data/tyc_data/",
        "keywords_direc": "../data/tyc_keywords/",
        "keywords_file": "keywords.txt",
        "request_sleep_seconds": 3             // 请求间隔（秒）
    }
}
```

#### 4.3 🔑 获取认证信息

1. 打开 https://www.tianyancha.com/
2. 按 F12 打开开发者工具
3. Network 标签 → 任何 API 请求
4. Headers 中查找 `X-Auth-Token` 和 `X-Tycid`
5. 复制更新到 `spider.cfg`


## 模块说明

### 1. 爬虫

#### 1.1 天眼查爬虫——命令行使用

- 搜索公司
```python
from tyc.spider import TYCSpider

spider = TYCSpider()
result = spider.search_companies("CVTE", max_page=5, save_to_file=True)
print(f"找到 {result['total_companies']} 家公司")
spider.close_session()
```

- 爬取投资信息
```python
spider = TYCSpider()
investments = spider.get_all_investment("1391758803", save_to_file=True)
spider.close_session()
```

- 运行测试
```bash
python -m tyc.spider -t search      # 测试搜索
python -m tyc.spider -t investment  # 测试投资爬取
```

#### 1.2 🔍 日志

爬虫日志保存在 `logs/spider.log`，查看实时日志：
```bash
tail -f logs/spider.log
```

## 📁 目录结构

```
financeKG_spider/
├── web_app/              # 现代前后端分离应用 ⭐️ (新)
│   ├── backend/          # FastAPI 后端 (含图谱查询、配置上传及爬虫守护线程)
│   └── frontend/         # React + Vite 前端 (图谱视图、主页控制台)
├── neo4j_utils.py        # Neo4j 连接查询及数据导库工具
├── tyc/spider.py         # 爬虫核心逻辑
├── spider.cfg            # 完整统一配置文件
├── start.ps1             # 自动环境一键拉起脚本 （windows）
├── start.sh              # 自动环境一键拉起脚本 （linux or mac）
└── data/                 # 本地缓存与上传数据区
```

---

**版本**：1.0  
**最后更新**：2026-01-30
