#!/usr/bin/env bash
# 一键检查并启动前后端
# 需要在支持 bash 的环境中运行（Git Bash, WSL 等）

set -e

echo "检查 Python 环境..."
if ! command -v python >/dev/null 2>&1; then
  echo "未找到 python，请先安装或激活虚拟环境。" >&2
  exit 1
fi

# 激活 venv if script在仓库根运行
if [ -f ".venv/Scripts/activate" ]; then
  source .venv/Scripts/activate
elif [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
fi

# 确认依赖
python - <<'PYCODE'
import importlib,sys
for pkg in ('fastapi','uvicorn','pydantic'):
    try:
        importlib.import_module(pkg)
    except ImportError:
        print(f"缺少依赖 {pkg}，请运行 'pip install {pkg}' 或使用 requirements.txt 安装。", file=sys.stderr)
        sys.exit(1)
print('Python 依赖检查通过。')
PYCODE

# 检查 node/npm
echo "检查 Node.js 环境..."
if ! command -v npm >/dev/null 2>&1; then
  echo "未找到 npm，请先安装 Node.js。" >&2
  exit 1
fi

# 检查 neo4j 是否启动中
echo "检查 Neo4j 数据库..."
if ! nc -z localhost 7687; then
  echo "Neo4j 数据库未启动，请先运行 Neo4j。" >&2
  exit 1
fi

# 启动后端和前端
cd web_app/frontend
npm install

# 使用简单的 & 将两个服务放到后台

# 启动后端
nohup uvicorn web_app.backend.main:app --reload --port 8000 &
backend_pid=$!
echo "后端启动 pid=$backend_pid"

# 启动前端
(cd web_app/frontend && npm run dev) &
frontend_pid=$!
echo "前端启动 pid=$frontend_pid"

echo "📦 Web 应用已启动：后台 http://localhost:8000 前端 http://localhost:5173 （Vite 默认）"
echo "按 Ctrl+C 可以停止，或 kill $backend_pid $frontend_pid"