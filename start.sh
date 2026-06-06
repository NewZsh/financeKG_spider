#!/usr/bin/env bash
# 一键检查并启动前后端
# 需要在支持 bash 的环境中运行（Git Bash, WSL 等）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
FRONTEND_DIR="$PROJECT_ROOT/web_app/frontend"
BACKEND_PORT=8000
FRONTEND_PORT=5173

cd "$PROJECT_ROOT"

# 跟踪已启动的 PID，用于失败时清理
BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
    [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null || true
    [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null || true
}

trap cleanup EXIT ERR INT TERM

# 杀掉占用目标端口的旧进程（避免端口冲突）
kill_port() {
    local port=$1 label=$2
    local pids
    pids=$(lsof -ti :"$port" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "端口 $port 被占用 ($label)，正在停止旧进程..."
        echo "$pids" | xargs kill 2>/dev/null || true
        sleep 1
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        [ -n "$pids" ] && echo "$pids" | xargs kill -9 2>/dev/null || true
        echo "端口 $port 已释放。"
    fi
}

kill_port $BACKEND_PORT "后端"
kill_port $FRONTEND_PORT "前端"

echo "检查 Python 环境..."
if ! command -v python >/dev/null 2>&1; then
  echo "未找到 python，请先安装或激活虚拟环境。" >&2
  exit 1
fi

# 激活 venv if script在仓库根运行
if [ -f "$PROJECT_ROOT/.venv/Scripts/activate" ]; then
  source "$PROJECT_ROOT/.venv/Scripts/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
  source "$PROJECT_ROOT/.venv/bin/activate"
elif [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
  source "$PROJECT_ROOT/venv/bin/activate"
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

# npm install
cd "$FRONTEND_DIR"
npm install

# 启动后端
(cd "$PROJECT_ROOT" && nohup uvicorn web_app.backend.main:app --reload --port $BACKEND_PORT) &
BACKEND_PID=$!
echo "后端启动 pid=$BACKEND_PID"

# 等待并验证后端是否成功启动
sleep 2
if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    echo "错误: 后端启动失败，请检查日志。" >&2
    BACKEND_PID=""  # 避免 cleanup 再次 kill
    exit 1
fi
echo "后端运行正常。"

# 启动前端
(cd "$FRONTEND_DIR" && npm run dev) &
FRONTEND_PID=$!
echo "前端启动 pid=$FRONTEND_PID"

# 等待并验证前端是否成功启动
sleep 3
if ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
    echo "错误: 前端启动失败，请检查日志。" >&2
    exit 1
fi
echo "前端运行正常。"

echo "Web 应用已启动：后台 http://localhost:$BACKEND_PORT 前端 http://localhost:$FRONTEND_PORT"
echo "按 Ctrl+C 停止所有服务。"

# 等待前台进程，保持脚本运行；被 Ctrl+C 中断时 trap 会触发 cleanup
wait
