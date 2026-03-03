# 未完善且未测试：缺少neo4j安装和配置
# 先用启动脚本 start.sh 或 start.ps1 本地启动

# 以官方 Python 镜像为基础
FROM python:3.11-slim

# 安装 nodejs 用于构建前端
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# 安装 Python 依赖
RUN pip install --no-cache-dir fastapi uvicorn pydantic

# 构建前端
WORKDIR /app/frontend
RUN npm install && npm run build

# 切换回后端路径
WORKDIR /app

# 暴露端口
EXPOSE 8000

CMD ["uvicorn", "web_app.backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
