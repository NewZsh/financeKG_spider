#!/usr/bin/env python
# -*- coding: utf-8 -*-

## spider_dashboard.py
## Flask 实现前端
## 1. 配置项可视化，用户也可以修改配置项
## 2. 各个爬虫的进度可视化展示
## 3. 关键词管理和上传

import flask
import os
import json
from datetime import datetime
from werkzeug.utils import secure_filename

import base_spider
from qxb.spider import QXBspider
from tyc.spider import TYCSpider

app = flask.Flask(__name__)
app.config['DEBUG'] = True
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['JSONIFY_MIMETYPE'] = 'application/json; charset=utf-8'
app.config['JSON_SORT_KEYS'] = False
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 最大上传16MB

# 获取项目根目录
cur_dir = os.path.dirname(os.path.abspath(__file__))
app.config['UPLOAD_FOLDER'] = os.path.join(cur_dir, 'data', 'tyc_keywords')

# 初始化爬虫实例
spider_instance = base_spider.base_spider()
qxb_spider_instance = qxb_spider()
tyc_spider_instance = TYCSpider()

# 确保上传目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


def validate_keywords_file(file_obj):
    """
    校验上传的关键词文件
    Args:
        file_obj: Flask FileStorage 对象
    
    Returns:
        tuple: (is_valid, error_message)
    """
    # 检查是否是txt文件
    if not file_obj.filename.endswith('.txt'):
        return False, "文件必须是 .txt 格式"
    
    # 读取文件内容
    try:
        content = file_obj.read().decode('utf-8')
        file_obj.seek(0)  # 重置文件指针
    except UnicodeDecodeError:
        return False, "文件编码必须是 UTF-8"
    except Exception as e:
        return False, f"文件读取失败: {str(e)}"
    
    # 检查文件是否为空
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    if not lines:
        return False, "文件不能为空，至少需要一个关键词"
    
    # 检查关键词有效性
    if len(lines) > 10000:
        return False, "关键词数量不能超过 10000"
    
    return True, None


def get_keywords_list():
    """
    获取当前的关键词列表
    
    Returns:
        dict: 关键词信息
    """
    keywords_file = os.path.join(
        app.config['UPLOAD_FOLDER'],
        tyc_spider_instance.s_cfg.get('keywords_file', 'keywords.txt')
    )
    
    if not os.path.exists(keywords_file):
        return {
            "exists": False,
            "keywords": [],
            "count": 0,
            "file_path": keywords_file
        }
    
    try:
        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
        
        file_stat = os.stat(keywords_file)
        return {
            "exists": True,
            "keywords": keywords,
            "count": len(keywords),
            "file_path": keywords_file,
            "file_size": file_stat.st_size,
            "last_modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        }
    except Exception as e:
        return {
            "exists": True,
            "error": str(e),
            "keywords": [],
            "count": 0,
            "file_path": keywords_file
        }


@app.route('/')
def index():
    """主页"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>FinanceKG Spider Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            .section { margin: 20px 0; padding: 15px; background: #f5f5f5; border-radius: 5px; }
            a { color: #0066cc; text-decoration: none; margin-right: 20px; }
            a:hover { text-decoration: underline; }
            button { padding: 10px 20px; background: #0066cc; color: white; border: none; border-radius: 5px; cursor: pointer; }
            button:hover { background: #0052a3; }
        </style>
    </head>
    <body>
        <h1>🕷️ FinanceKG Spider Dashboard</h1>
        
        <div class="section">
            <h2>配置管理</h2>
            <a href="/config">查看配置</a>
        </div>
        
        <div class="section">
            <h2>爬虫管理</h2>
            <a href="/qxb_spider">QXB Spider 状态</a>
        </div>
        
        <div class="section">
            <h2>天眼查爬虫</h2>
            <a href="/tyc/keywords">关键词管理</a>
            <a href="/tyc/search">搜索公司</a>
            <a href="/tyc/stats">爬取统计</a>
        </div>
    </body>
    </html>
    """
    return html


@app.route('/config')
def config():
    """查看配置"""
    return flask.jsonify(spider_instance.cfg)


@app.route('/qxb_spider')
def qxb_spider_status():
    """QXB Spider 状态"""
    return flask.jsonify({"status": "ok", "message": "QXB Spider is running"})


# ==================== 天眼查关键词管理 ====================

@app.route('/tyc/keywords', methods=['GET', 'POST'])
def tyc_keywords():
    """
    天眼查关键词管理页面
    GET: 显示当前关键词列表
    POST: 上传新的关键词文件
    """
    if flask.request.method == 'POST':
        # 处理文件上传
        if 'file' not in flask.request.files:
            return flask.jsonify({"error": "没有上传文件"}), 400
        
        file = flask.request.files['file']
        if file.filename == '':
            return flask.jsonify({"error": "文件名为空"}), 400
        
        # 校验文件
        is_valid, error_msg = validate_keywords_file(file)
        if not is_valid:
            return flask.jsonify({"error": error_msg}), 400
        
        try:
            # 保存文件
            filename = tyc_spider_instance.s_cfg.get('keywords_file', 'keywords.txt')
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            file.save(filepath)
            
            # 返回成功信息
            keywords_info = get_keywords_list()
            return flask.jsonify({
                "success": True,
                "message": f"成功上传 {keywords_info['count']} 个关键词",
                "keywords_info": keywords_info
            })
        
        except Exception as e:
            return flask.jsonify({"error": f"文件保存失败: {str(e)}"}), 500
    
    # GET 请求：返回关键词列表
    keywords_info = get_keywords_list()
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>天眼查 - 关键词管理</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            .section {{ margin: 20px 0; padding: 15px; background: #f5f5f5; border-radius: 5px; }}
            .upload-area {{ 
                border: 2px dashed #ccc; 
                padding: 20px; 
                text-align: center; 
                cursor: pointer;
                border-radius: 5px;
                transition: background 0.3s;
            }}
            .upload-area:hover {{ background: #e8e8e8; }}
            .keywords-list {{ 
                max-height: 400px; 
                overflow-y: auto; 
                border: 1px solid #ccc; 
                padding: 10px;
                border-radius: 5px;
                background: white;
            }}
            .keyword-item {{ padding: 5px; margin: 5px 0; background: #e3f2fd; border-radius: 3px; }}
            button {{ padding: 10px 20px; background: #0066cc; color: white; border: none; border-radius: 5px; cursor: pointer; }}
            button:hover {{ background: #0052a3; }}
            .success {{ color: green; }}
            .error {{ color: red; }}
            input[type="file"] {{ display: none; }}
            .stats {{ font-size: 14px; color: #666; }}
            a {{ color: #0066cc; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <a href="/">← 返回首页</a>
        <h1>天眼查 - 关键词管理</h1>
        
        <div class="section">
            <h2>上传关键词文件</h2>
            <div class="upload-area" onclick="document.getElementById('file-input').click()">
                <p>📁 点击或拖拽上传 .txt 文件</p>
                <p style="font-size: 12px; color: #666;">每行一个关键词，UTF-8 编码</p>
            </div>
            <input type="file" id="file-input" accept=".txt" />
            <div id="upload-status" style="margin-top: 10px;"></div>
        </div>
        
        <div class="section">
            <h2>当前关键词列表</h2>
            <div class="stats">
                <p>📊 已有关键词: <strong>{keywords_info.get('count', 0)}</strong> 个</p>
                {f'<p>📅 最后更新: {keywords_info.get("last_modified", "N/A")}</p>' if keywords_info.get('exists') else '<p>❌ 未上传关键词文件</p>'}
            </div>
            <div class="keywords-list">
                {''.join(f'<div class="keyword-item">{kw}</div>' for kw in keywords_info.get('keywords', [])[:100])}
                {f'<p style="color: #999; text-align: center;">... 还有 {keywords_info.get("count", 0) - 100} 个关键词</p>' if keywords_info.get('count', 0) > 100 else ''}
            </div>
        </div>
        
        <div class="section">
            <h2>快速操作</h2>
            <button onclick="downloadTemplate()">📥 下载示例文件</button>
            <button onclick="window.location.href='/tyc/search'">🔍 开始搜索</button>
        </div>
        
        <script>
            // 拖拽上传
            const uploadArea = document.querySelector('.upload-area');
            uploadArea.addEventListener('dragover', (e) => {{
                e.preventDefault();
                uploadArea.style.background = '#e8e8e8';
            }});
            
            uploadArea.addEventListener('dragleave', () => {{
                uploadArea.style.background = '';
            }});
            
            uploadArea.addEventListener('drop', (e) => {{
                e.preventDefault();
                uploadArea.style.background = '';
                const files = e.dataTransfer.files;
                if (files.length > 0) {{
                    document.getElementById('file-input').files = files;
                    uploadFile();
                }}
            }});
            
            // 文件输入变化
            document.getElementById('file-input').addEventListener('change', uploadFile);
            
            // 上传文件
            function uploadFile() {{
                const file = document.getElementById('file-input').files[0];
                if (!file) return;
                
                const formData = new FormData();
                formData.append('file', file);
                
                fetch(window.location.href, {{
                    method: 'POST',
                    body: formData
                }})
                .then(response => response.json())
                .then(data => {{
                    const statusDiv = document.getElementById('upload-status');
                    if (data.success) {{
                        statusDiv.innerHTML = `<p class="success">✅ ${{data.message}}</p>`;
                        setTimeout(() => location.reload(), 2000);
                    }} else {{
                        statusDiv.innerHTML = `<p class="error">❌ ${{data.error}}</p>`;
                    }}
                }})
                .catch(error => {{
                    document.getElementById('upload-status').innerHTML = 
                        `<p class="error">❌ 上传失败: ${{error.message}}</p>`;
                }});
            }}
            
            // 下载示例文件
            function downloadTemplate() {{
                const template = 'CVTE\\n百度\\n阿里\\n腾讯\\n小米\\n字节跳动\\n美团\\n滴滴\\n快手\\n抖音';
                const blob = new Blob([template], {{ type: 'text/plain;charset=utf-8' }});
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'keywords_example.txt';
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
                document.body.removeChild(a);
            }}
        </script>
    </body>
    </html>
    """
    return html


@app.route('/tyc/keywords/api', methods=['GET'])
def tyc_keywords_api():
    """
    获取关键词列表 API
    """
    keywords_info = get_keywords_list()
    return flask.jsonify(keywords_info)


@app.route('/tyc/search', methods=['GET', 'POST'])
def tyc_search():
    """
    天眼查搜索页面
    """
    if flask.request.method == 'POST':
        # 处理搜索请求
        keywords = flask.request.json.get('keywords', [])
        max_page = flask.request.json.get('max_page')
        
        if not keywords:
            return flask.jsonify({"error": "关键词列表为空"}), 400
        
        results = []
        for keyword in keywords:
            try:
                result = tyc_spider_instance.search_companies(
                    keyword,
                    max_page=max_page,
                    save_to_file=True
                )
                results.append({
                    "keyword": keyword,
                    "success": True,
                    "data": result
                })
            except Exception as e:
                results.append({
                    "keyword": keyword,
                    "success": False,
                    "error": str(e)
                })
        
        tyc_spider_instance.close_session()
        return flask.jsonify({"results": results})
    
    # GET 请求：显示搜索页面
    keywords_info = get_keywords_list()
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>天眼查 - 搜索公司</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            .section {{ margin: 20px 0; padding: 15px; background: #f5f5f5; border-radius: 5px; }}
            .controls {{ margin: 15px 0; }}
            input, button, select {{ padding: 8px; margin: 5px; border: 1px solid #ccc; border-radius: 3px; }}
            button {{ background: #0066cc; color: white; cursor: pointer; border: none; padding: 10px 20px; }}
            button:hover {{ background: #0052a3; }}
            .results {{ max-height: 500px; overflow-y: auto; border: 1px solid #ccc; padding: 10px; border-radius: 5px; }}
            .result-item {{ margin: 10px 0; padding: 10px; background: white; border-left: 4px solid #0066cc; }}
            .success {{ color: green; }}
            .error {{ color: red; }}
            .loading {{ color: #666; font-style: italic; }}
            a {{ color: #0066cc; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <a href="/">← 返回首页</a>
        <h1>天眼查 - 搜索公司</h1>
        
        <div class="section">
            <h2>搜索配置</h2>
            <div class="controls">
                <div>
                    <label>关键词数量: <strong>{keywords_info.get('count', 0)}</strong></label>
                </div>
                <div>
                    <label>最多爬取页数:</label>
                    <input type="number" id="max-page" placeholder="留空表示爬取所有页" />
                </div>
                <div>
                    <button onclick="startSearch()">🚀 开始搜索</button>
                    <button onclick="window.location.href='/tyc/keywords'">⚙️ 管理关键词</button>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>搜索进度</h2>
            <div id="results" class="results">
                <p class="loading">等待开始搜索...</p>
            </div>
        </div>
        
        <script>
            async function startSearch() {{
                const maxPage = document.getElementById('max-page').value || null;
                const resultsDiv = document.getElementById('results');
                resultsDiv.innerHTML = '<p class="loading">正在搜索...</p>';
                
                try {{
                    // 获取关键词列表
                    const keywordsRes = await fetch('/tyc/keywords/api');
                    const keywordsData = await keywordsRes.json();
                    const keywords = keywordsData.keywords;
                    
                    if (keywords.length === 0) {{
                        resultsDiv.innerHTML = '<p class="error">❌ 还没有上传关键词文件</p>';
                        return;
                    }}
                    
                    // 开始搜索
                    const searchRes = await fetch('/tyc/search', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{
                            keywords: keywords,
                            max_page: maxPage ? parseInt(maxPage) : null
                        }})
                    }});
                    
                    const results = await searchRes.json();
                    
                    // 显示结果
                    let html = '';
                    for (const result of results.results) {{
                        if (result.success) {{
                            html += `
                                <div class="result-item success">
                                    <strong>✅ ${{result.keyword}}</strong><br>
                                    找到 ${{result.data.total_companies}} 家公司，${{result.data.total_pages}} 页
                                </div>
                            `;
                        }} else {{
                            html += `
                                <div class="result-item error">
                                    <strong>❌ ${{result.keyword}}</strong><br>
                                    ${{result.error}}
                                </div>
                            `;
                        }}
                    }}
                    resultsDiv.innerHTML = html;
                }} catch (error) {{
                    resultsDiv.innerHTML = `<p class="error">❌ 搜索失败: ${{error.message}}</p>`;
                }}
            }}
        </script>
    </body>
    </html>
    """
    return html


@app.route('/tyc/stats')
def tyc_stats():
    """天眼查爬虫统计页面 - 显示数据库访问记录"""
    spider = tyc_spider_instance
    
    try:
        # 获取数据库统计信息
        import sqlite3
        conn = sqlite3.connect(spider.db_file)
        cursor = conn.cursor()
        
        # 获取总记录数
        cursor.execute("SELECT COUNT(*) FROM record")
        total_records = cursor.fetchone()[0]
        
        # 获取按源分类的统计
        cursor.execute("""
            SELECT src, COUNT(*) as count, SUM(visit_times) as total_visits
            FROM record
            GROUP BY src
        """)
        src_stats = cursor.fetchall()
        
        # 获取最近爬取的记录
        cursor.execute("""
            SELECT src, id, entity_type, visit_time, visit_times
            FROM record
            ORDER BY visit_time DESC
            LIMIT 20
        """)
        recent_records = cursor.fetchall()
        
        conn.close()
        
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>爬虫统计 - FinanceKG Spider Dashboard</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #333; }
                .section { margin: 20px 0; padding: 15px; background: #f5f5f5; border-radius: 5px; }
                table { width: 100%; border-collapse: collapse; background: white; }
                th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
                th { background: #0066cc; color: white; }
                tr:hover { background: #f9f9f9; }
                .stats-box { display: inline-block; margin: 10px; padding: 15px; background: white; border-radius: 5px; border-left: 4px solid #0066cc; }
                .stats-value { font-size: 24px; font-weight: bold; color: #0066cc; }
                .back-link { margin-bottom: 20px; }
                a { color: #0066cc; text-decoration: none; }
                a:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <div class="back-link">
                <a href="/tyc/keywords">← 返回关键词管理</a>
            </div>
            
            <h1>📊 天眼查爬虫统计</h1>
            
            <div class="section">
                <h2>总体统计</h2>
                <div class="stats-box">
                    <div class="stats-value">""" + str(total_records) + """</div>
                    <div>总爬取记录数</div>
                </div>
            </div>
            
            <div class="section">
                <h2>按来源分类统计</h2>
                <table>
                    <thead>
                        <tr>
                            <th>来源 (src)</th>
                            <th>记录数</th>
                            <th>总访问次数</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for src, count, total_visits in src_stats:
            html += f"""
                        <tr>
                            <td><strong>{src}</strong></td>
                            <td>{count}</td>
                            <td>{total_visits}</td>
                        </tr>
            """
        
        html += """
                    </tbody>
                </table>
            </div>
            
            <div class="section">
                <h2>最近爬取记录 (最近20条)</h2>
                <table>
                    <thead>
                        <tr>
                            <th>来源</th>
                            <th>ID</th>
                            <th>实体类型</th>
                            <th>最后访问时间</th>
                            <th>访问次数</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for src, id_val, entity_type, visit_time, visit_times in recent_records:
            html += f"""
                        <tr>
                            <td>{src}</td>
                            <td style="word-break: break-all; max-width: 200px; font-size: 12px;">{id_val}</td>
                            <td>{entity_type}</td>
                            <td>{visit_time}</td>
                            <td>{visit_times}</td>
                        </tr>
            """
        
        html += """
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        """
        return html
    
    except Exception as e:
        return f"<h1>❌ 错误</h1><p>获取统计信息失败: {e}</p><a href='/tyc/keywords'>返回</a>", 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
