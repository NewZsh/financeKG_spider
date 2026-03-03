#!/usr/bin/env python
# -*- coding: utf-8 -*-

## spider_dashboard.py
## Flask 实现前端
## 1. 配置项可视化，用户也可以修改配置项
## 2. 各个爬虫的进度可视化展示
## 3. 关键词管理和上传

import sys
import threading
import flask
import os
import time
import json
from datetime import datetime

from base_spider import ThreadSafeUniqueQueue, base_spider
from qxb.spider import QXBSpider
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
os.chdir(cur_dir)
app.config['UPLOAD_FOLDER'] = os.path.join(cur_dir, 'data', 'tyc_keywords')


# 初始化爬虫实例
tyc_id_collect_queue = ThreadSafeUniqueQueue()
spider_instance = base_spider()
qxb_spider_instance = QXBSpider()
tyc_spider_instance = TYCSpider(tyc_id_collect_queue)

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


def get_keywords_list(
    keywords_file=None, 
    keywords_direc=None,
    keywords_finished_fn=None
):
    """
    获取当前的关键词列表

    如果没有指定文件名，则从文件夹下全部读取，拼接返回
    
    Returns:
        dict: 关键词信息
    """
    if keywords_file:
        keywords_files = [keywords_file]
    else:
        keywords_files = []
        if keywords_direc:
            for filename in os.listdir(keywords_direc):
                fn = os.path.join(keywords_direc, filename)
                if fn != keywords_finished_fn:
                    keywords_files.append(fn)
    
    finished_keywords = set()
    if keywords_finished_fn and os.path.exists(keywords_finished_fn):
        with open(keywords_finished_fn, 'r', encoding='utf-8') as f:
            finished_keywords = set([line.strip() for line in f if line.strip()])
            
    keywords = set()
    for keywords_file in keywords_files:
        with open(keywords_file, 'r', encoding='utf-8') as f:
            file_keywords = [line.strip() for line in f if line.strip()]
            keywords.update(file_keywords)
    
    return len(finished_keywords), len(keywords)
      

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
            <a href="/tyc/stats">爬取统计</a>
        </div>
    </body>
    </html>
    """
    return html


@app.route('/config', methods=['GET', 'POST'])
def config():
    """查看和修改配置"""
    if flask.request.method == 'POST':
        try:
            # 尝试从 JSON body 或 form data 获取配置
            if flask.request.is_json:
                new_cfg = flask.request.json
            else:
                new_cfg_str = flask.request.form.get('config')
                if not new_cfg_str:
                     return flask.jsonify({"success": False, "error": "Missing config parameter"}), 400
                new_cfg = json.loads(new_cfg_str)
            
            # 保存到文件
            with open(spider_instance.cfg_file, 'w', encoding='utf-8') as f:
                json.dump(new_cfg, f, indent=4, ensure_ascii=False)
            
            # 更新内存中的配置
            spider_instance.cfg = new_cfg
            
            # 同时更新其他爬虫实例的配置，确保立即生效
            for spider in [qxb_spider_instance, tyc_spider_instance]:
                if hasattr(spider, 'cfg'):
                    spider.cfg = new_cfg
                    
                    # 尝试更新 s_cfg (子类特定配置)
                    spider_name = spider.__class__.__name__
                    if hasattr(spider, 's_cfg') and spider_name in new_cfg:
                        spider.s_cfg = new_cfg[spider_name]
            
            return flask.jsonify({"success": True, "message": "配置已更新，所有爬虫实例已同步"})
        except Exception as e:
            return flask.jsonify({"success": False, "error": str(e)}), 400

    # GET 请求 - 返回带有编辑器的HTML页面
    cfg_json = json.dumps(spider_instance.cfg, indent=4, ensure_ascii=False)
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>配置管理 - FinanceKG Spider</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            .editor-container {{ margin: 20px 0; }}
            textarea {{ 
                width: 100%; 
                height: 600px; 
                font-family: Consolas, 'Courier New', monospace; 
                padding: 10px;
                border: 1px solid #ccc;
                border-radius: 5px;
                font-size: 14px;
                background-color: #fafafa;
            }}
            .actions {{ margin-top: 20px; }}
            button {{ padding: 10px 20px; background: #0066cc; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }}
            button:hover {{ background: #0052a3; }}
            .back-link {{ margin-bottom: 20px; display: inline-block; }}
            #status {{ margin-top: 15px; font-weight: bold; min-height: 24px; }}
            .success {{ color: green; }}
            .error {{ color: red; }}
            a {{ color: #0066cc; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <a href="/" class="back-link">← 返回首页</a>
        <h1>⚙️ 配置管理</h1>
        
        <div class="editor-container">
            <textarea id="config-editor" spellcheck="false">{{cfg_json}}</textarea>
        </div>
        
        <div class="actions">
            <button onclick="saveConfig()">💾 保存配置</button>
            <div id="status"></div>
        </div>
        
        <script>
            function saveConfig() {{
                const editor = document.getElementById('config-editor');
                const configStr = editor.value;
                const statusDiv = document.getElementById('status');
                const saveBtn = document.querySelector('button');
                
                // 本地校验 JSON 格式
                try {{
                    JSON.parse(configStr);
                }} catch (e) {{
                    statusDiv.innerHTML = '<span class="error">❌ JSON 格式错误: ' + e.message + '</span>';
                    return;
                }}
                
                statusDiv.innerHTML = '⏳ 保存中...';
                saveBtn.disabled = true;
                
                const formData = new FormData();
                formData.append('config', configStr);
                
                fetch('/config', {{
                    method: 'POST',
                    body: formData
                }})
                .then(response => response.json())
                .then(data => {{
                    if (data.success) {{
                        statusDiv.innerHTML = '<span class="success">✅ ' + data.message + '</span>';
                        // 3秒后清除成功消息
                        setTimeout(() => {{
                             if (statusDiv.innerHTML.includes('✅')) statusDiv.innerHTML = '';
                        }}, 3000);
                    }} else {{
                        statusDiv.innerHTML = '<span class="error">❌ ' + data.error + '</span>';
                    }}
                }})
                .catch(error => {{
                    statusDiv.innerHTML = '<span class="error">❌ 请求失败: ' + error.message + '</span>';
                }})
                .finally(() => {{
                    saveBtn.disabled = false;
                }});
            }}
            
            // 支持 Tab 键缩进
            document.getElementById('config-editor').addEventListener('keydown', function(e) {{
                if (e.key == 'Tab') {{
                    e.preventDefault();
                    var start = this.selectionStart;
                    var end = this.selectionEnd;
                    
                    // set textarea value to: text before caret + tab + text after caret
                    this.value = this.value.substring(0, start) +
                        "    " + this.value.substring(end);
                    
                    // put caret at right position again
                    this.selectionStart = this.selectionEnd = start + 4;
                }}
            }});
        </script>
    </body>
    </html>
    """
    return html


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
            direc = tyc_spider_instance.s_cfg.get('keywords_direc')
            filename = len(os.listdir(direc)) - 1
            keywords_file = os.path.join(direc, f"keywords_{filename}.txt")
            
            file.save(keywords_file)
            
            # 返回成功信息
            _, keywords_cnt = get_keywords_list(keywords_file)
            return flask.jsonify({
                "success": True,
                "message": f"成功上传 {keywords_cnt} 个关键词",
            })
        
        except Exception as e:
            return flask.jsonify({"error": f"文件保存失败: {str(e)}"}), 500
    
    # GET 请求：返回关键词列表
    finished_keywords_cnt, keywords_cnt = get_keywords_list()
    
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
                <p style="font-size: 12px; color: #666;">每行一个关键词</p>
            </div>
            <input type="file" id="file-input" accept=".txt" />
            <div id="upload-status" style="margin-top: 10px;"></div>
        </div>
        
        <div class="section">
            <h2>当前关键词列表</h2>
            <div class="stats">
                <p>📊 已有关键词: <strong>{keywords_cnt}</strong> 个</p>
                <p>📅 已完成关键词：<strong>{finished_keywords_cnt}</strong> 个</p>
            </div>
        </div>
        
        <div class="section">
            <h2>示例</h2>
            <button onclick="downloadTemplate()">📥 下载示例文件</button>
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


# 启动后台线程：定时扫描配置中的 `keywords_direc`，处理新上传的关键词文件
def _tyc_kw_watcher(poll_interval=10):
    watcher_spider = TYCSpider(tyc_id_collect_queue)
    kw_dir_rel = watcher_spider.s_cfg.get("keywords_direc", "./data/tyc_keywords/")
    kw_dir = os.path.abspath(os.path.join(cur_dir, kw_dir_rel))
    os.makedirs(kw_dir, exist_ok=True)

    finished_file = watcher_spider.s_cfg.get("keywords_finised_fn", os.path.join(kw_dir, "keywords_finished.txt"))
    if not os.path.exists(finished_file):
        open(finished_file, 'a', encoding='utf-8').close()

    def _load_finished():
        try:
            with open(finished_file, 'r', encoding='utf-8') as f:
                return set([l.strip() for l in f if l.strip()])
        except Exception:
            return set()

    while True:
        finished = _load_finished()
        for fn in os.listdir(kw_dir):
            if fn == os.path.basename(finished_file):
                continue
            if fn.startswith('.'): # 避开重命名为.processed开头，也避开.finished开头
                continue
            path = os.path.join(kw_dir, fn)
            if not os.path.isfile(path):
                continue

            try:
                with open(path, 'r', encoding='utf-8') as f:
                    kws = [ln.strip() for ln in f if ln.strip()]
            except Exception:
                watcher_spider.logger.exception(f"读取关键词文件失败: {path}")
                continue

            for kw in kws:
                if kw in finished:
                    continue

                try:
                    result = watcher_spider.search_companies(kw, max_page=2, save_to_file=True)

                    with open(finished_file, 'a', encoding='utf-8') as ff:
                        ff.write(kw + '\n')
                    finished.add(kw)
                except Exception:
                    watcher_spider.logger.exception(f"Watcher 搜索失败: {kw}")

            # 尝试将已处理的文件重命名，避免再次扫描同一上传文件
            try:
                processed_fn = ".processed" + fn
                processed_path = os.path.join(kw_dir, processed_fn)
                if not os.path.exists(processed_path):
                    os.rename(path, processed_path)
            except Exception:
                watcher_spider.logger.debug(f"无法重命名文件: {path}")
            
        time.sleep(poll_interval)

# 启动后台线程：定时爬取对外投资和股东
def _tyc_id_watcher(poll_interval=60):
    watcher_spider = TYCSpider(tyc_id_collect_queue)

    # 从 db 中的 todo 表获取待爬取的 id，进行爬取
    watcher_spider.load_db()

    while True:
        if watcher_spider.id_collect_queue.empty():
            time.sleep(poll_interval)
            continue

        try:
            company_gid = watcher_spider.id_collect_queue.get()
            id_found = watcher_spider.get_all_investment(company_gid, save_to_file=True)
            if id_found:
                watcher_spider.logger.info(f"本次爬取完成，发现 {len(id_found)} 家被投资公司")
            result = watcher_spider.get_all_shareholder(company_gid, save_to_file=True)
            if result:
                gid_found, hid_found = result
                watcher_spider.logger.info(f"本次爬取完成，发现 {len(gid_found)} 家企业股东，{len(hid_found)} 位自然人股东")
            watcher_spider.write_db(src="tyc", id=company_gid, entity_type="1")
        except Exception as e:
            watcher_spider.logger.info(f"本次爬取失败，错误原因: {e}")
            watcher_spider.close_session()


# 启动仪表板
if __name__ == "__main__":
    port = 9000

    watcher_thread = threading.Thread(target=_tyc_kw_watcher, args=(10,), daemon=True)
    watcher_thread.start()
    print("关键词监控已启动（后台线程）")

    watcher_thread = threading.Thread(target=_tyc_id_watcher, args=(10,), daemon=True)
    watcher_thread.start()
    print("新id监控已启动（后台线程）")

    print("=" * 60)
    print("🕷️  FinanceKG Spider Dashboard 启动中...")
    print("=" * 60)
    print(f"\n📱 Web 仪表板地址：http://localhost:{port}")
    print("📝 功能列表：")
    print(f"   • 首页：http://localhost:{port}/")
    print(f"   • 关键词管理：http://localhost:{port}/tyc/keywords")
    print("\n💡 提示：")
    print("   1. 首次使用请先上传关键词文件")
    print("   2. 关键词文件在 data/tyc_keywords/ 目录下")
    print("   3. 爬取的公司数据保存在 data/tyc_data/ 目录下")
    print("\n按 Ctrl+C 停止服务器\n")

    try:
        app.run(host='0.0.0.0', port=port, debug=True)
    except KeyboardInterrupt:
        print("\n\n服务器已停止")
        sys.exit(0)

