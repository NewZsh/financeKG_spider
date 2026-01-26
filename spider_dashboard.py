## coding=utf-8
## flask 实现前端
## 1. 配置项可视化，用户也可以修改配置项
## 2. 各个爬虫的进度可视化：爬了多少（公司多少，人物多少），爬的数据时效性如何，爬虫的近期运行状态如何（按天的爬虫数是否有显著变化）

import flask
import os

import base_spider
from qxb import qxb_spider

app = flask.Flask(__name__)
app.config['DEBUG'] = True
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['JSONIFY_MIMETYPE'] = 'application/json; charset=utf-8'
app.config['JSON_SORT_KEYS'] = False

spider_instance = base_spider()
qxb_spider_instance = qxb_spider()

@app.route('/')
def index():
    html = f"""
    <h1>FinanceKG Spider Dashboard</h1>
    <ul>
        <li><a href="/config">View Configuration</a></li>
    </ul>
    <ul>
        <li><a href="/qxb_spider">View QXB Spider</a></li>
    </ul>
    """
    return html

@app.route('/config')
def config():
    return flask.jsonify(spider_instance.cfg)

@app.route('/qxb_spider')
def qxb_spider_status():
