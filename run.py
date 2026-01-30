#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
快速启动脚本 - 启动 Web 仪表板
"""

import sys
import os

# 确保在项目目录中
cur_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(cur_dir)

# 启动仪表板
if __name__ == "__main__":
    print("=" * 60)
    print("🕷️  FinanceKG Spider Dashboard 启动中...")
    print("=" * 60)
    print("\n📱 Web 仪表板地址：http://localhost:5000")
    print("📝 功能列表：")
    print("   • 首页：http://localhost:5000/")
    print("   • 关键词管理：http://localhost:5000/tyc/keywords")
    print("   • 搜索公司：http://localhost:5000/tyc/search")
    print("\n💡 提示：")
    print("   1. 首次使用请先上传关键词文件")
    print("   2. 关键词文件在 data/tyc_keywords/ 目录下")
    print("   3. 爬取的公司数据保存在 data/tyc_data/ 目录下")
    print("\n按 Ctrl+C 停止服务器\n")
    
    # 导入并启动仪表板
    from spider_dashboard import app
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=True)
    except KeyboardInterrupt:
        print("\n\n服务器已停止")
        sys.exit(0)
