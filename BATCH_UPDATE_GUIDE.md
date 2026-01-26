# 小批量更新机制使用指南

## 概述

本系统实现了爬虫后端和统计功能相互不干扰的小批量更新机制。通过缓冲区管理、文件锁和定时刷新等技术，确保在大数据量场景下的稳定运行。

## 核心特性

### 1. 小批量更新
- **缓冲区机制**: 爬虫数据先写入内存缓冲区
- **批量写入**: 达到设定数量后批量写入文件
- **减少IO**: 大幅减少文件操作次数，提升性能

### 2. 并发安全
- **文件锁**: 使用线程锁确保文件读写安全
- **缓冲区锁**: 保护缓冲区操作的原子性
- **统计隔离**: 统计查询不干扰爬虫操作

### 3. 配置热更
- **定时刷新**: 每分钟自动重新加载配置
- **无需重启**: 配置更改立即生效
- **可调间隔**: 支持动态调整刷新频率

## 使用方法

### 基本爬虫操作

```python
from qxb.spider import QXBSpider

# 创建爬虫实例
spider = QXBSpider()

# 爬取单个公司信息
company_info = spider.get_company_info_byID("100001")

# 批量爬取公司信息
company_ids = ["100001", "100002", "100003"]
stats = spider.crawl_companies_batch(company_ids, batch_size=50)
```

### 统计功能使用

```python
# 获取实时统计信息（不干扰爬虫）
stats = spider.stats()

# 统计信息包含：
# - 文件统计：总记录数、最后更新时间
# - 爬虫运行统计：成功/失败次数、最后爬取时间
# - 缓冲区状态：当前缓冲区大小、批量大小设置
# - 配置信息：刷新间隔、最后配置加载时间
```

### 配置管理

```python
# 设置批量大小
spider.set_batch_size(100)  # 设置每100条记录批量写入

# 设置配置刷新间隔
spider.set_refresh_interval(30)  # 改为30秒刷新

# 强制刷新缓冲区
spider.force_flush()  # 立即写入所有缓冲数据

# 停止配置热更
spider.stop_cfg_refresh()
```

## 性能优化建议

### 1. 批量大小设置
- **小数据量**: 设置较小的批量大小（如10-50）
- **大数据量**: 设置较大的批量大小（如100-500）
- **平衡点**: 根据内存和IO性能调整

### 2. 并发场景
- **多线程爬虫**: 自动处理并发写入
- **统计查询**: 可随时查询不影响爬虫
- **文件锁**: 确保数据一致性

### 3. 错误处理
- **缓冲区保护**: 异常情况下数据不会丢失
- **自动重试**: 文件操作失败自动重试
- **错误日志**: 详细记录操作异常

## 文件格式说明

### 历史记录文件格式
```
# 公司ID历史记录文件
# 格式: id \t name \t type \t spider_time \t last_spider_time

100001	阿里巴巴集团控股有限公司	上市公司	2026-01-26 10:00:00	2026-01-26 10:00:00
100002	腾讯控股有限公司	上市公司	2026-01-26 10:01:00	2026-01-26 10:01:00
```

### 配置文件格式
```json
{
    "QXBSpider": {
        "base_url": "https://www.qixin.com/",
        "id_file_snapshot": "../data/qxb_company_IDlist.txt"
    },
    "proxy_pool": {
        "enabled": false,
        "type": "file"
    }
}
```

## 测试验证

运行测试脚本验证功能：
```bash
python test_batch_update.py
```

测试内容包括：
- 小批量更新功能
- 并发访问测试
- 配置热更验证

## 故障排除

### 常见问题

1. **缓冲区未刷新**
   - 检查批量大小设置
   - 使用 `force_flush()` 强制刷新

2. **统计信息不准确**
   - 确认文件锁正常工作
   - 检查文件权限

3. **配置热更失效**
   - 验证配置文件格式
   - 检查文件路径正确性

### 日志监控

系统会输出以下日志信息：
- 配置加载成功/失败
- 批量更新完成通知
- 错误异常信息

## 扩展开发

### 添加新的爬虫类

继承 `base_spider` 类并实现相应方法：

```python
class NewSpider(base_spider):
    def __init__(self):
        super().__init__()
        # 自定义初始化
    
    def crawl_method(self):
        # 使用小批量更新
        self.add_to_update_buffer(data)
```

### 自定义统计功能

重写 `stats()` 方法添加自定义统计：

```python
def stats(self):
    base_stats = super().stats()
    base_stats["自定义统计"] = self.custom_stats
    return base_stats
```