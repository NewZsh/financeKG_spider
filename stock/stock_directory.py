"""兼容层：保留旧导入路径，实际实现位于 stock.directory。"""

from .directory import StockDirectoryService, ak, normalize_company_name, stock_directory_service

__all__ = ["StockDirectoryService", "ak", "normalize_company_name", "stock_directory_service"]