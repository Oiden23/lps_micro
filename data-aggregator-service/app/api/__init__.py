# app/api/__init__.py
"""
API модули Data Aggregator Service.
"""
# Экспортируем все роутеры для удобного импорта
from .reports import router as reports_router
from .aggregation import router as aggregation_router
from .export import router as export_router
from .analytics import router as analytics_router

__all__ = [
    "reports_router",
    "aggregation_router",
    "export_router",
    "analytics_router"
]