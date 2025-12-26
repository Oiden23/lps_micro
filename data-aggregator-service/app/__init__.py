# app/__init__.py
"""
Data Aggregator Service - Микросервис для агрегации данных о перемещениях объектов,
генерации отчетов и аналитики.
"""
__version__ = "1.0.0"
__author__ = "Your Name"
__description__ = "Микросервис для агрегации данных, генерации отчетов и аналитики перемещений объектов"

# Экспортируем основные модули для удобного импорта
from .main import app
from . import models
from . import database
from . import report_generator
from . import analytics_engine
from .api import reports_router, aggregation_router, export_router, analytics_router

__all__ = [
    "app",
    "models",
    "database",
    "report_generator",
    "analytics_engine",
    "reports_router",
    "aggregation_router",
    "export_router",
    "analytics_router"
]