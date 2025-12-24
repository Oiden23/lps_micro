# app/__init__.py
"""
Positioning Service - Микросервис для позиционирования персонала в реальном времени.
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__description__ = "Микросервис для обработки измерений и вычисления координат"

# Экспортируем основные модули для удобного импорта
from .main import app
from . import models
from . import database
from . import trilateration
from .api import measurements, positions, anchors

__all__ = [
    "app",
    "models",
    "database",
    "trilateration",
    "measurements",
    "positions",
    "anchors"
]
