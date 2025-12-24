# app/__init__.py
"""
Access Control Service - Микросервис для управления геозонами, правилами доступа 
и контроля соблюдения политик безопасности.
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__description__ = "Микросервис для управления сущностями, геозонами, правилами доступа и контроля соблюдения"

# Экспортируем основные модули для удобного импорта
from .main import app
from . import models
from . import database
from . import compliance_checker
from .api import entities_router, geofences_router, rules_router, compliance_router

__all__ = [
    "app",
    "models",
    "database",
    "compliance_checker",
    "entities_router",
    "geofences_router", 
    "rules_router",
    "compliance_router"
]