# app/api/__init__.py
"""
API модули Access Control Service.
"""

# Экспортируем все роутеры для удобного импорта
from .entities import router as entities_router
from .geofences import router as geofences_router
from .rules import router as rules_router
from .compliance import router as compliance_router

__all__ = [
    "entities_router",
    "geofences_router", 
    "rules_router",
    "compliance_router"
]
