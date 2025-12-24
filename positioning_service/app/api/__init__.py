from .measurements import router as measurements_router
from .positions import router as positions_router
from .anchors import router as anchors_router  # <-- ДОБАВЛЯЕМ

__all__ = ["measurements_router", "positions_router", "anchors_router"]  # <-- ОБНОВЛЯЕМ