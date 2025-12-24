import math
from typing import List, Dict, Tuple, Any
import numpy as np
from datetime import datetime
import numpy as np
from scipy.optimize import least_squares

# def simple_trilateration(measurements: List[Dict], 
#                           anchors: Dict[str, Tuple[float, float, float]]) -> Dict[str, float]:
def simple_trilateration(measurements: List[Dict], 
                       anchors: Dict[str, Tuple[float, float, float]],
                       initial_guess: Tuple[float, float, float] = (0, 0, 0)) -> Dict[str, float]:
    """
    Трилатерация с использованием SciPy (наиболее точный метод).
    Решает задачу минимизации невязки.
    """
    if len(measurements) < 3:
        raise ValueError("Нужно минимум 3 измерения")
    
    # Подготавливаем данные
    anchor_coords = []
    distances = []
    
    for meas in measurements:
        anchor_id = meas['anchor_id']
        if anchor_id not in anchors:
            continue
        anchor_coords.append(anchors[anchor_id])
        distances.append(meas['distance_m'])
    
    anchor_coords = np.array(anchor_coords)
    distances = np.array(distances)
    
    def residuals(point):
        """Функция невязки: разница между расчетными и измеренными расстояниями"""
        return np.sqrt(np.sum((anchor_coords - point)**2, axis=1)) - distances
    
    # Решаем задачу оптимизации
    result = least_squares(
        residuals,
        initial_guess,
        bounds=([-100, -100, -100], [100, 100, 100]),  # ограничения
        method='trf'  # метод доверительных областей
    )
    
    # Получаем точку
    x, y, z = result.x
    
    # Вычисляем точность
    final_residuals = residuals(result.x)
    accuracy = np.max(np.abs(final_residuals))
    
    return {
        'x': float(x),
        'y': float(y),
        'z': float(z),
        'accuracy': float(accuracy)
    }

def save_calculated_position(
    batch_id: str, 
    tag_id: str, 
    position: Dict[str, Any]
) -> None:
    """Сохранение вычисленной позиции в БД"""
    from app.database import get_db
    
    with get_db() as conn:
        conn.execute("""
            INSERT INTO calculated_positions 
            (batch_id, tag_id, x, y, z, accuracy, calculation_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            batch_id, tag_id, 
            position['x'], position['y'], position['z'],
            position['accuracy'], datetime.now()
        ))
        conn.commit()

def get_anchors_from_db() -> Dict[str, Tuple[float, float, float]]:
    """Получение анкеров из БД"""
    from app.database import get_all_anchors
    
    anchors = {}
    for anchor in get_all_anchors():
        if anchor['is_active']:
            anchors[anchor['anchor_id']] = (anchor['x'], anchor['y'], anchor['z'])
    return anchors
