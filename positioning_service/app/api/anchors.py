from fastapi import APIRouter, HTTPException, status
from typing import List

from app.models import Anchor
from app.database import get_all_anchors, get_anchor_by_id, get_all_anchors, delete_anchor

router = APIRouter()


@router.get(
    "/anchors",
    response_model=List[Anchor],
    responses={
        200: {"description": "Успешный запрос", "model": List[Anchor]}
    }
)
async def get_all_anchors_endpoint():
    """
    Получение списка всех зарегистрированных анкеров.
    
    Возвращает конфигурацию всех базовых станций, включая их установленные координаты в системе.
    """
    anchors_data = get_all_anchors()
    
    # Преобразуем данные из БД в модели Pydantic
    anchors = []
    for anchor_data in anchors_data:
        # Если в БД last_calibration может быть строкой или None
        if anchor_data.get('last_calibration'):
            if not isinstance(anchor_data['last_calibration'], str):
                # Если это datetime объект, конвертируем в строку
                anchor_data['last_calibration'] = anchor_data['last_calibration'].isoformat()
        anchors.append(Anchor(**anchor_data))
    
    return anchors


@router.get(
    "/anchors/{anchor_id}",
    response_model=Anchor,
    responses={
        200: {"description": "Успешный запрос", "model": Anchor},
        404: {"description": "Анкер с указанным ID не найден"}
    }
)
async def get_anchor_endpoint(anchor_id: str):  # ← ИЗМЕНИ ИМЯ
    """
    Получение информации о конкретном анкере.
    
    Возвращает детальную конфигурацию анкера по его ID.
    """
    from app.database import get_anchor_by_id
    
    anchor_data = get_anchor_by_id(anchor_id)  # ← Теперь это функция из БД
    
    if not anchor_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anchor '{anchor_id}' not found"
        )
    
    return Anchor(**anchor_data)


@router.delete(
    "/anchors/{anchor_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Анкер успешно удален"},
        404: {"description": "Анкер с указанным ID не найден"}
    }
)
async def delete_anchor_endpoint(anchor_id: str):
    """
    Удаление анкера из системы.
    
    Удаляет конфигурацию анкера. Используйте с осторожностью.
    """
    if not delete_anchor(anchor_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anchor '{anchor_id}' not found"
        )
    
    return None  # 204 No Content
