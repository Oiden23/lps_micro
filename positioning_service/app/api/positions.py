from fastapi import APIRouter, HTTPException, status, Query
from datetime import datetime
from typing import List

from app.models import Position, ErrorResponse
from app.database import get_latest_position_db, get_position_history_db

router = APIRouter()


@router.get(
    "/positions/current/{tag_id}",
    response_model=Position,
    responses={
        200: {"model": Position},
        404: {"model": ErrorResponse}
    }
)
async def get_current_position(tag_id: str):
    position_data = get_latest_position_db(tag_id)
    
    if not position_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": "POSITION_NOT_FOUND",
                "message": f"Position for tag '{tag_id}' not found",
                "calculation_timestamp": datetime.now().isoformat()
            }
        )
    
    return Position(**position_data)

@router.get(
    "/positions/history/{tag_id}",
    response_model=List[Position],
    responses={
        200: {
            "description": "Успешный запрос. Возвращает массив позиций.",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "tag_id": "tag-employee-123",
                            "x": 10.5,
                            "y": 20.3,
                            "z": 0.0,
                            "calculation_timestamp": "2023-11-28T14:30:00Z",
                            "accuracy": 0.5
                        }
                    ]
                }
            }
        },
        400: {
            "description": "Неверные параметры запроса",
            "model": ErrorResponse
        }
    }
)
async def get_position_history(
    tag_id: str,
    start_time: datetime = Query(
        ..., 
        description="Начало периода (включительно) в формате ISO 8601"
    ),
    end_time: datetime = Query(
        ..., 
        description="Конец периода (включительно) в формате ISO 8601"
    ),
    limit: int = Query(
        1000, 
        ge=1, 
        le=10000, 
        description="Максимальное количество записей для возврата"
    )
):
    """
    Получение истории перемещений метки за период.
    
    Данные извлекаются из постоянного хранилища (БД).
    """
    try:
        positions = get_position_history_db(tag_id, start_time, end_time, limit)
        
        if not positions:
            # Возвращаем пустой список вместо ошибки
            return []
        print (positions)
        return [Position(**pos) for pos in positions]
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_PARAMETERS",
                message=str(e)
            ).model_dump()
        )
