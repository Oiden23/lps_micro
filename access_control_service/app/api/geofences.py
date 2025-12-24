from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
import logging
from uuid import UUID

from app.models import (
    Geofence, GeofenceCreate, PointCheckRequest, GeofenceCheckResult,
    ErrorResponse, ValidationErrorResponse
)
from app.database import (
    create_geofence, get_all_geofences, get_geofence_by_id,
    update_geofence, delete_geofence, check_point_in_geofences
)

router = APIRouter(tags=["Geofences"])
logger = logging.getLogger(__name__)


@router.get(
    "/geofences",
    response_model=list[Geofence],
    responses={
        200: {"description": "Успешный запрос", "model": list[Geofence]},
        500: {"description": "Ошибка сервера", "model": ErrorResponse}
    }
)
async def get_all_geofences_endpoint():
    """
    Получение списка всех геозон.
    
    Возвращает список всех зарегистрированных геозон.
    """
    try:
        geofences_data = get_all_geofences()
        
        # Преобразуем в модели Pydantic
        result = []
        for geofence_data in geofences_data:
            result.append(Geofence(**geofence_data))
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting geofences: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get geofences: {str(e)}"
            ).model_dump()
        )


@router.post(
    "/geofences",
    response_model=Geofence,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Геозона успешно создана", "model": Geofence},
        400: {"description": "Некорректные данные геозоны", "model": ErrorResponse}
    }
)
async def create_geofence_endpoint(geofence: GeofenceCreate):
    """
    Создание новой геозоны.
    
    Создает новую геозону (зону безопасности, доступа и т.д.).
    """
    try:
        # Создаем геозону
        geofence_data = geofence.model_dump()
        created_geofence = create_geofence(geofence_data)
        
        return Geofence(**created_geofence)
        
    except ValueError as e:
        # Ошибки валидации Pydantic или бизнес-логики
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "coordinates", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error creating geofence: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="CREATE_GEOFENCE_ERROR",
                message=f"Failed to create geofence: {str(e)}"
            ).model_dump()
        )


@router.get(
    "/geofences/{geofence_id}",
    response_model=Geofence,
    responses={
        200: {"description": "Успешный запрос", "model": Geofence},
        404: {"description": "Геозона с указанным ID не найдена", "model": ErrorResponse}
    }
)
async def get_geofence_by_id_endpoint(geofence_id: str):
    """
    Получение информации о конкретной геозоне.
    
    Возвращает детальную информацию о геозоне.
    """
    try:
        # Проверяем что geofence_id - валидный UUID
        try:
            UUID(geofence_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_UUID",
                    message=f"Invalid geofence_id format: '{geofence_id}'"
                ).model_dump()
            )
        
        geofence_data = get_geofence_by_id(geofence_id)
        
        if not geofence_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="GEOFENCE_NOT_FOUND",
                    message=f"Geofence with ID '{geofence_id}' not found"
                ).model_dump()
            )
        
        return Geofence(**geofence_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting geofence {geofence_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get geofence: {str(e)}"
            ).model_dump()
        )


@router.put(
    "/geofences/{geofence_id}",
    response_model=Geofence,
    responses={
        200: {"description": "Геозона успешно обновлена", "model": Geofence},
        404: {"description": "Геозона с указанным ID не найдена", "model": ErrorResponse},
        400: {"description": "Некорректные данные геозоны", "model": ErrorResponse}
    }
)
async def update_geofence_endpoint(geofence_id: str, geofence: GeofenceCreate):
    """
    Обновление геозоны.
    
    Полностью обновляет информацию о геозоне.
    """
    try:
        # Проверяем что geofence_id - валидный UUID
        try:
            UUID(geofence_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_UUID",
                    message=f"Invalid geofence_id format: '{geofence_id}'"
                ).model_dump()
            )
        
        # Проверяем, существует ли геозона
        existing_geofence = get_geofence_by_id(geofence_id)
        if not existing_geofence:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="GEOFENCE_NOT_FOUND",
                    message=f"Geofence with ID '{geofence_id}' not found"
                ).model_dump()
            )
        
        # Обновляем геозону
        geofence_data = geofence.model_dump()
        updated_geofence = update_geofence(geofence_id, geofence_data)
        
        if not updated_geofence:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(
                    error_code="UPDATE_FAILED",
                    message="Failed to update geofence"
                ).model_dump()
            )
        
        return Geofence(**updated_geofence)
        
    except HTTPException:
        raise
    except ValueError as e:
        # Ошибки валидации Pydantic или бизнес-логики
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "coordinates", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error updating geofence {geofence_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="UPDATE_GEOFENCE_ERROR",
                message=f"Failed to update geofence: {str(e)}"
            ).model_dump()
        )


@router.delete(
    "/geofences/{geofence_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Геозона успешно удалена"},
        404: {"description": "Геозона с указанным ID не найдена", "model": ErrorResponse}
    }
)
async def delete_geofence_endpoint(geofence_id: str):
    """
    Удаление геозоны.
    
    Удаляет геозону из системы.
    """
    try:
        # Проверяем что geofence_id - валидный UUID
        try:
            UUID(geofence_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_UUID",
                    message=f"Invalid geofence_id format: '{geofence_id}'"
                ).model_dump()
            )
        
        # Проверяем, существует ли геозона
        existing_geofence = get_geofence_by_id(geofence_id)
        if not existing_geofence:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="GEOFENCE_NOT_FOUND",
                    message=f"Geofence with ID '{geofence_id}' not found"
                ).model_dump()
            )
        
        # Удаляем геозону
        deleted = delete_geofence(geofence_id)
        
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(
                    error_code="DELETE_FAILED",
                    message="Failed to delete geofence"
                ).model_dump()
            )
        
        return None  # 204 No Content
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting geofence {geofence_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DELETE_GEOFENCE_ERROR",
                message=f"Failed to delete geofence: {str(e)}"
            ).model_dump()
        )


@router.post(
    "/geofences/check",
    response_model=GeofenceCheckResult,
    responses={
        200: {"description": "Успешная проверка", "model": GeofenceCheckResult},
        400: {"description": "Некорректные данные запроса", "model": ErrorResponse}
    }
)
async def check_point_in_geofences_endpoint(check_request: PointCheckRequest):
    """
    Проверка нахождения точки в геозонах.
    
    Проверяет, находится ли указанная точка внутри какой-либо геозоны.
    """
    try:
        # Преобразуем UUID строки в список строк для БД
        geofence_ids = None
        if check_request.geofence_ids:
            geofence_ids = [str(geofence_id) for geofence_id in check_request.geofence_ids]
        
        # Проверяем точку
        intersections = check_point_in_geofences(
            x=check_request.x,
            y=check_request.y,
            z=check_request.z,
            geofence_ids=geofence_ids
        )
        
        # Формируем ответ
        return GeofenceCheckResult(
            point={
                "x": check_request.x,
                "y": check_request.y,
                "z": check_request.z
            },
            intersections=intersections
        )
        
    except ValueError as e:
        # Ошибки валидации Pydantic
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "coordinates", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error checking point in geofences: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="CHECK_POINT_ERROR",
                message=f"Failed to check point in geofences: {str(e)}"
            ).model_dump()
        )
