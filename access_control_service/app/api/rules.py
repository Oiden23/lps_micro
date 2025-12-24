from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse
import logging
from uuid import UUID

from app.models import Rule, RuleCreate, RuleUpdate, ErrorResponse, ValidationErrorResponse
from app.database import (
    create_rule, get_all_rules, get_rule_by_id,
    update_rule, delete_rule
)

router = APIRouter(tags=["Rules"])
logger = logging.getLogger(__name__)


@router.get(
    "/rules",
    response_model=list[Rule],
    responses={
        200: {"description": "Успешный запрос", "model": list[Rule]},
        500: {"description": "Ошибка сервера", "model": ErrorResponse}
    }
)
async def get_all_rules_endpoint(
    is_active: bool = Query(None, description="Фильтр по активности правил")
):
    """
    Получение списка всех правил.
    
    Возвращает список всех правил доступа и контроля.
    """
    try:
        rules_data = get_all_rules(is_active=is_active)
        
        # Преобразуем в модели Pydantic
        result = []
        for rule_data in rules_data:
            result.append(Rule(**rule_data))
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting rules: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get rules: {str(e)}"
            ).model_dump()
        )


@router.post(
    "/rules",
    response_model=Rule,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Правило успешно создано", "model": Rule},
        400: {"description": "Некорректные данные правила", "model": ErrorResponse}
    }
)
async def create_rule_endpoint(rule: RuleCreate):
    """
    Создание нового правила.
    
    Создает новое правило доступа или контроля.
    """
    try:
        # Проверяем, что geofence_id существует в БД
        from app.database import get_geofence_by_id
        geofence = get_geofence_by_id(str(rule.geofence_id))
        if not geofence:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="GEOFENCE_NOT_FOUND",
                    message=f"Geofence with ID '{rule.geofence_id}' not found"
                ).model_dump()
            )
        
        # Если указана конкретная сущность, проверяем что она существует
        if rule.entity_id and rule.entity_type != "all":
            from app.database import get_entity_by_id
            entity = get_entity_by_id(rule.entity_id)
            if not entity:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorResponse(
                        error_code="ENTITY_NOT_FOUND",
                        message=f"Entity with ID '{rule.entity_id}' not found"
                    ).model_dump()
                )
            # Проверяем соответствие типа сущности
            if entity['entity_type'] != rule.entity_type:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorResponse(
                        error_code="ENTITY_TYPE_MISMATCH",
                        message=f"Entity '{rule.entity_id}' is of type '{entity['entity_type']}', but rule expects '{rule.entity_type}'"
                    ).model_dump()
                )
        
        # Создаем правило
        rule_data = rule.model_dump()
        created_rule = create_rule(rule_data)
        
        return Rule(**created_rule)
        
    except HTTPException:
        raise
    except ValueError as e:
        # Ошибки валидации Pydantic
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "entity_type", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="CREATE_RULE_ERROR",
                message=f"Failed to create rule: {str(e)}"
            ).model_dump()
        )


@router.get(
    "/rules/{rule_id}",
    response_model=Rule,
    responses={
        200: {"description": "Успешный запрос", "model": Rule},
        404: {"description": "Правило с указанным ID не найдено", "model": ErrorResponse}
    }
)
async def get_rule_by_id_endpoint(rule_id: str):
    """
    Получение информации о правиле.
    
    Возвращает детальную информацию о правиле.
    """
    try:
        # Проверяем что rule_id - валидный UUID
        try:
            UUID(rule_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_UUID",
                    message=f"Invalid rule_id format: '{rule_id}'"
                ).model_dump()
            )
        
        rule_data = get_rule_by_id(rule_id)
        
        if not rule_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="RULE_NOT_FOUND",
                    message=f"Rule with ID '{rule_id}' not found"
                ).model_dump()
            )
        
        return Rule(**rule_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting rule {rule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get rule: {str(e)}"
            ).model_dump()
        )


@router.patch(
    "/rules/{rule_id}",
    response_model=Rule,
    responses={
        200: {"description": "Правило успешно обновлено", "model": Rule},
        404: {"description": "Правило с указанным ID не найдено", "model": ErrorResponse},
        400: {"description": "Некорректные данные", "model": ErrorResponse}
    }
)
async def update_rule_endpoint(rule_id: str, rule_update: RuleUpdate):
    """
    Обновление правила.
    
    Обновляет данные правила (например, активность).
    """
    try:
        # Проверяем что rule_id - валидный UUID
        try:
            UUID(rule_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_UUID",
                    message=f"Invalid rule_id format: '{rule_id}'"
                ).model_dump()
            )
        
        # Проверяем, существует ли правило
        existing_rule = get_rule_by_id(rule_id)
        if not existing_rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="RULE_NOT_FOUND",
                    message=f"Rule with ID '{rule_id}' not found"
                ).model_dump()
            )
        
        # Обновляем правило
        update_data = rule_update.model_dump(exclude_unset=True)
        updated_rule = update_rule(rule_id, update_data)
        
        if not updated_rule:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(
                    error_code="UPDATE_FAILED",
                    message="Failed to update rule"
                ).model_dump()
            )
        
        return Rule(**updated_rule)
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e)
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error updating rule {rule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="UPDATE_RULE_ERROR",
                message=f"Failed to update rule: {str(e)}"
            ).model_dump()
        )


@router.delete(
    "/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Правило успешно удалено"},
        404: {"description": "Правило с указанным ID не найдено", "model": ErrorResponse}
    }
)
async def delete_rule_endpoint(rule_id: str):
    """
    Удаление правила.
    
    Удаляет правило из системы.
    """
    try:
        # Проверяем что rule_id - валидный UUID
        try:
            UUID(rule_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_UUID",
                    message=f"Invalid rule_id format: '{rule_id}'"
                ).model_dump()
            )
        
        # Проверяем, существует ли правило
        existing_rule = get_rule_by_id(rule_id)
        if not existing_rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="RULE_NOT_FOUND",
                    message=f"Rule with ID '{rule_id}' not found"
                ).model_dump()
            )
        
        # Удаляем правило
        deleted = delete_rule(rule_id)
        
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(
                    error_code="DELETE_FAILED",
                    message="Failed to delete rule"
                ).model_dump()
            )
        
        return None  # 204 No Content
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting rule {rule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DELETE_RULE_ERROR",
                message=f"Failed to delete rule: {str(e)}"
            ).model_dump()
        )
