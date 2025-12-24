from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse
import logging

from app.models import Entity, EntityCreate, EntityUpdate, ErrorResponse, ValidationErrorResponse
from app.database import (
    create_entity, get_all_entities, get_entity_by_id, 
    get_entity_by_tag_id, update_entity, delete_entity
)

router = APIRouter(tags=["Entities"])
logger = logging.getLogger(__name__)


@router.get(
    "/entities",
    response_model=list[Entity],
    responses={
        200: {"description": "Успешный запрос", "model": list[Entity]},
        500: {"description": "Ошибка сервера", "model": ErrorResponse}
    }
)
async def get_all_entities_endpoint(
    entity_type: str = Query("all", description="Тип сущности для фильтрации", enum=["employee", "equipment", "all"]),
    limit: int = Query(100, description="Максимальное количество записей", ge=1, le=1000)
):
    """
    Получение списка всех зарегистрированных сущностей.
    
    Возвращает список сотрудников и оборудования с привязанными метками.
    """
    try:
        entities = get_all_entities(
            entity_type=entity_type if entity_type != "all" else None,
            limit=limit
        )
        
        # Преобразуем в модели Pydantic
        result = []
        for entity_data in entities:
            result.append(Entity(**entity_data))
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting entities: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get entities: {str(e)}"
            ).model_dump()
        )


@router.post(
    "/entities",
    response_model=Entity,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Сущность успешно создана", "model": Entity},
        400: {"description": "Некорректные данные", "model": ErrorResponse},
        409: {"description": "Конфликт (сущность с таким ID уже существует)", "model": ErrorResponse}
    }
)
async def create_entity_endpoint(entity: EntityCreate):
    """
    Регистрация новой сущности.
    
    Создает новую запись о сотруднике или оборудовании.
    """
    try:
        # Проверяем, существует ли уже сущность с таким ID
        existing_entity = get_entity_by_id(entity.entity_id)
        if existing_entity:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=ErrorResponse(
                    error_code="ENTITY_ALREADY_EXISTS",
                    message=f"Entity with ID '{entity.entity_id}' already exists"
                ).model_dump()
            )
        
        # Проверяем, не привязана ли метка уже к другой сущности
        if entity.tag_id:
            existing_with_tag = get_entity_by_tag_id(entity.tag_id)
            if existing_with_tag:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorResponse(
                        error_code="TAG_ALREADY_ASSIGNED",
                        message=f"Tag '{entity.tag_id}' is already assigned to entity '{existing_with_tag['entity_id']}'"
                    ).model_dump()
                )
        
        # Создаем сущность
        entity_data = entity.model_dump()
        created_entity = create_entity(entity_data)
        
        return Entity(**created_entity)
        
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
        logger.error(f"Error creating entity: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="CREATE_ENTITY_ERROR",
                message=f"Failed to create entity: {str(e)}"
            ).model_dump()
        )


@router.get(
    "/entities/{entity_id}",
    response_model=Entity,
    responses={
        200: {"description": "Успешный запрос", "model": Entity},
        404: {"description": "Сущность с указанным ID не найдена", "model": ErrorResponse}
    }
)
async def get_entity_by_id_endpoint(entity_id: str):
    """
    Получение информации о конкретной сущности.
    
    Возвращает детальную информацию о сущности по её ID.
    """
    try:
        entity_data = get_entity_by_id(entity_id)
        
        if not entity_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ENTITY_NOT_FOUND",
                    message=f"Entity with ID '{entity_id}' not found"
                ).model_dump()
            )
        
        return Entity(**entity_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting entity {entity_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get entity: {str(e)}"
            ).model_dump()
        )


@router.patch(
    "/entities/{entity_id}",
    response_model=Entity,
    responses={
        200: {"description": "Сущность успешно обновлена", "model": Entity},
        404: {"description": "Сущность с указанным ID не найдена"},
        400: {"description": "Некорректные данные"}
    }
)
async def update_entity_endpoint(entity_id: str, entity_update: EntityUpdate):
    """
    Обновление информации о сущности.
    
    Обновляет данные сущности (включая привязку/отвязку метки).
    """
    try:
        # Проверяем, существует ли сущность
        existing_entity = get_entity_by_id(entity_id)
        if not existing_entity:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ENTITY_NOT_FOUND",
                    message=f"Entity with ID '{entity_id}' not found"
                ).model_dump()
            )
        
        # Если пытаемся привязать метку, проверяем что она не занята
        if entity_update.tag_id is not None:
            if entity_update.tag_id == "":
                # Разрешаем отвязку метки (пустая строка или null)
                pass
            else:
                existing_with_tag = get_entity_by_tag_id(entity_update.tag_id)
                if existing_with_tag and existing_with_tag['entity_id'] != entity_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=ErrorResponse(
                            error_code="TAG_ALREADY_ASSIGNED",
                            message=f"Tag '{entity_update.tag_id}' is already assigned to entity '{existing_with_tag['entity_id']}'"
                        ).model_dump()
                    )
        
        # Обновляем сущность
        update_data = entity_update.model_dump(exclude_unset=True)
        
        # Если tag_id пустая строка - отвязываем метку (преобразуем в None)
        if 'tag_id' in update_data and update_data['tag_id'] == "":
            update_data['tag_id'] = None
        
        updated_entity = update_entity(entity_id, update_data)
        
        if not updated_entity:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(
                    error_code="UPDATE_FAILED",
                    message="Failed to update entity"
                ).model_dump()
            )
        
        return Entity(**updated_entity)
        
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
        logger.error(f"Error updating entity {entity_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="UPDATE_ENTITY_ERROR",
                message=f"Failed to update entity: {str(e)}"
            ).model_dump()
        )


@router.delete(
    "/entities/{entity_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Сущность успешно удалена"},
        404: {"description": "Сущность с указанным ID не найдена", "model": ErrorResponse}
    }
)
async def delete_entity_endpoint(entity_id: str):
    """
    Удаление сущности.
    
    Удаляет сущность из системы.
    """
    try:
        # Проверяем, существует ли сущность
        existing_entity = get_entity_by_id(entity_id)
        if not existing_entity:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ENTITY_NOT_FOUND",
                    message=f"Entity with ID '{entity_id}' not found"
                ).model_dump()
            )
        
        # Удаляем сущность
        deleted = delete_entity(entity_id)
        
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(
                    error_code="DELETE_FAILED",
                    message="Failed to delete entity"
                ).model_dump()
            )
        
        return None  # 204 No Content
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting entity {entity_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DELETE_ENTITY_ERROR",
                message=f"Failed to delete entity: {str(e)}"
            ).model_dump()
        )


@router.get(
    "/entities/tag/{tag_id}",
    response_model=Entity,
    responses={
        200: {"description": "Успешный запрос", "model": Entity},
        404: {"description": "Сущность с указанной меткой не найдена", "model": ErrorResponse}
    }
)
async def get_entity_by_tag_id_endpoint(tag_id: str):
    """
    Получение сущности по привязанной метке.
    
    Возвращает сущность, к которой привязана указанная метка.
    """
    try:
        entity_data = get_entity_by_tag_id(tag_id)
        
        if not entity_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ENTITY_NOT_FOUND",
                    message=f"No entity found with tag '{tag_id}'"
                ).model_dump()
            )
        
        return Entity(**entity_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting entity by tag {tag_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="DATABASE_ERROR",
                message=f"Failed to get entity by tag: {str(e)}"
            ).model_dump()
        )
