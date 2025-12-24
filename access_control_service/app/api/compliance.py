from fastapi import APIRouter, HTTPException, status, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
import asyncio
from typing import List, Dict, Any

from app.models import (
    ComplianceCheckRequest, ComplianceCheckResult,
    BatchComplianceCheckRequest, BatchComplianceCheckResult,
    Violation, ErrorResponse, ValidationErrorResponse
)
from app.database import (
    get_entity_by_id, get_applicable_rules,
    check_point_in_geofences, create_violation, get_violations
)
from app.compliance_checker import check_compliance_for_position

router = APIRouter(tags=["Compliance"])
logger = logging.getLogger(__name__)

# WebSocket соединения для реального времени
active_connections: List[WebSocket] = []


@router.post(
    "/compliance/check",
    response_model=ComplianceCheckResult,
    responses={
        200: {"description": "Результат проверки", "model": ComplianceCheckResult},
        400: {"description": "Некорректные данные запроса", "model": ErrorResponse},
        404: {"description": "Сущность не найдена", "model": ErrorResponse}
    }
)
async def check_compliance_endpoint(check_request: ComplianceCheckRequest):
    """
    Проверка соблюдения правил для позиции.
    
    Проверяет, соответствует ли текущая позиция сущности установленным правилам.
    """
    try:
        # Получаем информацию о сущности
        entity = get_entity_by_id(check_request.entity_id)
        if not entity:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ENTITY_NOT_FOUND",
                    message=f"Entity with ID '{check_request.entity_id}' not found"
                ).model_dump()
            )
        
        # Проверяем соблюдение правил
        compliance_result = check_compliance_for_position(
            entity=entity,
            position=check_request.position,
            timestamp=check_request.position.timestamp
        )
        
        # Если есть нарушения, сохраняем их
        if compliance_result.violations:
            for violation_data in compliance_result.violations:
                try:
                    create_violation(violation_data)
                    # Отправляем уведомление через WebSocket
                    await notify_violation_via_websocket(violation_data)
                except Exception as e:
                    logger.error(f"Failed to save violation: {e}")
        
        return compliance_result
        
    except HTTPException:
        raise
    except ValueError as e:
        # Ошибки валидации Pydantic
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "position", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error checking compliance: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="COMPLIANCE_CHECK_ERROR",
                message=f"Failed to check compliance: {str(e)}"
            ).model_dump()
        )


@router.post(
    "/compliance/check/batch",
    response_model=BatchComplianceCheckResult,
    responses={
        200: {"description": "Результаты массовой проверки", "model": BatchComplianceCheckResult},
        400: {"description": "Некорректные данные запроса", "model": ErrorResponse}
    }
)
async def check_compliance_batch_endpoint(batch_request: BatchComplianceCheckRequest):
    """
    Массовая проверка соблюдения правил.
    
    Проверяет соблюдение правил для массива позиций.
    """
    try:
        results = []
        violation_count = 0
        warning_count = 0
        
        for check in batch_request.checks:
            # Получаем информацию о сущности
            entity = get_entity_by_id(check.entity_id)
            if not entity:
                # Если сущность не найдена, пропускаем с ошибкой
                result = ComplianceCheckResult(
                    entity_id=check.entity_id,
                    position=check.position,
                    is_compliant=False,
                    violations=[],
                    warnings=[]
                )
                results.append(result)
                continue
            
            # Проверяем соблюдение правил
            compliance_result = check_compliance_for_position(
                entity=entity,
                position=check.position,
                timestamp=check.position.timestamp
            )
            
            # Если есть нарушения, сохраняем их
            if compliance_result.violations:
                violation_count += len(compliance_result.violations)
                for violation_data in compliance_result.violations:
                    try:
                        create_violation(violation_data)
                        # Отправляем уведомление через WebSocket
                        await notify_violation_via_websocket(violation_data)
                    except Exception as e:
                        logger.error(f"Failed to save violation: {e}")
            
            if compliance_result.warnings:
                warning_count += len(compliance_result.warnings)
            
            results.append(compliance_result)
        
        # Создаем итоговый результат
        summary = {
            "total_checks": len(results),
            "compliant": len([r for r in results if r.is_compliant]),
            "violations": violation_count,
            "warnings": warning_count
        }
        
        return BatchComplianceCheckResult(
            results=results,
            summary=summary
        )
        
    except ValueError as e:
        # Ошибки валидации Pydantic
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "checks", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error checking batch compliance: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="BATCH_COMPLIANCE_CHECK_ERROR",
                message=f"Failed to check batch compliance: {str(e)}"
            ).model_dump()
        )


@router.get(
    "/compliance/violations",
    response_model=list[Violation],
    responses={
        200: {"description": "Успешный запрос", "model": list[Violation]},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def get_violations_endpoint(
    start_time: str = Query(None, description="Начало периода"),
    end_time: str = Query(None, description="Конец периода"),
    entity_id: str = Query(None, description="Фильтр по сущности"),
    severity: str = Query(None, description="Фильтр по серьезности", enum=["low", "medium", "high", "critical"]),
    limit: int = Query(100, description="Максимальное количество записей", ge=1, le=1000)
):
    """
    Получение истории нарушений.
    
    Возвращает историю зафиксированных нарушений правил.
    """
    try:
        # Валидация параметров времени
        if start_time:
            try:
                datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorResponse(
                        error_code="INVALID_DATE_FORMAT",
                        message=f"Invalid start_time format: '{start_time}'. Use ISO 8601 format."
                    ).model_dump()
                )
        
        if end_time:
            try:
                datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorResponse(
                        error_code="INVALID_DATE_FORMAT",
                        message=f"Invalid end_time format: '{end_time}'. Use ISO 8601 format."
                    ).model_dump()
                )
        
        # Получаем нарушения
        violations_data = get_violations(
            start_time=start_time,
            end_time=end_time,
            entity_id=entity_id,
            severity=severity,
            limit=limit
        )
        
        # Преобразуем в модели Pydantic
        result = []
        for violation_data in violations_data:
            result.append(Violation(**violation_data))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting violations: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="GET_VIOLATIONS_ERROR",
                message=f"Failed to get violations: {str(e)}"
            ).model_dump()
        )


@router.websocket("/compliance/violations/realtime")
async def get_realtime_violations_endpoint(websocket: WebSocket):
    """
    WebSocket для получения нарушений в реальном времени.
    
    Устанавливает WebSocket соединение для получения уведомлений о нарушениях в реальном времени.
    """
    await websocket.accept()
    active_connections.append(websocket)
    
    try:
        while True:
            # Ожидаем сообщения от клиента (например, команды подписки/отписки)
            data = await websocket.receive_text()
            
            # Можно реализовать команды, например:
            # - "subscribe": подписаться на уведомления
            # - "unsubscribe": отписаться
            # - "filter:entity_id=emp-001": фильтровать по сущности
            
            logger.info(f"WebSocket message from client: {data}")
            
            # Простая эхо-ответ (можно расширить)
            await websocket.send_text(f"Server received: {data}")
            
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if websocket in active_connections:
            active_connections.remove(websocket)


async def notify_violation_via_websocket(violation_data: Dict[str, Any]):
    """Отправка уведомления о нарушении всем подключенным WebSocket клиентам"""
    if not active_connections:
        return
    
    message = {
        "type": "violation",
        "data": violation_data,
        "timestamp": datetime.now().isoformat()
    }
    
    # Отправляем всем активным соединениям
    disconnected = []
    for connection in active_connections:
        try:
            await connection.send_json(message)
        except Exception as e:
            logger.error(f"Failed to send WebSocket message: {e}")
            disconnected.append(connection)
    
    # Удаляем отключенные соединения
    for connection in disconnected:
        if connection in active_connections:
            active_connections.remove(connection)


# Вспомогательная функция для проверки подписки клиентов
async def get_connected_clients():
    """Получение информации о подключенных клиентах"""
    return {
        "connected_clients": len(active_connections),
        "clients": [str(id(conn)) for conn in active_connections]
    }
