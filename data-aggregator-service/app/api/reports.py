"""
Модуль для работы с отчетами в Data Aggregator Service.
Содержит эндпоинты для генерации и получения различных типов отчетов.
"""
from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse, StreamingResponse
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd
import io

from app.models import (
    ZoneOccupancyReport, TimeInZoneReport, WorkflowEfficiencyReport,
    ErrorResponse, ValidationErrorResponse
)
from app.report_generator import (
    generate_zone_occupancy_report,
    generate_time_in_zone_report,
    generate_workflow_efficiency_report
)
from app.database import get_report_by_id, get_reports_by_type

router = APIRouter(tags=["Reports"])
logger = logging.getLogger(__name__)

@router.get(
    "/reports/zone-occupancy",
    response_model=ZoneOccupancyReport,
    responses={
        200: {"description": "Успешный запрос", "model": ZoneOccupancyReport},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def get_zone_occupancy_report_endpoint(
    start_time: str = Query(..., description="Начало периода", example="2023-11-28T00:00:00"),
    end_time: str = Query(..., description="Конец периода", example="2023-11-28T23:59:59"),
    zone_ids: Optional[str] = Query(None, description="Список ID зон через запятую", example="zone1,zone2"),
    entity_types: Optional[str] = Query(None, description="Список типов сущностей через запятую", example="employee,equipment")
):
    """
    Отчет по посещаемости зон.
    Возвращает данные о посещаемости различных зон за указанный период.
    """
    try:
        # Преобразуем строки в datetime
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Преобразуем строки в списки
        zone_id_list = zone_ids.split(',') if zone_ids else None
        entity_type_list = entity_types.split(',') if entity_types else None
        
        # Генерируем отчет
        report = generate_zone_occupancy_report(
            start_time=start_dt,
            end_time=end_dt,
            zone_ids=zone_id_list,
            entity_types=entity_type_list
        )
        
        return report
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error generating zone occupancy report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="REPORT_GENERATION_ERROR",
                message=f"Failed to generate zone occupancy report: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/reports/time-in-zone",
    response_model=TimeInZoneReport,
    responses={
        200: {"description": "Успешный запрос", "model": TimeInZoneReport},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def get_time_in_zone_report_endpoint(
    start_time: str = Query(..., description="Начало периода"),
    end_time: str = Query(..., description="Конец периода"),
    entity_id: Optional[str] = Query(None, description="ID сущности для фильтрации"),
    zone_id: Optional[str] = Query(None, description="ID зоны для фильтрации"),
    group_by: str = Query("day", description="Группировка данных", enum=["hour", "day", "week", "month"])
):
    """
    Отчет по времени пребывания в зонах.
    Возвращает данные о времени, проведенном сущностями в различных зонах.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        report = generate_time_in_zone_report(
            entity_id=entity_id,
            zone_id=zone_id,
            start_time=start_dt,
            end_time=end_dt,
            group_by=group_by
        )
        
        return report
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error generating time in zone report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="REPORT_GENERATION_ERROR",
                message=f"Failed to generate time in zone report: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/reports/workflow-efficiency",
    response_model=WorkflowEfficiencyReport,
    responses={
        200: {"description": "Успешный запрос", "model": WorkflowEfficiencyReport},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def get_workflow_efficiency_report_endpoint(
    start_time: str = Query(..., description="Начало периода"),
    end_time: str = Query(..., description="Конец периода"),
    zone_ids: Optional[str] = Query(None, description="Список ID зон через запятую"),
    entity_ids: Optional[str] = Query(None, description="Список ID сущностей через запятую")
):
    """
    Отчет по эффективности рабочих зон.
    Анализ эффективности использования рабочих зон и маршрутов.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        zone_id_list = zone_ids.split(',') if zone_ids else None
        entity_id_list = entity_ids.split(',') if entity_ids else None
        
        report = generate_workflow_efficiency_report(
            start_time=start_dt,
            end_time=end_dt,
            zone_ids=zone_id_list,
            entity_ids=entity_id_list
        )
        
        return report
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error generating workflow efficiency report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="REPORT_GENERATION_ERROR",
                message=f"Failed to generate workflow efficiency report: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/reports/{report_id}",
    response_model=Dict[str, Any],
    responses={
        200: {"description": "Успешный запрос", "model": Dict[str, Any]},
        404: {"description": "Отчет не найден", "model": ErrorResponse}
    }
)
async def get_report_by_id_endpoint(report_id: str):
    """
    Получение отчета по ID.
    Возвращает ранее сгенерированный отчет по его уникальному идентификатору.
    """
    try:
        report = get_report_by_id(report_id)
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="REPORT_NOT_FOUND",
                    message=f"Report with ID '{report_id}' not found"
                ).model_dump()
            )
        
        # Преобразуем report_data из JSON в объект
        report_data = report.get('report_data', {})
        return report_data
        
    except Exception as e:
        logger.error(f"Error getting report {report_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="GET_REPORT_ERROR",
                message=f"Failed to get report: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/reports/history/{report_type}",
    response_model=List[Dict[str, Any]],
    responses={
        200: {"description": "Успешный запрос", "model": List[Dict[str, Any]]},
        400: {"description": "Некорректные параметры", "model": ErrorResponse}
    }
)
async def get_reports_history_endpoint(
    report_type: str = Query(..., description="Тип отчета", enum=["zone_occupancy", "time_in_zone", "workflow_efficiency", "anomalies"]),
    start_time: Optional[str] = Query(None, description="Начало периода"),
    end_time: Optional[str] = Query(None, description="Конец периода"),
    limit: int = Query(100, description="Максимальное количество записей", ge=1, le=1000)
):
    """
    История сгенерированных отчетов.
    Возвращает список ранее сгенерированных отчетов определенного типа.
    """
    try:
        start_dt = None
        end_dt = None
        
        if start_time:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        if end_time:
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        reports = get_reports_by_type(
            report_type=report_type,
            start_time=start_dt,
            end_time=end_dt,
            limit=limit
        )
        
        # Форматируем результаты для ответа
        result = []
        for report in reports:
            result.append({
                "report_id": report["report_id"],
                "report_type": report["report_type"],
                "generated_at": report["generated_at"],
                "period_start": report.get("period_start"),
                "period_end": report.get("period_end"),
                "parameters": report.get("parameters")
            })
        
        return result
        
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="INVALID_DATETIME_FORMAT",
                message=f"Invalid datetime format: {str(e)}"
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Error getting reports history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="GET_REPORTS_HISTORY_ERROR",
                message=f"Failed to get reports history: {str(e)}"
            ).model_dump()
        )