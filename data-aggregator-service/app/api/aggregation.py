"""
Модуль для управления процессами агрегации данных.
Содержит эндпоинты для запуска, мониторинга и управления задачами агрегации.
"""
from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import asyncio

from app.models import (
    AggregationTask, ErrorResponse, ValidationErrorResponse
)
from app.database import (
    create_aggregation_task, update_aggregation_task,
    get_aggregation_task, get_pending_aggregation_tasks,
    get_data_for_period, store_aggregated_data
)
from app.analytics_engine import AnalyticsEngine

router = APIRouter(tags=["Aggregation"])
logger = logging.getLogger(__name__)

# Инициализируем аналитический движок
analytics_engine = AnalyticsEngine()

@router.post(
    "/aggregation/trigger",
    response_model=AggregationTask,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        202: {"description": "Процесс агрегации запущен", "model": AggregationTask},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def trigger_aggregation_endpoint(
    start_time: Optional[str] = Query(None, description="Начало периода для агрегации"),
    end_time: Optional[str] = Query(None, description="Конец периода для агрегации"),
    force: bool = Query(False, description="Принудительная агрегация, даже если данные уже существуют")
):
    """
    Запуск процесса агрегации данных.
    Ручной запуск процесса агрегации данных за указанный период.
    """
    try:
        # Если время не указано, используем последний час
        if not start_time or not end_time:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(hours=1)
        else:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Проверяем валидность периода
        if start_dt >= end_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="INVALID_TIME_RANGE",
                    message="Start time must be before end time"
                ).model_dump()
            )
        
        if (end_dt - start_dt).total_seconds() > 86400 * 30:  # 30 дней
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorResponse(
                    error_code="TIME_RANGE_TOO_LARGE",
                    message="Time range cannot exceed 30 days"
                ).model_dump()
            )
        
        # Создаем задачу агрегации
        task = create_aggregation_task(
            start_time=start_dt,
            end_time=end_dt,
            aggregation_type="custom"
        )
        
        # Запускаем агрегацию в фоновом режиме
        asyncio.create_task(process_aggregation_task(task['task_id'], start_dt, end_dt, force))
        
        return AggregationTask(**task)
        
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
        logger.error(f"Error triggering aggregation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="AGGREGATION_TRIGGER_ERROR",
                message=f"Failed to trigger aggregation: {str(e)}"
            ).model_dump()
        )

async def process_aggregation_task(task_id: str, start_time: datetime, end_time: datetime, force: bool):
    """
    Фоновая задача для обработки агрегации данных.
    """
    try:
        logger.info(f"Starting aggregation task {task_id} for period {start_time} to {end_time}")
        
        # Обновляем статус задачи
        update_aggregation_task(task_id, status="processing")
        
        # Получаем данные за период
        data = get_data_for_period(start_time, end_time)
        
        if not data:
            logger.warning(f"No data found for aggregation period {start_time} to {end_time}")
            update_aggregation_task(task_id, status="completed", records_processed=0)
            return
        
        logger.info(f"Found {len(data)} records for aggregation")
        
        # Агрегируем данные
        aggregated_records = await aggregate_data_for_period(start_time, end_time, data, force)
        
        # Обновляем статус задачи
        update_aggregation_task(
            task_id,
            status="completed",
            records_processed=len(aggregated_records)
        )
        
        logger.info(f"Aggregation task {task_id} completed successfully. Processed {len(aggregated_records)} records")
        
    except Exception as e:
        logger.error(f"Error in aggregation task {task_id}: {e}")
        update_aggregation_task(
            task_id,
            status="failed",
            error_message=str(e)
        )

async def aggregate_data_for_period(start_time: datetime, end_time: datetime, 
                                   raw_data: List[Dict[str, Any]], force: bool) -> List[Dict[str, Any]]:
    """
    Агрегация данных за указанный период.
    
    Args:
        start_time: Начало периода
        end_time: Конец периода
        raw_data: Исходные данные для агрегации
        force: Принудительная агрегация даже если данные уже существуют
    
    Returns:
        List[Dict[str, Any]]: Список агрегированных записей
    """
    try:
        # Преобразуем данные в DataFrame
        df = pd.DataFrame(raw_data)
        
        # Добавляем временные признаки
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['week_number'] = df['timestamp'].dt.isocalendar().week
        df['month'] = df['timestamp'].dt.month
        df['year'] = df['timestamp'].dt.year
        
        aggregated_records = []
        
        # 1. Агрегация по зонам и сущностям
        zone_entity_groups = df.groupby(['zone_id', 'entity_id'])
        for (zone_id, entity_id), group in zone_entity_groups:
            # Вычисляем метрики для группы
            total_duration = group['duration_minutes'].sum()
            visit_count = len(group)
            avg_duration = group['duration_minutes'].mean()
            
            # Создаем агрегированную запись
            record = {
                'entity_id': entity_id,
                'entity_name': group['entity_name'].iloc[0] if 'entity_name' in group else '',
                'entity_type': group['entity_type'].iloc[0] if 'entity_type' in group else 'employee',
                'zone_id': zone_id,
                'zone_name': group['zone_name'].iloc[0] if 'zone_name' in group else '',
                'zone_type': group['zone_type'].iloc[0] if 'zone_type' in group else 'work_area',
                'timestamp': start_time.isoformat(),
                'duration_minutes': round(total_duration, 2),
                'hour': group['hour'].mode().iloc[0] if not group['hour'].mode().empty else None,
                'day_of_week': group['day_of_week'].mode().iloc[0] if not group['day_of_week'].mode().empty else None,
                'week_number': group['week_number'].mode().iloc[0] if not group['week_number'].mode().empty else None,
                'month': group['month'].mode().iloc[0] if not group['month'].mode().empty else None,
                'year': group['year'].mode().iloc[0] if not group['year'].mode().empty else None,
                'data_type': 'zone_entry',
                'raw_data': {
                    'visit_count': visit_count,
                    'avg_duration': round(avg_duration, 2),
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()
                }
            }
            aggregated_records.append(record)
        
        # 2. Вычисляем аналитические метрики
        workflow_metrics = _calculate_workflow_metrics(df, start_time, end_time)
        aggregated_records.extend(workflow_metrics)
        
        # 3. Обнаружение аномалий
        anomalies = analytics_engine.detect_anomalies(start_time, end_time)
        for anomaly in anomalies.anomalies:
            record = {
                'entity_id': anomaly['entity_id'],
                'entity_name': anomaly['entity_name'],
                'entity_type': anomaly['entity_type'],
                'zone_id': anomaly.get('zone_id', ''),
                'zone_name': anomaly.get('zone_name', ''),
                'timestamp': anomaly['timestamp'].isoformat(),
                'duration_minutes': 0,
                'data_type': 'anomaly',
                'raw_data': anomaly
            }
            aggregated_records.append(record)
        
        # 4. Сохраняем агрегированные данные
        if aggregated_records:
            store_aggregated_data(aggregated_records)
        
        return aggregated_records
        
    except Exception as e:
        logger.error(f"Error aggregating data for period {start_time} to {end_time}: {e}")
        raise

def _calculate_workflow_metrics(df: pd.DataFrame, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Вычисление метрик эффективности рабочих процессов.
    """
    metrics = []
    
    try:
        # Группируем по зонам для расчета эффективности
        zone_groups = df.groupby('zone_id')
        
        for zone_id, zone_group in zone_groups:
            total_minutes = (end_time - start_time).total_seconds() / 60
            
            if total_minutes > 0:
                # Коэффициент использования зоны
                zone_duration = zone_group['duration_minutes'].sum()
                utilization_rate = min(1.0, zone_duration / total_minutes)
                
                # Среднее количество сущностей в час
                hours_in_period = total_minutes / 60
                entities_per_hour = zone_group['entity_id'].nunique() / hours_in_period if hours_in_period > 0 else 0
                
                # Оценка узкого места
                bottleneck_score = _calculate_bottleneck_score(zone_group, utilization_rate)
                
                metric = {
                    'entity_id': 'system',
                    'entity_name': 'System Metrics',
                    'entity_type': 'system',
                    'zone_id': zone_id,
                    'zone_name': zone_group['zone_name'].iloc[0] if 'zone_name' in zone_group else zone_id,
                    'zone_type': zone_group['zone_type'].iloc[0] if 'zone_type' in zone_group else 'work_area',
                    'timestamp': start_time.isoformat(),
                    'duration_minutes': round(zone_duration, 2),
                    'data_type': 'workflow',
                    'raw_data': {
                        'utilization_rate': round(utilization_rate, 3),
                        'entities_per_hour': round(entities_per_hour, 2),
                        'bottleneck_score': round(bottleneck_score, 3),
                        'total_visits': len(zone_group),
                        'unique_entities': zone_group['entity_id'].nunique()
                    }
                }
                metrics.append(metric)
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating workflow metrics: {e}")
        return []

def _calculate_bottleneck_score(zone_group: pd.DataFrame, utilization_rate: float) -> float:
    """
    Расчет оценки узкого места для зоны.
    """
    score = utilization_rate
    
    # Добавляем баллы за длительное время пребывания
    if 'duration_minutes' in zone_group:
        avg_duration = zone_group['duration_minutes'].mean()
        if avg_duration > 30:  # Если среднее время пребывания больше 30 минут
            score += 0.2
    
    # Добавляем баллы за неравномерное распределение по времени
    if 'hour' in zone_group:
        hourly_counts = zone_group['hour'].value_counts()
        if len(hourly_counts) > 0:
            peak_to_valley_ratio = hourly_counts.max() / max(hourly_counts.min(), 1)
            if peak_to_valley_ratio > 3:  # Сильная неравномерность
                score += 0.3
    
    return min(1.0, score)

@router.get(
    "/aggregation/tasks/{task_id}",
    response_model=AggregationTask,
    responses={
        200: {"description": "Успешный запрос", "model": AggregationTask},
        404: {"description": "Задача не найдена", "model": ErrorResponse}
    }
)
async def get_aggregation_task_endpoint(task_id: str):
    """
    Получение статуса задачи агрегации.
    Возвращает информацию о задаче агрегации по её ID.
    """
    try:
        task = get_aggregation_task(task_id)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="TASK_NOT_FOUND",
                    message=f"Aggregation task with ID '{task_id}' not found"
                ).model_dump()
            )
        
        return AggregationTask(**task)
        
    except Exception as e:
        logger.error(f"Error getting aggregation task {task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="GET_TASK_ERROR",
                message=f"Failed to get aggregation task: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/aggregation/tasks/pending",
    response_model=List[AggregationTask],
    responses={
        200: {"description": "Успешный запрос", "model": List[AggregationTask]},
        500: {"description": "Ошибка сервера", "model": ErrorResponse}
    }
)
async def get_pending_tasks_endpoint(limit: int = Query(10, description="Максимальное количество задач", ge=1, le=100)):
    """
    Получение списка ожидающих задач.
    Возвращает список задач агрегации, ожидающих выполнения.
    """
    try:
        tasks = get_pending_aggregation_tasks(limit)
        return [AggregationTask(**task) for task in tasks]
        
    except Exception as e:
        logger.error(f"Error getting pending tasks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="GET_PENDING_TASKS_ERROR",
                message=f"Failed to get pending tasks: {str(e)}"
            ).model_dump()
        )