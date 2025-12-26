"""
Модуль для аналитических функций и обнаружения аномалий.
Содержит эндпоинты для выполнения аналитических вычислений и обнаружения аномального поведения.
"""
from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import JSONResponse
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from app.models import (
    AnomalyDetectionReport, BehaviorPatternReport, ErrorResponse,
    ValidationErrorResponse
)
from app.analytics_engine import AnalyticsEngine
from app.database import get_entity_statistics, get_zone_statistics

router = APIRouter(tags=["Analytics"])
logger = logging.getLogger(__name__)

# Инициализируем аналитический движок
analytics_engine = AnalyticsEngine()

@router.get(
    "/analytics/anomalies",
    response_model=AnomalyDetectionReport,
    responses={
        200: {"description": "Успешный запрос", "model": AnomalyDetectionReport},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def detect_anomalies_endpoint(
    start_time: str = Query(..., description="Начало периода для анализа"),
    end_time: str = Query(..., description="Конец периода для анализа"),
    entity_ids: Optional[str] = Query(None, description="Список ID сущностей через запятую"),
    anomaly_types: Optional[str] = Query(None, description="Типы аномалий для поиска через запятую", example="unexpected_zone,unusual_time")
):
    """
    Обнаружение аномалий в поведении.
    Выявление аномалий в перемещениях сотрудников и оборудовании.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Валидация периода
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
        
        # Преобразуем строки в списки
        entity_id_list = entity_ids.split(',') if entity_ids else None
        anomaly_type_list = anomaly_types.split(',') if anomaly_types else None
        
        # Запускаем обнаружение аномалий
        report = analytics_engine.detect_anomalies(
            start_time=start_dt,
            end_time=end_dt,
            entity_ids=entity_id_list,
            anomaly_types=anomaly_type_list
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
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="ANOMALY_DETECTION_ERROR",
                message=f"Failed to detect anomalies: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/analytics/patterns/entity/{entity_id}",
    response_model=BehaviorPatternReport,
    responses={
        200: {"description": "Успешный запрос", "model": BehaviorPatternReport},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse},
        404: {"description": "Сущность не найдена", "model": ErrorResponse}
    }
)
async def analyze_entity_patterns_endpoint(
    entity_id: str,
    start_time: str = Query(..., description="Начало периода для анализа"),
    end_time: str = Query(..., description="Конец периода для анализа")
):
    """
    Анализ паттернов поведения сущности.
    Анализирует маршруты, временные паттерны и зоны пребывания для конкретной сущности.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Получаем статистику по сущности
        stats = get_entity_statistics(entity_id, start_dt, end_dt)
        
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ENTITY_NOT_FOUND",
                    message=f"Entity with ID '{entity_id}' not found or no data in period"
                ).model_dump()
            )
        
        # Анализируем паттерны поведения
        pattern_report = analytics_engine.analyze_behavior_patterns(
            entity_id=entity_id,
            start_time=start_dt,
            end_time=end_dt
        )
        
        return BehaviorPatternReport(**pattern_report)
        
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
        logger.error(f"Error analyzing entity patterns: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="PATTERN_ANALYSIS_ERROR",
                message=f"Failed to analyze entity patterns: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/analytics/patterns/zone/{zone_id}",
    response_model=Dict[str, Any],
    responses={
        200: {"description": "Успешный запрос", "model": Dict[str, Any]},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse},
        404: {"description": "Зона не найдена", "model": ErrorResponse}
    }
)
async def analyze_zone_patterns_endpoint(
    zone_id: str,
    start_time: str = Query(..., description="Начало периода для анализа"),
    end_time: str = Query(..., description="Конец периода для анализа")
):
    """
    Анализ паттернов использования зоны.
    Анализирует посещаемость, временные паттерны и эффективность использования зоны.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Получаем статистику по зоне
        stats = get_zone_statistics(zone_id, start_dt, end_dt)
        
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorResponse(
                    error_code="ZONE_NOT_FOUND",
                    message=f"Zone with ID '{zone_id}' not found or no data in period"
                ).model_dump()
            )
        
        # Добавляем дополнительные аналитические метрики
        analysis = {
            "zone_id": zone_id,
            "period": {
                "start": start_time,
                "end": end_time
            },
            "statistics": stats,
            "analytics": {
                "peak_hours": _identify_peak_hours(stats.get('hourly_distribution', {})),
                "utilization_trend": _calculate_utilization_trend(stats),
                "entity_type_distribution": stats.get('entity_breakdown', {})
            },
            "recommendations": _generate_zone_recommendations(stats)
        }
        
        return analysis
        
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
        logger.error(f"Error analyzing zone patterns: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="ZONE_ANALYSIS_ERROR",
                message=f"Failed to analyze zone patterns: {str(e)}"
            ).model_dump()
        )

@router.get(
    "/analytics/recommendations",
    response_model=Dict[str, Any],
    responses={
        200: {"description": "Успешный запрос", "model": Dict[str, Any]},
        400: {"description": "Некорректные параметры запроса", "model": ErrorResponse}
    }
)
async def get_analytics_recommendations_endpoint(
    start_time: str = Query(..., description="Начало периода для анализа"),
    end_time: str = Query(..., description="Конец периода для анализа"),
    zone_ids: Optional[str] = Query(None, description="Список ID зон через запятую"),
    entity_ids: Optional[str] = Query(None, description="Список ID сущностей через запятую")
):
    """
    Генерация рекомендаций на основе аналитики.
    Автоматически генерирует рекомендации по оптимизации рабочих процессов и безопасности.
    """
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        zone_id_list = zone_ids.split(',') if zone_ids else None
        entity_id_list = entity_ids.split(',') if entity_ids else None
        
        # Получаем аномалии для периода
        anomalies_report = analytics_engine.detect_anomalies(
            start_time=start_dt,
            end_time=end_dt,
            entity_ids=entity_id_list,
            anomaly_types=['unexpected_zone', 'unusual_time', 'prolonged_stay']
        )
        
        # Генерируем рекомендации на основе аномалий
        recommendations = _generate_recommendations_from_anomalies(anomalies_report)
        
        # Добавляем рекомендации по оптимизации зон
        if zone_id_list:
            zone_recommendations = _generate_zone_optimization_recommendations(zone_id_list, start_dt, end_dt)
            recommendations.extend(zone_recommendations)
        
        # Добавляем рекомендации по оптимизации маршрутов
        if entity_id_list:
            for entity_id in entity_id_list:
                entity_stats = get_entity_statistics(entity_id, start_dt, end_dt)
                if entity_stats:
                    route_recommendations = _generate_route_optimization_recommendations(entity_stats)
                    recommendations.extend(route_recommendations)
        
        return {
            "period": {
                "start": start_time,
                "end": end_time
            },
            "total_recommendations": len(recommendations),
            "recommendations": recommendations,
            "priority_levels": {
                "high": len([r for r in recommendations if r['priority'] == 'high']),
                "medium": len([r for r in recommendations if r['priority'] == 'medium']),
                "low": len([r for r in recommendations if r['priority'] == 'low'])
            }
        }
        
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
        logger.error(f"Error generating recommendations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                error_code="RECOMMENDATION_GENERATION_ERROR",
                message=f"Failed to generate recommendations: {str(e)}"
            ).model_dump()
        )

def _identify_peak_hours(hourly_distribution: Dict[int, int]) -> List[int]:
    """Определяет часы пиковой активности"""
    if not hourly_distribution:
        return []
    
    # Сортируем часы по количеству посещений
    sorted_hours = sorted(hourly_distribution.items(), key=lambda x: x[1], reverse=True)
    
    # Берем топ-3 часа
    peak_hours = [hour for hour, count in sorted_hours[:3]]
    return sorted(peak_hours)

def _calculate_utilization_trend(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Рассчитывает тренд использования зоны"""
    total_time = stats.get('total_time', 0)
    total_visits = stats.get('total_visits', 0)
    
    if total_visits == 0:
        return {
            "trend": "no_data",
            "avg_duration_per_visit": 0,
            "utilization_score": 0
        }
    
    avg_duration = total_time / total_visits
    
    # Определяем тренд на основе средней продолжительности
    if avg_duration > 60:
        trend = "high_utilization"
    elif avg_duration > 30:
        trend = "medium_utilization"
    else:
        trend = "low_utilization"
    
    # Рассчитываем оценку использования (0-1)
    utilization_score = min(1.0, avg_duration / 120)
    
    return {
        "trend": trend,
        "avg_duration_per_visit": round(avg_duration, 2),
        "utilization_score": round(utilization_score, 2)
    }

def _generate_zone_recommendations(stats: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Генерирует рекомендации по оптимизации зоны"""
    recommendations = []
    
    total_visits = stats.get('total_visits', 0)
    avg_duration = stats.get('avg_duration', 0)
    unique_entities = stats.get('unique_entities', 0)
    
    # Рекомендации на основе посещаемости
    if total_visits > 100:
        recommendations.append({
            "type": "capacity_optimization",
            "description": "High zone occupancy detected. Consider expanding the zone or creating additional zones.",
            "priority": "medium"
        })
    
    # Рекомендации на основе времени пребывания
    if avg_duration > 60:
        recommendations.append({
            "type": "workflow_optimization",
            "description": "Long average duration detected. Consider optimizing workflow or providing additional resources.",
            "priority": "high"
        })
    elif avg_duration < 5:
        recommendations.append({
            "type": "zone_purpose_review",
            "description": "Very short average duration detected. Review if the zone serves its intended purpose.",
            "priority": "low"
        })
    
    # Рекомендации на основе уникальных сущностей
    if unique_entities > 20:
        recommendations.append({
            "type": "access_control_review",
            "description": "Large number of unique entities detected. Review access control policies for this zone.",
            "priority": "medium"
        })
    
    return recommendations

def _generate_recommendations_from_anomalies(anomalies_report: AnomalyDetectionReport) -> List[Dict[str, Any]]:
    """Генерирует рекомендации на основе обнаруженных аномалий"""
    recommendations = []
    
    for anomaly in anomalies_report.anomalies:
        anomaly_type = anomaly.get('anomaly_type', '')
        severity = anomaly.get('severity', 'medium')
        description = anomaly.get('description', '')
        
        priority = "high" if severity in ['high', 'critical'] else "medium"
        
        if anomaly_type == 'unexpected_zone':
            recommendations.append({
                "type": "access_control_violation",
                "description": f"Unexpected zone access detected: {description}",
                "priority": priority,
                "action": "Review access permissions and consider additional security measures"
            })
        elif anomaly_type == 'unusual_time':
            recommendations.append({
                "type": "time_policy_violation",
                "description": f"Unusual time activity detected: {description}",
                "priority": priority,
                "action": "Review time-based access policies and consider additional monitoring"
            })
        elif anomaly_type == 'prolonged_stay':
            recommendations.append({
                "type": "workflow_inefficiency",
                "description": f"Prolonged stay detected: {description}",
                "priority": priority,
                "action": "Review workflow efficiency and consider process optimization"
            })
    
    return recommendations

def _generate_zone_optimization_recommendations(zone_ids: List[str], 
                                               start_time: datetime, 
                                               end_time: datetime) -> List[Dict[str, Any]]:
    """Генерирует рекомендации по оптимизации зон"""
    recommendations = []
    
    for zone_id in zone_ids:
        stats = get_zone_statistics(zone_id, start_time, end_time)
        if stats:
            zone_recommendations = _generate_zone_recommendations(stats)
            for rec in zone_recommendations:
                rec['zone_id'] = zone_id
                recommendations.append(rec)
    
    return recommendations

def _generate_route_optimization_recommendations(entity_stats: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Генерирует рекомендации по оптимизации маршрутов"""
    recommendations = []
    
    top_zones = entity_stats.get('top_zones', [])
    total_time = entity_stats.get('total_time', 0)
    
    if len(top_zones) > 3:
        # Если сущность посещает много зон, возможно маршрут не оптимален
        recommendations.append({
            "type": "route_optimization",
            "description": f"Entity visits {len(top_zones)} zones frequently. Consider optimizing route sequence.",
            "priority": "medium",
            "action": "Analyze route sequence and consider reorganization of workflow"
        })
    
    if total_time > 480:  # 8 часов в минутах
        recommendations.append({
            "type": "workflow_balance",
            "description": f"Entity spends more than 8 hours in zones. Consider workload distribution.",
            "priority": "high",
            "action": "Review workload distribution and consider task delegation"
        })
    
    return recommendations