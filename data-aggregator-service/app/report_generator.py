"""
Модуль для генерации отчетов на основе агрегированных данных.
Содержит логику создания различных типов отчетов по посещаемости зон,
времени пребывания и эффективности рабочих процессов.
"""
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from .database import get_aggregated_data, get_data_for_period, store_report
from .models import ZoneOccupancyReport, TimeInZoneReport, WorkflowEfficiencyReport, AnomalyDetectionReport

logger = logging.getLogger(__name__)

def generate_zone_occupancy_report(start_time: datetime, end_time: datetime, 
                                  zone_ids: Optional[List[str]] = None, 
                                  entity_types: Optional[List[str]] = None) -> ZoneOccupancyReport:
    """
    Генерация отчета по посещаемости зон.
    
    Args:
        start_time: Начало периода
        end_time: Конец периода
        zone_ids: Список ID зон для фильтрации (опционально)
        entity_types: Список типов сущностей для фильтрации (опционально)
    
    Returns:
        ZoneOccupancyReport: Сгенерированный отчет
    """
    try:
        # Получаем агрегированные данные из базы
        data = get_data_for_period(start_time, end_time, zone_ids, entity_types)
        
        if not data:
            logger.warning(f"No data found for period {start_time} to {end_time}")
            return _create_empty_zone_occupancy_report(start_time, end_time)
        
        # Преобразуем данные в DataFrame для удобной обработки
        df = pd.DataFrame(data)
        
        # Группируем данные по зонам
        zone_reports = []
        unique_zones = df['zone_id'].unique()
        
        for zone_id in unique_zones:
            zone_df = df[df['zone_id'] == zone_id]
            
            # Вычисляем метрики для зоны
            total_visits = len(zone_df)
            unique_entities = zone_df['entity_id'].nunique()
            
            # Рассчитываем среднюю продолжительность визита
            avg_duration = zone_df['duration_minutes'].mean() if 'duration_minutes' in zone_df else 0
            
            # Находим час пик
            if 'hour' in zone_df:
                peak_hour = zone_df['hour'].mode().iloc[0] if not zone_df['hour'].mode().empty else None
            else:
                peak_hour = None
            
            # Распределение по часам
            hourly_distribution = {}
            if 'hour' in zone_df:
                hourly_counts = zone_df['hour'].value_counts().to_dict()
                hourly_distribution = {str(hour): count for hour, count in hourly_counts.items()}
            
            # Разбивка по типам сущностей
            entity_breakdown = {
                'employees': len(zone_df[zone_df['entity_type'] == 'employee']),
                'equipment': len(zone_df[zone_df['entity_type'] == 'equipment'])
            }
            
            zone_reports.append({
                'zone_id': zone_id,
                'zone_name': zone_df['zone_name'].iloc[0] if 'zone_name' in zone_df else f'Zone {zone_id}',
                'zone_type': zone_df['zone_type'].iloc[0] if 'zone_type' in zone_df else 'unknown',
                'total_visits': total_visits,
                'unique_entities': unique_entities,
                'avg_duration_minutes': round(avg_duration, 2),
                'peak_hour': peak_hour,
                'hourly_distribution': hourly_distribution,
                'entity_breakdown': entity_breakdown
            })
        
        # Создаем отчет
        report_id = f"zone_occ_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        report = ZoneOccupancyReport(
            report_id=report_id,
            generated_at=datetime.now(),
            period={
                'start_time': start_time,
                'end_time': end_time
            },
            zones=zone_reports
        )
        
        # Сохраняем отчет в базу
        store_report(report_id, 'zone_occupancy', report.dict())
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating zone occupancy report: {e}")
        raise

def generate_time_in_zone_report(entity_id: Optional[str] = None, zone_id: Optional[str] = None,
                                start_time: datetime, end_time: datetime,
                                group_by: str = 'day') -> TimeInZoneReport:
    """
    Генерация отчета по времени пребывания в зонах.
    """
    try:
        # Получаем данные о времени пребывания
        data = get_aggregated_data('time_in_zone', start_time, end_time, entity_id, zone_id)
        
        if not data:
            logger.warning(f"No time in zone data found for period {start_time} to {end_time}")
            return _create_empty_time_in_zone_report(start_time, end_time)
        
        # Группируем данные в соответствии с параметром group_by
        df = pd.DataFrame(data)
        
        # Добавляем столбец для группировки
        if group_by == 'hour':
            df['group_key'] = df['timestamp'].dt.floor('H')
        elif group_by == 'day':
            df['group_key'] = df['timestamp'].dt.date
        elif group_by == 'week':
            df['group_key'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)
        elif group_by == 'month':
            df['group_key'] = df['timestamp'].dt.to_period('M').apply(lambda r: r.start_time)
        else:
            df['group_key'] = df['timestamp'].dt.date
        
        # Агрегируем данные
        grouped_data = []
        for (group_key, entity_id, zone_id), group in df.groupby(['group_key', 'entity_id', 'zone_id']):
            total_time = group['duration_minutes'].sum()
            visits_count = len(group)
            avg_visit_duration = group['duration_minutes'].mean()
            
            grouped_data.append({
                'entity_id': entity_id,
                'entity_name': group['entity_name'].iloc[0],
                'entity_type': group['entity_type'].iloc[0],
                'zone_id': zone_id,
                'zone_name': group['zone_name'].iloc[0],
                'total_time_minutes': round(total_time, 2),
                'visits_count': visits_count,
                'avg_visit_duration': round(avg_visit_duration, 2),
                'first_entry': group['timestamp'].min().isoformat(),
                'last_exit': group['timestamp'].max().isoformat()
            })
        
        # Создаем отчет
        report_id = f"time_zone_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        report = TimeInZoneReport(
            report_id=report_id,
            generated_at=datetime.now(),
            period={
                'start_time': start_time,
                'end_time': end_time
            },
            group_by=group_by,
            data=grouped_data
        )
        
        # Сохраняем отчет в базу
        store_report(report_id, 'time_in_zone', report.dict())
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating time in zone report: {e}")
        raise

def generate_workflow_efficiency_report(start_time: datetime, end_time: datetime,
                                       zone_ids: Optional[List[str]] = None,
                                       entity_ids: Optional[List[str]] = None) -> WorkflowEfficiencyReport:
    """
    Генерация отчета по эффективности рабочих зон.
    """
    try:
        # Получаем данные для анализа эффективности
        data = get_aggregated_data('workflow', start_time, end_time, zone_ids=zone_ids, entity_ids=entity_ids)
        
        if not data:
            logger.warning(f"No workflow data found for period {start_time} to {end_time}")
            return _create_empty_workflow_report(start_time, end_time)
        
        df = pd.DataFrame(data)
        
        # Анализируем эффективность по зонам
        zone_efficiency = []
        unique_zones = df['zone_id'].unique()
        
        for zone_id in unique_zones:
            zone_df = df[df['zone_id'] == zone_id]
            
            # Коэффициент использования зоны (время использования / общее время)
            total_minutes = (end_time - start_time).total_seconds() / 60
            if total_minutes > 0:
                utilization_rate = min(1.0, zone_df['duration_minutes'].sum() / total_minutes)
            else:
                utilization_rate = 0
                
            # Среднее количество сущностей в час
            hours_in_period = total_minutes / 60
            avg_entities_per_hour = zone_df['entity_id'].nunique() / hours_in_period if hours_in_period > 0 else 0
            
            # Оценка узкого места (простой алгоритм на основе загрузки и времени ожидания)
            bottleneck_score = _calculate_bottleneck_score(zone_df, utilization_rate)
            
            # Пиковые часы
            peak_hours = []
            if 'hour' in zone_df:
                hourly_counts = zone_df['hour'].value_counts()
                if not hourly_counts.empty:
                    top_hours = hourly_counts.nlargest(3).index.tolist()
                    peak_hours = [int(hour) for hour in top_hours]
            
            # Метрики рабочего процесса
            workflow_metrics = {
                'avg_transition_time': _calculate_avg_transition_time(zone_df),
                'common_routes': _identify_common_routes(zone_df)
            }
            
            zone_efficiency.append({
                'zone_id': zone_id,
                'zone_name': zone_df['zone_name'].iloc[0],
                'utilization_rate': round(utilization_rate, 3),
                'avg_entities_per_hour': round(avg_entities_per_hour, 2),
                'bottleneck_score': round(bottleneck_score, 3),
                'peak_hours': peak_hours,
                'workflow_metrics': workflow_metrics
            })
        
        # Создаем отчет
        report_id = f"workflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        report = WorkflowEfficiencyReport(
            report_id=report_id,
            generated_at=datetime.now(),
            period={
                'start_time': start_time,
                'end_time': end_time
            },
            zones=zone_efficiency
        )
        
        # Сохраняем отчет в базу
        store_report(report_id, 'workflow_efficiency', report.dict())
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating workflow efficiency report: {e}")
        raise

def _calculate_bottleneck_score(zone_df: pd.DataFrame, utilization_rate: float) -> float:
    """Рассчитывает оценку узкого места для зоны"""
    # Базовый балл на основе загрузки
    score = utilization_rate
    
    # Добавляем баллы за длительное время пребывания
    if 'duration_minutes' in zone_df:
        avg_duration = zone_df['duration_minutes'].mean()
        if avg_duration > 30:  # Если среднее время пребывания больше 30 минут
            score += 0.2
    
    # Добавляем баллы за количество очередей/ожиданий
    if 'waiting_time' in zone_df:
        avg_waiting = zone_df['waiting_time'].mean()
        if avg_waiting > 10:  # Если среднее время ожидания больше 10 минут
            score += 0.3
    
    return min(1.0, score)

def _calculate_avg_transition_time(zone_df: pd.DataFrame) -> float:
    """Рассчитывает среднее время перехода между зонами"""
    if 'transition_time' in zone_df:
        return round(zone_df['transition_time'].mean(), 2)
    return 0.0

def _identify_common_routes(zone_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Определяет распространенные маршруты перемещения"""
    if 'route' not in zone_df:
        return []
    
    # Группируем по маршрутам и считаем частоту
    route_counts = zone_df['route'].value_counts().nlargest(5)
    
    common_routes = []
    for route, frequency in route_counts.items():
        try:
            route_list = json.loads(route) if isinstance(route, str) else route
            common_routes.append({
                'route': route_list,
                'frequency': int(frequency)
            })
        except (json.JSONDecodeError, TypeError):
            continue
    
    return common_routes

def _create_empty_zone_occupancy_report(start_time: datetime, end_time: datetime) -> ZoneOccupancyReport:
    """Создает пустой отчет по посещаемости зон"""
    return ZoneOccupancyReport(
        report_id=f"zone_occ_empty_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        generated_at=datetime.now(),
        period={
            'start_time': start_time,
            'end_time': end_time
        },
        zones=[]
    )

def _create_empty_time_in_zone_report(start_time: datetime, end_time: datetime) -> TimeInZoneReport:
    """Создает пустой отчет по времени пребывания в зонах"""
    return TimeInZoneReport(
        report_id=f"time_zone_empty_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        generated_at=datetime.now(),
        period={
            'start_time': start_time,
            'end_time': end_time
        },
        group_by='day',
        data=[]
    )

def _create_empty_workflow_report(start_time: datetime, end_time: datetime) -> WorkflowEfficiencyReport:
    """Создает пустой отчет по эффективности рабочих зон"""
    return WorkflowEfficiencyReport(
        report_id=f"workflow_empty_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        generated_at=datetime.now(),
        period={
            'start_time': start_time,
            'end_time': end_time
        },
        zones=[]
    )