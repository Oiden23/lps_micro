"""
Модуль для выполнения аналитических вычислений и обнаружения аномалий.
Содержит алгоритмы машинного обучения для анализа поведения объектов,
обнаружения аномалий и прогнозирования.
"""
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional, Tuple
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import json

from .database import get_data_for_period, get_entity_statistics, get_zone_statistics, store_anomaly
from .models import AnomalyDetectionReport, AnomalyBase

logger = logging.getLogger(__name__)

class AnalyticsEngine:
    """
    Движок для выполнения аналитических вычислений и обнаружения аномалий.
    """
    
    def __init__(self):
        self.isolation_forest = IsolationForest(
            n_estimators=100,
            contamination=0.1,
            random_state=42,
            n_jobs=-1
        )
        self.scaler = StandardScaler()
        self.feature_columns = [
            'duration_minutes', 
            'hour', 
            'day_of_week',
            'time_since_last_visit',
            'avg_duration_similarity'
        ]
    
    def detect_anomalies(self, start_time: datetime, end_time: datetime,
                        entity_ids: Optional[List[str]] = None,
                        anomaly_types: Optional[List[str]] = None) -> AnomalyDetectionReport:
        """
        Обнаружение аномалий в поведении сущностей за указанный период.
        
        Args:
            start_time: Начало периода анализа
            end_time: Конец периода анализа
            entity_ids: Список ID сущностей для анализа (опционально)
            anomaly_types: Типы аномалий для поиска (опционально)
        
        Returns:
            AnomalyDetectionReport: Отчет с обнаруженными аномалиями
        """
        try:
            # Получаем данные за период
            data = get_data_for_period(start_time, end_time, entity_ids=entity_ids)
            
            if not data:
                logger.warning(f"No data found for anomaly detection in period {start_time} to {end_time}")
                return self._create_empty_anomaly_report(start_time, end_time)
            
            # Преобразуем в DataFrame
            df = pd.DataFrame(data)
            
            # Подготавливаем данные для анализа
            prepared_data = self._prepare_data_for_anomaly_detection(df, start_time, end_time)
            
            if prepared_data.empty:
                logger.warning("No valid data after preparation for anomaly detection")
                return self._create_empty_anomaly_report(start_time, end_time)
            
            # Обнаруживаем аномалии
            anomalies = []
            
            # 1. Unexpected zone anomalies
            if not anomaly_types or 'unexpected_zone' in anomaly_types:
                unexpected_zone_anomalies = self._detect_unexpected_zone_anomalies(prepared_data)
                anomalies.extend(unexpected_zone_anomalies)
            
            # 2. Unusual time anomalies
            if not anomaly_types or 'unusual_time' in anomaly_types:
                unusual_time_anomalies = self._detect_unusual_time_anomalies(prepared_data)
                anomalies.extend(unusual_time_anomalies)
            
            # 3. Abnormal speed anomalies
            if not anomaly_types or 'abnormal_speed' in anomaly_types:
                abnormal_speed_anomalies = self._detect_abnormal_speed_anomalies(prepared_data)
                anomalies.extend(abnormal_speed_anomalies)
            
            # 4. Prolonged stay anomalies
            if not anomaly_types or 'prolonged_stay' in anomaly_types:
                prolonged_stay_anomalies = self._detect_prolonged_stay_anomalies(prepared_data)
                anomalies.extend(prolonged_stay_anomalies)
            
            # 5. Machine learning based anomalies
            ml_anomalies = self._detect_ml_based_anomalies(prepared_data)
            anomalies.extend(ml_anomalies)
            
            # Сохраняем аномалии в базу
            for anomaly in anomalies:
                store_anomaly(anomaly)
            
            # Создаем отчет
            report_id = f"anomaly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            report = AnomalyDetectionReport(
                report_id=report_id,
                generated_at=datetime.now(),
                period={
                    'start_time': start_time,
                    'end_time': end_time
                },
                anomalies=anomalies
            )
            
            return report
            
        except Exception as e:
            logger.error(f"Error in anomaly detection: {e}")
            import traceback
            traceback.print_exc()
            return self._create_empty_anomaly_report(start_time, end_time)
    
    def _prepare_data_for_anomaly_detection(self, df: pd.DataFrame, 
                                           start_time: datetime, 
                                           end_time: datetime) -> pd.DataFrame:
        """Подготовка данных для обнаружения аномалий"""
        try:
            # Добавляем временные признаки
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
            df['week_number'] = df['timestamp'].dt.isocalendar().week
            df['month'] = df['timestamp'].dt.month
            
            # Рассчитываем время с последнего посещения для каждой сущности и зоны
            df = df.sort_values(['entity_id', 'zone_id', 'timestamp'])
            df['prev_timestamp'] = df.groupby(['entity_id', 'zone_id'])['timestamp'].shift(1)
            df['time_since_last_visit'] = (df['timestamp'] - df['prev_timestamp']).dt.total_seconds() / 60  # в минутах
            
            # Заполняем пропущенные значения
            df['time_since_last_visit'] = df['time_since_last_visit'].fillna(0)
            
            # Получаем статистику по сущностям для расчета отклонений
            entity_stats = {}
            for entity_id in df['entity_id'].unique():
                stats = get_entity_statistics(entity_id, start_time, end_time)
                entity_stats[entity_id] = stats
            
            # Рассчитываем сходство с средним временем пребывания
            df['avg_duration_similarity'] = df.apply(
                lambda row: self._calculate_duration_similarity(
                    row, entity_stats.get(row['entity_id'], {})
                ),
                axis=1
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error preparing data for anomaly detection: {e}")
            return pd.DataFrame()
    
    def _calculate_duration_similarity(self, row: pd.Series, entity_stats: Dict[str, Any]) -> float:
        """Рассчитывает сходство длительности визита со средним значением для сущности"""
        if not entity_stats or 'total_time' not in entity_stats or entity_stats['total_time'] == 0:
            return 1.0
        
        avg_duration = entity_stats.get('total_time', 0) / max(entity_stats.get('total_visits', 1), 1)
        current_duration = row.get('duration_minutes', 0)
        
        if avg_duration == 0:
            return 1.0
        
        # Коэффициент сходства (1.0 = точно соответствует среднему)
        similarity = 1.0 - abs(current_duration - avg_duration) / avg_duration
        return max(0.0, min(1.0, similarity))
    
    def _detect_unexpected_zone_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обнаружение аномалий: неожиданное посещение зон"""
        anomalies = []
        
        # Группируем по сущностям
        for entity_id, entity_group in df.groupby('entity_id'):
            # Для каждой сущности находим наиболее часто посещаемые зоны
            zone_counts = entity_group['zone_id'].value_counts()
            top_zones = zone_counts.nlargest(3).index.tolist() if not zone_counts.empty else []
            
            # Находим редко посещаемые зоны
            rare_zones = zone_counts[zone_counts < zone_counts.mean() * 0.5].index.tolist()
            
            # Помечаем посещения редких зон как потенциальные аномалии
            for _, row in entity_group.iterrows():
                if row['zone_id'] in rare_zones and row['zone_id'] not in top_zones:
                    duration = row.get('duration_minutes', 0)
                    if duration > 5:  # Если провел в редкой зоне больше 5 минут
                        anomaly = {
                            'anomaly_id': str(uuid4()),
                            'entity_id': entity_id,
                            'entity_name': row.get('entity_name', ''),
                            'entity_type': row.get('entity_type', 'employee'),
                            'anomaly_type': 'unexpected_zone',
                            'zone_id': row['zone_id'],
                            'zone_name': row.get('zone_name', ''),
                            'position': {
                                'x': row.get('x', 0),
                                'y': row.get('y', 0),
                                'z': row.get('z', 0)
                            },
                            'timestamp': row['timestamp'],
                            'description': f"Unexpected visit to zone '{row.get('zone_name', row['zone_id'])}' - rarely visited by this entity",
                            'severity': 'medium' if duration < 30 else 'high',
                            'confidence': min(0.9, 0.3 + duration / 60),  # Уверенность растет со временем пребывания
                            'related_violations': []
                        }
                        anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_unusual_time_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обнаружение аномалий: необычное время посещения"""
        anomalies = []
        
        # Стандартные рабочие часы (9:00 - 18:00)
        standard_start_hour = 9
        standard_end_hour = 18
        
        for _, row in df.iterrows():
            hour = row['hour']
            day_of_week = row['day_of_week']
            
            # Проверяем на необычное время
            is_outside_standard_hours = hour < standard_start_hour or hour > standard_end_hour
            is_weekend = day_of_week >= 5  # Суббота (5) и воскресенье (6)
            
            if is_outside_standard_hours or is_weekend:
                duration = row.get('duration_minutes', 0)
                confidence = 0.7
                
                if is_weekend:
                    severity = 'high' if duration > 15 else 'medium'
                    confidence += 0.2
                else:
                    severity = 'medium' if duration > 30 else 'low'
                
                if duration > 5:  # Значимое пребывание
                    anomaly = {
                        'anomaly_id': str(uuid4()),
                        'entity_id': row['entity_id'],
                        'entity_name': row.get('entity_name', ''),
                        'entity_type': row.get('entity_type', 'employee'),
                        'anomaly_type': 'unusual_time',
                        'zone_id': row['zone_id'],
                        'zone_name': row.get('zone_name', ''),
                        'position': {
                            'x': row.get('x', 0),
                            'y': row.get('y', 0),
                            'z': row.get('z', 0)
                        },
                        'timestamp': row['timestamp'],
                        'description': f"Unusual time visit: {hour:02d}:00 on {'weekend' if is_weekend else 'weekday'}",
                        'severity': severity,
                        'confidence': min(1.0, confidence),
                        'related_violations': []
                    }
                    anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_abnormal_speed_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обнаружение аномалий: аномальная скорость перемещения"""
        anomalies = []
        
        # Рассчитываем скорость перемещения между зонами
        df_sorted = df.sort_values(['entity_id', 'timestamp'])
        
        for entity_id, entity_group in df_sorted.groupby('entity_id'):
            if len(entity_group) < 2:
                continue
            
            # Рассчитываем расстояние между последовательными позициями
            entity_group['prev_x'] = entity_group['x'].shift(1)
            entity_group['prev_y'] = entity_group['y'].shift(1)
            entity_group['prev_timestamp'] = entity_group['timestamp'].shift(1)
            
            # Удаляем первую строку (нет предыдущей позиции)
            entity_group = entity_group.iloc[1:]
            
            for _, row in entity_group.iterrows():
                if pd.isna(row['prev_x']) or pd.isna(row['prev_y']) or pd.isna(row['prev_timestamp']):
                    continue
                
                # Рассчитываем расстояние (упрощенно, без учета z-координаты)
                distance = np.sqrt((row['x'] - row['prev_x'])**2 + (row['y'] - row['prev_y'])**2)
                time_diff = (row['timestamp'] - row['prev_timestamp']).total_seconds() / 60  # в минутах
                
                if time_diff <= 0:
                    continue
                
                speed = distance / time_diff  # метров в минуту
                
                # Пороговые значения скорости (реалистичные для человека)
                normal_speed_min = 10   # 10 метров/мин = 0.17 м/с (очень медленно)
                normal_speed_max = 500  # 500 метров/мин = 8.3 м/с (очень быстро, бег)
                
                if speed < normal_speed_min or speed > normal_speed_max:
                    severity = 'high' if speed > normal_speed_max * 2 else 'medium'
                    confidence = 0.8 if speed > normal_speed_max else 0.6
                    
                    anomaly = {
                        'anomaly_id': str(uuid4()),
                        'entity_id': entity_id,
                        'entity_name': row.get('entity_name', ''),
                        'entity_type': row.get('entity_type', 'employee'),
                        'anomaly_type': 'abnormal_speed',
                        'zone_id': row['zone_id'],
                        'zone_name': row.get('zone_name', ''),
                        'position': {
                            'x': row['x'],
                            'y': row['y'],
                            'z': row.get('z', 0)
                        },
                        'timestamp': row['timestamp'],
                        'description': f"Abnormal movement speed: {speed:.1f} m/min (normal: {normal_speed_min}-{normal_speed_max} m/min)",
                        'severity': severity,
                        'confidence': confidence,
                        'related_violations': []
                    }
                    anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_prolonged_stay_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обнаружение аномалий: продолжительное пребывание в зоне"""
        anomalies = []
        
        # Группируем по сущностям и зонам
        for (entity_id, zone_id), group in df.groupby(['entity_id', 'zone_id']):
            if len(group) == 0:
                continue
            
            # Находим максимальную длительность пребывания
            max_duration = group['duration_minutes'].max()
            
            # Получаем тип зоны для определения пороговых значений
            zone_type = group['zone_type'].iloc[0] if 'zone_type' in group else 'work_area'
            zone_name = group['zone_name'].iloc[0] if 'zone_name' in group else zone_id
            
            # Определяем пороговые значения в зависимости от типа зоны
            threshold_mapping = {
                'restricted': 10,    # Запрещенные зоны - любое пребывание подозрительно
                'danger': 15,        # Опасные зоны
                'parking': 120,      # Зоны парковки
                'work_area': 240,    # Рабочие зоны
                'safe': 180,         # Безопасные зоны (отдых)
                'other': 180
            }
            
            threshold = threshold_mapping.get(zone_type, 180)
            
            if max_duration > threshold:
                # Рассчитываем отклонение от нормы
                deviation_factor = max_duration / threshold
                
                severity = 'medium'
                if deviation_factor > 3:
                    severity = 'critical'
                elif deviation_factor > 2:
                    severity = 'high'
                
                confidence = min(0.95, 0.4 + deviation_factor * 0.2)
                
                anomaly = {
                    'anomaly_id': str(uuid4()),
                    'entity_id': entity_id,
                    'entity_name': group['entity_name'].iloc[0] if 'entity_name' in group else '',
                    'entity_type': group['entity_type'].iloc[0] if 'entity_type' in group else 'employee',
                    'anomaly_type': 'prolonged_stay',
                    'zone_id': zone_id,
                    'zone_name': zone_name,
                    'position': {
                        'x': group['x'].mean() if 'x' in group else 0,
                        'y': group['y'].mean() if 'y' in group else 0,
                        'z': group['z'].mean() if 'z' in group else 0
                    },
                    'timestamp': group['timestamp'].max(),
                    'description': f"Prolonged stay in {zone_type} zone '{zone_name}': {max_duration:.1f} minutes (threshold: {threshold} min)",
                    'severity': severity,
                    'confidence': confidence,
                    'related_violations': []
                }
                anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_ml_based_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обнаружение аномалий с использованием машинного обучения"""
        if len(df) < 10:  # Нужно достаточно данных для ML
            return []
        
        try:
            # Подготавливаем признаки
            features = df[self.feature_columns].copy()
            
            # Заполняем пропущенные значения
            features = features.fillna(0)
            
            # Стандартизируем признаки
            scaled_features = self.scaler.fit_transform(features)
            
            # Обучаем и применяем Isolation Forest
            self.isolation_forest.fit(scaled_features)
            anomaly_scores = self.isolation_forest.decision_function(scaled_features)
            anomaly_predictions = self.isolation_forest.predict(scaled_features)
            
            # Фильтруем аномалии
            anomalies = []
            for i, (score, prediction) in enumerate(zip(anomaly_scores, anomaly_predictions)):
                if prediction == -1:  # Аномалия
                    row = df.iloc[i]
                    confidence = min(1.0, max(0.0, (score + 0.5) * 2))  # Нормализуем score в диапазон [0, 1]
                    
                    if confidence > 0.6:  # Порог уверенности
                        severity = 'medium'
                        if confidence > 0.8:
                            severity = 'high'
                        if confidence > 0.9:
                            severity = 'critical'
                        
                        anomaly = {
                            'anomaly_id': str(uuid4()),
                            'entity_id': row['entity_id'],
                            'entity_name': row.get('entity_name', ''),
                            'entity_type': row.get('entity_type', 'employee'),
                            'anomaly_type': 'ml_anomaly',
                            'zone_id': row['zone_id'],
                            'zone_name': row.get('zone_name', ''),
                            'position': {
                                'x': row.get('x', 0),
                                'y': row.get('y', 0),
                                'z': row.get('z', 0)
                            },
                            'timestamp': row['timestamp'],
                            'description': f"ML-detected anomaly with confidence {confidence:.2f}",
                            'severity': severity,
                            'confidence': confidence,
                            'related_violations': []
                        }
                        anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error in ML-based anomaly detection: {e}")
            return []
    
    def _create_empty_anomaly_report(self, start_time: datetime, end_time: datetime) -> AnomalyDetectionReport:
        """Создает пустой отчет об аномалиях"""
        return AnomalyDetectionReport(
            report_id=f"anomaly_empty_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            generated_at=datetime.now(),
            period={
                'start_time': start_time,
                'end_time': end_time
            },
            anomalies=[]
        )
    
    def analyze_behavior_patterns(self, entity_id: str, 
                                start_time: datetime, 
                                end_time: datetime) -> Dict[str, Any]:
        """
        Анализ паттернов поведения сущности.
        
        Args:
            entity_id: ID сущности
            start_time: Начало периода анализа
            end_time: Конец периода анализа
        
        Returns:
            Dict[str, Any]: Словарь с паттернами поведения
        """
        try:
            data = get_data_for_period(start_time, end_time, entity_ids=[entity_id])
            
            if not data:
                return {
                    'entity_id': entity_id,
                    'analysis_period': {
                        'start': start_time,
                        'end': end_time
                    },
                    'patterns': {},
                    'recommendations': []
                }
            
            df = pd.DataFrame(data)
            
            # 1. Анализ маршрутов
            common_routes = self._analyze_common_routes(df)
            
            # 2. Анализ временных паттернов
            time_patterns = self._analyze_time_patterns(df)
            
            # 3. Анализ зон пребывания
            zone_patterns = self._analyze_zone_patterns(df)
            
            # 4. Анализ скорости перемещения
            speed_patterns = self._analyze_speed_patterns(df)
            
            # 5. Генерация рекомендаций
            recommendations = self._generate_recommendations(
                common_routes, time_patterns, zone_patterns, speed_patterns
            )
            
            return {
                'entity_id': entity_id,
                'analysis_period': {
                    'start': start_time,
                    'end': end_time
                },
                'patterns': {
                    'common_routes': common_routes,
                    'time_patterns': time_patterns,
                    'zone_patterns': zone_patterns,
                    'speed_patterns': speed_patterns
                },
                'recommendations': recommendations
            }
            
        except Exception as e:
            logger.error(f"Error analyzing behavior patterns: {e}")
            return {
                'entity_id': entity_id,
                'analysis_period': {
                    'start': start_time,
                    'end': end_time
                },
                'patterns': {},
                'recommendations': []
            }
    
    def _analyze_common_routes(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Анализ распространенных маршрутов"""
        if len(df) < 2:
            return []
        
        # Сортируем по времени
        df_sorted = df.sort_values('timestamp')
        
        # Группируем по дням для анализа ежедневных маршрутов
        df_sorted['date'] = df_sorted['timestamp'].dt.date
        
        routes = []
        for date, date_group in df_sorted.groupby('date'):
            if len(date_group) < 2:
                continue
            
            # Создаем маршрут из последовательности зон
            route = date_group['zone_id'].tolist()
            route_name = ' -> '.join(route)
            
            routes.append({
                'date': date.isoformat(),
                'route': route,
                'route_name': route_name,
                'zones_count': len(route),
                'duration_minutes': (date_group['timestamp'].max() - date_group['timestamp'].min()).total_seconds() / 60
            })
        
        # Находим наиболее распространенные маршруты
        if routes:
            route_counts = {}
            for route in routes:
                route_key = tuple(route['route'])
                route_counts[route_key] = route_counts.get(route_key, 0) + 1
            
            # Сортируем по частоте
            common_routes = sorted(route_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            
            result = []
            for route, frequency in common_routes:
                result.append({
                    'route': [str(zone) for zone in route],
                    'frequency': frequency,
                    'percentage': round(frequency / len(routes) * 100, 1)
                })
            
            return result
        
        return []
    
    def _analyze_time_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Анализ временных паттернов"""
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        # Распределение по часам
        hourly_distribution = df['hour'].value_counts().sort_index().to_dict()
        
        # Распределение по дням недели
        weekday_distribution = df['day_of_week'].value_counts().sort_index().to_dict()
        
        # Среднее время пребывания по часам
        avg_duration_by_hour = df.groupby('hour')['duration_minutes'].mean().round(2).to_dict()
        
        # Определяем основные часы активности
        main_hours = [hour for hour, count in hourly_distribution.items() if count > np.mean(list(hourly_distribution.values()))]
        
        return {
            'hourly_distribution': hourly_distribution,
            'weekday_distribution': weekday_distribution,
            'avg_duration_by_hour': avg_duration_by_hour,
            'main_activity_hours': sorted(main_hours),
            'most_active_hour': max(hourly_distribution, key=hourly_distribution.get) if hourly_distribution else None
        }
    
    def _analyze_zone_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Анализ паттернов пребывания в зонах"""
        # Наиболее посещаемые зоны
        zone_visits = df.groupby('zone_id').agg({
            'zone_name': 'first',
            'zone_type': 'first',
            'duration_minutes': ['count', 'sum', 'mean']
        }).round(2)
        
        zone_visits.columns = ['zone_name', 'zone_type', 'visit_count', 'total_duration', 'avg_duration']
        zone_visits = zone_visits.sort_values('visit_count', ascending=False).reset_index()
        
        # Основные зоны (более 20% от общего времени)
        total_time = zone_visits['total_duration'].sum()
        zone_visits['time_percentage'] = (zone_visits['total_duration'] / total_time * 100).round(1)
        main_zones = zone_visits[zone_visits['time_percentage'] > 20]
        
        return {
            'most_visited_zones': zone_visits.head(5).to_dict('records'),
            'main_zones': main_zones.to_dict('records'),
            'zone_transition_matrix': self._calculate_zone_transition_matrix(df)
        }
    
    def _calculate_zone_transition_matrix(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """Рассчитывает матрицу переходов между зонами"""
        df_sorted = df.sort_values(['timestamp'])
        
        transitions = {}
        prev_zone = None
        
        for _, row in df_sorted.iterrows():
            current_zone = row['zone_id']
            
            if prev_zone is not None and prev_zone != current_zone:
                if prev_zone not in transitions:
                    transitions[prev_zone] = {}
                
                transitions[prev_zone][current_zone] = transitions[prev_zone].get(current_zone, 0) + 1
            
            prev_zone = current_zone
        
        return transitions
    
    def _analyze_speed_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Анализ паттернов скорости перемещения"""
        if len(df) < 2:
            return {}
        
        df_sorted = df.sort_values('timestamp')
        df_sorted['prev_x'] = df_sorted['x'].shift(1)
        df_sorted['prev_y'] = df_sorted['y'].shift(1)
        df_sorted['prev_timestamp'] = df_sorted['timestamp'].shift(1)
        df_sorted = df_sorted.iloc[1:]  # Удаляем первую строку
        
        speeds = []
        for _, row in df_sorted.iterrows():
            if pd.isna(row['prev_x']) or pd.isna(row['prev_y']) or pd.isna(row['prev_timestamp']):
                continue
            
            distance = np.sqrt((row['x'] - row['prev_x'])**2 + (row['y'] - row['prev_y'])**2)
            time_diff = (row['timestamp'] - row['prev_timestamp']).total_seconds() / 60  # в минутах
            
            if time_diff > 0:
                speed = distance / time_diff  # метров в минуту
                speeds.append(speed)
        
        if not speeds:
            return {}
        
        speeds_arr = np.array(speeds)
        
        return {
            'avg_speed': float(np.mean(speeds_arr)),
            'max_speed': float(np.max(speeds_arr)),
            'min_speed': float(np.min(speeds_arr)),
            'std_speed': float(np.std(speeds_arr)),
            'speed_distribution': self._calculate_speed_distribution(speeds_arr)
        }
    
    def _calculate_speed_distribution(self, speeds: np.ndarray) -> Dict[str, int]:
        """Рассчитывает распределение скоростей по категориям"""
        categories = {
            'very_slow': (0, 50),    # 0-50 м/мин (0-0.83 м/с)
            'slow': (50, 150),       # 50-150 м/мин (0.83-2.5 м/с)
            'normal': (150, 300),    # 150-300 м/мин (2.5-5 м/с)
            'fast': (300, 500),      # 300-500 м/мин (5-8.3 м/с)
            'very_fast': (500, float('inf'))  # >500 м/мин (>8.3 м/с)
        }
        
        distribution = {category: 0 for category in categories}
        
        for speed in speeds:
            for category, (min_speed, max_speed) in categories.items():
                if min_speed <= speed < max_speed:
                    distribution[category] += 1
                    break
        
        return distribution
    
    def _generate_recommendations(self, common_routes: List[Dict[str, Any]], 
                                time_patterns: Dict[str, Any], 
                                zone_patterns: Dict[str, Any],
                                speed_patterns: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций на основе анализа"""
        recommendations = []
        
        # Анализ маршрутов
        if common_routes:
            most_common_route = common_routes[0]
            if most_common_route['frequency'] > 3:
                recommendations.append(
                    f"Оптимизируйте маршрут: {most_common_route['route_name']} проходит {most_common_route['frequency']} раз"
                )
        
        # Анализ временных паттернов
        if 'main_activity_hours' in time_patterns:
            main_hours = time_patterns['main_activity_hours']
            if main_hours:
                peak_hours = [f"{hour}:00-{hour+1}:00" for hour in main_hours[:2]]
                recommendations.append(
                    f"Пиковая активность в часы: {', '.join(peak_hours)}. Рассмотрите распределение нагрузки."
                )
        
        # Анализ зон
        if 'main_zones' in zone_patterns:
            main_zones = zone_patterns['main_zones']
            for zone in main_zones:
                if zone.get('time_percentage', 0) > 50:
                    recommendations.append(
                        f"Обнаружена высокая концентрация времени в зоне '{zone.get('zone_name', '')}' ({zone.get('time_percentage', 0)}%)."
                    )
        
        # Анализ скорости
        if 'avg_speed' in speed_patterns:
            avg_speed = speed_patterns['avg_speed']
            if avg_speed > 400:  # 400 м/мин = 6.7 м/с
                recommendations.append("Обнаружены высокие скорости перемещения. Проверьте безопасность маршрутов.")
            elif avg_speed < 50:  # 50 м/мин = 0.83 м/с
                recommendations.append("Обнаружены низкие скорости перемещения. Возможны простои или узкие места.")
        
        return recommendations