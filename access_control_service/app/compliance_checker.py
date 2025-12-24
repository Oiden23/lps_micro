"""
Модуль для проверки соблюдения правил доступа.
Содержит логику проверки позиций сущностей против установленных правил.
"""
from datetime import datetime, time
from typing import Dict, Any, List, Optional
import logging

from app.models import ComplianceCheckResult, Violation, PositionCheck
from app.database import get_applicable_rules, check_point_in_geofences
from uuid import uuid4

logger = logging.getLogger(__name__)


def check_compliance_for_position(
    entity: Dict[str, Any],
    position: PositionCheck,
    timestamp: datetime
) -> ComplianceCheckResult:
    """
    Проверка соблюдения правил для конкретной позиции сущности.
    
    Args:
        entity: Словарь с данными сущности
        position: Позиция для проверки
        timestamp: Временная метка позиции
    
    Returns:
        ComplianceCheckResult с результатами проверки
    """
    # Получаем применимые правила для сущности
    rules = get_applicable_rules(
        entity_type=entity['entity_type'],
        entity_id=entity['entity_id'],
        role=entity.get('role')
    )
    
    violations = []
    warnings = []
    is_compliant = True
    
    # Проверяем каждое правило
    for rule in rules:
        rule_result = check_rule_compliance(
            rule=rule,
            entity=entity,
            position=position,
            timestamp=timestamp
        )
        
        if rule_result:
            if rule['severity'] in ['high', 'critical']:
                violations.append(rule_result)
                is_compliant = False
            else:
                warnings.append(rule_result)
    
    return ComplianceCheckResult(
        entity_id=entity['entity_id'],
        position=position,
        is_compliant=is_compliant,
        violations=violations,
        warnings=warnings
    )


def check_rule_compliance(
    rule: Dict[str, Any],
    entity: Dict[str, Any],
    position: PositionCheck,
    timestamp: datetime
) -> Optional[Dict[str, Any]]:
    """
    Проверка конкретного правила для позиции сущности.
    
    Returns:
        Dict с данными нарушения или None если правило соблюдено
    """
    try:
        # 1. Проверяем расписание
        if not check_schedule_compliance(rule, timestamp):
            return None  # Правило не активно в это время
        
        # 2. Проверяем находится ли позиция в геозоне правила
        geofence_intersections = check_point_in_geofences(
            x=position.x,
            y=position.y,
            z=position.z,
            geofence_ids=[rule['geofence_id']]
        )
        
        if not geofence_intersections:
            return None  # Позиция не в геозоне
        
        # 3. Проверяем действие правила
        is_inside = geofence_intersections[0]['is_inside']
        
        if rule['action'] == 'allow':
            # Разрешен доступ - нарушение если НЕ внутри зоны при action=allow?
            # На самом деле, action=allow означает разрешен доступ В зону
            # Так что если внутри зоны и правило allow - все ок
            # Если внутри зоны и правило deny - нарушение
            # Если вне зоны - правило не применяется
            if is_inside:
                return None  # Разрешено быть в зоне
            else:
                # Вне зоны - правило не применяется
                return None
                
        elif rule['action'] == 'deny':
            if is_inside:
                # Запрещено быть в зоне, но сущность внутри - нарушение
                return create_violation_data(rule, entity, position, timestamp, is_inside)
            else:
                return None  # Вне зоны - правило не применяется
                
        elif rule['action'] == 'alert':
            if is_inside:
                # Тревога при нахождении в зоне
                return create_violation_data(rule, entity, position, timestamp, is_inside)
            else:
                return None
        
        return None
        
    except Exception as e:
        logger.error(f"Error checking rule compliance: {e}")
        return None


def check_schedule_compliance(rule: Dict[str, Any], timestamp: datetime) -> bool:
    """Проверка соответствия расписанию правила"""
    schedule = rule.get('schedule')
    if not schedule:
        return True  # Нет расписания - правило всегда активно
    
    # Проверяем день недели
    weekday = timestamp.weekday()  # 0 = Monday, 6 = Sunday
    if 'days_of_week' in schedule and schedule['days_of_week']:
        if weekday not in schedule['days_of_week']:
            return False
    
    # Проверяем время
    if 'start_time' in schedule and schedule['start_time']:
        start_time = time.fromisoformat(schedule['start_time'])
        current_time = timestamp.time()
        if current_time < start_time:
            return False
    
    if 'end_time' in schedule and schedule['end_time']:
        end_time = time.fromisoformat(schedule['end_time'])
        current_time = timestamp.time()
        if current_time > end_time:
            return False
    
    return True


def create_violation_data(
    rule: Dict[str, Any],
    entity: Dict[str, Any],
    position: PositionCheck,
    timestamp: datetime,
    is_inside: bool
) -> Dict[str, Any]:
    """Создание данных нарушения для сохранения"""
    
    description = f"{entity['name']} violated rule '{rule['name']}'"
    if rule['action'] == 'deny':
        description += f": Access denied to restricted area"
    elif rule['action'] == 'alert':
        description += f": Alert triggered in monitored area"
    
    return {
        'violation_id': str(uuid4()),
        'rule_id': rule['rule_id'],
        'rule_name': rule['name'],
        'entity_id': entity['entity_id'],
        'entity_name': entity['name'],
        'entity_type': entity['entity_type'],
        'geofence_id': rule['geofence_id'],
        'geofence_name': rule.get('geofence_name', 'Unknown'),
        'position': {
            'x': position.x,
            'y': position.y,
            'z': position.z,
            'timestamp': timestamp.isoformat()
        },
        'severity': rule['severity'],
        'description': description,
        'timestamp': timestamp.isoformat(),
        'acknowledged': False,
        'acknowledged_by': None,
        'acknowledged_at': None
    }
