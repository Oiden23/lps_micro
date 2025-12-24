from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime, time
from typing import List, Optional, Any, Dict
from uuid import UUID, uuid4


# === Базовые модели ошибок (оставляем как есть) ===
class ErrorResponse(BaseModel):
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class ValidationErrorResponse(ErrorResponse):
    details: List[Dict[str, str]]


# === Модели сущностей (Entities) ===
class EntityCreate(BaseModel):
    entity_id: str = Field(..., description="Уникальный идентификатор сущности (например, emp-001)")
    name: str = Field(..., description="Имя сущности (ФИО сотрудника или название оборудования)")
    entity_type: str = Field(..., description="Тип сущности", enum=["employee", "equipment"])
    tag_id: Optional[str] = Field(None, description="Идентификатор метки для привязки (опционально)")
    department: Optional[str] = Field(None, description="Отдел/подразделение (для сотрудников)")
    role: Optional[str] = Field(None, description="Роль/должность (для сотрудников)")
    equipment_type: Optional[str] = Field(None, description="Тип оборудования (для оборудования)")
    is_active: bool = Field(True, description="Активна ли сущность")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Дополнительные метаданные")

    @field_validator('entity_type')
    @classmethod
    def validate_entity_type(cls, v: str) -> str:
        if v not in ["employee", "equipment"]:
            raise ValueError('Entity type must be either "employee" or "equipment"')
        return v

class EntityUpdate(BaseModel):
    name: Optional[str] = None
    tag_id: Optional[str] = Field(None, description="Привязать/отвязать метку (null для отвязки)")
    department: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None

class Entity(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    entity_id: str
    name: str
    entity_type: str
    tag_id: Optional[str] = None
    department: Optional[str] = None
    role: Optional[str] = None
    equipment_type: Optional[str] = None
    is_active: bool
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime


# === Модели геозон (Geofences) ===
class GeofenceCoordinates(BaseModel):
    """Базовые координаты геозоны"""
    min_x: Optional[float] = None
    max_x: Optional[float] = None
    min_y: Optional[float] = None
    max_y: Optional[float] = None
    min_z: Optional[float] = Field(0.0, description="Минимальная высота")
    max_z: Optional[float] = Field(3.0, description="Максимальная высота")
    
    # Для круглых зон
    center_x: Optional[float] = None
    center_y: Optional[float] = None
    radius: Optional[float] = None
    
    # Для полигонов
    vertices: Optional[List[Dict[str, float]]] = None
    
    @field_validator('vertices')
    @classmethod
    def validate_vertices(cls, v: Optional[List[Dict[str, float]]]) -> Optional[List[Dict[str, float]]]:
        if v is not None:
            if len(v) < 3:
                raise ValueError('Polygon must have at least 3 vertices')
            for vertex in v:
                if 'x' not in vertex or 'y' not in vertex:
                    raise ValueError('Each vertex must have x and y coordinates')
        return v

class GeofenceCreate(BaseModel):
    name: str = Field(..., description="Название геозоны")
    zone_type: str = Field(
        ...,
        description="Тип зоны",
        enum=["restricted", "danger", "safe", "work_area", "parking", "other"]
    )
    description: Optional[str] = Field(None, description="Описание зоны")
    shape: str = Field(..., description="Форма зоны", enum=["rectangle", "circle", "polygon"])
    coordinates: GeofenceCoordinates
    buffer_meters: float = Field(0.0, description="Буферная зона вокруг геозоны (в метрах)")
    is_active: bool = Field(True, description="Активна ли геозона")
    
    @field_validator('coordinates')
    @classmethod
    def validate_coordinates(cls, v: GeofenceCoordinates, info):
        shape = info.data.get('shape')
        
        if shape == "rectangle":
            required_fields = ['min_x', 'max_x', 'min_y', 'max_y']
            for field in required_fields:
                if getattr(v, field) is None:
                    raise ValueError(f'Rectangle geofence requires {field}')
            if v.min_x >= v.max_x:
                raise ValueError('min_x must be less than max_x')
            if v.min_y >= v.max_y:
                raise ValueError('min_y must be less than max_y')
                
        elif shape == "circle":
            required_fields = ['center_x', 'center_y', 'radius']
            for field in required_fields:
                if getattr(v, field) is None:
                    raise ValueError(f'Circle geofence requires {field}')
            if v.radius <= 0:
                raise ValueError('Radius must be greater than 0')
                
        elif shape == "polygon":
            if not v.vertices or len(v.vertices) < 3:
                raise ValueError('Polygon must have at least 3 vertices')
                
        return v

class Geofence(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    geofence_id: UUID
    name: str
    zone_type: str
    description: Optional[str]
    shape: str
    coordinates: Dict[str, Any]  # JSON поле из БД
    buffer_meters: float
    is_active: bool
    created_at: datetime
    updated_at: datetime


# === Модели правил (Rules) ===
class RuleSchedule(BaseModel):
    days_of_week: List[int] = Field(
        default=[0, 1, 2, 3, 4, 5, 6],
        description="Дни недели (0-воскресенье, 6-суббота)",
        # ge=0,
        # le=6
    )
    start_time: Optional[str] = Field(
        None,
        description="Время начала (HH:MM)",
        pattern=r'^([0-1][0-9]|2[0-3]):[0-5][0-9]$'
    )
    end_time: Optional[str] = Field(
        None,
        description="Время окончания (HH:MM)",
        pattern=r'^([0-1][0-9]|2[0-3]):[0-5][0-9]$'
    )
    
    @field_validator('days_of_week')
    @classmethod
    def validate_days_of_week(cls, v: List[int]) -> List[int]:
        if not all(0 <= day <= 6 for day in v):
            raise ValueError('Days of week must be between 0 and 6')
        return list(set(v))  # Удаляем дубликаты
    
    @field_validator('end_time')
    @classmethod
    def validate_times(cls, v: Optional[str], info) -> Optional[str]:
        start_time = info.data.get('start_time')
        if start_time and v:
            if start_time >= v:
                raise ValueError('end_time must be after start_time')
        return v

class RuleCreate(BaseModel):
    name: str = Field(..., description="Название правила")
    description: Optional[str] = Field(None, description="Описание правила")
    entity_type: str = Field(
        ...,
        description="Тип сущности, к которой применяется правило",
        enum=["employee", "equipment", "all"]
    )
    entity_id: Optional[str] = Field(None, description="Конкретная сущность (если null - применяется ко всем)")
    role_required: Optional[str] = Field(None, description="Требуемая роль (только для сотрудников)")
    geofence_id: UUID = Field(..., description="ID геозоны, к которой применяется правило")
    action: str = Field(..., description="Действие правила", enum=["allow", "deny", "alert"])
    schedule: Optional[RuleSchedule] = Field(None, description="Расписание действия правила")
    severity: str = Field(
        "medium",
        description="Серьезность нарушения",
        enum=["low", "medium", "high", "critical"]
    )
    is_active: bool = Field(True, description="Активно ли правило")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Дополнительные метаданные")
    
    @field_validator('entity_id')
    @classmethod
    def validate_entity_specificity(cls, v: Optional[str], info) -> Optional[str]:
        entity_type = info.data.get('entity_type')
        if entity_type == "all" and v is not None:
            raise ValueError('entity_id must be null when entity_type is "all"')
        return v

class RuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    severity: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class Rule(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    rule_id: UUID
    name: str
    description: Optional[str]
    entity_type: str
    entity_id: Optional[str]
    role_required: Optional[str]
    geofence_id: UUID
    action: str
    schedule: Optional[Dict[str, Any]]  # JSON поле из БД
    severity: str
    is_active: bool
    metadata: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime


# === Модели для проверки соблюдения (Compliance) ===
class PositionCheck(BaseModel):
    x: float = Field(..., description="Координата X")
    y: float = Field(..., description="Координата Y")
    z: float = Field(0.0, description="Координата Z (высота)")
    timestamp: datetime = Field(..., description="Время позиции")

class ComplianceCheckRequest(BaseModel):
    entity_id: str = Field(..., description="ID сущности")
    position: PositionCheck

class Violation(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    violation_id: UUID
    rule_id: UUID
    rule_name: str
    entity_id: str
    entity_name: Optional[str]
    entity_type: Optional[str]
    geofence_id: UUID
    geofence_name: Optional[str]
    position: Dict[str, Any]  # JSON поле из БД
    severity: str
    description: Optional[str]
    timestamp: datetime
    acknowledged: bool
    acknowledged_by: Optional[str]
    acknowledged_at: Optional[datetime]

class ComplianceCheckResult(BaseModel):
    entity_id: str
    position: PositionCheck
    is_compliant: bool = Field(..., description="Соответствует ли позиция всем правилам")
    violations: List[Violation] = Field(default_factory=list)
    warnings: List[Violation] = Field(default_factory=list, description="Предупреждения (низкая серьезность)")

class BatchComplianceCheckRequest(BaseModel):
    checks: List[ComplianceCheckRequest]

class BatchComplianceCheckResult(BaseModel):
    results: List[ComplianceCheckResult]
    summary: Dict[str, int] = Field(
        default_factory=lambda: {
            "total_checks": 0,
            "compliant": 0,
            "violations": 0,
            "warnings": 0
        }
    )


# === Вспомогательные модели для геозон ===
class PointCheckRequest(BaseModel):
    x: float = Field(..., description="Координата X")
    y: float = Field(..., description="Координата Y")
    z: float = Field(0.0, description="Координата Z")
    geofence_ids: Optional[List[UUID]] = Field(
        None,
        description="Список ID геозон для проверки (если не указано - проверяются все)"
    )

class GeofenceIntersection(BaseModel):
    geofence_id: UUID
    geofence_name: str
    zone_type: str
    is_inside: bool

class GeofenceCheckResult(BaseModel):
    point: Dict[str, float]
    intersections: List[GeofenceIntersection]
