from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import List, Optional, Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime
from typing import List, Optional, Any


class SingleMeasurement(BaseModel):
    anchor_id: str
    tag_id: str
    distance_m: float = Field(..., gt=0, description="Измеренное расстояние в метрах > 0")


class MeasurementBatch(BaseModel):
    gateway_id: str
    timestamp: datetime
    measurements: List[SingleMeasurement] = Field(..., min_length=3, description="Минимум 3 измерения")
    
    @field_validator('measurements')
    @classmethod
    def validate_unique_anchors(cls, v: List[SingleMeasurement]) -> List[SingleMeasurement]:
        """Проверяем, что измерения от разных анкеров"""
        anchor_ids = {m.anchor_id for m in v}
        if len(anchor_ids) < 3:
            raise ValueError('Для трилатерации нужны измерения от минимум 3 разных анкеров')
        return v


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class ValidationErrorResponse(ErrorResponse):
    details: List[dict[str, str]]

class Position(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    tag_id: str
    x: float = Field(..., description="Координата X в метрах")
    y: float = Field(..., description="Координата Y в метрах")
    z: float = Field(default=0.0, description="Координата Z в метрах (высота)")
    calculation_timestamp: datetime
    accuracy: float = Field(..., ge=0, description="Погрешность в метрах")
    source_measurements: Optional[List[SingleMeasurement]] = Field(
        default=None, 
        description="Сырые измерения, использованные для расчета"
    )

class Anchor(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    anchor_id: str = Field(..., description="Уникальный аппаратный или логический идентификатор анкера")
    x: float = Field(..., description="Фиксированная координата X анкера в системе")
    y: float = Field(...)
    z: float = Field(...)
    is_active: bool = Field(default=True, description="Флаг, указывающий, используется ли анкер в данный момент для вычислений")
    description: Optional[str] = Field(
        default=None, 
        description="Человеко-читаемое описание местоположения"
    )
    last_calibration: Optional[datetime] = Field(
        default=None,
        description="Дата последней калибровки"
    )

