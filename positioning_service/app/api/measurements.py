from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from datetime import datetime
import uuid
import logging

from app.models import MeasurementBatch, ErrorResponse, ValidationErrorResponse
from app.database import save_measurements_batch, init_db
from app.trilateration import simple_trilateration

router = APIRouter()
logger = logging.getLogger(__name__)

# Демо-анкеры (в реальности из БД)
DEMO_ANCHORS = {
    "anchor-1": (0.0, 0.0, 3.0),
    "anchor-2": (50.0, 0.0, 3.0),
    "anchor-3": (25.0, 30.0, 3.0),
    "anchor-4": (0.0, 30.0, 3.0),
}


async def process_batch_async(batch_id: str, batch_data: MeasurementBatch):
    """Асинхронная обработка пакета измерений"""
    try:
        # Сохраняем измерения
        measurements_dict = [
            {
                'anchor_id': m.anchor_id,
                'tag_id': m.tag_id,
                'distance_m': m.distance_m,
                'timestamp': batch_data.timestamp
            }
            for m in batch_data.measurements
        ]
        
        save_measurements_batch(
            batch_id=batch_id,
            gateway_id=batch_data.gateway_id,
            measurements=measurements_dict
        )
        
        # Группируем по tag_id для вычисления позиций
        measurements_by_tag = {}
        for m in batch_data.measurements:
            if m.tag_id not in measurements_by_tag:
                measurements_by_tag[m.tag_id] = []
            measurements_by_tag[m.tag_id].append({
                'anchor_id': m.anchor_id,
                'distance_m': m.distance_m
            })
        
        # Вычисляем позиции для каждой метки
        calculated_positions = {}
        for tag_id, measurements in measurements_by_tag.items():
            if len(measurements) >= 3:
                position = simple_trilateration(measurements, DEMO_ANCHORS)
                calculated_positions[tag_id] = position
                logger.info(f"Calculated position for {tag_id}: {position}")
        
        # Здесь можно сохранить вычисленные позиции в БД
        # и обновить кэш
        
        logger.info(f"Batch {batch_id} processed successfully")
        
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")


@router.post(
    "/measurements",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        202: {
            "description": "Пакет измерений успешно принят в обработку",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Measurements accepted for processing",
                        "batch_id": "123e4567-e89b-12d3-a456-426614174000"
                    }
                }
            }
        },
        400: {
            "description": "Неверный формат или некорректные данные",
            "model": ErrorResponse
        },
        422: {
            "description": "Данные не прошли валидацию",
            "model": ValidationErrorResponse
        }
    }
)
async def submit_measurements(batch: MeasurementBatch):
    """
    Прием пакета измерений от анкеров.
    
    - Принимает массив измерений расстояний
    - Минимум 3 измерения от разных анкеров
    - Возвращает batch_id для отслеживания
    """
    try:
        # Генерируем уникальный ID для пакета
        batch_id = str(uuid.uuid4())
        
        # Запускаем асинхронную обработку
        import asyncio
        asyncio.create_task(process_batch_async(batch_id, batch))
        
        # Немедленно возвращаем ответ
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "message": "Measurements accepted for processing",
                "batch_id": batch_id
            }
        )
        
    except ValueError as e:
        # Ошибки валидации Pydantic
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=ValidationErrorResponse(
                error_code="VALIDATION_ERROR",
                message=str(e),
                details=[{"field": "measurements", "error": str(e)}]
            ).model_dump()
        )
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(
                error_code="PROCESSING_ERROR",
                message=f"Failed to process measurements: {str(e)}"
            ).model_dump()
        )
from app.trilateration import save_calculated_position  # <-- ИМПОРТ

async def process_batch_async(batch_id: str, batch_data: MeasurementBatch):
    """Асинхронная обработка пакета измерений"""
    try:
        # Получаем актуальные анкеры из БД
        from app.trilateration import get_anchors_from_db
        anchors = get_anchors_from_db()
        
        if not anchors:
            logger.warning(f"No active anchors found in database")
            return
        
        # Сохраняем сырые измерения
        measurements_dict = [
            {
                'anchor_id': m.anchor_id,
                'tag_id': m.tag_id,
                'distance_m': m.distance_m,
                'timestamp': batch_data.timestamp
            }
            for m in batch_data.measurements
        ]
        
        from app.database import save_measurements_batch
        save_measurements_batch(
            batch_id=batch_id,
            gateway_id=batch_data.gateway_id,
            measurements=measurements_dict
        )
        
        # Группируем по tag_id для вычисления позиций
        measurements_by_tag = {}
        for m in batch_data.measurements:
            if m.tag_id not in measurements_by_tag:
                measurements_by_tag[m.tag_id] = []
            measurements_by_tag[m.tag_id].append({
                'anchor_id': m.anchor_id,
                'distance_m': m.distance_m
            })
        
        # Вычисляем позиции для каждой метки
        from app.trilateration import simple_trilateration, save_calculated_position
        for tag_id, measurements in measurements_by_tag.items():
            if len(measurements) >= 3:
                try:
                    position = simple_trilateration(measurements, anchors)
                    
                    # Сохраняем вычисленную позицию в БД
                    save_calculated_position(batch_id, tag_id, position)
                    logger.info(f"Calculated position for {tag_id}: {position}")
                except Exception as trilat_error:
                    logger.error(f"Trilateration failed for {tag_id}: {trilat_error}")
                    
        logger.info(f"Batch {batch_id} processed successfully")
        
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")
