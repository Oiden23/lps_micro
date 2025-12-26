from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError
from app.models import ValidationErrorResponse, ErrorResponse
from app.api.reports import router as reports_router
from app.api.aggregation import router as aggregation_router
from app.api.export import router as export_router
from app.api.analytics import router as analytics_router
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    from app.database import init_db
    init_db()
    logger.info("Data Aggregator Service started")
    
    # Запускаем планировщик задач для периодической агрегации
    scheduler = AsyncIOScheduler()
    
    # Добавляем задачу для ежечасной агрегации данных
    from app.api.aggregation import aggregate_data_for_period
    
    async def hourly_aggregation():
        try:
            logger.info("Starting hourly data aggregation")
            await aggregate_data_for_period(
                start_time=datetime.now() - timedelta(hours=1),
                end_time=datetime.now(),
                force=False
            )
            logger.info("Hourly data aggregation completed successfully")
        except Exception as e:
            logger.error(f"Error in hourly aggregation: {e}")
    
    # Добавляем задачу для ежедневной очистки старых данных
    from app.database import cleanup_old_data
    
    async def daily_cleanup():
        try:
            logger.info("Starting daily data cleanup")
            deleted_count = cleanup_old_data(days_to_keep=90)
            logger.info(f"Daily cleanup completed. Deleted {deleted_count} records")
        except Exception as e:
            logger.error(f"Error in daily cleanup: {e}")
    
    # Запускаем задачи
    scheduler.add_job(
        hourly_aggregation,
        trigger=IntervalTrigger(hours=1),
        id='hourly_aggregation',
        replace_existing=True
    )
    
    scheduler.add_job(
        daily_cleanup,
        trigger=IntervalTrigger(days=1),
        id='daily_cleanup',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started with periodic tasks")
    
    yield
    
    # Shutdown
    scheduler.shutdown()
    logger.info("Data Aggregator Service shutting down")

app = FastAPI(
    title="Data Aggregator Service API",
    description="Микросервис для агрегации данных о перемещениях объектов, генерации отчетов и аналитики",
    version="1.0.0",
    lifespan=lifespan
)

# Обработчики ошибок
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error.get("loc", []))
        errors.append({
            "field": field,
            "error": error.get("msg")
        })
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=ValidationErrorResponse(
            error_code="VALIDATION_ERROR",
            message="Invalid request data",
            details=errors
        ).model_dump()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    import traceback
    traceback.print_exc()  # Для отладки
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=ErrorResponse(
            error_code="INTERNAL_ERROR",
            message=str(exc)
        ).model_dump()
    )

# Корневой endpoint
@app.get("/", include_in_schema=False)
async def root():
    """Перенаправление на документацию Swagger"""
    return RedirectResponse(url="/docs")

# Health endpoint
@app.get("/health", include_in_schema=False)
async def health_check():
    from app.database import get_database_stats
    try:
        db_stats = get_database_stats()
        return {
            "status": "healthy",
            "service": "data_aggregator",
            "timestamp": datetime.now().isoformat(),
            "database_stats": db_stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "service": "data_aggregator",
                "error": str(e)
            }
        )

# Подключаем роутеры
app.include_router(reports_router, prefix="/api/v1")
app.include_router(aggregation_router, prefix="/api/v1")
app.include_router(export_router, prefix="/api/v1")
app.include_router(analytics_router, prefix="/api/v1")