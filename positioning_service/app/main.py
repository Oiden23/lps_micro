from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError

from app.models import ValidationErrorResponse, ErrorResponse
from app.api.measurements import router as measurements_router
from app.api.positions import router as positions_router
from app.api.anchors import router as anchors_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    from app.database import init_db
    init_db()
    print("Positioning service started")
    yield
    # Shutdown
    print("Positioning service shutting down")

app = FastAPI(
    title="Positioning Service API",
    description="Микросервис для позиционирования персонала в реальном времени",
    version="1.0.0",
    lifespan=lifespan
)

# Обработчики ошибок (оставляем как есть)
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

# Подключаем роутеры
app.include_router(measurements_router, prefix="/api/v1")
app.include_router(positions_router, prefix="/api/v1")
app.include_router(anchors_router, prefix="/api/v1")
