from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError

from app.models import ValidationErrorResponse, ErrorResponse
from app.api.entities import router as entities_router
from app.api.geofences import router as geofences_router
from app.api.rules import router as rules_router
from app.api.compliance import router as compliance_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    from app.database import init_db
    init_db()
    print("Access Control Service started")
    yield
    # Shutdown
    print("Access Control Service shutting down")

app = FastAPI(
    title="Compliance & Access Control Service API",
    description="Микросервис для управления геозонами, правилами доступа и контроля соблюдения политик безопасности",
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

# Health endpoint (упрощенный)
@app.get("/health", include_in_schema=False)
async def health_check():
    return {"status": "healthy", "service": "access_control"}

# Подключаем роутеры
app.include_router(entities_router, prefix="/api/v1")
app.include_router(geofences_router, prefix="/api/v1")
app.include_router(rules_router, prefix="/api/v1")
app.include_router(compliance_router, prefix="/api/v1")
