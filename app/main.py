from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import time
import structlog
from datetime import datetime

from app.api.routes import router
from config.settings import settings, get_cors_config
from app.utils.logging import setup_logging, RequestLoggingMiddleware, error_handler
from app.utils.metrics import setup_metrics_collection, influx_metrics, system_metrics
from app.utils.cache import cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = structlog.get_logger(__name__)
    
    logger.info("Starting Energy API application", version=settings.VERSION)
    
    setup_logging()
    
    if settings.MONITORING_ENABLED:
        setup_metrics_collection()
        logger.info("Metrics collection enabled")
    
    try:
        redis_health = await cache.cache.ping()
        logger.info("Redis connection", status="healthy" if redis_health else "unhealthy")
    except Exception as e:
        logger.error("Redis connection failed", error=str(e))
    
    logger.info("Application startup complete")
    
    yield
    
    logger.info("Shutting down application")
    
    try:
        await cache.cache.close()
        influx_metrics.close()
        logger.info("Connections closed successfully")
    except Exception as e:
        logger.error("Error during shutdown", error=str(e))


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="Scalable Async Energy Data Processing API",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    lifespan=lifespan
)

app.add_middleware(CORSMiddleware, **get_cors_config())
app.add_middleware(RequestLoggingMiddleware)

app.include_router(router, prefix="/api/v1", tags=["energy"])


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.get("/health")
async def health_check():
    logger = structlog.get_logger(__name__)
    
    health_status = {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.VERSION,
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    try:
        redis_health = await cache.cache.ping()
        health_status["checks"]["redis"] = "healthy" if redis_health else "unhealthy"
    except Exception as e:
        health_status["checks"]["redis"] = "unhealthy"
        logger.warning("Redis health check failed", error=str(e))
    
    try:
        from app.tasks import health_check as task_health_check
        task_result = task_health_check.delay()
        health_status["checks"]["celery"] = "healthy"
    except Exception as e:
        health_status["checks"]["celery"] = "unhealthy"
        logger.warning("Celery health check failed", error=str(e))
    
    overall_healthy = all(
        status == "healthy" for status in health_status["checks"].values()
    )
    
    if not overall_healthy:
        health_status["status"] = "degraded"
    
    return health_status


@app.get("/metrics")
async def get_metrics():
    if not settings.MONITORING_ENABLED:
        return {"message": "Monitoring disabled"}
    
    try:
        cache_stats = await cache.get_cache_stats()
        
        error_stats = error_handler.get_error_stats(24)
        
        from app.utils.logging import dead_letter_queue
        dlq_entries = dead_letter_queue.get_dlq_entries(10)
        
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "cache": cache_stats,
            "errors": error_stats,
            "dead_letter_queue": {
                "size": len(dlq_entries),
                "recent_entries": dlq_entries[:5]
            },
            "system": {
                "monitoring_enabled": settings.MONITORING_ENABLED,
                "debug_mode": settings.DEBUG
            }
        }
        
        return metrics
        
    except Exception as e:
        logger = structlog.get_logger(__name__)
        error_id = error_handler.handle_exception(e, {"endpoint": "/metrics"})
        return {"error": "Failed to collect metrics", "error_id": error_id}


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        workers=settings.WORKERS,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
