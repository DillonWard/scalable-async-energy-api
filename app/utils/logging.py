import logging
import logging.config
import structlog
import sys
import traceback
from typing import Any, Dict, Optional, List
from datetime import datetime
import json
import redis
from config.settings import settings, get_logging_config
from app.utils.metrics import influx_metrics


def setup_logging():
    
    logging.config.dictConfig(get_logging_config())
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if settings.LOG_FORMAT == "json" else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


class ErrorHandler:
    
    def __init__(self):
        self.logger = structlog.get_logger(__name__)
        self.redis_client = None
        
        if settings.REDIS_HOST:
            try:
                self.redis_client = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=settings.REDIS_DB,
                    decode_responses=True,
                    socket_timeout=5
                )
            except Exception as e:
                self.logger.warning("Could not connect to Redis for error tracking", error=str(e))
    
    def handle_exception(
        self,
        exc: Exception,
        context: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
        request_id: Optional[str] = None,
        send_to_sentry: bool = True
    ) -> str:
        
        error_id = f"error_{datetime.utcnow().isoformat()}_{id(exc)}"
        
        error_data = {
            "error_id": error_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
            "context": context or {},
            "user_id": user_id,
            "request_id": request_id,
            "traceback": traceback.format_exc()
        }
        
        self.logger.error(
            "Exception occurred",
            **error_data
        )
        
        self._store_error(error_data)
        
        influx_metrics.write_point(
            "application_errors",
            {
                "error_type": type(exc).__name__,
                "user_id": user_id or "anonymous",
                "has_context": "yes" if context else "no"
            },
            {
                "error_count": 1,
                "has_traceback": bool(error_data["traceback"])
            },
            async_write=True
        )
        
        if send_to_sentry and settings.SENTRY_DSN:
            self._send_to_sentry(exc, error_data)
        
        return error_id
    
    def handle_validation_error(
        self,
        field_errors: List[Dict[str, Any]],
        request_data: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None
    ) -> str:
        
        error_id = f"validation_error_{datetime.utcnow().isoformat()}"
        
        error_data = {
            "error_id": error_id,
            "error_type": "ValidationError",
            "field_errors": field_errors,
            "request_data": request_data,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": user_id
        }
        
        self.logger.warning(
            "Validation error occurred",
            **error_data
        )
        
        self._store_error(error_data)
        
        influx_metrics.write_point(
            "validation_errors",
            {
                "user_id": user_id or "anonymous",
                "field_count": str(len(field_errors))
            },
            {
                "error_count": 1,
                "field_error_count": len(field_errors)
            },
            async_write=True
        )
        
        return error_id
    
    def _store_error(self, error_data: Dict[str, Any]):
        if not self.redis_client:
            return
        
        try:
            self.redis_client.lpush("application_errors", json.dumps(error_data, default=str))
            self.redis_client.ltrim("application_errors", 0, 9999)
            
            self.redis_client.expire("application_errors", 86400 * 7)
            
        except Exception as e:
            self.logger.warning("Failed to store error in Redis", error=str(e))
    
    def _send_to_sentry(self, exc: Exception, error_data: Dict[str, Any]):
        try:
            import sentry_sdk
            
            with sentry_sdk.push_scope() as scope:
                scope.set_tag("error_id", error_data["error_id"])
                scope.set_context("error_context", error_data["context"])
                
                if error_data["user_id"]:
                    scope.user = {"id": error_data["user_id"]}
                
                sentry_sdk.capture_exception(exc)
                
        except ImportError:
            self.logger.warning("Sentry SDK not available for error reporting")
        except Exception as e:
            self.logger.warning("Failed to send error to Sentry", error=str(e))
    
    def get_error_stats(self, hours: int = 24) -> Dict[str, Any]:
        if not self.redis_client:
            return {}
        
        try:
            errors = self.redis_client.lrange("application_errors", 0, -1)
            
            stats = {
                "total_errors": len(errors),
                "error_types": {},
                "errors_by_hour": {},
                "recent_errors": []
            }
            
            cutoff_time = datetime.utcnow().timestamp() - (hours * 3600)
            
            for error_json in errors:
                try:
                    error = json.loads(error_json)
                    error_time = datetime.fromisoformat(error["timestamp"]).timestamp()
                    
                    if error_time < cutoff_time:
                        continue
                    
                    error_type = error.get("error_type", "Unknown")
                    stats["error_types"][error_type] = stats["error_types"].get(error_type, 0) + 1
                    
                    hour_key = datetime.fromtimestamp(error_time).strftime("%Y-%m-%d %H:00")
                    stats["errors_by_hour"][hour_key] = stats["errors_by_hour"].get(hour_key, 0) + 1
                    
                    if len(stats["recent_errors"]) < 10:
                        stats["recent_errors"].append({
                            "error_id": error.get("error_id"),
                            "error_type": error_type,
                            "message": error.get("error_message", "")[:200],
                            "timestamp": error.get("timestamp")
                        })
                
                except json.JSONDecodeError:
                    continue
            
            return stats
            
        except Exception as e:
            self.logger.error("Failed to get error stats", error=str(e))
            return {}


class DeadLetterQueue:
    
    def __init__(self):
        self.logger = structlog.get_logger(__name__)
        self.redis_client = None
        
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
        except Exception as e:
            self.logger.error("Could not connect to Redis for DLQ", error=str(e))
    
    def send_to_dlq(
        self,
        task_name: str,
        task_data: Dict[str, Any],
        error_message: str,
        retry_count: int = 0,
        max_retries: int = 3
    ) -> bool:
        
        if not self.redis_client:
            self.logger.error("Redis not available for DLQ operation")
            return False
        
        try:
            dlq_entry = {
                "task_name": task_name,
                "task_data": task_data,
                "error_message": error_message,
                "retry_count": retry_count,
                "max_retries": max_retries,
                "timestamp": datetime.utcnow().isoformat(),
                "can_retry": retry_count < max_retries,
                "dlq_id": f"dlq_{task_name}_{datetime.utcnow().timestamp()}"
            }
            
            self.redis_client.lpush("dead_letter_queue", json.dumps(dlq_entry, default=str))
            
            self.redis_client.ltrim("dead_letter_queue", 0, 9999)
            
            self.logger.warning(
                "Task sent to dead letter queue",
                task_name=task_name,
                error=error_message,
                retry_count=retry_count
            )
            
            influx_metrics.write_point(
                "dead_letter_queue",
                {
                    "task_name": task_name,
                    "can_retry": "yes" if retry_count < max_retries else "no"
                },
                {
                    "dlq_count": 1,
                    "retry_count": retry_count
                },
                async_write=True
            )
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to send task to DLQ", error=str(e))
            return False
    
    def get_dlq_entries(self, limit: int = 100) -> List[Dict[str, Any]]:
        if not self.redis_client:
            return []
        
        try:
            entries = self.redis_client.lrange("dead_letter_queue", 0, limit - 1)
            
            dlq_entries = []
            for entry_json in entries:
                try:
                    entry = json.loads(entry_json)
                    dlq_entries.append(entry)
                except json.JSONDecodeError:
                    continue
            
            return dlq_entries
            
        except Exception as e:
            self.logger.error("Failed to get DLQ entries", error=str(e))
            return []
    
    def retry_dlq_entry(self, dlq_id: str) -> bool:
        if not self.redis_client:
            return False
        
        try:
            entries = self.get_dlq_entries(1000)
            
            for i, entry in enumerate(entries):
                if entry.get("dlq_id") == dlq_id:
                    if not entry.get("can_retry", False):
                        self.logger.warning("DLQ entry cannot be retried", dlq_id=dlq_id)
                        return False
                    
                    from app.tasks import process_energy_reading, batch_process_readings
                    
                    task_name = entry["task_name"]
                    task_data = entry["task_data"]
                    
                    if task_name == "process_energy_reading":
                        task = process_energy_reading.delay(task_data)
                    elif task_name == "batch_process_readings":
                        task = batch_process_readings.delay(task_data)
                    else:
                        self.logger.error("Unknown task type for retry", task_name=task_name)
                        return False
                    
                    self.redis_client.lrem("dead_letter_queue", 1, json.dumps(entry, default=str))
                    
                    self.logger.info("DLQ entry retried", dlq_id=dlq_id, task_id=task.id)
                    return True
            
            self.logger.warning("DLQ entry not found", dlq_id=dlq_id)
            return False
            
        except Exception as e:
            self.logger.error("Failed to retry DLQ entry", dlq_id=dlq_id, error=str(e))
            return False
    
    def clear_dlq(self, max_age_hours: int = 168) -> int:
        if not self.redis_client:
            return 0
        
        try:
            entries = self.get_dlq_entries(10000)
            cutoff_time = datetime.utcnow().timestamp() - (max_age_hours * 3600)
            
            removed_count = 0
            
            for entry in entries:
                try:
                    entry_time = datetime.fromisoformat(entry["timestamp"]).timestamp()
                    
                    if entry_time < cutoff_time:
                        self.redis_client.lrem("dead_letter_queue", 1, json.dumps(entry, default=str))
                        removed_count += 1
                
                except (ValueError, KeyError):
                    continue
            
            self.logger.info("Cleared old DLQ entries", removed_count=removed_count)
            return removed_count
            
        except Exception as e:
            self.logger.error("Failed to clear DLQ", error=str(e))
            return 0


error_handler = ErrorHandler()
dead_letter_queue = DeadLetterQueue()


def log_api_request(
    endpoint: str,
    method: str,
    status_code: int,
    response_time_ms: float,
    user_id: Optional[str] = None,
    request_size: Optional[int] = None,
    response_size: Optional[int] = None
):
    logger = structlog.get_logger("api")
    
    log_data = {
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "response_time_ms": response_time_ms,
        "user_id": user_id,
        "request_size": request_size,
        "response_size": response_size
    }
    
    if status_code >= 500:
        logger.error("API request failed", **log_data)
    elif status_code >= 400:
        logger.warning("API request client error", **log_data)
    else:
        logger.info("API request completed", **log_data)
    
    influx_metrics.record_api_request(
        endpoint, method, status_code, response_time_ms, user_id
    )


def log_task_execution(
    task_name: str,
    status: str,
    execution_time_ms: Optional[float] = None,
    error_message: Optional[str] = None,
    retries: int = 0
):
    logger = structlog.get_logger("tasks")
    
    log_data = {
        "task_name": task_name,
        "status": status,
        "execution_time_ms": execution_time_ms,
        "retries": retries,
        "error_message": error_message
    }
    
    if status == "success":
        logger.info("Task completed successfully", **log_data)
    elif status == "retry":
        logger.warning("Task will be retried", **log_data)
    else:
        logger.error("Task failed", **log_data)
    
    influx_metrics.record_task_execution(
        task_name, status, execution_time_ms, retries=retries, error_message=error_message
    )


class RequestLoggingMiddleware:
    
    def __init__(self, app):
        self.app = app
        self.logger = structlog.get_logger("middleware")
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        start_time = datetime.utcnow().timestamp()
        
        async def send_with_logging(message):
            if message["type"] == "http.response.start":
                end_time = datetime.utcnow().timestamp()
                response_time_ms = (end_time - start_time) * 1000
                
                log_api_request(
                    endpoint=scope["path"],
                    method=scope["method"],
                    status_code=message["status"],
                    response_time_ms=response_time_ms
                )
            
            await send(message)
        
        await self.app(scope, receive, send_with_logging)