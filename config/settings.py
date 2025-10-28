from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional, List

class Settings(BaseSettings):
    APP_NAME: str = Field(default="Scalable Async Energy API", env="APP_NAME")
    VERSION: str = Field(default="1.0.0", env="VERSION")
    DEBUG: bool = Field(default=False, env="DEBUG")
    HOST: str = Field(default="0.0.0.0", env="HOST")
    PORT: int = Field(default=8000, env="PORT")
    WORKERS: int = Field(default=1, env="WORKERS")
    
    DB_HOST: str = Field(default="postgres", env="DB_HOST")
    DB_PORT: int = Field(default=5432, env="DB_PORT")
    DB_NAME: str = Field(default="energy_db", env="DB_NAME")
    DB_USER: str = Field(default="energy_user", env="DB_USER")
    DB_PASSWORD: str = Field(default="energy_pass", env="DB_PASSWORD")
    
    REDIS_HOST: str = Field(default="redis", env="REDIS_HOST")
    REDIS_PORT: int = Field(default=6379, env="REDIS_PORT")
    REDIS_DB: int = Field(default=0, env="REDIS_DB")
    REDIS_PASSWORD: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    REDIS_MAX_CONNECTIONS: int = Field(default=20, env="REDIS_MAX_CONNECTIONS")
    
    CELERY_BROKER_URL: str = Field(default="redis://redis:6379/0", env="CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND: str = Field(default="redis://redis:6379/0", env="CELERY_RESULT_BACKEND")
    
    INFLUXDB_HOST: str = Field(default="influxdb", env="INFLUXDB_HOST")
    INFLUXDB_PORT: int = Field(default=8086, env="INFLUXDB_PORT")
    INFLUXDB_TOKEN: str = Field(default="energy_token", env="INFLUXDB_TOKEN")
    INFLUXDB_ORG: str = Field(default="energy_org", env="INFLUXDB_ORG")
    INFLUXDB_BUCKET: str = Field(default="energy_metrics", env="INFLUXDB_BUCKET")
    
    CACHE_TTL_DEFAULT: int = Field(default=300, env="CACHE_TTL_DEFAULT")
    CACHE_TTL_LATEST_READINGS: int = Field(default=60, env="CACHE_TTL_LATEST_READINGS")
    CACHE_TTL_AGGREGATES: int = Field(default=900, env="CACHE_TTL_AGGREGATES")
    
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(default="text", env="LOG_FORMAT")
    
    MONITORING_ENABLED: bool = Field(default=True, env="MONITORING_ENABLED")
    METRICS_COLLECTION_INTERVAL: int = Field(default=30, env="METRICS_COLLECTION_INTERVAL")
    
    SENTRY_DSN: Optional[str] = Field(default=None, env="SENTRY_DSN")
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def REDIS_URL(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    @property
    def INFLUXDB_URL(self) -> str:
        return f"http://{self.INFLUXDB_HOST}:{self.INFLUXDB_PORT}"
    
    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

def get_celery_config() -> dict:
    return {
        "broker_url": settings.CELERY_BROKER_URL,
        "result_backend": settings.CELERY_RESULT_BACKEND,
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "timezone": "UTC",
        "enable_utc": True,
        "worker_prefetch_multiplier": 1,
        "task_acks_late": True,
        "task_reject_on_worker_lost": True,
    }

def get_cors_config() -> dict:
    return {
        "allow_origins": ["*"],
        "allow_credentials": True,
        "allow_methods": ["GET", "POST", "PUT", "DELETE"],
        "allow_headers": ["*"],
    }

def get_logging_config() -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": settings.LOG_LEVEL,
                "formatter": "default",
            },
        },
        "root": {
            "level": settings.LOG_LEVEL,
            "handlers": ["console"],
        },
    }