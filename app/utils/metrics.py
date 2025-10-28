import logging
import asyncio
import threading
import time
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from config.settings import settings

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - system metrics will be limited")

logger = logging.getLogger(__name__)

class SystemMetrics:
    
    def __init__(self, influx_metrics: 'InfluxDBMetrics'):
        self.influx = influx_metrics
    
    def collect_system_metrics(self) -> bool:
        if not PSUTIL_AVAILABLE:
            logger.warning("psutil not available - skipping system metrics collection")
            return False
            
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            network = psutil.net_io_counters()
            
            points = []
            
            points.append(
                Point("system_cpu")
                .field("percent", cpu_percent)
                .time(datetime.utcnow())
            )
            
            points.append(
                Point("system_memory")
                .field("total_bytes", memory.total)
                .field("available_bytes", memory.available)
                .field("used_bytes", memory.used)
                .field("percent", memory.percent)
                .time(datetime.utcnow())
            )
            
            points.append(
                Point("system_disk")
                .field("total_bytes", disk.total)
                .field("used_bytes", disk.used)
                .field("free_bytes", disk.free)
                .field("percent", (disk.used / disk.total) * 100)
                .time(datetime.utcnow())
            )
            
            if network:
                points.append(
                    Point("system_network")
                    .field("bytes_sent", network.bytes_sent)
                    .field("bytes_recv", network.bytes_recv)
                    .field("packets_sent", network.packets_sent)
                    .field("packets_recv", network.packets_recv)
                    .time(datetime.utcnow())
                )
            
            return self.influx.write_points(points)
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {str(e)}")
            return False
    
    def collect_process_metrics(self, process_name: str = "energy_api") -> bool:
        if not PSUTIL_AVAILABLE:
            return False
            
        try:
            current_process = psutil.Process()
            
            memory_info = current_process.memory_info()
            cpu_percent = current_process.cpu_percent()
            
            open_files = len(current_process.open_files())
            num_threads = current_process.num_threads()
            
            tags = {"process_name": process_name}
            
            fields = {
                "memory_rss_bytes": memory_info.rss,
                "memory_vms_bytes": memory_info.vms,
                "cpu_percent": cpu_percent,
                "open_files": open_files,
                "num_threads": num_threads,
            }
            
            return self.influx.write_point(
                "process_metrics", 
                tags, 
                fields, 
                async_write=True
            )
            
        except Exception as e:
            logger.error(f"Error collecting process metrics: {str(e)}")
            return False

class InfluxDBMetrics:
    
    def __init__(self):
        self._client = None
        self._write_api = None
        self._async_write_api = None
        self._connection_healthy = True
        self._last_connection_check = 0
        self._connection_check_interval = 60
    
    def _check_connection_health(self) -> bool:
        current_time = time.time()
        if current_time - self._last_connection_check < self._connection_check_interval:
            return self._connection_healthy
        
        try:
            if self._client:
                self._client.ping()
                self._connection_healthy = True
            else:
                self._connection_healthy = False
        except Exception as e:
            logger.warning(f"InfluxDB connection check failed: {str(e)}")
            self._connection_healthy = False
        
        self._last_connection_check = current_time
        return self._connection_healthy
    
    @property
    def client(self) -> Optional[InfluxDBClient]:
        if not self._check_connection_health() and self._client is None:
            return None
            
        if self._client is None:
            try:
                self._client = InfluxDBClient(
                    url=settings.INFLUXDB_URL,
                    token=settings.INFLUXDB_TOKEN,
                    org=settings.INFLUXDB_ORG,
                    timeout=10000,
                    enable_gzip=True
                )
                self._client.ping()
                self._connection_healthy = True
                logger.info("InfluxDB connection established successfully")
            except Exception as e:
                logger.error(f"Failed to create InfluxDB client: {str(e)}")
                self._connection_healthy = False
                self._client = None
                return None
        return self._client
    
    @property
    def write_api(self):
        if not self._connection_healthy:
            return None
            
        if self._write_api is None:
            try:
                client = self.client
                if client is None:
                    return None
                self._write_api = client.write_api(write_options=SYNCHRONOUS)
            except Exception as e:
                logger.error(f"Failed to create sync write API: {str(e)}")
                self._write_api = None
        return self._write_api
    
    @property    
    def async_write_api(self):
        if not self._connection_healthy:
            return None
            
        if self._async_write_api is None:
            try:
                client = self.client
                if client is None:
                    return None
                    
                write_options = WriteOptions(
                    batch_size=50, 
                    flush_interval=5000,
                    jitter_interval=1000,
                    retry_interval=2000,
                    max_retries=2,
                    max_retry_delay=10000,
                    exponential_base=2
                )
                self._async_write_api = client.write_api(write_options=write_options)
            except Exception as e:
                logger.error(f"Failed to create async write API: {str(e)}")
                self._async_write_api = None
        return self._async_write_api
    
    def write_point(
        self, 
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, Union[int, float, str, bool]],
        timestamp: Optional[datetime] = None,
        async_write: bool = False
    ) -> bool:
        if not self._check_connection_health():
            logger.debug("InfluxDB connection unhealthy, skipping metric write")
            return False
            
        try:
            point = Point(measurement)
            
            for key, value in tags.items():
                point = point.tag(key, str(value))
            
            for key, value in fields.items():
                point = point.field(key, value)
            
            if timestamp:
                point = point.time(timestamp)
            else:
                point = point.time(datetime.utcnow())
            
            write_api = self.async_write_api if async_write else self.write_api
            if write_api is None:
                logger.debug("Write API not available, skipping metric")
                return False
                
            write_api.write(
                bucket=settings.INFLUXDB_BUCKET,
                org=settings.INFLUXDB_ORG,
                record=point
            )
            
            return True
            
        except Exception as e:
            logger.warning(f"Failed to write point to InfluxDB: {str(e)}")
            self._connection_healthy = False
            return False
    
    def write_points(
        self, 
        points: List[Point],
        async_write: bool = True
    ) -> bool:
        if not self._check_connection_health():
            logger.debug("InfluxDB connection unhealthy, skipping metrics write")
            return False
            
        try:
            write_api = self.async_write_api if async_write else self.write_api
            if write_api is None:
                logger.debug("Write API not available, skipping metrics")
                return False
                
            write_api.write(
                bucket=settings.INFLUXDB_BUCKET,
                org=settings.INFLUXDB_ORG,
                record=points
            )
            
            return True
            
        except Exception as e:
            logger.warning(f"Failed to write points to InfluxDB: {str(e)}")
            self._connection_healthy = False
            return False
    
    def record_api_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        response_time_ms: float,
        user_id: Optional[str] = None,
        request_size: Optional[int] = None,
        response_size: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> bool:
        if not self._check_connection_health():
            return False
            
        try:
            tags = {
                "endpoint": endpoint,
                "method": method,
                "status_code": str(status_code),
                "status_class": f"{status_code // 100}xx"
            }
            
            if user_id:
                tags["user_id"] = str(user_id)
            
            fields = {
                "response_time_ms": response_time_ms,
                "count": 1
            }
            
            if request_size is not None:
                fields["request_size_bytes"] = request_size
            
            if response_size is not None:
                fields["response_size_bytes"] = response_size
            
            if error_message:
                fields["error_message"] = error_message
            
            return self.write_point(
                "api_requests",
                tags,
                fields,
                async_write=True
            )
            
        except Exception as e:
            logger.warning(f"Failed to record API request metric: {str(e)}")
            return False
    
    def record_database_query(
        self,
        operation: str,
        table: str,
        duration_ms: float,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> bool:
        if not self._check_connection_health():
            return False
            
        try:
            tags = {
                "operation": operation,
                "table": table,
                "success": str(success).lower()
            }
            
            fields = {
                "duration_ms": duration_ms,
                "count": 1
            }
            
            if error_message:
                fields["error_message"] = error_message
            
            return self.write_point(
                "database_queries",
                tags,
                fields,
                async_write=True
            )
            
        except Exception as e:
            logger.warning(f"Failed to record database query metric: {str(e)}")
            return False
    
    def record_cache_operation(
        self,
        operation: str,
        key_pattern: str,
        duration_ms: Optional[float] = None,
        success: bool = True
    ) -> bool:
        if not self._check_connection_health():
            return False
            
        try:
            tags = {
                "operation": operation,
                "key_pattern": key_pattern,
                "success": str(success).lower()
            }
            
            fields = {
                "count": 1
            }
            
            if duration_ms is not None:
                fields["duration_ms"] = duration_ms
            
            return self.write_point(
                "cache_operations",
                tags,
                fields,
                async_write=True
            )
            
        except Exception as e:
            logger.warning(f"Failed to record cache operation metric: {str(e)}")
            return False
    
    def record_task_execution(
        self,
        task_name: str,
        duration_ms: float,
        success: bool = True,
        retry_count: int = 0,
        error_message: Optional[str] = None
    ) -> bool:
        if not self._check_connection_health():
            return False
            
        try:
            tags = {
                "task_name": task_name,
                "success": str(success).lower(),
                "retry_count": str(retry_count)
            }
            
            fields = {
                "duration_ms": duration_ms,
                "count": 1
            }
            
            if error_message:
                fields["error_message"] = error_message
            
            return self.write_point(
                "task_executions",
                tags,
                fields,
                async_write=True
            )
            
        except Exception as e:
            logger.warning(f"Failed to record task execution metric: {str(e)}")
            return False
    
    def close(self):
        try:
            if self._write_api:
                self._write_api.close()
            if self._async_write_api:
                self._async_write_api.close()
            if self._client:
                self._client.close()
        except Exception as e:
            logger.error(f"Error closing InfluxDB client: {str(e)}")

influx_metrics = InfluxDBMetrics()
system_metrics = SystemMetrics(influx_metrics)

async def collect_and_record_metrics():
    try:
        system_metrics.collect_system_metrics()
        system_metrics.collect_process_metrics()
        
        logger.info("System metrics collected successfully")
        
    except Exception as e:
        logger.error(f"Error in metrics collection: {str(e)}")

def setup_metrics_collection():
    if settings.MONITORING_ENABLED:
        
        def metrics_loop():
            while True:
                try:
                    asyncio.run(collect_and_record_metrics())
                    time.sleep(settings.METRICS_COLLECTION_INTERVAL)
                except Exception as e:
                    logger.error(f"Metrics collection error: {str(e)}")
                    time.sleep(60)
        
        thread = threading.Thread(target=metrics_loop, daemon=True)
        thread.start()
        logger.info("Metrics collection thread started")

def record_api_request_metric(
    endpoint: str,
    method: str,
    status_code: int,
    response_time_ms: float,
    user_id: Optional[str] = None,
    request_size: Optional[int] = None,
    response_size: Optional[int] = None,
    error_message: Optional[str] = None
):
    try:
        return influx_metrics.record_api_request(
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            response_time_ms=response_time_ms,
            user_id=user_id,
            request_size=request_size,
            response_size=response_size,
            error_message=error_message
        )
    except Exception as e:
        logger.debug(f"Error recording API request metric: {str(e)}")
        return False

def record_database_metric(
    operation: str,
    table: str,
    duration_ms: float,
    success: bool = True,
    error_message: Optional[str] = None
):
    try:
        return influx_metrics.record_database_query(
            operation=operation,
            table=table,
            duration_ms=duration_ms,
            success=success,
            error_message=error_message
        )
    except Exception as e:
        logger.debug(f"Error recording database metric: {str(e)}")
        return False