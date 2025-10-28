from celery import current_app
from config.settings import settings
import asyncpg
import asyncio
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

try:
    from config.celery import app as celery_app
except ImportError:
    celery_app = current_app

@celery_app.task(bind=True, max_retries=3)
def process_energy_reading(self, reading_data):
    start_time = time.time()
    
    try:
        logger.info(f"Processing energy reading for device: {reading_data.get('device_id', 'unknown')}")
        
        result = asyncio.run(insert_reading_to_db(reading_data))
        
        processing_time = (time.time() - start_time) * 1000
        logger.info(f"Successfully processed reading in {processing_time:.2f}ms")
        
        return {
            "status": "success", 
            "message": "Reading processed successfully",
            "device_id": reading_data.get('device_id'),
            "processing_time_ms": processing_time
        }
        
    except Exception as exc:
        processing_time = (time.time() - start_time) * 1000
        logger.error(f"Error processing reading: {str(exc)}")
        
        if self.request.retries < self.max_retries:
            countdown = min(60 * (2 ** self.request.retries), 300)
            logger.warning(f"Retrying in {countdown} seconds (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc, countdown=countdown)
        
        return {
            "status": "failed", 
            "message": str(exc),
            "device_id": reading_data.get('device_id'),
            "processing_time_ms": processing_time
        }

@celery_app.task(bind=True, max_retries=3)
def batch_process_readings(self, readings_data):
    start_time = time.time()
    
    try:
        logger.info(f"Processing batch of {len(readings_data)} readings")
        
        successful_count = 0
        failed_count = 0
        for reading in readings_data:
            try:
                asyncio.run(insert_reading_to_db(reading))
                successful_count += 1
            except Exception as e:
                logger.error(f"Failed to process reading in batch: {str(e)}")
                failed_count += 1
        
        processing_time = (time.time() - start_time) * 1000
        
        logger.info(f"Batch processing complete: {successful_count} successful, {failed_count} failed")
        
        return {
            "status": "success", 
            "count": len(readings_data),
            "successful": successful_count,
            "failed": failed_count,
            "processing_time_ms": processing_time
        }
        
    except Exception as exc:
        processing_time = (time.time() - start_time) * 1000
        logger.error(f"Error processing batch: {str(exc)}")
        
        if self.request.retries < self.max_retries:
            countdown = min(60 * (2 ** self.request.retries), 300)
            logger.warning(f"Retrying batch in {countdown} seconds")
            raise self.retry(exc=exc, countdown=countdown)
        
        return {
            "status": "failed", 
            "message": str(exc),
            "count": len(readings_data),
            "processing_time_ms": processing_time
        }

@celery_app.task
def aggregate_energy_data(home_id=None, date_range=None):
    try:
        logger.info(f"Aggregating energy data for home: {home_id}")
        
        aggregated_count = asyncio.run(perform_aggregation(home_id, date_range))
        
        return {
            "status": "success",
            "home_id": home_id,
            "aggregated_records": aggregated_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in aggregation: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "home_id": home_id
        }

@celery_app.task
def cleanup_old_data(retention_days=90):
    try:
        logger.info(f"Starting cleanup of data older than {retention_days} days")
        
        deleted_count = asyncio.run(perform_cleanup(retention_days))
        
        return {
            "status": "success",
            "deleted_records": deleted_count,
            "retention_days": retention_days
        }
        
    except Exception as e:
        logger.error(f"Error in cleanup: {str(e)}")
        return {"status": "error", "message": str(e)}

@celery_app.task
def health_check():
    try:
        asyncio.run(test_database_connection())
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "worker": "celery"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy", 
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@celery_app.task
def system_stats():
    try:
        import psutil
        
        stats = {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return {"status": "ok", "stats": stats}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def insert_reading_to_db(reading_data):
    conn = None
    try:
        conn = await asyncpg.connect(settings.DATABASE_URL)
        
        await conn.execute("""
            INSERT INTO device_readings (
                home_id, appliance_type, energy_consumption, 
                timestamp, date, outdoor_temperature, season, household_size
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, 
            reading_data.get('device_id', 'unknown'),
            'smart_device',
            reading_data.get('energy_kwh', 0.0),
            reading_data.get('timestamp', datetime.utcnow()),
            datetime.fromisoformat(reading_data.get('timestamp', datetime.utcnow().isoformat())).date(),
            None,
            None,
            None
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Database insertion failed: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

async def perform_aggregation(home_id=None, date_range=None):
    conn = None
    try:
        conn = await asyncpg.connect(settings.DATABASE_URL)
        
        if home_id:
            result = await conn.fetchval(
                "SELECT COUNT(*) FROM device_readings WHERE home_id = $1", 
                str(home_id)
            )
        else:
            result = await conn.fetchval("SELECT COUNT(*) FROM device_readings")
            
        return result or 0
        
    except Exception as e:
        logger.error(f"Aggregation failed: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

async def perform_cleanup(retention_days):
    conn = None
    try:
        conn = await asyncpg.connect(settings.DATABASE_URL)
        
        cutoff_date = datetime.utcnow().date() - timedelta(days=retention_days)
        
        result = await conn.execute(
            "DELETE FROM device_readings WHERE date < $1",
            cutoff_date
        )
        
        deleted_count = int(result.split()[-1]) if result.split()[-1].isdigit() else 0
        return deleted_count
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

async def test_database_connection():
    conn = None
    try:
        conn = await asyncpg.connect(settings.DATABASE_URL)
        await conn.fetchval("SELECT 1")
        
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()