from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from typing import Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field, validator
import asyncpg
import os
from app.tasks import process_energy_reading, batch_process_readings

router = APIRouter()

class EnergyReading(BaseModel):
    device_id: str = Field(..., description="Unique device identifier")
    timestamp: datetime = Field(..., description="Reading timestamp")
    energy_kwh: float = Field(..., ge=0, description="Energy consumption in kWh")
    power_kw: Optional[float] = Field(None, ge=0, description="Power in kW")
    voltage_v: Optional[float] = Field(None, ge=0, description="Voltage in V")
    current_a: Optional[float] = Field(None, ge=0, description="Current in A")
    location: Optional[str] = Field(None, description="Device location")

class BatchEnergyReadings(BaseModel):
    readings: List[EnergyReading] = Field(..., max_items=1000)
    
class DataIngestionResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None
    count: Optional[int] = None

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://energy_user:energy_pass@postgres:5432/energy_db"
)

async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

@router.get("/energy-readings")
async def get_energy_readings(
    home_id: Optional[int] = Query(None, description="Filter by home ID"),
    appliance_type: Optional[str] = Query(None, description="Filter by appliance type"),
    season: Optional[str] = Query(None, description="Filter by season"),
    household_size: Optional[int] = Query(None, description="Filter by household size"),
    min_energy: Optional[float] = Query(None, description="Minimum energy consumption"),
    max_energy: Optional[float] = Query(None, description="Maximum energy consumption"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(100, description="Number of records to return"),
    offset: Optional[int] = Query(0, description="Number of records to skip")
):
    try:
        conn = await get_db_connection()
        
        query = "SELECT * FROM device_readings WHERE 1=1"
        params = []
        param_count = 0
        
        if home_id:
            param_count += 1
            query += f" AND home_id = ${param_count}"
            params.append(home_id)
            
        if appliance_type:
            param_count += 1
            query += f" AND appliance_type = ${param_count}"
            params.append(appliance_type)
            
        if season:
            param_count += 1
            query += f" AND season = ${param_count}"
            params.append(season)
            
        if household_size:
            param_count += 1
            query += f" AND household_size = ${param_count}"
            params.append(household_size)
            
        if min_energy:
            param_count += 1
            query += f" AND energy_consumption >= ${param_count}"
            params.append(min_energy)
            
        if max_energy:
            param_count += 1
            query += f" AND energy_consumption <= ${param_count}"
            params.append(max_energy)
            
        if start_date:
            param_count += 1
            query += f" AND date >= ${param_count}"
            params.append(start_date)
            
        if end_date:
            param_count += 1
            query += f" AND date <= ${param_count}"
            params.append(end_date)
        
        query += " ORDER BY date DESC, timestamp DESC"
        
        if limit:
            param_count += 1
            query += f" LIMIT ${param_count}"
            params.append(limit)
            
        if offset:
            param_count += 1
            query += f" OFFSET ${param_count}"
            params.append(offset)
        
        rows = await conn.fetch(query, *params)
        await conn.close()
        
        return {
            "total": len(rows),
            "limit": limit,
            "offset": offset,
            "data": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/energy-summary")
async def get_energy_summary(
    home_id: Optional[int] = Query(None, description="Filter by home ID"),
    appliance_type: Optional[str] = Query(None, description="Filter by appliance type"),
    season: Optional[str] = Query(None, description="Filter by season")
):
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            COUNT(*) as total_readings,
            AVG(energy_consumption) as avg_consumption,
            MIN(energy_consumption) as min_consumption,
            MAX(energy_consumption) as max_consumption,
            SUM(energy_consumption) as total_consumption,
            AVG(outdoor_temperature) as avg_temperature,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM device_readings WHERE 1=1
        """
        params = []
        param_count = 0
        
        if home_id:
            param_count += 1
            query += f" AND home_id = ${param_count}"
            params.append(home_id)
            
        if appliance_type:
            param_count += 1
            query += f" AND appliance_type = ${param_count}"
            params.append(appliance_type)
            
        if season:
            param_count += 1
            query += f" AND season = ${param_count}"
            params.append(season)
        
        row = await conn.fetchrow(query, *params)
        await conn.close()
        
        return dict(row)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/appliance-types")
async def get_appliance_types():
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            appliance_type,
            COUNT(*) as reading_count,
            AVG(energy_consumption) as avg_consumption,
            COUNT(DISTINCT home_id) as home_count
        FROM device_readings 
        GROUP BY appliance_type 
        ORDER BY avg_consumption DESC
        """
        
        rows = await conn.fetch(query)
        await conn.close()
        
        return {
            "appliance_types": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/homes")
async def get_homes():
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            home_id,
            household_size,
            COUNT(*) as reading_count,
            AVG(energy_consumption) as avg_consumption,
            COUNT(DISTINCT appliance_type) as appliance_count
        FROM device_readings 
        GROUP BY home_id, household_size 
        ORDER BY home_id
        """
        
        rows = await conn.fetch(query)
        await conn.close()
        
        return {
            "homes": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/consumption-by-season")
async def get_consumption_by_season():
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            season,
            COUNT(*) as reading_count,
            AVG(energy_consumption) as avg_consumption,
            SUM(energy_consumption) as total_consumption,
            AVG(outdoor_temperature) as avg_temperature
        FROM device_readings 
        GROUP BY season 
        ORDER BY avg_consumption DESC
        """
        
        rows = await conn.fetch(query)
        await conn.close()
        
        return {
            "seasonal_data": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/consumption-by-appliance")
async def get_consumption_by_appliance(
    season: Optional[str] = Query(None, description="Filter by season")
):
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            appliance_type,
            season,
            COUNT(*) as reading_count,
            AVG(energy_consumption) as avg_consumption,
            SUM(energy_consumption) as total_consumption
        FROM device_readings
        """
        params = []
        param_count = 0
        
        if season:
            query += " WHERE season = $1"
            params.append(season)
            param_count = 1
        
        query += " GROUP BY appliance_type, season ORDER BY avg_consumption DESC"
        
        rows = await conn.fetch(query, *params)
        await conn.close()
        
        return {
            "appliance_consumption": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/daily-consumption")
async def get_daily_consumption(
    home_id: Optional[int] = Query(None, description="Filter by home ID"),
    days: Optional[int] = Query(7, description="Number of days to include")
):
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            date,
            COUNT(*) as reading_count,
            AVG(energy_consumption) as avg_consumption,
            SUM(energy_consumption) as total_consumption,
            AVG(outdoor_temperature) as avg_temperature
        FROM device_readings 
        WHERE 1=1
        """
        params = []
        param_count = 0
        
        if home_id:
            param_count += 1
            query += f" AND home_id = ${param_count}"
            params.append(home_id)
        
        query += " GROUP BY date ORDER BY date DESC"
        
        if days:
            param_count += 1
            query += f" LIMIT ${param_count}"
            params.append(days)
        
        rows = await conn.fetch(query, *params)
        await conn.close()
        
        return {
            "daily_consumption": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/high-consumption")
async def get_high_consumption_readings(
    threshold: Optional[float] = Query(1.0, description="Energy consumption threshold"),
    limit: Optional[int] = Query(50, description="Number of records to return")
):
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT *
        FROM device_readings 
        WHERE energy_consumption >= $1
        ORDER BY energy_consumption DESC
        LIMIT $2
        """
        
        rows = await conn.fetch(query, threshold, limit)
        await conn.close()
        
        return {
            "high_consumption_readings": [dict(row) for row in rows],
            "threshold": threshold,
            "count": len(rows)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/temperature-analysis")
async def get_temperature_analysis():
    try:
        conn = await get_db_connection()
        
        query = """
        SELECT 
            CASE 
                WHEN outdoor_temperature < 0 THEN 'Very Cold'
                WHEN outdoor_temperature >= 0 AND outdoor_temperature < 10 THEN 'Cold'
                WHEN outdoor_temperature >= 10 AND outdoor_temperature < 20 THEN 'Mild'
                WHEN outdoor_temperature >= 20 AND outdoor_temperature < 30 THEN 'Warm'
                ELSE 'Hot'
            END as temperature_range,
            COUNT(*) as reading_count,
            AVG(energy_consumption) as avg_consumption,
            AVG(outdoor_temperature) as avg_temperature
        FROM device_readings 
        GROUP BY 
            CASE 
                WHEN outdoor_temperature < 0 THEN 'Very Cold'
                WHEN outdoor_temperature >= 0 AND outdoor_temperature < 10 THEN 'Cold'
                WHEN outdoor_temperature >= 10 AND outdoor_temperature < 20 THEN 'Mild'
                WHEN outdoor_temperature >= 20 AND outdoor_temperature < 30 THEN 'Warm'
                ELSE 'Hot'
            END
        ORDER BY avg_consumption DESC
        """
        
        rows = await conn.fetch(query)
        await conn.close()
        
        return {
            "temperature_analysis": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats")
async def get_database_stats():
    try:
        conn = await get_db_connection()
        
        stats_query = """
        SELECT 
            COUNT(*) as total_readings,
            COUNT(DISTINCT home_id) as unique_homes,
            COUNT(DISTINCT appliance_type) as unique_appliances,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(energy_consumption) as overall_avg_consumption
        FROM device_readings
        """
        
        stats = await conn.fetchrow(stats_query)
        await conn.close()
        
        return {
            "database_stats": dict(stats)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data/", response_model=DataIngestionResponse)
async def ingest_energy_data(
    reading: EnergyReading,
    background_tasks: BackgroundTasks
):
    try:
        task = process_energy_reading.delay(reading.dict())
        
        return DataIngestionResponse(
            status="accepted",
            message="Energy reading queued for processing",
            task_id=task.id
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue reading: {str(e)}")

@router.post("/data/batch/", response_model=DataIngestionResponse)
async def ingest_batch_energy_data(
    batch: BatchEnergyReadings,
    background_tasks: BackgroundTasks
):
    try:
        readings_data = [reading.dict() for reading in batch.readings]
        task = batch_process_readings.delay(readings_data)
        
        return DataIngestionResponse(
            status="accepted",
            message="Batch readings queued for processing",
            task_id=task.id,
            count=len(batch.readings)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue batch: {str(e)}")

@router.get("/health")
async def health_check():
    try:
        conn = await get_db_connection()
        await conn.fetchval("SELECT 1")
        await conn.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow(),
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")