# async-energy-pipeline

## Overview
This project is a backend system for ingesting, processing, and monitoring smart home energy consumption data. It demonstrates asynchronous processing, autoscaling, load balancing, caching, and monitoring.

## Architecture
- API: FastAPI for data ingestion
- Queue: Redis for async task queue
- Workers: Celery for processing tasks asynchronously
- Database: PostgreSQL for raw and aggregated data
- Metrics: InfluxDB for monitoring system metrics
- Cache: Redis for caching recent results
- Load Balancer: Nginx for distributing API requests
- Monitoring: Grafana for dashboards and visualizations
- Containerization: Docker / Docker Compose
- Autoscaling: Docker Swarm 

## Features
- Stateless API for easy horizontal scaling
- Async task processing via Celery and Redis
- Aggregation of energy, temperature, humidity, and light data
- Caching of latest readings in Redis
- Metrics tracking with InfluxDB
- Visual dashboards with Grafana
- Autoscaling of workers based on queue length or CPU usage
- Retry and dead-letter queue handling for failed tasks

## Data
The system uses the Smart Home Energy Consumption Dataset:
- Timestamped readings of energy usage, temperature, humidity, and light
- Each device is identified by a unique device_id
- Dataset can be found on Kaggle: Smart Home Dataset with Weather Information

## Usage
1. Start services using Docker Compose or Swarm
2. Send POST requests to `/data/` with device readings
3. Workers process data asynchronously and store in PostgreSQL and InfluxDB
4. Latest readings are cached in Redis
5. Metrics can be visualized in Grafana dashboards
6. API endpoints allow querying raw and aggregated data

## Example Data
```json
{
  "device_id": "home_12",
  "timestamp": "2025-10-27T10:00:00Z",
  "temperature": 21.3,
  "humidity": 43,
  "light": 120,
  "energy_usage": 0.34
}
