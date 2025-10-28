import redis
import json
import asyncio
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from config.settings import settings
import concurrent.futures
import threading

logger = logging.getLogger(__name__)


class RedisCache:
    
    def __init__(self):
        self._sync_client = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix="redis")
    
    def get_sync_client(self):
        if self._sync_client is None:
            try:
                self._sync_client = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=settings.REDIS_DB,
                    password=settings.REDIS_PASSWORD,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
                self._sync_client.ping()
            except Exception as e:
                logger.error(f"Failed to create Redis client: {str(e)}")
                raise
        return self._sync_client
    
    async def _run_in_executor(self, func, *args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, func, *args, **kwargs)
    
    async def set(
        self, 
        key: str, 
        value: Any, 
        ttl: Optional[int] = None,
        serialize: bool = True
    ) -> bool:
        try:
            def _set():
                client = self.get_sync_client()
                
                if serialize and not isinstance(value, str):
                    serialized_value = json.dumps(value, default=str)
                else:
                    serialized_value = value
                
                ttl_value = ttl or settings.CACHE_TTL_DEFAULT
                result = client.setex(key, ttl_value, serialized_value)
                return result is True
            
            return await self._run_in_executor(_set)
            
        except Exception as e:
            logger.error(f"Redis set error for key {key}: {str(e)}")
            return False
    
    async def get(
        self, 
        key: str, 
        deserialize: bool = True,
        default: Any = None
    ) -> Any:
        try:
            def _get():
                client = self.get_sync_client()
                value = client.get(key)
                
                if value is None:
                    return default
                
                if deserialize:
                    try:
                        return json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        return value
                
                return value
            
            return await self._run_in_executor(_get)
            
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {str(e)}")
            return default
    
    async def delete(self, *keys: str) -> int:
        try:
            def _delete():
                client = self.get_sync_client()
                return client.delete(*keys)
            
            return await self._run_in_executor(_delete)
            
        except Exception as e:
            logger.error(f"Redis delete error for keys {keys}: {str(e)}")
            return 0
    
    async def exists(self, key: str) -> bool:
        try:
            def _exists():
                client = self.get_sync_client()
                result = client.exists(key)
                return result == 1
            
            return await self._run_in_executor(_exists)
            
        except Exception as e:
            logger.error(f"Redis exists error for key {key}: {str(e)}")
            return False
    
    async def hset(
        self, 
        name: str, 
        key: str, 
        value: Any,
        serialize: bool = True
    ) -> bool:
        try:
            def _hset():
                client = self.get_sync_client()
                
                if serialize and not isinstance(value, str):
                    serialized_value = json.dumps(value, default=str)
                else:
                    serialized_value = value
                
                result = client.hset(name, key, serialized_value)
                return result is not None
            
            return await self._run_in_executor(_hset)
            
        except Exception as e:
            logger.error(f"Redis hset error for {name}:{key}: {str(e)}")
            return False
    
    async def hget(
        self, 
        name: str, 
        key: str,
        deserialize: bool = True,
        default: Any = None
    ) -> Any:
        try:
            def _hget():
                client = self.get_sync_client()
                value = client.hget(name, key)
                
                if value is None:
                    return default
                
                if deserialize:
                    try:
                        return json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        return value
                
                return value
            
            return await self._run_in_executor(_hget)
            
        except Exception as e:
            logger.error(f"Redis hget error for {name}:{key}: {str(e)}")
            return default
    
    async def hgetall(
        self, 
        name: str,
        deserialize: bool = True
    ) -> Dict[str, Any]:
        try:
            def _hgetall():
                client = self.get_sync_client()
                data = client.hgetall(name)
                
                if not data:
                    return {}
                
                if deserialize:
                    result = {}
                    for k, v in data.items():
                        try:
                            result[k] = json.loads(v)
                        except (json.JSONDecodeError, TypeError):
                            result[k] = v
                    return result
                
                return data
            
            return await self._run_in_executor(_hgetall)
            
        except Exception as e:
            logger.error(f"Redis hgetall error for {name}: {str(e)}")
            return {}
    
    async def expire(self, key: str, ttl: int) -> bool:
        try:
            def _expire():
                client = self.get_sync_client()
                result = client.expire(key, ttl)
                return result is True
            
            return await self._run_in_executor(_expire)
            
        except Exception as e:
            logger.error(f"Redis expire error for key {key}: {str(e)}")
            return False
    
    async def ttl(self, key: str) -> int:
        try:
            def _ttl():
                client = self.get_sync_client()
                return client.ttl(key)
            
            return await self._run_in_executor(_ttl)
            
        except Exception as e:
            logger.error(f"Redis TTL error for key {key}: {str(e)}")
            return -1
    
    async def ping(self) -> bool:
        try:
            def _ping():
                client = self.get_sync_client()
                result = client.ping()
                return result is True
            
            return await self._run_in_executor(_ping)
            
        except Exception as e:
            logger.error(f"Redis ping error: {str(e)}")
            return False
    
    async def scan_iter(self, match: str = "*", count: int = 1000):
        try:
            def _get_keys():
                client = self.get_sync_client()
                return list(client.scan_iter(match=match, count=count))
            
            keys = await self._run_in_executor(_get_keys)
            for key in keys:
                yield key
                
        except Exception as e:
            logger.error(f"Redis scan error for pattern {match}: {str(e)}")
            return
    
    async def info(self) -> Dict[str, Any]:
        try:
            def _info():
                client = self.get_sync_client()
                return client.info()
            
            return await self._run_in_executor(_info)
            
        except Exception as e:
            logger.error(f"Redis info error: {str(e)}")
            return {}
    
    async def close(self):
        try:
            if self._sync_client:
                def _close():
                    self._sync_client.close()
                await self._run_in_executor(_close)
            
            if self._executor:
                self._executor.shutdown(wait=True)
                
        except Exception as e:
            logger.error(f"Redis close error: {str(e)}")


class EnergyDataCache:
    
    def __init__(self):
        self.cache = RedisCache()
    
    async def cache_latest_reading(
        self, 
        device_id: str, 
        reading: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        key = f"latest_reading:{device_id}"
        ttl = ttl or settings.CACHE_TTL_LATEST_READINGS
        
        success = await self.cache.set(key, reading, ttl)
        
        if success and 'location' in reading:
            location_key = f"latest_by_location:{reading['location']}"
            await self.cache.hset(location_key, device_id, reading)
            await self.cache.expire(location_key, ttl)
        
        return success
    
    async def get_latest_reading(
        self, 
        device_id: str
    ) -> Optional[Dict[str, Any]]:
        key = f"latest_reading:{device_id}"
        return await self.cache.get(key)
    
    async def get_latest_readings_by_location(
        self, 
        location: str
    ) -> Dict[str, Any]:
        key = f"latest_by_location:{location}"
        return await self.cache.hgetall(key)
    
    async def cache_aggregated_data(
        self,
        period_type: str,
        period: str,
        data: List[Dict[str, Any]],
        ttl: Optional[int] = None
    ) -> bool:
        key = f"aggregates:{period_type}:{period}"
        ttl = ttl or settings.CACHE_TTL_AGGREGATES
        
        return await self.cache.set(key, data, ttl)
    
    async def get_aggregated_data(
        self,
        period_type: str,
        period: str
    ) -> Optional[List[Dict[str, Any]]]:
        key = f"aggregates:{period_type}:{period}"
        return await self.cache.get(key)
    
    async def cache_query_result(
        self,
        query_hash: str,
        result: Any,
        ttl: Optional[int] = None
    ) -> bool:
        key = f"query:{query_hash}"
        ttl = ttl or settings.CACHE_TTL_DEFAULT
        
        return await self.cache.set(key, result, ttl)
    
    async def get_query_result(self, query_hash: str) -> Any:
        key = f"query:{query_hash}"
        return await self.cache.get(key)
    
    async def invalidate_location_cache(self, location: str) -> int:
        keys_to_delete = [
            f"latest_by_location:{location}",
        ]
        
        deleted_count = 0
        
        for key in keys_to_delete:
            deleted_count += await self.cache.delete(key)
        
        pattern = f"aggregates:*:{location}*"
        keys = []
        async for key in self.cache.scan_iter(match=pattern):
            keys.append(key)
        
        if keys:
            deleted_count += await self.cache.delete(*keys)
        
        return deleted_count
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        try:
            info = await self.cache.info()
            
            latest_readings_count = 0
            aggregates_count = 0
            query_cache_count = 0
            
            async for key in self.cache.scan_iter():
                if key.startswith('latest_reading:'):
                    latest_readings_count += 1
                elif key.startswith('aggregates:'):
                    aggregates_count += 1
                elif key.startswith('query:'):
                    query_cache_count += 1
            
            return {
                'redis_info': {
                    'connected_clients': info.get('connected_clients', 0),
                    'used_memory_human': info.get('used_memory_human', '0B'),
                    'keyspace_hits': info.get('keyspace_hits', 0),
                    'keyspace_misses': info.get('keyspace_misses', 0),
                },
                'cache_counts': {
                    'latest_readings': latest_readings_count,
                    'aggregates': aggregates_count,
                    'query_results': query_cache_count,
                },
                'hit_ratio': self._calculate_hit_ratio(
                    info.get('keyspace_hits', 0),
                    info.get('keyspace_misses', 0)
                )
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {}
    
    def _calculate_hit_ratio(self, hits: int, misses: int) -> float:
        total = hits + misses
        if total == 0:
            return 0.0
        return round(hits / total * 100, 2)


cache = EnergyDataCache()