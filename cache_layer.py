# cache_layer.py
import asyncio
import logging
import json
from typing import Dict, Any, Optional
from redis.asyncio import Redis
from redis.asyncio.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

logger = logging.getLogger("bot.cache_layer")

class CacheLayer:
    """
    Optional Fully-Async Redis Cache Layer.
    DB reads ko reduce karta hai।
    """
    
    CACHE_KEY_MOVIE = "movie:{imdb_id}"
    CACHE_EXPIRY_MOVIE = 3600 * 24 * 7 # 7 days
    
    def __init__(self, redis_url: str | None):
        self._redis_url = redis_url
        self._redis: Optional[Redis] = None
        self.is_active = False

    async def connect(self):
        """Redis se connection pool establish karta hai।"""
        if not self._redis_url:
            logger.info("Redis URL set nahi hai. CacheLayer INACTIVE rahega।")
            return
            
        try:
            # redis-py async client use karein
            self._redis = Redis.from_url(
                self._redis_url, 
                decode_responses=True, 
                socket_timeout=5, 
                socket_connect_timeout=5
            )
            # Connection check ke liye ek simple command chalaayein
            await self._redis.ping()
            self.is_active = True
            logger.info("✅ Redis CacheLayer active aur connected hai।")
        except RedisConnectionError as e:
            logger.error(f"❌ Redis connection fail: {e}. CacheLayer INACTIVE rahega।", exc_info=False)
            self._redis = None
        except Exception as e:
            logger.error(f"❌ Anjaani Redis connection error: {e}. CacheLayer INACTIVE।", exc_info=True)
            self._redis = None

    async def close(self):
        """Redis connection close karta hai।"""
        if self._redis:
            try:
                await self._redis.close()
                logger.info("Redis client connection close ho gaya।")
            except Exception as e:
                logger.error(f"Redis close karte waqt error: {e}")
        self.is_active = False
        self._redis = None

    async def _safe_redis_call(self, method: str, *args, **kwargs) -> Any:
        """
        Redis API calls ke liye wrapper jo fail hone par DB bypass/fallback karega।
        """
        if not self.is_active or self._redis is None:
            return None # Fallback to DB
            
        try:
            return await asyncio.wait_for(getattr(self._redis, method)(*args, **kwargs), timeout=5)
        except RedisConnectionError:
            logger.error(f"Redis connection lost during call: {method}. Deactivating cache।")
            self.is_active = False # Deactivate so it bypasses on next call
            return None
        except (RedisTimeoutError, asyncio.TimeoutError):
            logger.warning(f"Redis call timeout: {method}")
            return None
        except Exception as e:
            logger.error(f"Redis call error in {method}: {e}", exc_info=True)
            return None

    # --- Cache Logic ---

    async def get_movie(self, imdb_id: str) -> Optional[Dict]:
        """Cache se movie data nikalta hai।"""
        key = self.CACHE_KEY_MOVIE.format(imdb_id=imdb_id)
        data = await self._safe_redis_call("get", key)
        
        if data:
            try:
                movie_dict = json.loads(data)
                logger.debug(f"Cache HIT: Movie {imdb_id}")
                return movie_dict
            except json.JSONDecodeError:
                logger.error(f"Redis data parse error for {imdb_id}. Deleting corrupt entry।")
                await self.delete_movie(imdb_id)
                return None
        
        logger.debug(f"Cache MISS: Movie {imdb_id}")
        return None

    async def set_movie(self, imdb_id: str, movie_data: Dict) -> bool:
        """Movie data ko cache mein save karta hai।"""
        if not self.is_active or not self._redis: return False
        
        key = self.CACHE_KEY_MOVIE.format(imdb_id=imdb_id)
        
        # Ensure only JSON-serializable types are stored (Mongo BSON objects hata dein)
        data_to_store = {k: v for k, v in movie_data.items() if not k.startswith('_')}

        try:
            # Pipelined set: Set data aur set expiry
            pipeline = self._redis.pipeline()
            pipeline.set(key, json.dumps(data_to_store))
            pipeline.expire(key, self.CACHE_EXPIRY_MOVIE)
            await self._safe_redis_call("execute", pipeline)
            logger.debug(f"Cache SET: Movie {imdb_id} (Expiry {self.CACHE_EXPIRY_MOVIE}s)")
            return True
        except Exception as e:
            logger.error(f"Redis SET fail for {imdb_id}: {e}")
            return False

    async def delete_movie(self, imdb_id: str) -> bool:
        """Cache se movie entry hatata hai।"""
        if not self.is_active or not self._redis: return False
        key = self.CACHE_KEY_MOVIE.format(imdb_id=imdb_id)
        return await self._safe_redis_call("delete", key)

