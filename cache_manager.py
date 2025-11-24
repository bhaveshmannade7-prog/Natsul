# cache_manager.py
import asyncio
import logging
from typing import Optional, Any
import json
import os

try:
    import redis.asyncio as redis
except ImportError:
    logging.critical("--- redis.asyncio library nahi mili! ---")
    logging.critical("Kripya install karein: pip install redis")
    raise SystemExit("Missing dependency: redis")

logger = logging.getLogger("bot.cache_manager")

class CacheManager:
    """
    Async Redis client ko manage karta hai। Agar REDIS_URL missing hai toh gracefully fail karta hai।
    """
    
    def __init__(self, redis_url: Optional[str]):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None
        self.is_ready = False
        
        if not self.redis_url:
            logger.warning("CacheManager: REDIS_URL environment mein nahi mila. Cache disable hai।")

    async def init_cache(self):
        """Redis connection pool shuru karta hai।"""
        if not self.redis_url:
            self.is_ready = False
            return

        try:
            self.client = redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
            # Connection check
            await self.client.ping()
            self.is_ready = True
            logger.info("CacheManager: Redis connection pool safal. Cache active hai।")
        except Exception as e:
            self.client = None
            self.is_ready = False
            logger.error(f"CacheManager: Redis connection fail ho gaya: {e}. Cache disable hai।", exc_info=False)

    async def get_json(self, key: str) -> Optional[Any]:
        """Key se JSON data nikalta hai।"""
        if not self.is_ready: return None
        try:
            data = await self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"CacheManager GET error for key {key}: {e}", exc_info=False)
            return None

    async def set_json(self, key: str, value: Any, ttl: int = 3600):
        """JSON data ko cache mein store karta hai।"""
        if not self.is_ready: return
        try:
            data = json.dumps(value)
            await self.client.set(key, data, ex=ttl)
        except Exception as e:
            logger.error(f"CacheManager SET error for key {key}: {e}", exc_info=False)

    async def close(self):
        """Connection pool close karta hai।"""
        if self.client and self.is_ready:
            try:
                await self.client.close()
                logger.info("CacheManager: Redis pool close ho gaya।")
            except Exception as e:
                logger.error(f"CacheManager pool close error: {e}")
        self.is_ready = False

