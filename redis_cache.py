# redis_cache.py
import logging
import asyncio
from typing import List, Dict, Any, Optional

try:
    import json
    import redis.asyncio as redis
except ImportError:
    # If redis/aioredis is not installed, the class will still initialize
    # but all methods will return failure (safe fallback).
    redis = None
    json = None

logger = logging.getLogger("bot.redis_cache")

class SearchCache:
    """
    Async Redis client for caching search results. 
    It is safe and optional; if connection fails, it silently falls back.
    """
    def __init__(self, redis_url: Optional[str]):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None
        self.is_ready = False
        self.lock = asyncio.Lock()

    async def init_cache(self):
        """Initialize the Redis connection."""
        if not redis or not self.redis_url:
            logger.warning("Redis not installed or REDIS_URL missing. Search cache is disabled.")
            return

        async with self.lock:
            try:
                self.client = redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
                # PING to check connection
                await self.client.ping()
                self.is_ready = True
                logger.info("✅ Async Redis client connected and ready for search caching.")
            except Exception as e:
                self.client = None
                self.is_ready = False
                logger.error(f"❌ Redis connection failed: {e}. Search cache is disabled.", exc_info=False)

    async def close(self):
        """Close the Redis connection."""
        if self.client:
            try:
                await self.client.close()
                self.client = None
                self.is_ready = False
                logger.info("Redis client connection closed.")
            except Exception as e:
                logger.error(f"Error closing Redis client: {e}")

    async def get_search_results(self, query_key: str) -> Optional[List[Dict[str, Any]]]:
        """Retrieve cached search results."""
        if not self.is_ready: return None

        try:
            cached_json = await self.client.get(query_key)
            if cached_json and json:
                results = json.loads(cached_json)
                logger.info(f"Cache HIT for query: {query_key}")
                return results
            
            logger.debug(f"Cache MISS for query: {query_key}")
            return None
        except Exception as e:
            logger.error(f"Redis GET failed: {e}. Falling back.", exc_info=False)
            # On failure, disable readiness for subsequent calls
            self.is_ready = False
            return None

    async def set_search_results(self, query_key: str, results: List[Dict[str, Any]], ttl_seconds: int = 3600):
        """Cache search results with a Time-To-Live."""
        if not self.is_ready: return

        try:
            if json:
                await self.client.set(query_key, json.dumps(results), ex=ttl_seconds)
                logger.info(f"Cache SET successful for query: {query_key}")
        except Exception as e:
            logger.error(f"Redis SET failed: {e}. Falling back.", exc_info=False)
            self.is_ready = False
