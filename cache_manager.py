import logging
import json
import os
import asyncio
from typing import Optional, Any, Dict, List

logger = logging.getLogger("bot.cache_manager")

class RedisCache:
    def __init__(self):
        self.redis = None
        self.enabled = False
        self.ttl = 3600  # 1 Hour Cache

    async def connect(self, redis_url: str):
        if not redis_url:
            logger.info("Redis URL missing. Caching disabled.")
            return

        try:
            import redis.asyncio as redis
            self.redis = redis.from_url(redis_url, decode_responses=True)
            await self.redis.ping()
            self.enabled = True
            logger.info("✅ Redis Cache Connected Successfully.")
        except ImportError:
            logger.critical("Redis library not installed. Install via: pip install redis")
            self.enabled = False
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}. Running without cache.")
            self.enabled = False

    async def close(self):
        if self.redis:
            await self.redis.close()
            logger.info("Redis connection closed.")

    def _get_key(self, query: str) -> str:
        # Normalize key
        clean = "".join(e for e in query if e.isalnum()).lower()
        return f"search:v5:{clean}"

    async def get_search_results(self, query: str) -> Optional[List[Dict]]:
        """Fetch results from Redis."""
        if not self.enabled or not self.redis:
            return None
        
        try:
            key = self._get_key(query)
            data = await self.redis.get(key)
            if data:
                # Reset TTL on hit (Slide expiry)
                await self.redis.expire(key, self.ttl)
                return json.loads(data)
        except Exception as e:
            logger.error(f"Redis Read Error: {e}")
            return None
        return None

    async def set_search_results(self, query: str, results: List[Dict]):
        """Save results to Redis."""
        if not self.enabled or not self.redis or not results:
            return

        try:
            key = self._get_key(query)
            # Only cache minimal data to save RAM
            minimal_results = [
                {
                    'imdb_id': r.get('imdb_id'),
                    'title': r.get('title'),
                    'year': r.get('year'),
                    'score': r.get('score'),
                    'match_type': 'cached'
                } for r in results
            ]
            await self.redis.setex(key, self.ttl, json.dumps(minimal_results))
        except Exception as e:
            logger.error(f"Redis Write Error: {e}")
