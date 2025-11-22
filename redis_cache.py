# redis_cache.py
import logging
import asyncio
import json
from typing import Any, Dict, List, Literal
from datetime import timedelta
from redis.asyncio import Redis, ConnectionPool
from redis.asyncio.sentinel import Sentinel

logger = logging.getLogger("bot.redis")

class RedisCache:
    """
    Async Redis interface for caching search results।
    """
    
    def __init__(self, redis_url: str | None = None, enabled: bool = True):
        self.enabled = enabled and bool(redis_url)
        self.redis_url = redis_url
        self.pool: ConnectionPool | None = None
        self.client: Redis | None = None
        self.lock = asyncio.Lock() # Atomicity ke liye
        self.is_connected = False
        self.TTL = timedelta(hours=24) # Cache TTL 24 hours
        
        if not self.enabled:
            logger.warning("RedisCache: REDIS_URL missing ya disabled. Cache layer skip kiya jayega।")
        else:
            logger.info("RedisCache: Enabled. Connection shuru kiya ja raha hai।")

    async def init_cache(self):
        if not self.enabled: return

        # --- Simple URL Parse (Sentinel support nahi hai is implementation mein) ---
        try:
            self.client = Redis.from_url(
                self.redis_url, 
                encoding="utf-8", 
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            # Connection check (ping)
            await self.client.ping()
            self.is_connected = True
            logger.info("RedisCache: Connection safal.")
        except Exception as e:
            logger.error(f"RedisCache: Connection fail! Isse disable kiya ja raha hai. Error: {e}")
            self.enabled = False
            self.is_connected = False
            self.client = None
            self.pool = None

    async def is_ready(self) -> bool:
        if not self.enabled or not self.is_connected or not self.client:
            return False
        try:
            async with self.lock:
                await self.client.ping()
                return True
        except Exception as e:
            logger.warning(f"RedisCache: Ping fail. Connection lost: {e}")
            self.is_connected = False
            self.client = None
            return False

    async def close(self):
        if self.client:
            try:
                await self.client.close()
                logger.info("RedisCache: Client connection close ho gaya।")
            except Exception as e:
                logger.error(f"RedisCache close karte waqt error: {e}")

    # --- Search Cache Methods ---
    
    def _get_search_key(self, query: str) -> str:
        # Search query ko clean karke key banayein
        clean_q = clean_text_for_search(query)
        return f"search:{clean_q}"

    async def get_search_results(self, query: str) -> List[Dict] | None:
        if not await self.is_ready(): return None
        
        key = self._get_search_key(query)
        try:
            # Atomic operation (read/write race condition se bachne ke liye lock ka istemal)
            async with self.lock:
                cached_data = await self.client.get(key)
                if cached_data:
                    logger.debug(f"RedisCache: Hit for '{query}'")
                    return json.loads(cached_data)
                else:
                    logger.debug(f"RedisCache: Miss for '{query}'")
                    return None
        except Exception as e:
            logger.error(f"RedisCache: Get error for '{query}': {e}", exc_info=False)
            return None

    async def set_search_results(self, query: str, results: List[Dict]) -> bool:
        if not await self.is_ready(): return False
        
        if not results: return True # Empty result cache na karein (ya TTL chota rakhein)
        
        key = self._get_search_key(query)
        try:
            # Atomic operation
            async with self.lock:
                data = json.dumps(results)
                await self.client.set(key, data, ex=self.TTL)
                logger.debug(f"RedisCache: Set for '{query}'")
                return True
        except Exception as e:
            logger.error(f"RedisCache: Set error for '{query}': {e}", exc_info=False)
            return False

    async def delete_search_key(self, query: str) -> int:
        if not await self.is_ready(): return 0
        key = self._get_search_key(query)
        try:
            async with self.lock:
                deleted_count = await self.client.delete(key)
                return deleted_count
        except Exception as e:
            logger.error(f"RedisCache: Delete error for '{query}': {e}", exc_info=False)
            return 0
