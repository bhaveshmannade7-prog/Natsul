# redis_cache.py
import os
import asyncio
import logging
import json
from datetime import datetime, timezone
from typing import Optional, List

try:
    from redis.asyncio import Redis, ConnectionPool, ConnectionError
except ImportError:
    # Production mein zaroori hai, lekin agar local environment mein missing ho toh gracefully handle karein.
    # Upar 'bot.py' mein pip install list de di jayegi.
    class Redis:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): pass
        def __init__(self, *args, **kwargs): logging.warning("Redis library missing. RedisCacheLayer will be disabled.")
    class ConnectionPool: pass
    class ConnectionError(Exception): pass
    
logger = logging.getLogger("bot.redis")

REDIS_URL = os.getenv("REDIS_URL")
ACTIVE_WINDOW_SECONDS = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5")) * 60

class RedisCacheLayer:
    
    def __init__(self):
        self._pool: Optional[ConnectionPool] = None
        self.redis: Optional[Redis] = None
        self._is_ready = False
        self._lock = asyncio.Lock()
        
    async def init_cache(self):
        """Connection pool banata hai।"""
        if not REDIS_URL:
            logger.warning("REDIS_URL set nahi hai. Redis caching disabled rahega।")
            return
            
        async with self._lock:
            try:
                self._pool = ConnectionPool.from_url(REDIS_URL)
                self.redis = Redis(connection_pool=self._pool)
                # Check connection
                await self.redis.ping()
                self._is_ready = True
                logger.info("✅ Redis cache connection safal.")
            except ConnectionError as e:
                logger.error(f"❌ Redis connection error: {e}. Caching disabled.", exc_info=False)
                self._is_ready = False
                self.redis = None
            except Exception as e:
                logger.error(f"❌ Redis initialization error: {e}. Caching disabled.", exc_info=True)
                self._is_ready = False
                self.redis = None

    async def close(self):
        if self.redis:
            try:
                await self.redis.close()
                logger.info("Redis connection close ho gaya।")
            except Exception as e:
                logger.error(f"Redis close karte waqt error: {e}")
            finally:
                self.redis = None
                self._is_ready = False

    def is_ready(self) -> bool:
        return self._is_ready and self.redis is not None
        
    # =======================================================
    # +++++ CORE CACHE OPERATIONS (For High Concurrency) +++++
    # =======================================================
        
    async def update_user_activity(self, user_id: int):
        """User activity ko Redis Sorted Set (ZSET) mein update karta hai।"""
        if not self.is_ready(): return
        
        try:
            # Key: active_users_set, Score: timestamp, Member: user_id
            timestamp = int(datetime.now(timezone.utc).timestamp())
            await self.redis.zadd("active_users_set", {str(user_id): timestamp}, xx=False)
            
            # Key ko TTL set nahi karte, ZREM se clean karenge
            
        except Exception as e:
            logger.error(f"Redis update_user_activity error: {e}", exc_info=False)
            self._is_ready = False # Connection fail hua ho sakta hai
            
    async def get_concurrent_user_count(self) -> Optional[int]:
        """
        ZSET se purani entries hata kar, active users ki count deta hai।
        (O(log(N)+M) complexity - Redis mein fast hai)
        """
        if not self.is_ready(): return None
        
        try:
            cutoff_timestamp = int(datetime.now(timezone.utc).timestamp()) - ACTIVE_WINDOW_SECONDS
            
            # Purane members ko hatao (score < cutoff)
            await self.redis.zremrangebyscore("active_users_set", min=0, max=cutoff_timestamp)
            
            # Active members ki count return karo
            count = await self.redis.zcard("active_users_set")
            return count
            
        except Exception as e:
            logger.error(f"Redis get_concurrent_user_count error: {e}", exc_info=False)
            self._is_ready = False
            return None # Fallback to Mongo

    # =======================================================
    # +++++ FUZZY CACHE (Persistence) +++++
    # =======================================================
    
    # NOTE: 'fuzzy_movie_cache' abhi bhi in-memory Dict hai. 
    # Redis sirf persistence aur failover ke liye use hoga.
    
    FUZZY_KEY = "fuzzy_cache_v1"
    
    async def save_fuzzy_cache(self, cache_data: dict) -> bool:
        """In-memory cache ko Redis mein store karta hai।"""
        if not self.is_ready(): return False
        try:
            # Dict ko JSON string mein convert karein
            json_data = json.dumps(cache_data)
            await self.redis.set(self.FUZZY_KEY, json_data, ex=86400 * 7) # 7 din ki TTL
            logger.info("Redis: Fuzzy cache saved.")
            return True
        except Exception as e:
            logger.error(f"Redis save_fuzzy_cache error: {e}", exc_info=False)
            self._is_ready = False
            return False
            
    async def load_fuzzy_cache(self) -> Optional[dict]:
        """Redis se cache load karta hai।"""
        if not self.is_ready(): return None
        try:
            json_data = await self.redis.get(self.FUZZY_KEY)
            if json_data:
                # JSON string ko Dict mein convert karein
                cache_data = json.loads(json_data)
                logger.info(f"Redis: Fuzzy cache loaded ({len(cache_data):,} titles).")
                return cache_data
            return None
        except Exception as e:
            logger.error(f"Redis load_fuzzy_cache error: {e}", exc_info=False)
            self._is_ready = False
            return None # Fallback to MongoDB
            
    # =======================================================
    # +++++ OTHER CACHE OPS (Future Proofing) +++++
    # =======================================================

    async def get(self, key: str) -> Optional[str]:
        if not self.is_ready(): return None
        try: return await self.redis.get(key)
        except Exception as e: self._is_ready = False; logger.debug(f"Redis GET fail: {e}"); return None

    async def set(self, key: str, value: str, ttl: int = 3600) -> bool:
        if not self.is_ready(): return False
        try: await self.redis.set(key, value, ex=ttl); return True
        except Exception as e: self._is_ready = False; logger.debug(f"Redis SET fail: {e}"); return False

# Global Redis Instance
redis_cache = RedisCacheLayer()
