# core_services.py
import asyncio
import logging
import random
import time
from typing import List, Dict, Tuple, Any, Callable, Literal
import json

from aiogram import Bot, types, Dispatcher
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Update

import aioredis # New dependency

# --- DB Imports for Type Hinting ---
from database import Database
from neondb import NeonDB

logger = logging.getLogger("bot.core_services")

# =======================================================
# +++++ 1. Bot Manager (Multi-Token Load Balancing) +++++
# =======================================================

class BotManager:
    """
    Manages multiple bot tokens for async load balancing and flood control.
    """
    def __init__(self, primary_token: str, alternate_tokens: List[str]):
        self.tokens: List[str] = [primary_token] + alternate_tokens
        self.bots: List[Bot] = []
        self.bot_status: Dict[str, float] = {}  # token: retry_after_timestamp
        self.flood_wait_sec = 600  # 10 minutes wait for 429 error
        self._init_bots()

    def _init_bots(self):
        """Initializes all Bot instances."""
        for token in self.tokens:
            bot_instance = Bot(
                token=token,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML)
            )
            self.bots.append(bot_instance)
            self.bot_status[token] = 0.0
        logger.info(f"BotManager initialized with {len(self.bots)} bot tokens.")

    def get_available_bot(self) -> Bot:
        """Returns the next available bot instance in a round-robin fashion."""
        now = time.time()
        
        # 1. Active tokens nikaalein (jo flood wait mein nahi hain)
        available_tokens = [
            bot for bot in self.bots 
            if self.bot_status.get(bot.token, 0.0) < now
        ]
        
        if not available_tokens:
            # Agar sabhi tokens flood wait mein hain, toh bas pehla token chunein
            # (Yeh usse wapas check karne ka mauka dega)
            logger.warning("All bot tokens are currently flood-limited. Retrying primary token.")
            return self.bots[0] 
        
        # 2. Round-robin / random chunein (round-robin + load balancing)
        # Abhi ke liye random load balancing use kar rahe hain.
        return random.choice(available_tokens)

    async def safe_api_call(self, coro: Callable, timeout: int = 8, semaphore: asyncio.Semaphore | None = None) -> Any:
        """
        Runs a Telegram API call using the next available bot, 
        handling 429 Flood Waits by instantly switching tokens.
        """
        token_to_use = None
        
        async with (semaphore if semaphore else asyncio.Semaphore(1)):
            
            # --- Load Balancing Logic ---
            max_attempts = len(self.bots)
            for attempt in range(max_attempts):
                
                bot_instance = self.get_available_bot()
                token_to_use = bot_instance.token
                
                # Original Coroutine ko bind karein naye bot instance ke saath
                # (Zaroori agar coro.im_self mein bot instance hard-coded ho)
                bound_coro = self._rebind_coroutine(coro, bot_instance)
                
                try:
                    # 100ms ka chhota sleep load balancing ko behtar karta hai
                    if semaphore: await asyncio.sleep(0.1) 
                    
                    return await asyncio.wait_for(bound_coro, timeout=timeout)
                
                except TelegramAPIError as e:
                    error_msg = str(e).lower()
                    
                    if "too many requests" in error_msg:
                        # --- INSTANT FLOOD SWITCH (FEATURE A) ---
                        wait_time_match = re.search(r'retry after (\d+)', error_msg)
                        wait_time = int(wait_time_match.group(1)) if wait_time_match else self.flood_wait_sec
                        
                        self.bot_status[token_to_use] = time.time() + wait_time
                        logger.warning(f"⚠️ Token {token_to_use[:5]}... flood-limited for {wait_time}s. Switching tokens.")
                        
                        if attempt == max_attempts - 1:
                            # Agar sabhi tokens fail ho gaye
                            logger.critical("ALL bot tokens are under flood limit.")
                            raise # Sabhi tokens busy hain, abhi throw karein
                        
                        # Agle token par switch karein (loop phir se chalega)
                        await asyncio.sleep(0.5) # Thoda delay next attempt se pehle
                        continue
                        
                    elif "bot was blocked" in error_msg or "user is deactivated" in error_msg:
                        logger.info(f"TG: Bot block ya user deactivated."); return False
                    elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
                        logger.info(f"TG: Chat nahi mila."); return False
                    elif "message is not modified" in error_msg:
                        logger.debug(f"TG: Message modify nahi hua."); return None
                    elif "message to delete not found" in error_msg or "message to copy not found" in error_msg:
                        logger.debug(f"TG: Message (delete/copy) nahi mila."); return None
                    else:
                        logger.warning(f"TG Error (Token: {token_to_use[:5]}...): {e}"); return None
                        
                except asyncio.TimeoutError: 
                    logger.warning(f"TG call timeout (Token: {token_to_use[:5]}...): {getattr(coro, '__name__', 'unknown_coro')}"); return None
                except Exception as e:
                    logger.exception(f"TG Unexpected error (Token: {token_to_use[:5]}...): {e}"); return None
                    
            return None # Agar loop poora ho gaya

    def _rebind_coroutine(self, coro: Callable, bot_instance: Bot):
        """Replaces the bot instance in the coroutine if it was bound to an old bot object."""
        if hasattr(coro, '__self__') and isinstance(coro.__self__, Bot):
            # Agar coroutine Bot instance par bound hai (e.g., bot.send_message)
            method_name = coro.__name__
            if hasattr(bot_instance, method_name):
                return getattr(bot_instance, method_name)
        # Agar coroutine un-bound hai ya function ko seedha pass kiya gaya hai (e.g. get_chat_member)
        # Toh user code mein Bot ko seedha pass karna hoga (jo abhi bot.py mein hai)
        # Jiss Bot instance ko hum rebind kar sakte hain, usse yahaan return karein
        return coro
        
    async def close(self):
        """Closes all bot sessions."""
        for bot in self.bots:
            if bot.session:
                try: await bot.session.close()
                except Exception as e: logger.error(f"Bot session close karte waqt error: {e}")
        logger.info("BotManager: Sabhi bot sessions close ho gaye.")


# =======================================================
# +++++ 2. Redis Cache (Optional + Async) +++++
# =======================================================

class RedisCache:
    """Async Redis interface for caching."""
    def __init__(self, redis_url: str | None, ttl: int = 86400):
        self.redis_url = redis_url
        self.ttl = ttl
        self.client: aioredis.Redis | None = None
        self.is_enabled = bool(redis_url)

    async def connect(self):
        if not self.is_enabled: return
        try:
            self.client = aioredis.from_url(self.redis_url, decode_responses=True)
            await self.client.ping()
            logger.info("✅ Redis connection successful.")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}. Caching disabled.", exc_info=False)
            self.client = None
            self.is_enabled = False

    async def close(self):
        if self.client:
            try: await self.client.close()
            except: pass
            self.client = None

    async def get(self, key: str) -> Any | None:
        if not self.is_enabled or not self.client: return None
        try:
            value = await self.client.get(key)
            if value:
                # Assuming all stored values are JSON dumps of dicts/lists
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Redis GET error for key {key}: {e}", exc_info=False)
            return None

    async def set(self, key: str, value: Any, ttl: int | None = None):
        if not self.is_enabled or not self.client: return
        try:
            ttl_to_use = ttl if ttl is not None else self.ttl
            value_to_store = json.dumps(value)
            await self.client.set(key, value_to_store, ex=ttl_to_use)
        except Exception as e:
            logger.error(f"Redis SET error for key {key}: {e}", exc_info=False)

    async def delete(self, key: str):
        if not self.is_enabled or not self.client: return
        try:
            await self.client.delete(key)
        except Exception as e:
            logger.error(f"Redis DELETE error for key {key}: {e}", exc_info=False)

class DatabaseCacheWrapper:
    """
    Wraps existing DB methods to add Redis caching layer (TTL 24 hours).
    Original DB logic is NOT modified.
    """
    def __init__(self, primary_db: Database, neon_db: NeonDB, redis_cache: RedisCache):
        self.primary_db = primary_db
        self.neon_db = neon_db
        self.cache = redis_cache
        self.TTL_24H = 86400

    # --- Movie Retrieval (HIGH PRIORITY CACHE) ---
    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        cache_key = f"movie:imdb:{imdb_id}"
        
        # 1. Cache hit
        movie = await self.cache.get(cache_key)
        if movie is not None:
            logger.debug(f"Cache HIT: get_movie_by_imdb {imdb_id}")
            return movie
            
        # 2. Cache miss, call original function
        movie = await self.primary_db.get_movie_by_imdb(imdb_id)
        
        # 3. Cache set (even for None/empty result, for short TTL)
        if movie is not None:
            await self.cache.set(cache_key, movie, ttl=self.TTL_24H)
        elif self.cache.is_enabled:
             # Negative caching for 5 minutes
             await self.cache.set(cache_key, {}, ttl=300) 
             
        return movie

    # --- Count Retrieval (MEDIUM PRIORITY CACHE) ---
    async def get_movie_count(self, db_instance: Literal['primary', 'neon'] = 'primary') -> int:
        db = self.primary_db if db_instance == 'primary' else self.neon_db
        cache_key = f"count:movies:{db_instance}"
        
        # 1. Cache hit (TTL 1 hour)
        count = await self.cache.get(cache_key)
        if isinstance(count, int) and count >= 0:
            logger.debug(f"Cache HIT: get_movie_count {db_instance}")
            return count
            
        # 2. Cache miss, call original function
        count = await db.get_movie_count()
        
        # 3. Cache set (TTL 1 hour)
        if count >= 0:
            await self.cache.set(cache_key, count, ttl=3600)
            
        return count

    # --- Original Functions ko pass through karein ---
    def __getattr__(self, name):
        """Pass all other calls to the primary DB instance."""
        if name in ['is_ready', '_connect', 'init_db', '_handle_db_error', 'add_user', 'get_concurrent_user_count']:
            return getattr(self.primary_db, name)
        # Agar koi dusra call (jisse hum cache nahi kar rahe) call kiya jaaye, toh use primary DB ko forward karein.
        if hasattr(self.primary_db, name):
            return getattr(self.primary_db, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


# =======================================================
# +++++ 3. Priority Queue Dispatcher +++++
# =======================================================

class PriorityDispatcher:
    """
    Manages updates using a priority queue and a global concurrency semaphore.
    """
    def __init__(self, dp: Dispatcher, max_concurrent: int):
        self.dp = dp
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.queue_high = asyncio.Queue()
        self.queue_medium = asyncio.Queue()
        self.queue_low = asyncio.Queue()
        self.is_running = False
        self.worker_task = None
        logger.info(f"PriorityDispatcher initialized with limit {max_concurrent}.")

    def start_workers(self):
        """Shuru karta hai worker loop ko."""
        if self.is_running: return
        self.is_running = True
        self.worker_task = asyncio.create_task(self._worker_loop())
        logger.info("PriorityDispatcher worker shuru ho gaya.")

    async def stop_workers(self):
        """Band karta hai worker loop ko."""
        if not self.is_running: return
        self.is_running = False
        if self.worker_task:
            self.worker_task.cancel()
            try: await asyncio.wait_for(self.worker_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError): pass
            self.worker_task = None
        logger.info("PriorityDispatcher worker band ho gaya.")

    async def _worker_loop(self):
        """Queue se updates nikalta hai aur process karta hai."""
        while self.is_running:
            # Priority Order: HIGH -> MEDIUM -> LOW
            try:
                if not self.queue_high.empty():
                    update_data = await self.queue_high.get()
                elif not self.queue_medium.empty():
                    update_data = await self.queue_medium.get()
                elif not self.queue_low.empty():
                    update_data = await self.queue_low.get()
                else:
                    # Agar sabhi queues khaali hain, toh intezaar karein
                    await asyncio.sleep(0.01)
                    continue
                    
                asyncio.create_task(self._process_update_with_semaphore(update_data))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"PriorityDispatcher loop error: {e}", exc_info=True)
                await asyncio.sleep(1) # Error ke baad chhota sa delay

    async def _process_update_with_semaphore(self, update_data: Tuple[Update, Bot, Dict[str, Any]]):
        """Semaphore ke andar update ko process karta hai."""
        update, bot_instance, context = update_data
        
        try:
            async with self.semaphore:
                # Original _process_update_safe logic ko chalayein
                await self.dp.feed_update(bot=bot_instance, update=update, **context)
                
        except Exception as e:
            # Update process karte waqt koi bhi error
            logger.exception(f"Priority Queue se update process karte waqt error {update.update_id}: {e}")

    async def dispatch(self, update: Update, bot_instance: Bot, priority: Literal['HIGH', 'MEDIUM', 'LOW'], **context):
        """
        Update ko priority ke aadhaar par queue mein daalta hai.
        """
        update_data = (update, bot_instance, context)
        
        if priority == 'HIGH':
            await self.queue_high.put(update_data)
        elif priority == 'MEDIUM':
            await self.queue_medium.put(update_data)
        elif priority == 'LOW':
            await self.queue_low.put(update_data)
        else:
            logger.warning(f"Invalid priority '{priority}' for update {update.update_id}. Defaulting to LOW.")
            await self.queue_low.put(update_data)
        
        # Non-freezing behavior: Update ko queue mein daal diya gaya hai,
        # webhook response turant wapas chala jayega.

# =======================================================
# +++++ 4. Priority Wrapper Decorator +++++
# =======================================================

def priority_wrapper(priority: Literal['HIGH', 'MEDIUM', 'LOW']):
    """
    Aiogram handler ko priority assign karne ke liye decorator.
    """
    def decorator(func: Callable):
        setattr(func, '__handler_priority__', priority)
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        return wrapper
    return decorator

