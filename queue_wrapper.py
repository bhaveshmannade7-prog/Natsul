# queue_wrapper.py
import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Callable, Any, Dict, Coroutine, Optional, List
from aiogram import types, Bot
from aiogram.fsm.storage.memory import MemoryStorage

logger = logging.getLogger("bot.queue")

# Priority levels
# 0: Admin/Essential (highest priority)
# 1: User Actions (search, start, callbacks)
# 2: Background tasks (sync, cleanup)
PRIORITY_ADMIN = 0
PRIORITY_USER_ACTION = 1
PRIORITY_BACKGROUND = 2

# Max workers ko ENV se load karein
# FIX: Default value ko 1 kiya gaya hai. User ko zyada chahiye toh ENV set karein.
QUEUE_CONCURRENCY = int(os.getenv("QUEUE_CONCURRENCY", "1"))

class PriorityQueueWrapper:
    """
    A non-blocking queue to manage incoming Telegram updates, ensuring high 
    priority tasks (Admin, essential DB updates) are processed first.
    """
    def __init__(self, concurrency_limit: int):
        # FIX: Added maxsize to prevent OOM on free tier
        self._queue = asyncio.PriorityQueue(maxsize=5000)
        self._concurrency_limit = concurrency_limit
        self._active_workers = 0
        self._workers: List[asyncio.Task] = []
        
    def start_workers(self, bot_instance: Bot, dp_instance: Any, db_objects: Dict[str, Any]):
        """Queue processing workers ko shuru karta haiред"""
        if self._workers:
            logger.warning("Workers pehle se chal rahe hainред")
            return
            
        logger.info(f"Starting {self._concurrency_limit} priority queue workersред")
        for i in range(self._concurrency_limit):
            worker = asyncio.create_task(self._worker_loop(bot_instance, dp_instance, db_objects), name=f"QueueWorker-{i}")
            self._workers.append(worker)

    async def stop_workers(self):
        """Gracefully workers ko band karta haiред"""
        for worker in self._workers:
            worker.cancel()
        results = await asyncio.gather(*self._workers, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                logger.error(f"Worker shutdown error: {res}")
        self._workers.clear()
        logger.info("Priority queue workers band ho gayeред")
        
    def submit(self, update: types.Update, bot: Bot, db_objects: Dict[str, Any]):
        """
        Update ko queue mein submit karta haiред
        """
        priority = PRIORITY_USER_ACTION
        
        # Admin / Critical commands ko higher priority dein
        user_id = update.message.from_user.id if update.message and update.message.from_user else (
            update.callback_query.from_user.id if update.callback_query and update.callback_query.from_user else None
        )
        
        if user_id == db_objects.get('admin_id'):
            priority = PRIORITY_ADMIN
        elif update.message and update.message.text and (update.message.text.startswith("/start") or update.message.text.startswith("/help") or update.message.text.startswith("/stats")):
            priority = PRIORITY_ADMIN # Essential commands
        
        # PriorityQueue mein tuple (priority, timestamp, update, bot, db_objects) jayega
        # Timestamp tie-breaker ka kaam karega (pehle aao, pehle pao agar priority same ho)
        try:
            self._queue.put_nowait((priority, datetime.now(timezone.utc), update, bot, db_objects))
            logger.debug(f"Update {update.update_id} submitted with priority {priority} (Queue size: {self._queue.qsize()})")
        except asyncio.QueueFull:
            logger.warning(f"⚠️ Queue FULL! Update {update.update_id} dropped to preserve RAM.")

    async def _worker_loop(self, bot_instance: Bot, dp_instance: Any, db_objects: Dict[str, Any]):
        """Worker jo queue se tasks pick karta haiред"""
        while True:
            # Yeh worker loop non-blocking hai, isliye free-tier rule 3 break nahi hoga.
            try:
                # PriorityQueue se item nikalo (blocking wait)
                priority, timestamp, update, bot, db_objects = await self._queue.get()
                
                # Yeh asli processing call hai
                db_kwargs = {
                    'db_primary': db_objects['db_primary'],
                    'db_fallback': db_objects['db_fallback'],
                    'db_neon': db_objects['db_neon'],
                    'redis_cache': db_objects['redis_cache'] # Naya Redis object
                }

                # Update ko Dispatcher mein feed karo
                await dp_instance.feed_update(
                    bot=bot, 
                    update=update, 
                    **db_kwargs
                )
                
                self._queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Priority Queue Worker mein unhandled error: {e}")
                if '_queue' in locals(): self._queue.task_done()
            except BaseException:
                break

# Global Queue Instance
priority_queue = PriorityQueueWrapper(concurrency_limit=QUEUE_CONCURRENCY)
