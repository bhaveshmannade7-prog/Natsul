# queue_wrapper.py
import asyncio
import logging
from typing import Callable, Any, Dict, Coroutine, Optional
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

class PriorityQueueWrapper:
    """
    A non-blocking queue to manage incoming Telegram updates, ensuring high 
    priority tasks (Admin, essential DB updates) are processed first.
    """
    def __init__(self, concurrency_limit: int):
        self._queue = asyncio.PriorityQueue()
        self._concurrency_limit = concurrency_limit
        self._active_workers = 0
        self._workers: List[asyncio.Task] = []
        
    def start_workers(self, bot_instance: Bot, dp_instance: Any, db_objects: Dict[str, Any]):
        """Queue processing workers ko shuru karta hai।"""
        if self._workers:
            logger.warning("Workers pehle se chal rahe hain।")
            return
            
        logger.info(f"Starting {self._concurrency_limit} priority queue workers।")
        for i in range(self._concurrency_limit):
            worker = asyncio.create_task(self._worker_loop(bot_instance, dp_instance, db_objects), name=f"QueueWorker-{i}")
            self._workers.append(worker)

    async def stop_workers(self):
        """Gracefully workers ko band karta hai।"""
        for worker in self._workers:
            worker.cancel()
        results = await asyncio.gather(*self._workers, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                logger.error(f"Worker shutdown error: {res}")
        self._workers.clear()
        logger.info("Priority queue workers band ho gaye।")
        
    def submit(self, update: types.Update, bot: Bot, db_objects: Dict[str, Any]):
        """
        Update ko queue mein submit karta hai।
        Update ko process karne wala coroutine (_process_update_safe) ab
        worker loop ke andar chalega।
        """
        priority = PRIORITY_USER_ACTION
        
        # Admin / Critical commands ko higher priority dein
        if update.message and update.message.from_user and update.message.from_user.id == db_objects.get('admin_id'):
            priority = PRIORITY_ADMIN
        elif update.message and (update.message.text in ["/start", "/help", "/stats"]):
            priority = PRIORITY_ADMIN # Essential commands
        
        self._queue.put_nowait((priority, datetime.now(timezone.utc), update, bot, db_objects))
        logger.debug(f"Update {update.update_id} submitted with priority {priority} (Queue size: {self._queue.qsize()})")

    async def _worker_loop(self, bot_instance: Bot, dp_instance: Any, db_objects: Dict[str, Any]):
        """Worker jo queue se tasks pick karta hai।"""
        while True:
            try:
                # PriorityQueue se item nikalo (blocking wait)
                priority, timestamp, update, bot, db_objects = await self._queue.get()
                
                # Yeh asli processing call hai (bot.py se liya gaya)
                db_kwargs = {
                    'db_primary': db_objects['db_primary'],
                    'db_fallback': db_objects['db_fallback'],
                    'db_neon': db_objects['db_neon'],
                    'redis_cache': db_objects['redis_cache']
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
                # Task ko done mark karein taki queue stuck na ho
                if '_queue' in locals(): self._queue.task_done()
            except BaseException:
                # SystemExit, KeyboardInterrupt jaise exceptions ko handle karein
                break

# Global Queue Instance
# Max workers ko 'CURRENT_CONC_LIMIT' se thoda zyada rakhein.
# Yeh Concurrency Limit Redis/Mongo throttling se alag hai.
QUEUE_CONCURRENCY = int(os.getenv("QUEUE_CONCURRENCY", "40")) # Default 40 for EC2 free tier
priority_queue = PriorityQueueWrapper(concurrency_limit=QUEUE_CONCURRENCY)
