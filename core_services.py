# core_services.py (STABILIZED FIX for Priority Queue Deadlock)

import asyncio
import logging
# ... (other imports)
import redis.asyncio as aioredis 

# --- DB Imports for Type Hinting ---
from database import Database
from neondb import NeonDB

logger = logging.getLogger("bot.core_services")

# --- NAYA FIX: NeonDB Initialization Lock (Isse maintain rakheingay) ---
NEON_INIT_LOCK = asyncio.Lock()


# ... (BotManager aur RedisCache classes NO CHANGE)


# =======================================================
# +++++ 4. Priority Queue Dispatcher (STABILIZED) +++++
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
        self.worker_tasks: List[asyncio.Task] = [] # NAYA: Worker tasks ko track karne ke liye
        logger.info(f"PriorityDispatcher initialized with limit {max_concurrent}.")

    def start_workers(self):
        """Shuru karta hai worker loop ko।"""
        if self.is_running: return
        self.is_running = True
        # 5 worker tasks shuru karein 
        for i in range(5):
             task = asyncio.create_task(self._worker_loop(i + 1))
             self.worker_tasks.append(task)
        logger.info("PriorityDispatcher worker shuru ho gaya.")

    async def stop_workers(self):
        """Band karta hai worker loop ko।"""
        if not self.is_running: return
        self.is_running = False
        
        # Shutdown signal queues mein daalein
        for _ in range(len(self.worker_tasks) + 1):
             await self.queue_high.put(None)
             await self.queue_medium.put(None)
             await self.queue_low.put(None)
             
        # Sabhi worker tasks ko wait karein (ya cancel karein)
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks.clear()
        logger.info("PriorityDispatcher workers band ho gaye.")


    async def _worker_loop(self, worker_id: int):
        """
        Queue se updates nikalta hai aur process karta hai।
        FIXED: Blocking asyncio.wait() on coroutines ko hata diya gaya hai.
        """
        while self.is_running:
            try:
                update_data = None
                
                # 1. High Priority Queue se non-blocking tarike se uthayein
                if not self.queue_high.empty():
                    update_data = await self.queue_high.get()
                
                # 2. Medium Priority Queue se non-blocking tarike se uthayein
                elif not self.queue_medium.empty():
                    update_data = await self.queue_medium.get()
                    
                # 3. Low Priority Queue se non-blocking tarike se uthayein
                elif not self.queue_low.empty():
                    update_data = await self.queue_low.get()

                # 4. Process the item
                if update_data is not None:
                    if update_data is None:
                        # Shutdown signal mila (put in stop_workers)
                        return 
                    
                    # Process the item using semaphore, non-blocking to the worker loop
                    asyncio.create_task(self._process_update_with_semaphore(update_data))
                    
                else:
                    # Agar queues khaali hain, toh short sleep karein
                    await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                return
            except asyncio.QueueEmpty:
                 # Agar queue.get_nowait() use karte toh ye aata. get() use karne par nahi aayega.
                 await asyncio.sleep(0.01)
            except Exception as e:
                logger.error(f"PriorityDispatcher Worker {worker_id} unexpected error: {e}", exc_info=True)
                await asyncio.sleep(1) 

    async def _process_update_with_semaphore(self, update_data: Tuple[Update, Bot, Dict[str, Any]]):
        """Semaphore ke andar update ko process karta है।"""
        update, bot_instance, context = update_data
        
        try:
            # Global concurrency limit (Semaphore)
            async with self.semaphore:
                # Original _process_update_safe logic ko chalayein
                await self.dp.feed_update(bot=bot_instance, update=update, **context)
                
        except Exception as e:
            logger.exception(f"Priority Queue se update process karte waqt error {update.update_id}: {e}")

    async def dispatch(self, update: Update, bot_instance: Bot, priority: Literal['HIGH', 'MEDIUM', 'LOW'], **context):
        # ... (NO CHANGE)
        update_data = (update, bot_instance, context)
        
        if priority == 'HIGH':
            await self.queue_high.put(update_data)
        elif priority == 'MEDIUM':
            await self.queue_medium.put(update_data)
        elif priority == 'LOW':
            await self.queue_low.put(update_data)
        else:
            await self.queue_low.put(update_data)
