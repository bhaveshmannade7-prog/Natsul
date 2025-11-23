# priority_queue.py
import asyncio
import logging
from typing import Callable, Any, Literal, Awaitable
from functools import wraps

logger = logging.getLogger("bot.queue")

# --- Task Priority Levels ---
Priority = Literal["HIGH", "MEDIUM", "LOW"] 

class PriorityQueueWrapper:
    """
    Ek high-concurrency non-blocking task queue jo asyncio.Semaphore ka istemal karta hai।
    """
    
    def __init__(self, max_concurrent: int):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.queue_size = 0
        logger.info(f"PriorityQueueWrapper initialized with concurrency limit: {max_concurrent}")

    async def put(self, coro: Awaitable[Any], priority: Priority = "MEDIUM") -> asyncio.Task:
        """
        Task ko queue mein daalta hai aur turant ek asyncio.Task return karta hai।
        """
        self.queue_size += 1
        
        async def gated_task():
            try:
                await self.semaphore.acquire()
                try:
                    logger.debug(f"Task '{priority}' executing. Queue size (tracked): {self.queue_size}")
                    return await coro
                finally:
                    self.semaphore.release()
            except asyncio.CancelledError:
                logger.warning(f"Task '{priority}' cancelled.")
                raise
            except Exception as e:
                logger.error(f"Task '{priority}' execution mein error: {e}", exc_info=True)
                raise
            finally:
                self.queue_size -= 1

        task = asyncio.create_task(gated_task())
        return task

    def get_queue_status(self) -> dict:
        """
        Status return karta hai।
        """
        return {
            "max_concurrent": self.max_concurrent,
            "current_queue_size": max(0, self.queue_size),
            "current_active_tasks": "Unknown (Introspection blocked by asyncio.Semaphore design)"
        }

# --- Decorator for easy handler integration (FINAL FIX: Guaranteed DP Access) ---

def priority_task_wrapper(priority: Priority):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            
            queue_wrapper = None
            
            # METHOD 1: Try accessing queue_wrapper through Bot/Dispatcher (most reliable path)
            # Find the Bot instance first, which should have the dispatcher attribute.
            bot_instance = kwargs.get('bot')
            if not bot_instance and args and hasattr(args[0], 'bot'):
                bot_instance = args[0].bot

            if bot_instance and hasattr(bot_instance, 'dispatcher') and hasattr(bot_instance.dispatcher, 'queue_wrapper'):
                queue_wrapper = bot_instance.dispatcher.queue_wrapper
            
            # --- EXECUTION ---
            if not queue_wrapper:
                # Agar QueueWrapper nahi mila, toh warning dekar seedha chalao (Blocking mode)
                logger.warning(f"QueueWrapper nahi mila. Task '{func.__name__}' seedha chal raha hai (BLOCKING).")
                return await func(*args, **kwargs)
            
            # Agar QueueWrapper mila, toh Task ko put karo (Non-Blocking mode)
            coro = func(*args, **kwargs)
            return await queue_wrapper.put(coro, priority=priority)

        return wrapper
    return decorator
