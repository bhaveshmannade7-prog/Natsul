# priority_queue.py
import asyncio
import logging
from typing import Callable, Any, Literal, Awaitable
from functools import wraps

logger = logging.getLogger("bot.queue")

# --- Task Priority Levels (FIX: Must be defined before use in the wrapper function) ---
Priority = Literal["HIGH", "MEDIUM", "LOW"] # <-- FIX: Moved this definition up

class PriorityQueueWrapper:
    """
    Ek high-concurrency non-blocking task queue jo asyncio.Semaphore ka istemal karta hai।
    FIX: 'Semaphore' object has no attribute 'value' error resolved.
    """
    
    def __init__(self, max_concurrent: int):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        # self.queue_size is now a simple counter for all pending/running tasks.
        self.queue_size = 0
        logger.info(f"PriorityQueueWrapper initialized with concurrency limit: {max_concurrent}")

    async def put(self, coro: Awaitable[Any], priority: Priority = "MEDIUM") -> asyncio.Task:
        """
        Task ko queue mein daalta hai aur turant ek asyncio.Task return karta hai।
        """
        # Task queue mein aate hi counter badhayein
        self.queue_size += 1
        
        async def gated_task():
            try:
                # Semaphore acquire karta hai ki execution active ho sake
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
                # Task complete ya fail hone par counter kam karein
                self.queue_size -= 1

        # Turant Task banayein aur return karein - Non-Blocking
        task = asyncio.create_task(gated_task())
        return task

    def get_queue_status(self) -> dict:
        """
        Status return karta hai. FIX: .value attribute se bachne ke liye.
        'current_active_tasks' is now an approximation/status string.
        """
        return {
            "max_concurrent": self.max_concurrent,
            # Current queue size waiting aur executing dono tasks ka total hai.
            "current_queue_size": max(0, self.queue_size),
            "current_active_tasks": "Unknown (Introspection blocked by asyncio.Semaphore design)"
        }

# --- Decorator for easy handler integration ---

def priority_task_wrapper(priority: Priority):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            dp = kwargs.get('dispatcher') or (args[0].bot.dispatcher if args and hasattr(args[0], 'bot') and hasattr(args[0].bot, 'dispatcher') else None)
            queue_wrapper: PriorityQueueWrapper = dp.queue_wrapper if dp and hasattr(dp, 'queue_wrapper') else None
            
            if not queue_wrapper:
                logger.warning(f"QueueWrapper nahi mila. Task '{func.__name__}' seedha chal raha hai.")
                return await func(*args, **kwargs)
            
            coro = func(*args, **kwargs)
            return await queue_wrapper.put(coro, priority=priority)

        return wrapper
    return decorator
