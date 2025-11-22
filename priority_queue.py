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
        # Semaphore ko .env se set kiya ja raha hai (default 200)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.queue_size = 0
        logger.info(f"PriorityQueueWrapper initialized with concurrency limit: {max_concurrent}")

    async def put(self, coro: Awaitable[Any], priority: Priority = "MEDIUM") -> asyncio.Task:
        """
        Task ko queue mein daalta hai aur turant ek asyncio.Task return karta hai।
        Task priority yahan sirf logging ke liye hai, asli execution order FIFO hi rahega
        kyunki yeh ek simple semaphore-gated queue hai. Real-time priority ke liye
        Python mein PriorityQueue ka istemal zaroori hota hai jo blocking hota hai.
        Yahan Non-Blocking/Concurrency ko priority di gayi hai।
        """
        self.queue_size += 1
        
        async def gated_task():
            try:
                # Execution shuru hone se pehle semaphore wait karein
                async with self.semaphore:
                    logger.debug(f"Task '{priority}' executing. Queue size: {self.queue_size}")
                    return await coro
            except asyncio.CancelledError:
                logger.warning(f"Task '{priority}' cancelled.")
                raise
            except Exception as e:
                logger.error(f"Task '{priority}' execution mein error: {e}", exc_info=True)
                raise
            finally:
                self.queue_size -= 1

        # Turant Task banayein aur return karein - Non-Blocking
        task = asyncio.create_task(gated_task())
        return task

    def get_queue_status(self) -> dict:
        return {
            "max_concurrent": self.max_concurrent,
            "current_queue_size": self.queue_size,
            "current_active_tasks": self.max_concurrent - self.semaphore.value
        }

# --- Decorator for easy handler integration ---

def priority_task_wrapper(priority: Priority):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Dispatcher se queue object nikalein
            dp = kwargs.get('dispatcher') or (args[0].bot.dispatcher if args and hasattr(args[0], 'bot') and hasattr(args[0].bot, 'dispatcher') else None)
            queue_wrapper: PriorityQueueWrapper = dp.queue_wrapper if dp and hasattr(dp, 'queue_wrapper') else None
            
            if not queue_wrapper:
                logger.warning(f"QueueWrapper nahi mila. Task '{func.__name__}' seedha chal raha hai.")
                return await func(*args, **kwargs)
            
            # Handler ke asli execution ko ek task mein wrap karein
            coro = func(*args, **kwargs)
            
            # Task ko queue mein daalein (aur turant task object return karein)
            return await queue_wrapper.put(coro, priority=priority)

        return wrapper
    return decorator

