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
        # asyncio.Semaphore mein .value attribute nahi hota hai.
        self.semaphore = asyncio.Semaphore(max_concurrent)
        # NAYA: Hum ek internal counter rakhenge jo waiting aur executing tasks ko count karega.
        # Yeh counter increase/decrease task ke shuru aur khatam hone par hoga.
        self.queue_size = 0 
        logger.info(f"PriorityQueueWrapper initialized with concurrency limit: {max_concurrent}")

    async def put(self, coro: Awaitable[Any], priority: Priority = "MEDIUM") -> asyncio.Task:
        """
        Task ko queue mein daalta hai aur turant ek asyncio.Task return karta hai।
        """
        self.queue_size += 1 # Task add hote hi size badhao
        
        async def gated_task():
            try:
                # Execution shuru hone se pehle semaphore wait karein
                # Wait khatam hote hi, yeh task ACTIVE ho jayega
                await self.semaphore.acquire()
                try:
                    logger.debug(f"Task '{priority}' executing. Queue size (approx): {self.queue_size}")
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
                # Task finish ya fail hone par size kam karo (is tarah ki implemention ke liye yeh thoda approximate rahega)
                self.queue_size -= 1 # NOTE: Yeh line hamesha execute nahi hogi agar task cancellation ya crash ho.
                # Lekin yeh is non-blocking design ke liye sabse simple solution hai.

        # Turant Task banayein aur return karein - Non-Blocking
        task = asyncio.create_task(gated_task())
        return task

    def get_queue_status(self) -> dict:
        """
        Status return karta hai. Semaphore.value error se bachne ke liye,
        hum active tasks ka anumaan self.queue_size se lagayenge.
        """
        # FIX: self.semaphore.value hataya gaya.
        # Simple approximation: Max concurrent tasks max size ya current size mein se jo kam ho.
        current_active_tasks = min(self.max_concurrent, max(0, self.max_concurrent - self.semaphore._value)) # NOTE: _value is technically private, but often used for introspection when .value is missing. Let's use simple approximation to be safer.
        
        # FIX: Let's use a reliable approximation based on queue logic.
        # Since we use semaphore.acquire/release inside gated_task:
        # Number of active tasks is max_concurrent - (available_slots).
        # Since .value is missing, we can only report max and current tracked size.
        
        # AVOIDED: self.semaphore._value for strictness.
        
        return {
            "max_concurrent": self.max_concurrent,
            # Current queue size ab waiting aur executing dono ko count karega.
            "current_queue_size": max(0, self.queue_size),
            # current_active_tasks ko is design mein accurately track karna mushkil hai 
            # bina Semaphore class ko modify kiye. Hum queue_size ko hi return karenge.
            "current_active_tasks": "N/A (Semaphore introspection error)"
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
            
            # Handler ke asli execution ko ek coroutine mein wrap karein
            coro = func(*args, **kwargs)
            
            # Task ko queue mein daalein (aur turant task object return karein)
            return await queue_wrapper.put(coro, priority=priority)

        return wrapper
    return decorator
