# priority_queue.py
import asyncio
import logging
from typing import Callable, Any, Tuple

logger = logging.getLogger("bot.priority_queue")

class PriorityQueueSystem:
    """
    High Concurrency Priority Queue System.
    Admin commands/critical tasks (Priority 0) ko pehle process karega.
    """
    
    # Priority levels
    PRIORITY_ADMIN = 0
    PRIORITY_CRITICAL = 5
    PRIORITY_SEARCH = 10
    PRIORITY_COPY = 20
    PRIORITY_LOW = 30
    
    def __init__(self, max_workers: int = 20):
        self._queue = asyncio.PriorityQueue()
        self._workers: list[asyncio.Task] = []
        self._max_workers = max_workers
        self._running = False
        logger.info(f"PriorityQueueSystem {max_workers} workers ke saath initialize hua.")

    def start(self):
        """Worker coroutines ko shuru karta hai।"""
        if self._running:
            logger.warning("PriorityQueue pehle se hi running hai।")
            return
        # FIX: Async self.stop() call ko hata diya gaya. Lifespan hook pehle hi await kar raha hai.
        # self.stop() # Ensure clean start
        self._running = True
        
        for i in range(self._max_workers):
            task = asyncio.create_task(self._worker_routine(i + 1))
            self._workers.append(task)
            
        logger.info(f"PriorityQueue ke {self._max_workers} worker shuru ho gaye hain।")

    async def stop(self):
        """Graceful shutdown।"""
        if not self._running:
            return
            
        logger.info("PriorityQueue workers ko band kiya ja raha hai...")
        self._running = False
        
        # Ab workers ko queue se nikalne ke liye dummy tasks daalein
        for _ in range(self._max_workers):
            self._queue.put_nowait((self.PRIORITY_LOW, self._stop_dummy))
            
        # Workers ko cancel karein aur unke complete hone ka wait karein
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("PriorityQueue shutdown poora hua।")

    async def _stop_dummy(self, *args, **kwargs):
        """Worker ko exit karne ke liye dummy task।"""
        pass

    async def _worker_routine(self, worker_id: int):
        """Queue se task nikalta hai aur execute karta hai।"""
        logger.debug(f"Worker {worker_id} shuru hua।")
        while self._running:
            try:
                # 30 second timeout, taki stop hone par zyada der tak na ruke
                priority, task_coro = await asyncio.wait_for(self._queue.get(), timeout=30.0) 
            except asyncio.TimeoutError:
                # Timeout ke baad check karein ki shutdown ho raha hai ya nahi
                continue 
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} mein queue fetch error: {e}")
                continue
            
            # Agar yeh stop dummy task hai, toh loop se nikal jaayein
            if task_coro == self._stop_dummy:
                break
                
            try:
                # Task execute karein
                await task_coro
                self._queue.task_done()
            except asyncio.CancelledError:
                 # Agar task execute hone ke dauran cancel hua
                 self._queue.task_done()
                 break 
            except Exception as e:
                logger.error(f"Worker {worker_id} mein task execution error (P{priority}): {e}", exc_info=True)
                self._queue.task_done()

        logger.debug(f"Worker {worker_id} band hua।")

    def submit_task(self, priority: int, coro: Callable[..., Any]):
        """
        Priority ke saath ek naya task queue mein daalata hai।
        """
        if not self._running:
            logger.warning("Task submit nahi kiya gaya: PriorityQueue running nahi hai।")
            return
        
        # Tuple: (priority, insertion_order, task)
        # Insertion order ensure karega ki same priority tasks FIFO mein rahen
        item = (priority, asyncio.get_running_loop().time(), coro)
        self._queue.put_nowait(item)
        logger.debug(f"Task (P{priority}) queue mein submit hua. Size: {self._queue.qsize()}")
        
    def get_queue_size(self) -> int:
        """Queue ka current size return karta hai।"""
        return self._queue.qsize()

# Global Priority Queue instance
priority_queue = PriorityQueueSystem(max_workers=25) 
