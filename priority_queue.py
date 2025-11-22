# priority_queue.py

# ... (rest of the file remains the same until priority_task_wrapper)

# --- Decorator for easy handler integration ---

def priority_task_wrapper(priority: Priority):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            
            # FIX: DP se sidha fetch karne ke bajaye, kwargs mein se fetch karein.
            # Aiogram DI system kwargs mein dependencies (jaise db_primary, redis_cache) inject karta hai, 
            # aur Dispatcher ko initialize karte waqt humne queue_wrapper ko inject kiya tha.
            
            queue_wrapper: PriorityQueueWrapper = kwargs.get('queue_wrapper')
            
            if not queue_wrapper:
                logger.warning(f"QueueWrapper nahi mila. Task '{func.__name__}' seedha chal raha hai.")
                # Agar queue nahi mila, toh blocking hone ki risk par bhi, asli function chalana zaroori hai.
                return await func(*args, **kwargs)
            
            # Agar queue_wrapper mila, toh task ko queue mein daalein.
            coro = func(*args, **kwargs)
            return await queue_wrapper.put(coro, priority=priority)

        return wrapper
    return decorator
