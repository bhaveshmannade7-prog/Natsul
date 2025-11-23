# priority_queue.py

# ... (rest of the file remains the same until priority_task_wrapper)

# --- Decorator for easy handler integration (FINAL FIX) ---

def priority_task_wrapper(priority: Priority):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            
            queue_wrapper = None
            
            # METHOD 1: Try accessing queue_wrapper explicitly passed in kwargs (most guaranteed path from bot.py)
            queue_wrapper = kwargs.get('queue_wrapper') 
            
            if not queue_wrapper:
                # METHOD 2: Fallback to DP access logic (Less reliable in webhook context)
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
