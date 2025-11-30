# core_utils.py
import asyncio
import logging
import inspect # New: Function/method type check ke liye
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL SEMAPHORES & CONSTANTS ============
# Rule: Semaphores ko yahan define karein taaki multiple files mein access ho sake.
TG_OP_TIMEOUT = 8
DB_OP_TIMEOUT = 10 # Database timeout is needed by safe_db_call

# Semaphores for safe concurrent DB/TG access
DB_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(25)
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1) 


# --- SAFE API CALL WRAPPERS (F.I.X.E.D.) ---
async def safe_db_call(coro_or_func, timeout=DB_OP_TIMEOUT, default=None):
    # F.I.X: Get the current event loop (executor ko use karne ke liye)
    loop = asyncio.get_running_loop()
    
    # F.I.X: Identify if the input is an awaitable coroutine or a blocking function/method
    # Agar input ek coroutine hai (jaise motor ka call), to use seedha chalao
    is_blocking_sync_call = not (asyncio.iscoroutine(coro_or_func) or inspect.isawaitable(coro_or_func))
    
    # F.I.X: Agar blocking function hai, to use executor mein daalo
    # Blocking function/method ko Future mein wrap karein
    if is_blocking_sync_call:
        # Blocking I/O ko ThreadPoolExecutor mein daalein
        # (DB methods jaise db_primary.get_user_count() ab blocking hain, unhe yahan daalein)
        # Note: coro_or_func is now the callable function (method) and its args are not needed here
        future_or_coro = loop.run_in_executor(None, coro_or_func)
    else:
        # Async coroutine/awaitable ko seedha await karein
        future_or_coro = coro_or_func

    try:
        # DB_SEMAPHORE ko yahan use karein
        async with DB_SEMAPHORE: 
            return await asyncio.wait_for(future_or_coro, timeout=timeout)
    except asyncio.TimeoutError:
        # F.I.X: Future ko cancel karna zaroori hai agar woh executor mein hai
        if is_blocking_sync_call and isinstance(future_or_coro, asyncio.Future):
            if not future_or_coro.done():
                 future_or_coro.cancel()
                 logger.warning(f"DB call timeout aur future cancel kiya gaya: {getattr(coro_or_func, '__name__', 'unknown_blocking_func')}")
        
        logger.error(f"DB call timeout: {getattr(coro_or_func, '__name__', 'unknown_coro_or_func')}")
        return default
    except Exception as e:
         logger.error(f"DB error in {getattr(coro_or_func, '__name__', 'unknown_coro_or_func')}: {e}", exc_info=True)
         return default


async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None):
    # Rule: DO NOT delete, rewrite, or “optimize” ANY existing working feature
    # Note: Telegram calls are inherently async, so no run_in_executor is needed here.
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    try:
        async with semaphore_to_use:
            # COPY_MESSAGE jaise calls mein rate-limit se bachne ke liye chota delay
            if semaphore: await asyncio.sleep(0.1) 
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError: 
        logger.warning(f"TG call timeout: {getattr(coro, '__name__', 'unknown_coro')}"); return None
    except (TelegramAPIError, TelegramBadRequest) as e:
        error_msg = str(e).lower()
        if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
            logger.info(f"TG: Bot block ya user deactivated."); return False
        elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
            logger.info(f"TG: Chat nahi mila."); return False
        elif "message is not modified" in error_msg:
            logger.debug(f"TG: Message modify nahi hua."); return None
        elif "message to delete not found" in error_msg or "message to copy not found" in error_msg:
            logger.debug(f"TG: Message (delete/copy) nahi mila."); return None
        elif "too many requests" in error_msg:
            logger.warning(f"TG: FLOOD WAIT (Too Many Requests). {e}"); await asyncio.sleep(5); return None
        else:
            logger.warning(f"TG Error: {e}"); return None
    except Exception as e:
        logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None
