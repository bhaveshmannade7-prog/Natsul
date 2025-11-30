# core_utils.py
import asyncio
import logging
# F.I.X: inspect import ki zaroorat nahi agar hum init_db ko directly await karein
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL SEMAPHORES & CONSTANTS ============
TG_OP_TIMEOUT = 8
DB_OP_TIMEOUT = 10 

DB_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(25)
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1) 


# --- SAFE API CALL WRAPPERS (F.I.X.E.D. for robustness and clarity) ---
# F.I.X: Yahan par yeh maan rahe hain ki agar coro blocking hai to use call karne se pehle
# uske method/function ka naam pass kiya jaayega, ya agar async hai to use call karke coroutine object pass kiya jaayega.
# Blocking functions ko manually loop.run_in_executor mein dalenge.
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    # F.I.X: Agar coro awaitable nahi hai, to ise execute karne ke liye loop.run_in_executor ka use karein
    if not asyncio.iscoroutine(coro):
         # Agar yeh ek callable function/method hai (jaise init_db ko bina brackets ke pass karna), to yeh fail hoga
         # Lekin agar aapko yahan blocking function chahiye, to use manually run_in_executor mein wrap karna chahiye
         
         # F.I.X: Yahan hum man rahe hain ki user ne pehle hi coroutine object pass kiya hai
         # Agar yeh future/coroutine nahi hai, to hum isse bahar nikal jaayenge.
         logger.error(f"SAFE_DB_CALL ERROR: Non-coroutine object passed for {getattr(coro, '__name__', 'unknown_func')}")
         return default
         
    try:
        # DB_SEMAPHORE ko yahan use karein
        async with DB_SEMAPHORE: 
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB call timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         # Note: Database class ka _handle_db_error yahan call nahi ho sakta
         return default


async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None):
    # Rule: DO NOT delete, rewrite, or “optimize” ANY existing working feature
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    try:
        async with semaphore_to_use:
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
