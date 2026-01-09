# core_utils.py
import asyncio
import logging
from aiogram import Bot # Added this for Bot instance handling
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL SEMAPHORES & CONSTANTS ============
TG_OP_TIMEOUT = 8
DB_OP_TIMEOUT = 10 

# FIX: Reduced limits for Free Tier stability
DB_SEMAPHORE = asyncio.Semaphore(5) 
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(10) 
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(15) 
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1) 


# --- SAFE API CALL WRAPPERS (Final Fix) ---
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """
    Async database coroutine (motor, asyncpg) ko execute karta hai.
    """
    if not asyncio.iscoroutine(coro):
         logger.error(f"SAFE_DB_CALL ERROR: Non-coroutine object passed for {getattr(coro, '__name__', 'unknown_func')}")
         return default
         
    try:
        async with DB_SEMAPHORE: 
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB call timeout ({timeout}s): {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         return default


async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None, bot: Bot | None = None):
    """
    Telegram API calls ko safely execute karta hai.
    FIX: RuntimeError "method not mounted to bot instance" ko handle karta hai.
    """
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    try:
        async with semaphore_to_use:
            if semaphore: await asyncio.sleep(0.1) 
            
            # AGAR RuntimeError aaye toh bot instance ko attach karna zaroori hai
            if bot and hasattr(coro, "as_"):
                coro = coro.as_(bot)
                
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
        # Screenshot 1000074685 wala error yahan handle hoga
        logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None
