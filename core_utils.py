# core_utils.py
import asyncio
import logging
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logger = logging.getLogger("bot.core_utils")

# --- GLOBAL SEMAPHORES (bot.py se move kiye gaye) ---
TG_OP_TIMEOUT = 8
DB_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(25)
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1) 

# --- SAFE API CALL WRAPPER (bot.py se move kiya gaya) ---
async def safe_db_call(coro, timeout=10, default=None):
    try:
        # DB_SEMAPHORE ko yahan import karna mushkil hai, isliye hum isse core_utils mein define kar denge.
        # Lekin DB_SEMAPHORE ka istemaal bot.py mein database.py calls ke liye hota hai. 
        # Safety ke liye, hum isse yahaan define kar denge aur bot.py mein import karenge.
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

