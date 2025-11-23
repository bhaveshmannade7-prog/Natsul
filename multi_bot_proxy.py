# multi_bot_proxy.py
import asyncio
import logging
from typing import List, Dict, Callable, Any, Optional
from aiogram import Bot, types
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
# NAYA ZAROORI IMPORT FIX:
from aiogram.client.default import DefaultBotProperties 

logger = logging.getLogger("bot.multi_bot_proxy")

# --- Error Prevention Helper (bot.py se copy kiya gaya) ---
async def safe_tg_call_proxy(
    coro: Callable[..., Any], 
    timeout: int, 
    semaphore: Optional[asyncio.Semaphore] = None
):
# ... (rest of the function is unchanged)
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    try:
        async with semaphore_to_use:
            if semaphore: await asyncio.sleep(0.05) 
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError: 
        logger.warning(f"TG call timeout: {getattr(coro, '__name__', 'unknown_coro')}"); return None
    except (TelegramAPIError, TelegramBadRequest) as e:
        error_msg = str(e).lower()
        if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
            logger.info(f"TG Proxy: Bot block ya user deactivated."); return False
        elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
            logger.info(f"TG Proxy: Chat nahi mila."); return False
        elif "message is not modified" in error_msg:
            logger.debug(f"TG Proxy: Message modify nahi hua."); return None
        elif "too many requests" in error_msg:
            logger.warning(f"TG Proxy: FLOOD WAIT (Too Many Requests). {e}"); await asyncio.sleep(5); return None
        else:
            logger.warning(f"TG Proxy Error: {e}"); return None
    except Exception as e:
        logger.exception(f"TG Proxy Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None

class MultiBotProxy:
    """
    Multiple Bot Tokens ke liye Load Balancer aur Failover Layer.
    """
    def __init__(self, primary_bot: Bot, alternate_tokens: List[str]):
        self._primary_bot = primary_bot
        self._alternate_tokens = alternate_tokens
        self._bots: List[Bot] = [primary_bot] # Primary bot is the first one
        self._bot_index = 0
        self._semaphore = asyncio.Semaphore(1)
        self._initialized = False

    # FIX: Correct type hint for DefaultBotProperties
    async def initialize_alternate_bots(self, default_properties: DefaultBotProperties):
        """Alternate tokens se Bot instances banata hai।"""
        if self._initialized:
# ... (rest of the class is unchanged)
            return

        logger.info(f"MultiBotProxy {len(self._alternate_tokens)} alternate tokens ko initialize kar raha hai...")
        
        for token in self._alternate_tokens:
            try:
                # Primary bot ke default properties use karein
                bot_instance = Bot(token=token, default=default_properties)
                self._bots.append(bot_instance)
                logger.info(f"Alternate Bot '{bot_instance.user.username}' successfully initialized.")
            except Exception as e:
                logger.error(f"Failed to initialize bot with token ending in '...{token[-5:]}': {e}")

        self._initialized = True
        logger.info(f"MultiBotProxy mein kul {len(self._bots)} active bots hain।")

    def _get_next_bot(self) -> Bot:
# ... (rest of the class is unchanged)
