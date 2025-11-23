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
    """Safe Telegram API call wrapper (Flood wait and error handling ke saath)"""
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    try:
        async with semaphore_to_use:
            if semaphore: await asyncio.sleep(0.05) # Small buffer between calls
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
        """Round-Robin tarike se next Bot instance deta hai।"""
        if not self._bots:
            raise RuntimeError("No active bots in the proxy!")

        # Async context se bahar hone ke karan, simple thread-safe index rotation ka use karein
        # Critical section nahi hai, isliye simple index increment ka use kar sakte hain
        bot_to_use = self._bots[self._bot_index]
        self._bot_index = (self._bot_index + 1) % len(self._bots)
        return bot_to_use

    def _get_bot_by_index(self, index: int) -> Optional[Bot]:
        """Index se specific bot instance deta hai।"""
        if 0 <= index < len(self._bots):
            return self._bots[index]
        return None

    def get_primary_bot(self) -> Bot:
        """Primary bot (index 0) return karta hai।"""
        return self._bots[0] if self._bots else self._primary_bot
        
    async def close_all_sessions(self):
        """Sabhi bot sessions ko gracefully close karta hai।"""
        for bot in self._bots:
            try:
                if bot.session:
                    await bot.session.close()
                    logger.debug(f"Bot session for '{bot.user.username}' closed।")
            except Exception as e:
                logger.error(f"Error closing bot session: {e}")
        self._bots.clear()
        self._initialized = False

    # --- PROXIED METHODS (Load Balancing yahan hota hai) ---

    async def _proxy_method(self, method_name: str, *args, **kwargs) -> Any:
        """Generic method call with load balancing/failover।"""
        
        # Agar sirf ek hi bot hai ya yeh critical task hai jisse primary bot se hi karna hai, toh skip load balancing
        if len(self._bots) == 1:
             bot_instance = self._bots[0]
        else:
             bot_instance = self._get_next_bot()
             
        coro = getattr(bot_instance, method_name)(*args, **kwargs)
        
        # safe_tg_call_proxy mein throttling aur error handling shamil hai
        result = await safe_tg_call_proxy(
            coro, 
            timeout=kwargs.pop('timeout', 10),
            semaphore=kwargs.pop('semaphore', None) # Use semaphore if passed
        )
        
        return result

    # --- Copy Message (Heavy Load) ---
    async def copy_message(self, chat_id: int, from_chat_id: int, message_id: int, **kwargs) -> Optional[types.Message | bool]:
        """Load-balanced copy_message (most critical)"""
        return await self._proxy_method("copy_message", chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id, **kwargs)

    # --- Send Message (Moderate Load) ---
    async def send_message(self, chat_id: int, text: str, **kwargs) -> Optional[types.Message | bool]:
        """Load-balanced send_message"""
        return await self._proxy_method("send_message", chat_id=chat_id, text=text, **kwargs)

    # --- Edit Message Text (Moderate Load) ---
    async def edit_message_text(self, chat_id: int, message_id: int, text: str, **kwargs) -> Optional[types.Message | bool]:
        """Load-balanced edit_message_text"""
        return await self._proxy_method("edit_message_text", chat_id=chat_id, message_id=message_id, text=text, **kwargs)

    # --- Send Document/Video (Moderate Load) ---
    async def send_document(self, chat_id: int, document: str, **kwargs) -> Optional[types.Message | bool]:
        """Load-balanced send_document"""
        return await self._proxy_method("send_document", chat_id=chat_id, document=document, **kwargs)

    # --- Answer Callback (Low Load - always use primary for safety/consistency) ---
    async def answer_callback_query(self, callback_query_id: str, **kwargs) -> Optional[bool]:
        """Primary bot se answer_callback_query (Low load)"""
        return await safe_tg_call_proxy(
            self.get_primary_bot().answer_callback_query(callback_query_id=callback_query_id, **kwargs),
            timeout=5
        )

    # --- Delete Message (Moderate Load) ---
    async def delete_message(self, chat_id: int, message_id: int, **kwargs) -> Optional[bool]:
        """Load-balanced delete_message"""
        return await self._proxy_method("delete_message", chat_id=chat_id, message_id=message_id, **kwargs)

    # --- Get Chat Member (Moderate Load) ---
    async def get_chat_member(self, chat_id: int | str, user_id: int, **kwargs) -> Optional[types.ChatMember | bool]:
        """Load-balanced get_chat_member"""
        return await self._proxy_method("get_chat_member", chat_id=chat_id, user_id=user_id, **kwargs)

    # Note: Primary bot methods (get_webhook_info, set_webhook, delete_webhook, get_file)
    # ko seedha `proxy.get_primary_bot().method()` se call kiya ja sakta hai.
