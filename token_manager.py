# token_manager.py
import logging
import asyncio
from typing import List, Dict, Callable
from itertools import cycle
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

logger = logging.getLogger("bot.token_mgr")

# --- Default Bot Properties ---
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

class TokenManager:
    """
    Multi-token management class for non-blocking rotation on API errors।
    """
    
    def __init__(self, primary_token: str, alternate_tokens: List[str]):
        # Saare tokens ko ek list mein rakhein
        self.all_tokens = [primary_token] + alternate_tokens
        if not self.all_tokens:
            raise ValueError("TokenManager requires at least one token.")
            
        # Saare tokens ke liye Bot instances banayein
        self.bots: List[Bot] = []
        default_props = DefaultBotProperties(parse_mode=ParseMode.HTML)
        for token in self.all_tokens:
            self.bots.append(Bot(token=token, default=default_props))
            
        self.bot_cycler = cycle(self.bots)
        self.current_bot: Bot = next(self.bot_cycler)
        self.bot_map: Dict[str, Bot] = {bot.token: bot for bot in self.bots}
        self.current_index = 0
        self.lock = asyncio.Lock()
        
        logger.info(f"TokenManager initialized with {len(self.all_tokens)} bots.")
        if len(self.all_tokens) > 1:
            logger.warning(f"Alternate tokens: {len(alternate_tokens)}")
        
    def get_current_bot(self) -> Bot:
        """Non-blocking: Seedha current bot instance dega।"""
        return self.current_bot

    async def _rotate_token_internal(self):
        """Internal: Token ko rotate karta hai aur current_bot ko update karta hai।"""
        async with self.lock:
            old_token = self.current_bot.token
            self.current_bot = next(self.bot_cycler)
            self.current_index = (self.current_index + 1) % len(self.bots)
            logger.warning(f"🚨 TOKEN ROTATION: Token '{old_token[:10]}...' fail hua, ab naya bot token '{self.current_bot.token[:10]}...' istemal hoga।")

    async def safe_tg_call(self, coro: Awaitable[Any], *args, **kwargs) -> Any:
        """
        Token rotation logic ke saath API call wrapper।
        Yeh TelegramAPIError aur 429 par token ko rotate karega।
        """
        retries = len(self.bots) # Max retries jitne tokens hain
        
        for attempt in range(retries):
            bot_instance = self.get_current_bot()
            
            # --- Original safe_tg_call logic ---
            try:
                # Coroutine ko current bot instance ke saath bind karein
                # Note: Aiogram ke methods already bot instance se bound hote hain
                return await coro
                
            except (TelegramAPIError, TelegramBadRequest) as e:
                error_msg = str(e).lower()
                
                # --- 429 FLOOD WAIT (CRITICAL ROTATION POINT) ---
                if "too many requests" in error_msg or "429" in error_msg:
                    logger.warning(f"🚨 Flood Wait (429) detected for bot {bot_instance.token[:10]}... Rotating token instantly.")
                    
                    # Token ko rotate karein, non-blocking
                    await self._rotate_token_internal()
                    
                    # Thoda wait karein agar FloodWait-X second ka info ho (optional)
                    # Yahan hum maan rahe hain ki naya token turant available hoga
                    # Agar yeh aakhiri token tha, toh firse pehla token istemal hoga
                    if attempt < retries - 1:
                        # Sirf tabhi retry karein agar hamare paas naye tokens bache hon
                        await asyncio.sleep(0.5) # Chota sa backoff
                        continue
                    else:
                        # Sabhi tokens try kar liye, ab ruk jao
                        logger.error("🚫 Sabhi bot tokens fail ho gaye (Flood Wait / 429)। Operation fail.")
                        return None 
                
                # --- Other Fatal Errors ---
                elif "bot was blocked" in error_msg or "user is deactivated" in error_msg:
                    logger.info(f"TG: Bot block ya user deactivated."); return False
                elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
                    logger.info(f"TG: Chat nahi mila."); return False
                elif "message is not modified" in error_msg:
                    logger.debug(f"TG: Message modify nahi hua."); return None
                elif "message to delete not found" in error_msg or "message to copy not found" in error_msg:
                    logger.debug(f"TG: Message (delete/copy) nahi mila."); return None
                else:
                    logger.warning(f"TG Error (Non-429): {e}"); return None
            
            except asyncio.TimeoutError:
                logger.warning(f"TG call timeout for bot {bot_instance.token[:10]}..."); return None
            
            except Exception as e:
                logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None

        return None # Sabhi retries fail hone par

    async def close_sessions(self):
        """Sabhi bot sessions ko close karta hai।"""
        for bot in self.bots:
            try:
                if bot.session: await bot.session.close()
                logger.info(f"Bot session {bot.token[:10]}... close ho gaya.")
            except Exception as e:
                logger.error(f"Bot session close karte waqt error: {e}")

