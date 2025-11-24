# token_manager.py
import asyncio
import logging
import time
from typing import List, Dict, Optional
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

logger = logging.getLogger("bot.token_manager")

class TokenManager:
    """
    Bot token pool ko manage karta hai.
    Rate limit (429) par token ko quiet-switch karta hai.
    """
    
    def __init__(self, bot_tokens: List[str]):
        if not bot_tokens:
            raise ValueError("TokenManager ko shuru karne ke liye koi token nahi mila.")
            
        self.raw_tokens = bot_tokens
        
        # Key: Token (str), Value: Status Dict
        # Status Dict: {'bot': Bot, 'cooldown_until': float}
        self.token_pool: Dict[str, Dict] = {}
        self._init_pool()
        
        self.next_token_index = 0
        self.pool_size = len(self.raw_tokens)
        logger.info(f"TokenManager: {self.pool_size} tokens ke saath initialize kiya.")

    def _init_pool(self):
        """Tokens ko Bot instances mein badalta hai aur pool banata hai।"""
        for token in self.raw_tokens:
            self.token_pool[token] = {
                'bot': Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML)),
                'cooldown_until': 0.0, # Jab tak token rate-limited nahi hota
                'last_used': time.time()
            }

    def get_token_info_by_token(self, token: str) -> Optional[Dict]:
        """Token string se uska info dict nikalta hai।"""
        return self.token_pool.get(token)

    def select_bot(self) -> Bot:
        """
        Token rotation:
        1. Round-Robin se token chunein.
        2. Agar token cooldown par hai, agla token chunein.
        3. Agar sab cooldown par hain, sabse jaldi free hone waale token ka istemal karein.
        """
        
        # Best candidate (round-robin)
        start_index = self.next_token_index
        
        for _ in range(self.pool_size):
            token_key = self.raw_tokens[self.next_token_index]
            info = self.token_pool[token_key]
            
            # Rotation
            self.next_token_index = (self.next_token_index + 1) % self.pool_size
            
            if info['cooldown_until'] < time.time():
                # Cooldown nahi hai, yeh best hai
                info['last_used'] = time.time()
                return info['bot']
            
            # Agar hum loop kar chuke hain aur koi ready nahi mila, ab ruk jaayein
            if self.next_token_index == start_index:
                 break
        
        # Agar koi ready nahi hai, sabse kam cooldown waale ka istemal karein (Blocking nahi)
        # Yeh sirf webhook/broadcast ke liye hai, normal calls ke liye `safe_tg_call` handle karega.
        best_bot = self.token_pool[self.raw_tokens[start_index]]['bot'] 
        logger.warning(f"Sabhi tokens cooldown par hain. Round-robin se token {start_index} chuna. (Non-blocking)")
        return best_bot

    async def mark_token_flood_wait(self, bot_instance: Bot, retry_after: float):
        """
        Token ko rate-limited mark karta hai.
        """
        for token, info in self.token_pool.items():
            if info['bot'] == bot_instance:
                cooldown_until = time.time() + retry_after + 1.0 # Buffer add karein
                info['cooldown_until'] = cooldown_until
                logger.warning(f"Token ending {token[-4:]} flood-wait par set kiya gaya hai. Cooldown: {retry_after:.1f}s.")
                # Current bot ko flush karein, agla bot round-robin se chunein
                break

    async def close_all_sessions(self):
        """Sabhi bot sessions ko close karta hai।"""
        for info in self.token_pool.values():
            try:
                if info['bot'].session:
                    await info['bot'].session.close()
            except Exception as e:
                logger.error(f"Bot session close karte waqt error: {e}")
        logger.info("TokenManager: Sabhi bot sessions close ho gaye।")


async def token_manager_safe_tg_call(
    coro: asyncio.Future, 
    token_manager: TokenManager, 
    timeout: int, 
    semaphore: asyncio.Semaphore | None = None
):
    pass
