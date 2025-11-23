# token_manager.py
import asyncio
import logging
import random
from typing import Dict, List, Set, Tuple
from datetime import datetime, timedelta, timezone

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.client.default import DefaultBotProperties

logger = logging.getLogger("bot.token_manager")

class TokenManager:
    """
    Manages multiple bot tokens for load balancing and flood control.
    """
    
    def __init__(self, bot_tokens: str, fallback_tokens: List[str]):
        # All available tokens (Primary + Alternates)
        all_tokens = [bot_tokens] + fallback_tokens 
        if not all_tokens:
            raise ValueError("No bot tokens provided.")
            
        self.tokens = [t.strip() for t in all_tokens if t.strip()]
        self.bots: Dict[str, Bot] = {}
        # Stores flooded tokens: {token: recovery_time}
        self.flooded_tokens: Dict[str, datetime] = {} 
        # Stores bot assignment: {user_id: token}
        self.user_token_map: Dict[int, str] = {}
        self.bot_lock = asyncio.Lock()
        
        # Initialize Bot objects (Do not modify existing Bot properties)
        for token in self.tokens:
            if token not in self.bots:
                self.bots[token] = Bot(
                    token=token, 
                    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
                )
        
        if not self.bots:
             raise ValueError("No valid Bot objects could be initialized.")

        self.available_tokens = set(self.bots.keys())
        self.logger_info()

    def logger_info(self):
        logger.info(f"TokenManager initialized with {len(self.bots)} bot tokens.")
        for i, token in enumerate(self.bots.keys()):
            logger.info(f"  Bot {i+1}: Token ending in ...{token[-4:]}")

    async def get_bot(self, user_id: int | None = None) -> Bot:
        """
        Retrieves a non-flooded bot instance, prioritizing the user's assigned bot.
        """
        async with self.bot_lock:
            # 1. Clean up expired floods
            self._cleanup_flooded_tokens()
            
            # 2. Try to get the user's currently assigned bot
            if user_id in self.user_token_map and self.user_token_map[user_id] in self.available_tokens:
                token = self.user_token_map[user_id]
                logger.debug(f"User {user_id} assigned to existing token ...{token[-4:]}")
                return self.bots[token]
            
            # 3. Choose a new token (Round-robin or random from available)
            available_list = list(self.available_tokens)
            if not available_list:
                # All tokens are flooded, fall back to a random one, hoping for recovery
                token = random.choice(list(self.bots.keys()))
                logger.warning(f"All tokens are flooded. Falling back to ...{token[-4:]}")
            else:
                # Simple random choice for load distribution
                token = random.choice(available_list)

            # 4. Assign and return
            if user_id is not None:
                self.user_token_map[user_id] = token
            logger.debug(f"User {user_id} assigned to new token ...{token[-4:]}")
            return self.bots[token]
            
    async def get_main_bot(self) -> Bot:
        """Always return the first bot for main operations (webhook setup, admin msgs etc.)"""
        return next(iter(self.bots.values()))

    def _cleanup_flooded_tokens(self):
        """Removes tokens from the flooded list if their recovery time has passed."""
        now = datetime.now(timezone.utc)
        keys_to_remove = [t for t, rt in self.flooded_tokens.items() if rt < now]
        
        for token in keys_to_remove:
            self.flooded_tokens.pop(token, None)
            self.available_tokens.add(token)
            logger.info(f"Token ...{token[-4:]} recovered from flood limit.")

    async def mark_token_flooded(self, token: str, retry_after: int = 60):
        """Marks a token as flooded and removes it from the available set."""
        async with self.bot_lock:
            recovery_time = datetime.now(timezone.utc) + timedelta(seconds=retry_after + 5)
            self.flooded_tokens[token] = recovery_time
            self.available_tokens.discard(token)
            logger.warning(f"Token ...{token[-4:]} marked as flooded. Recovery at {recovery_time.strftime('%H:%M:%S')}. Available tokens remaining: {len(self.available_tokens)}")
            
            # Remove all users currently assigned to this flooded token
            users_to_unassign = [u for u, t in self.user_token_map.items() if t == token]
            for user_id in users_to_unassign:
                self.user_token_map.pop(user_id, None)

    async def close_all_sessions(self):
        """Closes all underlying bot sessions."""
        for token, bot in self.bots.items():
            try:
                if bot.session:
                    await bot.session.close()
                    logger.info(f"Bot session ...{token[-4:]} closed.")
            except Exception as e:
                logger.error(f"Error closing bot session ...{token[-4:]}: {e}")
