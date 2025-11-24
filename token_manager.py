import logging
import asyncio
from typing import List, Any, Callable
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramAPIError

logger = logging.getLogger("bot.token_manager")

class TokenManager:
    def __init__(self, main_bot: Bot):
        self.main_bot = main_bot
        self.bots: List[Bot] = [main_bot]
        self.current_index = 0
        self.lock = asyncio.Lock()
        self.active = False

    async def start(self, token_list_str: str):
        """Initializes extra worker bots from the comma-separated string."""
        if not token_list_str:
            return

        tokens = [t.strip() for t in token_list_str.split(",") if t.strip()]
        if not tokens:
            return

        # Initialize extra bots
        count = 0
        for token in tokens:
            # Skip if it's the main bot token to avoid duplicates
            if token == self.main_bot.token:
                continue
            try:
                new_bot = Bot(token=token)
                # Verify token validity
                await new_bot.get_me()
                self.bots.append(new_bot)
                count += 1
            except Exception as e:
                logger.error(f"Worker bot initialization failed for token ending ...{token[-5:]}: {e}")

        self.active = True
        logger.info(f"TokenManager: {count} extra worker bots loaded. Total pool: {len(self.bots)}")

    async def stop(self):
        """Closes all worker bot sessions."""
        for bot in self.bots:
            if bot != self.main_bot:
                try:
                    await bot.session.close()
                except:
                    pass
        logger.info("TokenManager: All worker sessions closed.")

    async def _get_next_bot(self) -> Bot:
        """Rotates to the next bot in a Round-Robin fashion."""
        async with self.lock:
            self.current_index = (self.current_index + 1) % len(self.bots)
            return self.bots[self.current_index]

    async def smart_copy_message(self, chat_id: int, from_chat_id: int, message_id: int, reply_markup=None) -> bool:
        """
        Tries to copy message using available bots.
        Logic:
        1. Try current bot.
        2. If 429 (Flood), switch to next bot and retry.
        3. If Forbidden (User didn't start worker), fallback to Main Bot.
        """
        # Attempt up to 3 times (or number of bots)
        max_attempts = len(self.bots) if len(self.bots) > 1 else 1
        
        # Always start with Main Bot if it's a privacy/permission sensitive call, 
        # but for load balancing we try the pool.
        # However, COPY message requires user to have started the specific bot.
        # Strategy: Try Primary First. If Flood, try others.
        
        # Attempt 1: Try with the current active bot (usually Main)
        try:
            await self.main_bot.copy_message(
                chat_id=chat_id, 
                from_chat_id=from_chat_id, 
                message_id=message_id, 
                reply_markup=reply_markup
            )
            return True
        except TelegramRetryAfter as e:
            logger.warning(f"Main Bot hit FloodWait ({e.retry_after}s). Switching to workers...")
            # Fallthrough to rotation logic
        except Exception as e:
            # Other errors (Block, etc) - return False immediately
            logger.error(f"Copy failed on Main Bot: {e}")
            return False

        # If we are here, Main Bot is flooded. Try workers.
        # We iterate through workers.
        for _ in range(max_attempts):
            bot = await self._get_next_bot()
            if bot.id == self.main_bot.id: 
                continue # We already tried main

            try:
                await bot.copy_message(
                    chat_id=chat_id, 
                    from_chat_id=from_chat_id, 
                    message_id=message_id, 
                    reply_markup=reply_markup
                )
                return True
            except TelegramForbiddenError:
                # User hasn't started this worker bot. This is expected.
                # Just continue to next bot.
                continue
            except TelegramRetryAfter:
                # This worker is also flooded. Next.
                continue
            except Exception as e:
                logger.error(f"Worker bot failed: {e}")
                continue
        
        return False

    async def smart_send_document(self, chat_id: int, document: str, reply_markup=None) -> bool:
        """Same logic as copy_message but for send_document."""
        try:
            await self.main_bot.send_document(chat_id=chat_id, document=document, reply_markup=reply_markup)
            return True
        except TelegramRetryAfter as e:
            logger.warning(f"Main Bot hit FloodWait sending doc ({e.retry_after}s). Switching...")
        except Exception:
            return False

        for _ in range(len(self.bots)):
            bot = await self._get_next_bot()
            if bot.id == self.main_bot.id: continue

            try:
                await bot.send_document(chat_id=chat_id, document=document, reply_markup=reply_markup)
                return True
            except (TelegramForbiddenError, TelegramRetryAfter):
                continue
            except Exception:
                continue
        return False
