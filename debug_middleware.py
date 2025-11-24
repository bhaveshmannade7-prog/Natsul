# debug_middleware.py
import logging
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import Update

logger = logging.getLogger("bot.debug_middleware")

class DebugMiddleware(BaseMiddleware):
    """
    Har incoming Telegram Update ko log karta hai.
    """
    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any],
    ) -> Any:
        # Check if it's a message or callback to avoid logging chat member updates excessively
        if event.message:
            log_id = event.message.chat.id
            log_type = "Msg"
        elif event.callback_query:
            log_id = event.callback_query.from_user.id
            log_type = "Callback"
        elif event.my_chat_member:
            log_id = event.my_chat_member.chat.id
            log_type = "ChatMember"
        else:
             log_id = "N/A"
             log_type = "Other"
             
        logger.info(f"-> INCOMING {log_type} (ID: {log_id}, Update ID: {event.update_id})")
        
        # Original handler ko execute karein
        return await handler(event, data)
