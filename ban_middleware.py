# ban_middleware.py
import asyncio
import logging
from typing import Callable, Any, Dict, Awaitable
from aiogram import BaseMiddleware, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import TelegramObject
from core_utils import safe_db_call

logger = logging.getLogger("bot.ban_middleware")

class BanCheckMiddleware(BaseMiddleware):
    """
    Har incoming update par check karta hai ki user banned hai ya nahi.
    Agar user banned hai to update ko process nahi hone deta.
    """
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        user = data['event_from_user']
        # Agar user object nahi mila to skip
        if not user or not user.id:
            return await handler(event, data)
        
        user_id = user.id
        
        # Admin ko bypass karein
        admin_id = data.get('admin_id')
        if admin_id and user_id == admin_id:
            return await handler(event, data)
            
        # Context se db_primary nikalein (Dependency Injection se aayega)
        db_primary = data.get('db_primary')
        if not db_primary:
            # DB connection fail hai, log karke continue karein taaki bot hang na ho
            logger.error("BanCheckMiddleware: db_primary instance missing in DI data.")
            return await handler(event, data)
            
        # Ban status check karein
        is_banned = await safe_db_call(db_primary.is_user_banned(user_id), default=False)
        
        if is_banned:
            logger.warning(f"Banned User {user_id} ne attempt kiya: {event.update_id}")
            # User ko response bhejein ki woh banned hai (agar message ya callback ho)
            if isinstance(event, types.Message):
                try:
                    # Non-blocking Telegram call, agar user ne block kar rakha ho to fail ho jaye
                    await data['bot'].send_message(
                        user_id,
                        "🚫 **Aapko is Bot ka istemal karne se rok (ban) diya gaya hai.**\n"
                        "Kisi bhi sawaal ke liye admin se sampark karein.",
                        parse_mode="Markdown"
                    )
                except TelegramBadRequest:
                     logger.debug(f"Banned user {user_id} ne bot ko block kar rakha hai।")
                except Exception as e:
                     logger.error(f"Banned user ko notify karne mein error: {e}")
                     
            elif isinstance(event, types.CallbackQuery):
                try:
                    await event.answer("🚫 Aapko is Bot ka istemal karne se rok diya gaya hai।", show_alert=True)
                except Exception:
                    pass
            
            # Request ko aage nahi jaane dein
            return 
        
        # Agar banned nahi hai, toh aage process hone dein
        return await handler(event, data)
