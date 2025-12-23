# ad_manager.py
import logging
import asyncio
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import Database

logger = logging.getLogger("bot.ads")

async def send_sponsor_ad(user_id: int, bot: Bot, db: Database, redis_cache):
    """
    Sponsor message logic isolated from download flow.
    """
    # 1. Frequency Control (max 1 ad per 30 mins)
    if redis_cache.is_ready():
        ad_lock_key = f"ad_limit:{user_id}"
        if await redis_cache.get(ad_lock_key):
            return # Skip if user saw ad recently
        await redis_cache.set(ad_lock_key, "active", ttl=1800)

    # 2. Get Random Ad
    ad = await db.get_random_ad()
    if not ad:
        return

    # 3. Format Message
    text = f"📢 **SPONSORED**\n━━━━━━━━━━━━━━\n{ad['text']}"
    kb = None
    if ad.get('btn_text') and ad.get('btn_url'):
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=ad['btn_text'], url=ad['btn_url'])
        ]])

    # 4. Non-blocking Send
    try:
        await bot.send_message(user_id, text, reply_markup=kb, parse_mode="HTML")
        # Track View Analytics
        asyncio.create_task(db.track_event("ad_view", extra_id=ad['ad_id']))
    except Exception as e:
        logger.warning(f"Ad delivery failed for {user_id}: {e}")
