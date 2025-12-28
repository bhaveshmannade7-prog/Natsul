# ad_manager.py
import logging
import asyncio
import random # Fix: Random import add kiya
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import Database

logger = logging.getLogger("bot.ads")

async def send_sponsor_ad(user_id: int, bot: Bot, db: Database, redis_cache):
    """
    Sponsor message logic isolated from download flow.
    Fixed for Higher Frequency (High Chance).
    """
    # 1. Frequency Control
    if redis_cache.is_ready():
        ad_lock_key = f"ad_limit:{user_id}"
        
        # Check lock (Agar user ne abhi add dekha hai)
        if await redis_cache.get(ad_lock_key):
            return # Skip if user saw ad recently
            
        # FIX: Randomness Logic (Add Aane ke chance badha diye - 80%)
        # Agar 0.2 (20%) se kam aaya toh skip karo (natural feel ke liye)
        # Agar aapko 100% chahiye to ye if block hata sakte hain
        if random.random() < 0.2:
            return

        # FIX: Lock Duration Reduced
        # Pehle 1800 (30 min) tha, ab 300 (5 min) kar diya hai.
        # Isse ads jyada frequent aayenge.
        await redis_cache.set(ad_lock_key, "active", ttl=300)

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
