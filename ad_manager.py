# ad_manager.py
# -*- coding: utf-8 -*-
import logging
import asyncio
import random
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import Any

logger = logging.getLogger("bot.ads")

async def send_sponsor_ad(user_id: int, bot: Bot, db: Any, redis_cache: Any):
    """
    Sponsor message logic.
    Configured for: High Frequency but Natural Feel.
    Logic: 5 Minute Cooldown + 70% Probability when cooldown is off.
    """
    try:
        # 1. Frequency Control (Flood limit check)
        if redis_cache and redis_cache.is_ready():
            ad_lock_key = f"ad_limit:{user_id}"
            
            # Check lock (Agar user ne pichle 5 minute me ad dekha hai to skip)
            if await redis_cache.get(ad_lock_key):
                return 

        # 2. Randomness Logic (Har movie ke sath na mile, lekin aksar mile)
        # 0.3 matlab 30% chance hai ki Ad SKIP ho jayega.
        # 70% chance hai ki Ad DIKHEGA.
        if random.random() < 0.3:
            return

        # 3. Get Random Ad from Database
        ad = await db.get_random_ad()
        if not ad:
            return # Agar DB me koi ad nahi hai

        # 4. Set Lock (Ad dikhane ja rahe hain, to ab 5 min ka lock laga do)
        if redis_cache and redis_cache.is_ready():
            await redis_cache.set(f"ad_limit:{user_id}", "active", ttl=300) # 300 seconds = 5 Minutes

        # 5. Format Message (Better UI)
        # HTML ParseMode use karenge taaki bold/formatting achi dikhe
        # Borders add kiye hain taaki content se alag dikhe
        text = (
            f"📢 <b>SPONSORED ADVERTISEMENT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{ad['text']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        # Button Logic (Agar button text aur URL hai)
        kb = None
        if ad.get('btn_text') and ad.get('btn_url'):
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                # Button par bhi thoda decoration
                InlineKeyboardButton(text=f"✨ {ad['btn_text']} ↗️", url=ad['btn_url'])
            ]])

        # 6. Send Message
        # disable_web_page_preview=True kiya hai taaki sirf button dikhe, link ka preview gandagi na kare
        await bot.send_message(user_id, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        
        # Track View Analytics
        if hasattr(db, 'track_event'):
            asyncio.create_task(db.track_event("ad_view", extra_id=ad.get('ad_id')))
            
        logger.info(f"Ad sent to user {user_id}")

    except Exception as e:
        logger.warning(f"Ad delivery failed for {user_id}: {e}")
