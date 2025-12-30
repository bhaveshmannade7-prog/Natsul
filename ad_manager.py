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
    Configured for: Very High Frequency (Aggressive Mode).
    Logic: 30 Seconds Cooldown + 90% Probability.
    """
    try:
        # 1. Frequency Control (Spam Prevention only)
        # Humne lock time 5 min se ghata kar 30 seconds kar diya hai.
        # Matlab "Lagatar" feel hoga, par bot spam nahi karega.
        if redis_cache and redis_cache.is_ready():
            ad_lock_key = f"ad_limit:{user_id}"
            
            # Check lock
            if await redis_cache.get(ad_lock_key):
                return 

        # 2. Randomness Logic (Probability Check)
        # OLD: 0.3 (30% Skip)
        # NEW: 0.1 (10% Skip) -> 90% Chance Ad Dikhega.
        if random.random() < 0.1:
            return

        # 3. Get Random Ad from Database
        ad = await db.get_random_ad()
        if not ad:
            return # Agar DB me koi ad nahi hai to ruk jao

        # 4. Set Lock (Next ad kab dikhega)
        # OLD: ttl=300 (5 Minutes)
        # NEW: ttl=30 (30 Seconds) -> User ko "Har baar" feel hoga.
        if redis_cache and redis_cache.is_ready():
            await redis_cache.set(f"ad_limit:{user_id}", "active", ttl=30) 

        # 5. Format Message (High Visibility UI)
        text = (
            f"📢 <b>SPONSORED ADVERTISEMENT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{ad['text']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        # Button Logic
        kb = None
        if ad.get('btn_text') and ad.get('btn_url'):
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text=f"✨ {ad['btn_text']} ↗️", url=ad['btn_url'])
            ]])

        # 6. Send Message
        await bot.send_message(user_id, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        
        # Track View Analytics
        if hasattr(db, 'track_event'):
            asyncio.create_task(db.track_event(user_id, "ad_view", ad_id=ad.get('ad_id')))

    except Exception as e:
        # Silent fail taaki user ka experience kharab na ho
        logger.error(f"Ad send failed for {user_id}: {e}")
