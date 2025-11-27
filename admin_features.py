# admin_features.py
import asyncio
import logging
import json
import io
from typing import Literal
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import BufferedInputFile
from core_utils import safe_tg_call, safe_db_call, TELEGRAM_COPY_SEMAPHORE
from bot import AdminFilter, executor # executor import karein

logger = logging.getLogger("bot.admin_commands")

# Admin Router alag se banayein
admin_router = Router(name="admin_features")

# --- Helper Function (UserID nikalne ke liye) ---
def _parse_user_id(message: types.Message) -> int | None:
    """Command args ya reply se user ID nikalta hai।"""
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id
        
    args = message.text.split(maxsplit=1)
    if len(args) == 2 and args[1].isdigit():
        return int(args[1])
        
    return None
    
# =======================================================
# +++++ FEATURE 1: USER EXPORT +++++
# =======================================================

@admin_router.message(Command("export_users"), AdminFilter())
async def export_users_command(message: types.Message, db_primary):
    user_id = message.from_user.id
    
    msg = await safe_tg_call(
        message.answer("⏳ Sabhi users ki list (ID, Username) MongoDB se fetch ki ja rahi hai..."), 
        semaphore=TELEGRAM_COPY_SEMAPHORE
    )
    if not msg: return
    
    # DB call: user_id aur username nikalega
    user_list_raw = await safe_db_call(db_primary.export_all_users_for_json(), timeout=600, default=[])
    
    if not user_list_raw:
        await safe_tg_call(msg.edit_text("❌ Database mein koi users nahi mile ya DB Error।"))
        return
        
    # CPU-bound JSON dump ko executor mein run karein
    loop = asyncio.get_running_loop()
    try:
        json_data_bytes = await loop.run_in_executor(
            executor, 
            lambda: json.dumps(user_list_raw, indent=2).encode('utf-8')
        )
        
        fio = io.BytesIO(json_data_bytes)
        fio.name = f"bot_users_{len(user_list_raw)}_{message.date.strftime('%Y%m%d_%H%M')}.json"
        
        await safe_tg_call(msg.edit_text(f"✅ {len(user_list_raw):,} Users ka data taiyaar hai. File bheji ja rahi hai..."))
        
        # JSON file ko document ke roop mein bhej dein
        await safe_tg_call(
            message.answer_document(
                document=BufferedInputFile(fio.getvalue(), filename=fio.name),
                caption=f"✅ User Export: {len(user_list_raw):,} records.",
            ), 
            semaphore=TELEGRAM_COPY_SEMAPHORE
        )
        
        await safe_tg_call(msg.delete())

    except Exception as e:
        logger.error(f"User export failed: {e}", exc_info=True)
        await safe_tg_call(msg.edit_text(f"❌ User export karte waqt error: {e}"))

# =======================================================
# +++++ FEATURE 2: PERMANENT BAN USER +++++
# =======================================================

@admin_router.message(Command("ban_user"), AdminFilter())
async def ban_user_command(message: types.Message, db_primary):
    target_id = _parse_user_id(message)
    
    if not target_id:
        await safe_tg_call(
            message.answer("❌ Istemal: /ban_user `USER_ID` ya user ke message ko reply karein।"), 
            semaphore=TELEGRAM_COPY_SEMAPHORE
        )
        return
        
    if target_id == message.from_user.id:
        await safe_tg_call(message.answer("❌ Khud ko ban nahi kar sakte।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    # Ban status check karein
    is_banned = await safe_db_call(db_primary.is_user_banned(target_id), default=False)
    
    if is_banned:
        # Unban karein
        success = await safe_db_call(db_primary.set_user_ban_status(target_id, False), default=False)
        if success:
            await safe_tg_call(message.answer(f"✅ User ID <code>{target_id}</code> ko <b>Unban</b> kar diya gaya hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            await safe_tg_call(message.answer(f"❌ User ID <code>{target_id}</code> ko unban karne mein DB error aayi।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        # Ban karein
        success = await safe_db_call(db_primary.set_user_ban_status(target_id, True), default=False)
        if success:
            await safe_tg_call(message.answer(f"✅ User ID <code>{target_id}</code> ko <b>Permanently Ban</b> kar diya gaya hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            await safe_tg_call(message.answer(f"❌ User ID <code>{target_id}</code> ko ban karne mein DB error aayi।"), semaphore=TELEGRAM_COPY_SEMAPHORE)

