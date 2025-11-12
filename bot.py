# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import signal
import json
import hashlib
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import List, Dict
from functools import wraps
import concurrent.futures

from dotenv import load_dotenv
load_dotenv()

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Imports ---
from database import Database
from neondb import NeonDB
from secondary_db import SecondaryDB

# --- Config ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL") 
SECONDARY_DATABASE_URL = os.getenv("SECONDARY_DATABASE_URL") # New Variable
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "").replace("@", "")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "").replace("@", "")
CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)-12s %(message)s")
logger = logging.getLogger("bot")
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)

# --- Initialization ---
if not BOT_TOKEN: raise SystemExit("Missing BOT_TOKEN")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

db = Database(DATABASE_URL)
sec_db = SecondaryDB(SECONDARY_DATABASE_URL)
neondb = NeonDB(NEON_DATABASE_URL)

DB_SEMAPHORE = asyncio.Semaphore(15)
TG_OP_TIMEOUT = 10

# --- Helpers ---
def clean_text_for_search(text: str) -> str:
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except Exception as e:
        logger.error(f"TG Error: {e}")
        return None

async def check_user_membership(user_id: int) -> bool:
    if not JOIN_CHANNEL_USERNAME and not USER_GROUP_USERNAME: return True
    try:
        status = True
        if JOIN_CHANNEL_USERNAME:
            m = await bot.get_chat_member(f"@{JOIN_CHANNEL_USERNAME}", user_id)
            if m.status not in ["member", "administrator", "creator"]: status = False
        if status and USER_GROUP_USERNAME:
            m = await bot.get_chat_member(f"@{USER_GROUP_USERNAME}", user_id)
            if m.status not in ["member", "administrator", "creator"]: status = False
        return status
    except Exception:
        return True # Fail safe: allow access on error

# --- Database Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    await sec_db.connect()
    await neondb.init_db()
    webhook_url = RENDER_EXTERNAL_URL or PUBLIC_URL
    if webhook_url:
        await bot.set_webhook(f"{webhook_url}/bot/{BOT_TOKEN}", drop_pending_updates=True, secret_token=WEBHOOK_SECRET)
    yield
    await bot.session.close()
    if db.client: db.client.close()
    if sec_db.client: sec_db.client.close()
    await neondb.close()

app = FastAPI(lifespan=lifespan)

# --- Webhook ---
@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=403)
    telegram_update = Update(**update)
    background_tasks.add_task(dp.feed_update, bot=bot, update=telegram_update)
    return {"ok": True}

@app.get("/health")
async def health():
    return {
        "mongo_primary": await db.is_ready(),
        "mongo_secondary": await sec_db.is_ready(),
        "neondb": await neondb.is_ready(),
        "status": "running"
    }

# --- Search Logic (The Waterfall) ---
async def search_waterfall(query: str):
    """
    Smart Search System:
    1. Try Secondary Mongo (Search Engine)
    2. Try NeonDB (Backup Index)
    3. Try Primary Mongo (Last Resort)
    """
    results = []
    source = ""
    
    # 1. Secondary Mongo
    if await sec_db.is_ready():
        try:
            results = await sec_db.search(query)
            if results: source = "Speed DB"
        except Exception as e:
            logger.error(f"Secondary Search Failed: {e}")

    # 2. NeonDB (Failover)
    if not results and await neondb.is_ready():
        try:
            results = await neondb.neondb_search(query)
            if results: source = "Index DB"
        except Exception as e:
            logger.error(f"Neon Search Failed: {e}")

    # 3. Primary Mongo (Last Resort)
    if not results and await db.is_ready():
        try:
            results = await db.mongo_search_internal(query)
            if results: source = "Main Storage"
        except Exception as e:
            logger.error(f"Primary Search Failed: {e}")

    return results, source

# --- Handlers ---

@dp.message(CommandStart())
async def start_command(message: types.Message):
    user = message.from_user
    await db.add_user(user.id, user.username, user.first_name, user.last_name)
    
    if not await check_user_membership(user.id):
        buttons = []
        if JOIN_CHANNEL_USERNAME: buttons.append([InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")])
        if USER_GROUP_USERNAME: buttons.append([InlineKeyboardButton(text="👥 Join Group", url=f"https://t.me/{USER_GROUP_USERNAME}")])
        buttons.append([InlineKeyboardButton(text="✅ I Have Joined", callback_data="check_join")])
        await message.answer("👋 Welcome! Please join our channels to use the bot.", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        return

    await message.answer(f"🎬 <b>Namaste {user.first_name}!</b>\n\nBas movie ka naam bhejo (e.g., 'Jawan' or 'Pushpa 2021').\nBot automatically best database se search karega.")

@dp.callback_query(F.data == "check_join")
async def check_join_handler(callback: types.CallbackQuery):
    if await check_user_membership(callback.from_user.id):
        await callback.message.edit_text("✅ Verified! Ab movie ka naam bhejo.")
    else:
        await callback.answer("❌ Aapne abhi tak join nahi kiya!", show_alert=True)

@dp.message(F.text & ~F.text.startswith("/"))
async def search_handler(message: types.Message):
    user = message.from_user
    # Simple Rate Limit/Capacity Check
    active = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    if active > CONCURRENT_LIMIT and user.id != ADMIN_USER_ID:
        await message.answer("⚠️ Server busy hai. Kripya 2 minute baad try karein.")
        return

    await db.add_user(user.id, user.username, user.first_name, user.last_name)
    msg = await message.answer(f"🔍 Searching <b>{message.text}</b>...")
    
    results, source = await search_waterfall(message.text)
    
    if not results:
        await msg.edit_text(f"❌ <b>{message.text}</b> nahi mili.\nSpelling check karein ya year add karein.")
        return

    buttons = []
    for movie in results[:15]:
        title = movie['title'][:50]
        year = f" ({movie.get('year')})" if movie.get('year') else ""
        buttons.append([InlineKeyboardButton(text=f"{title}{year}", callback_data=f"dl_{movie['imdb_id']}")])

    await msg.edit_text(f"✅ <b>Found {len(results)} results</b>\nvia {source}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(F.data.startswith("dl_"))
async def download_handler(callback: types.CallbackQuery):
    imdb_id = callback.data.split("_")[1]
    await callback.answer("📂 File fetch ho rahi hai...")
    
    movie = await db.get_movie_by_imdb(imdb_id)
    if not movie:
        await callback.message.edit_text("❌ File database se delete ho gayi hai.")
        return

    caption = f"🎬 <b>{movie['title']}</b>\n📅 Year: {movie.get('year', 'N/A')}\n🤖 Via: @{callback.message.via_bot.username if callback.message.via_bot else 'Bot'}"
    
    try:
        # === MAGIC: copy_message (Hides 'Forwarded from') ===
        if movie.get('channel_id') and movie.get('message_id'):
            await bot.copy_message(
                chat_id=callback.from_user.id,
                from_chat_id=movie['channel_id'],
                message_id=movie['message_id'],
                caption=caption,
                parse_mode=ParseMode.HTML
            )
        elif movie.get('file_id'):
            await bot.send_document(
                chat_id=callback.from_user.id,
                document=movie['file_id'],
                caption=caption
            )
        else:
            await callback.message.answer("❌ File corrupted/missing.")
            return
            
        # Try to delete the search menu to keep chat clean
        try: await callback.message.delete()
        except: pass
        
    except Exception as e:
        logger.error(f"Send Error: {e}")
        await callback.message.answer(f"❌ Bhejne me error aaya. Shayad file channel se delete ho gayi hai.\nError: {str(e)[:50]}")

# --- Admin Commands ---

@dp.message(Command("stats"), lambda m: m.from_user.id == ADMIN_USER_ID)
async def stats_cmd(message: types.Message):
    u_count = await db.get_user_count()
    m_count = await db.get_movie_count()
    active = await db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES)
    
    status = f"📊 <b>Bot Stats</b>\n\n" \
             f"👥 Users: {u_count}\n" \
             f"🎬 Movies: {m_count}\n" \
             f"⚡ Active: {active}/{CONCURRENT_LIMIT}\n\n" \
             f"🟢 Primary DB: {await db.is_ready()}\n" \
             f"🟢 Secondary DB: {await sec_db.is_ready()}\n" \
             f"🟢 Neon DB: {await neondb.is_ready()}"
    await message.answer(status)

@dp.message(Command("sync_all"), lambda m: m.from_user.id == ADMIN_USER_ID)
async def sync_cmd(message: types.Message):
    msg = await message.answer("⏳ Syncing all databases... This might take time.")
    
    # Fetch all movies
    movies = await db.get_all_movies_for_sync()
    if not movies:
        await msg.edit_text("❌ No movies in Primary DB.")
        return

    # Sync Secondary Mongo
    sec_ok = await sec_db.sync_batch(movies)
    
    # Sync Neon
    neon_data = []
    for m in movies:
        # Construct data for neon (handling missing fields safely)
        neon_data.append({
            "message_id": 0, # Dummy, assume sync logic handles this inside neondb.py or just skip neon sync for now if complex
            "channel_id": 0,
            "file_id": "sync_file",
            "file_unique_id": m.get('imdb_id'), # Use IMDB as unique here
            "imdb_id": m['imdb_id'],
            "title": m['title']
        })
    # Note: Neon sync needs complete message_id/channel_id which might be heavy to fetch. 
    # Simplified sync for Secondary DB mainly.
    
    await msg.edit_text(f"✅ Sync Result:\nSecondary DB: {sec_ok}\nItems: {len(movies)}")

# --- Auto Indexer ---
@dp.channel_post()
async def index_post(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID: return
    if not (message.video or message.document): return
    
    # Simple extraction logic
    fname = message.caption or ""
    lines = fname.split('\n')
    title = lines[0][:100] if lines else "Untitled"
    clean_t = clean_text_for_search(title)
    imdb_id = f"auto_{message.message_id}"
    
    file_id = (message.video or message.document).file_id
    file_unique_id = (message.video or message.document).file_unique_id
    
    # Add to all DBs
    await db.add_movie(imdb_id, title, None, file_id, message.message_id, message.chat.id, clean_t, file_unique_id)
    await sec_db.add_movie({"imdb_id": imdb_id, "title": title, "clean_title": clean_t, "year": None})
    await neondb.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title)
    
    logger.info(f"Indexed: {title}")

if __name__ == "__main__":
    try:
        asyncio.run(dp.start_polling(bot))
    except:
        pass
