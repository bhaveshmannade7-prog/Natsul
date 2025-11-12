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

# --- Load dotenv FIRST ---
from dotenv import load_dotenv
load_dotenv()

# --- Uvloop activation (AFTER dotenv) ---
try:
    import uvloop
    uvloop.install()
    logging.info("Uvloop installed successfully.")
except ImportError:
    logging.info("Uvloop not found, using default asyncio event loop.")

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.client.default import DefaultBotProperties

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Updated Database Imports ---
from database import Database
from neondb import NeonDB

# --- Helpers ---
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

def clean_text_for_search(text: str) -> str:
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
# --- End Helpers ---


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)-12s %(message)s")
logger = logging.getLogger("bot")
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))

JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "thegreatmoviesl9").replace("@", "")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "MOVIEMAZASU").replace("@", "")

# --- TRIPLE DB CONFIG ---
DATABASE_URL = os.getenv("DATABASE_URL") # Primary Mongo
SECONDARY_DATABASE_URL = os.getenv("SECONDARY_DATABASE_URL") # Secondary Mongo (Load Balancing)
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL") # Postgres/NeonDB

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "")
ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

# ============ TIMEOUTS ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 10
TG_OP_TIMEOUT = 5

# ============ SEMAPHORE ============
DB_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_FORWARD_SEMAPHORE = asyncio.Semaphore(15)

# --- Critical Configuration Checks ---
if not BOT_TOKEN: raise SystemExit("Missing BOT_TOKEN!")
if not DATABASE_URL: raise SystemExit("Missing DATABASE_URL!")
if not NEON_DATABASE_URL: raise SystemExit("Missing NEON_DATABASE_URL!")
if not SECONDARY_DATABASE_URL: logger.warning("SECONDARY_DATABASE_URL missing. Failover capability reduced.")


# --- Webhook URL ---
def build_webhook_url() -> str:
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        webhook_path = f"/bot/{BOT_TOKEN}"
        if base.endswith('/bot'): base = base.rsplit('/bot', 1)[0]
        elif base.endswith('/bot/'): base = base.rsplit('/bot/', 1)[0]
        return f"{base}{webhook_path}"
    return ""

WEBHOOK_URL = build_webhook_url()

# Initialize Bot and Dispatcher
try:
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
except Exception as e:
    raise SystemExit(f"Bot Init Failed: {e}")

# Initialize Databases (Primary, Secondary, Neon)
try:
    db_primary = Database(DATABASE_URL, db_name_suffix="Primary")
    db_secondary = Database(SECONDARY_DATABASE_URL or DATABASE_URL, db_name_suffix="Secondary") # Fallback to Primary if no secondary
    neondb = NeonDB(NEON_DATABASE_URL)
    logger.info("All Database objects created.")
except Exception as e:
    raise SystemExit(f"DB Object Creation Failed: {e}")


start_time = datetime.now(timezone.utc)
monitor_task = None
executor = None

# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure(loop):
    logger.info("Initiating graceful shutdown...")
    if monitor_task and not monitor_task.done(): monitor_task.cancel()
    if WEBHOOK_URL:
        try: await bot.delete_webhook(drop_pending_updates=True)
        except: pass
    try: await bot.session.close()
    except: pass
    if executor: executor.shutdown(wait=True)
    try:
        if neondb: await neondb.close()
    except: pass
    logger.info("Shutdown complete.")


def handle_shutdown_signal(signum, frame):
    try:
        loop = asyncio.get_running_loop()
        asyncio.ensure_future(shutdown_procedure(loop), loop=loop)
    except: pass

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# ============ SAFE WRAPPERS ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                if args and isinstance(args[0], (types.Message, types.CallbackQuery)):
                    try: await args[0].answer("⚠️ Timeout.", show_alert=False) if isinstance(args[0], types.CallbackQuery) else await args[0].answer("⚠️ Request timeout.")
                    except: pass
            except Exception as e: logger.error(f"Handler Error: {e}")
        return wrapper
    return decorator

async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except Exception as e:
         logger.error(f"DB error: {e}")
         return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore = None):
    try:
        if semaphore:
            async with semaphore:
                await asyncio.sleep(0.5)
                return await asyncio.wait_for(coro, timeout=timeout)
        return await asyncio.wait_for(coro, timeout=timeout)
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError) as e:
        logger.warning(f"TG API Error: {e}")
        return None if "message is not modified" in str(e) else False
    except Exception as e:
        logger.error(f"TG Error: {e}"); return None


# ============ FILTERS & HELPERS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    delta = datetime.now(timezone.utc) - start_time; total_seconds = int(delta.total_seconds())
    days, r = divmod(total_seconds, 86400); hours, r = divmod(r, 3600); minutes, seconds = divmod(r, 60)
    return f"{days}d{hours}h{minutes}m" if days > 0 else f"{hours}h{minutes}m"

async def check_user_membership(user_id: int) -> bool:
    check_channel = bool(JOIN_CHANNEL_USERNAME)
    check_group = bool(USER_GROUP_USERNAME)
    if not check_channel and not check_group: return True

    try:
        tasks = []
        if check_channel: tasks.append(safe_tg_call(bot.get_chat_member(f"@{JOIN_CHANNEL_USERNAME}", user_id), timeout=5))
        if check_group: tasks.append(safe_tg_call(bot.get_chat_member(f"@{USER_GROUP_USERNAME}", user_id), timeout=5))
        results = await asyncio.gather(*tasks)
        
        valid = {"member", "administrator", "creator"}
        if check_channel and (not results[0] or results[0].status not in valid): return False
        if check_group:
            idx = 1 if check_channel else 0
            if not results[idx] or results[idx].status not in valid: return False
        return True
    except: return False


def get_join_keyboard():
    buttons = []
    if JOIN_CHANNEL_USERNAME: buttons.append([InlineKeyboardButton(text="📢 Channel Join", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")])
    if USER_GROUP_USERNAME: buttons.append([InlineKeyboardButton(text="👥 Group Join", url=f"https://t.me/{USER_GROUP_USERNAME}")])
    if buttons: buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

def get_full_limit_keyboard():
    if not ALTERNATE_BOTS: return None
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🚀 @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS])

def extract_movie_info(caption: str):
    if not caption: return None
    info = {}; lines = caption.splitlines(); title = lines[0].strip() if lines else ""
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]): title += " " + lines[1].strip()
    if title: info["title"] = title
    imdb = re.search(r"(tt\d{7,})", caption); year = re.findall(r"\b(19[89]\d|20[0-2]\d)\b", caption)
    if imdb: info["imdb_id"] = imdb.group(1)
    if year: info["year"] = year[-1]
    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str]:
    if not filename: return {"title": "Untitled", "year": None}
    year = None
    match_paren = re.search(r"\(((19[89]\d|20[0-3]\d))\)", filename)
    if match_paren: year = match_paren.group(1)
    else:
        matches_bare = re.findall(r"\b((19[89]\d|20[0-3]\d))\b", filename)
        if matches_bare: year = matches_bare[-1][0]
    title = os.path.splitext(filename)[0].strip()
    if year: title = re.sub(rf"(\s*\(?{year}\)?\s*)$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\[.*?\]|\(.*?\)|[._]", " ", title).strip()
    return {"title": title or "Untitled", "year": year}

def overflow_message(active_users: int) -> str: return f"⚠️ Server Busy ({active_users}/{CURRENT_CONC_LIMIT}). Nayi requests hold par hain."

async def monitor_event_loop():
    while True:
        await asyncio.sleep(60)

# ============ LIFESPAN ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global executor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    
    # Init All 3 DBs
    await asyncio.gather(
        db_primary.init_db(),
        db_secondary.init_db(),
        neondb.init_db()
    )
    logger.info("Databases Initialized (Mongo P+S, Neon).")

    if WEBHOOK_URL:
        await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types(), secret_token=(WEBHOOK_SECRET or None), drop_pending_updates=True)
    
    yield
    await shutdown_procedure(asyncio.get_running_loop())

app = FastAPI(lifespan=lifespan)

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET: raise HTTPException(403)
    try: background_tasks.add_task(dp.feed_update, bot=bot, update=Update(**update)); return {"ok": True}
    except Exception as e: return {"ok": False, "error": str(e)}

@app.get("/")
@app.get("/health")
async def health_check():
    p_ok = await safe_db_call(db_primary.is_ready(), default=False)
    s_ok = await safe_db_call(db_secondary.is_ready(), default=False)
    n_ok = await safe_db_call(neondb.is_ready(), default=False)
    return {"status": "ok", "primary_mongo": p_ok, "secondary_mongo": s_ok, "neon_db": n_ok, "uptime": get_uptime()}

async def ensure_capacity_or_inform(obj) -> bool:
    user = obj.from_user
    if not user: return True
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    if user.id == ADMIN_USER_ID: return True
    active = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=CURRENT_CONC_LIMIT + 1)
    if active >= CURRENT_CONC_LIMIT:
        if isinstance(obj, types.Message): await safe_tg_call(obj.answer(overflow_message(active), reply_markup=get_full_limit_keyboard()))
        else: await safe_tg_call(obj.answer("Server busy.", show_alert=False))
        return False
    return True

# ============ HANDLERS ============

@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user = message.from_user
    if not user: return
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))

    if user.id == ADMIN_USER_ID:
        c_p = await safe_db_call(db_primary.get_movie_count(), default=-1)
        c_s = await safe_db_call(db_secondary.get_movie_count(), default=-1)
        c_n = await safe_db_call(neondb.get_movie_count(), default=-1)
        await safe_tg_call(message.answer(
            f"👑 <b>Admin Panel</b>\n\n"
            f"<b>Databases:</b>\n"
            f"🟢 Mongo Primary: {c_p:,}\n"
            f"🟡 Mongo Secondary: {c_s:,}\n"
            f"🔵 NeonDB: {c_n:,}\n\n"
            f"<b>Sync Commands:</b>\n"
            f"/sync_secondary (Primary -> Secondary)\n"
            f"/sync_mongo_to_neon (Primary -> Neon)\n"
        ))
        return

    if not await ensure_capacity_or_inform(message): return
    is_member = await check_user_membership(user.id)
    if is_member:
        await safe_tg_call(message.answer(f"🎬 Hello <b>{user.first_name}</b>!\nMovie ka naam bhejein (e.g., <code>Jawan</code>)."))
    else:
        await safe_tg_call(message.answer("👋 Welcome! Access ke liye channels join karein.", reply_markup=get_join_keyboard()))

@dp.callback_query(F.data == "check_join")
async def check_join_callback(callback: types.CallbackQuery):
    if await check_user_membership(callback.from_user.id):
        await safe_tg_call(callback.message.edit_text("✅ Verified! Ab movie search karein."))
    else:
        await safe_tg_call(callback.answer("Join nahi kiya!", show_alert=True))

# =======================================================
# +++++ SMART MULTI-DB SEARCH HANDLER (FAILOVER) +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20)
async def search_movie_handler(message: types.Message):
    user = message.from_user
    if not await ensure_capacity_or_inform(message): return
    query = message.text.strip()
    if len(query) < 2: return await message.answer("⚠️ Query too short.")

    searching_msg = await safe_tg_call(message.answer(f"⚡️ <b>{query}</b> dhoond raha hu..."))
    if not searching_msg: return

    results = []
    source = ""
    
    # Strategy: Primary -> Secondary -> Neon -> Fallback
    
    # 1. Try Primary Mongo
    try:
        if await db_primary.is_ready():
            results = await db_primary.mongo_search_internal(query, limit=20)
            if results: source = "Primary DB"
    except Exception: pass

    # 2. Try Secondary Mongo (Load Balance / Failover)
    if not results:
        try:
            if await db_secondary.is_ready():
                results = await db_secondary.mongo_search_internal(query, limit=20)
                if results: source = "Secondary DB"
        except Exception: pass

    # 3. Try NeonDB (Ultimate Backup)
    if not results:
        try:
            if await neondb.is_ready():
                results = await neondb.neondb_search(query, limit=20)
                if results: source = "Neon DB"
        except Exception: pass

    if not results:
        await safe_tg_call(searching_msg.edit_text(f"❌ '<b>{query}</b>' nahi mili."))
        return

    buttons = []
    for m in results[:15]:
        title = m["title"][:50]
        year = f" ({m['year']})" if m.get('year') else ""
        buttons.append([InlineKeyboardButton(text=f"{title}{year}", callback_data=f"get_{m['imdb_id']}")])

    await safe_tg_call(searching_msg.edit_text(
        f"🎬 <b>{query}</b> - {len(results)} results ({source}):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    ))

@dp.callback_query(F.data.startswith("get_"))
async def get_movie_callback(callback: types.CallbackQuery):
    imdb_id = callback.data.split("_", 1)[1]
    
    # Fetch movie details (Try Primary, then Secondary)
    movie = await db_primary.get_movie_by_imdb(imdb_id)
    if not movie: movie = await db_secondary.get_movie_by_imdb(imdb_id)
    
    if not movie:
        return await safe_tg_call(callback.message.edit_text("❌ Movie details missing (DB sync error)."))

    await safe_tg_call(callback.answer("Sending..."))
    
    # --- CRITICAL UPGRADE: HIDE FORWARD TAG ---
    # Use copy_message instead of forward_message
    
    sent = False
    if movie.get("channel_id") and movie.get("message_id"):
        try:
            # copy_message behaves like the bot sent it directly (No 'Forwarded from')
            await bot.copy_message(
                chat_id=callback.from_user.id,
                from_chat_id=movie["channel_id"],
                message_id=movie["message_id"],
                caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'N/A'})\n\n🤖 @{str((await bot.get_me()).username)}",
                parse_mode=ParseMode.HTML
            )
            sent = True
        except Exception as e:
            logger.warning(f"Copy failed ({e}), trying standard document send...")
    
    # Fallback: Send by File ID if copy fails
    if not sent and movie.get("file_id"):
        try:
            await bot.send_document(
                chat_id=callback.from_user.id,
                document=movie["file_id"],
                caption=f"🎬 <b>{movie['title']}</b>\n(Backup Source)"
            )
            sent = True
        except Exception: pass
    
    if sent:
        await safe_tg_call(callback.message.delete()) # Remove search result to clean chat
    else:
        await safe_tg_call(callback.message.edit_text("❌ File bhejne mein error aaya (File removed or restricted)."))

# --- ADMIN MIGRATION & SYNC ---

@dp.message(AdminFilter(), F.forward_from_chat)
async def migration_handler(message: types.Message):
    if message.forward_from_chat.id != LIBRARY_CHANNEL_ID: return
    if not (message.video or message.document): return

    info = extract_movie_info(message.caption or "")
    if not info: return await message.answer("❌ Caption parse error.")

    file_data = message.video or message.document
    imdb = info.get("imdb_id") or f"auto_{message.forward_from_message_id}"
    title, year = info["title"], info.get("year")
    clean = clean_text_for_search(title)
    
    # Save to ALL DBs (Redundancy)
    r1 = await db_primary.add_movie(imdb, title, year, file_data.file_id, message.forward_from_message_id, message.forward_from_chat.id, clean, file_data.file_unique_id)
    r2 = await db_secondary.add_movie(imdb, title, year, file_data.file_id, message.forward_from_message_id, message.forward_from_chat.id, clean, file_data.file_unique_id)
    r3 = await neondb.add_movie(message.forward_from_message_id, message.forward_from_chat.id, file_data.file_id, file_data.file_unique_id, imdb, title)

    await message.answer(f"✅ <b>{title}</b> Migrated.\nPrimary: {r1} | Secondary: {r2} | Neon: {r3}")

@dp.message(Command("sync_secondary"), AdminFilter())
async def sync_secondary_command(message: types.Message):
    msg = await message.answer("⏳ Syncing Primary -> Secondary Mongo...")
    movies = await db_primary.get_all_movies_raw()
    if not movies: return await msg.edit_text("❌ No data in Primary.")
    
    count = await db_secondary.add_movie_batch(movies)
    await msg.edit_text(f"✅ Synced {count} movies to Secondary DB.")

@dp.message(Command("sync_mongo_to_neon"), AdminFilter())
async def sync_neon_command(message: types.Message):
    msg = await message.answer("⏳ Syncing Primary -> NeonDB...")
    movies = await db_primary.get_all_movies_raw()
    count = await neondb.sync_from_mongo(movies)
    await msg.edit_text(f"✅ Synced {count} movies to NeonDB.")

@dp.channel_post()
async def auto_index(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID: return
    info = extract_movie_info(message.caption or "")
    if not info: return
    
    fd = message.video or message.document
    if not fd: return
    
    imdb = info.get("imdb_id") or f"auto_{message.message_id}"
    t, y = info["title"], info.get("year")
    c = clean_text_for_search(t)
    
    # Add to all 3
    await db_primary.add_movie(imdb, t, y, fd.file_id, message.message_id, message.chat.id, c, fd.file_unique_id)
    await db_secondary.add_movie(imdb, t, y, fd.file_id, message.message_id, message.chat.id, c, fd.file_unique_id)
    await neondb.add_movie(message.message_id, message.chat.id, fd.file_id, fd.file_unique_id, imdb, t)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass
