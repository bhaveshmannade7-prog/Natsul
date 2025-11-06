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
from typing import List, Dict, Tuple
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
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.client.default import DefaultBotProperties

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Database aur Typesense Imports ---
from database import Database
from neondb import NeonDB # <-- NAYA NEONDB
from typesense_client import (
    initialize_typesense, is_typesense_ready, 
    typesense_search, typesense_add_movie, typesense_add_batch_movies, 
    typesense_remove_movie, typesense_sync_data
)


# --- Helpers ---
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
# --- End Helpers ---


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)-12s %(message)s")
logger = logging.getLogger("bot")
# Reduce log noise
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("typesense").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING) # NeonDB logs

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))

JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "THEGREATMOVIESL9")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "MOVIEMAZASU")
BACKUP_CHANNEL_ID = os.getenv("BACKUP_CHANNEL_ID", "-1002417767287")

DATABASE_URL = os.getenv("DATABASE_URL") # MongoDB
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL") # NeonDB (Postgres)

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "")
ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

# --- NAYA: Triple Search Mode ---
SEARCH_MODES = ['typesense', 'neondb', 'mongodb']
CURRENT_SEARCH_MODE = "typesense" # Default mode

# ============ TIMEOUTS ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 10
TG_OP_TIMEOUT = 5
LONG_TG_OP_TIMEOUT = 20 # File forwarding/deleting ke liye

# ============ SEMAPHORE ============
DB_SEMAPHORE = asyncio.Semaphore(10)
NEONDB_SEMAPHORE = asyncio.Semaphore(10)

# --- Critical Configuration Checks ---
if not BOT_TOKEN:
    logger.critical("Missing BOT_TOKEN environment variable! Exiting.")
    raise SystemExit(1)
if not DATABASE_URL:
    logger.critical("Missing DATABASE_URL (MongoDB) environment variable! Exiting.")
    raise SystemExit(1)
if not NEON_DATABASE_URL:
    logger.critical("Missing NEON_DATABASE_URL (Postgres) environment variable! Exiting.")
    raise SystemExit(1)
if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID is not set. Admin commands will not work.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID is not set. Auto-indexing and Migration will not work.")

# --- Webhook URL ---
def build_webhook_url() -> str:
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        webhook_path = f"/bot/{BOT_TOKEN}"
        if base.endswith('/bot'): base = base.rsplit('/bot', 1)[0]
        elif base.endswith('/bot/'): base = base.rsplit('/bot/', 1)[0]
        final_url = f"{base}{webhook_path}"
        logger.info(f"Generated Webhook URL: {final_url}")
        return final_url
    logger.warning("No RENDER_EXTERNAL_URL or PUBLIC_URL found; webhook cannot be set.")
    return ""

WEBHOOK_URL = build_webhook_url()

# Initialize Bot and Dispatcher
try:
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    logger.info("Bot and Dispatcher initialized.")
except Exception as e:
    logger.critical(f"Failed to initialize Bot/Dispatcher: {e}", exc_info=True)
    raise SystemExit("Bot initialization failed.")

# Initialize Databases
try:
    db = Database(DATABASE_URL)
    neondb = NeonDB(NEON_DATABASE_URL)
    logger.info("Database objects (Mongo & Neon) created.")
except Exception as e:
    logger.critical(f"Failed to create Database objects: {e}", exc_info=True)
    raise SystemExit("Database object creation failed.")


start_time = datetime.now(timezone.utc)
monitor_task = None
executor = None

# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure(loop):
    logger.info("Initiating graceful shutdown...")
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try: await asyncio.wait_for(monitor_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError): logger.warning("Monitor task cancellation timed out.")
    if WEBHOOK_URL:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted.")
        except Exception as e: logger.error(f"Webhook delete error: {e}")
    try:
        if bot.session: await bot.session.close()
        logger.info("Bot session closed.")
    except Exception as e: logger.error(f"Error closing bot session: {e}")
    if executor:
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("ThreadPoolExecutor shut down.")
    try:
        if db and db.client:
            db.client.close()
            logger.info("MongoDB client connection closed.")
        if neondb and neondb.pool:
            await neondb.close()
            logger.info("NeonDB (Postgres) pool closed.")
    except Exception as e:
        logger.error(f"Error closing database clients: {e}")
    logger.info("Graceful shutdown completed.")


def handle_shutdown_signal(signum, frame):
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    try:
        loop = asyncio.get_running_loop()
        asyncio.ensure_future(shutdown_procedure(loop), loop=loop)
    except RuntimeError:
        logger.error("No running event loop found in signal handler.")
    except Exception as e:
        logger.error(f"Error scheduling shutdown from signal handler: {e}")

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                await asyncio.sleep(0)
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} timed out after {timeout}s")
                target_chat_id = None; callback_query = None
                if args:
                    if isinstance(args[0], types.Message): target_chat_id = args[0].chat.id
                    elif isinstance(args[0], types.CallbackQuery): callback_query = args[0]; target_chat_id = callback_query.message.chat.id if callback_query.message else None
                if target_chat_id:
                    try: await bot.send_message(target_chat_id, "⚠️ Request timeout, please try again.")
                    except: pass
                if callback_query:
                    try: await callback_query.answer("Timeout.", show_alert=False)
                    except: pass
            except Exception as e: logger.exception(f"Handler {func.__name__} error: {e}")
        return wrapper
    return decorator

# ============ SAFE WRAPPERS ============
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB (Mongo) timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB (Mongo) error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         await db._handle_db_error(e);
         return default

async def safe_neondb_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    try:
        async with NEONDB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB (Neon) timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB (Neon) error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    try: return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError: logger.warning(f"TG timeout: {getattr(coro, '__name__', 'unknown_coro')}"); return None
    except (TelegramAPIError, TelegramBadRequest) as e:
        if "bot was blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
            logger.info(f"TG: Bot blocked or user deactivated."); return False
        elif "chat not found" in str(e).lower() or "peer_id_invalid" in str(e).lower():
            logger.info(f"TG: Chat not found or Peer ID invalid."); return False
        elif "message is not modified" in str(e).lower():
            logger.debug(f"TG: Message not modified."); return None
        elif "message to delete not found" in str(e).lower():
            logger.debug(f"TG: Message to delete not found."); return None
        else:
            logger.warning(f"TG Error: {e}"); return None
    except Exception as e:
        logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None


# ============ FILTERS & HELPERS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    delta = datetime.now(timezone.utc) - start_time; total_seconds = int(delta.total_seconds())
    days, r = divmod(total_seconds, 86400); hours, r = divmod(r, 3600); minutes, seconds = divmod(r, 60)
    if days > 0: return f"{days}d{hours}h{minutes}m"
    if hours > 0: return f"{hours}h{minutes}m"
    return f"{minutes}m{seconds}s"

async def check_user_membership(user_id: int) -> bool:
    if not JOIN_CHANNEL_USERNAME or not USER_GROUP_USERNAME:
        logger.warning("Membership check skipped: Usernames not configured.")
        return True 
    try:
        channel_username = JOIN_CHANNEL_USERNAME.lstrip('@')
        group_username = USER_GROUP_USERNAME.lstrip('@')
        
        channel_member_task = bot.get_chat_member(chat_id=f"@{channel_username}", user_id=user_id)
        group_member_task = bot.get_chat_member(chat_id=f"@{group_username}", user_id=user_id)
        
        channel_member, group_member = await asyncio.gather(channel_member_task, group_member_task, return_exceptions=True)
        
        valid_statuses = {"member", "administrator", "creator"}
        
        if isinstance(channel_member, (TelegramBadRequest, TelegramAPIError)):
            logger.warning(f"Channel check failed for @{channel_username}: {channel_member}")
            is_in_channel = False
        elif isinstance(channel_member, types.ChatMember):
            is_in_channel = channel_member.status in valid_statuses
        else:
            is_in_channel = False
            
        if isinstance(group_member, (TelegramBadRequest, TelegramAPIError)):
            logger.warning(f"Group check failed for @{group_username}: {group_member}")
            is_in_group = False
        elif isinstance(group_member, types.ChatMember):
            is_in_group = group_member.status in valid_statuses
        else:
            is_in_group = False

        return is_in_channel and is_in_group
        
    except Exception as e:
         if not isinstance(e, (TelegramBadRequest, TelegramAPIError)):
              logger.error(f"Unexpected error during membership check for {user_id}: {e}", exc_info=True)
         else:
             logger.info(f"Membership check API error for {user_id}: {e}")
         return False 

def get_join_keyboard():
    buttons = []
    if JOIN_CHANNEL_USERNAME: 
        buttons.append([InlineKeyboardButton(text="📢 Channel Join", url=f"https://t.me/{JOIN_CHANNEL_USERNAME.lstrip('@')}")])
    if USER_GROUP_USERNAME: 
        buttons.append([InlineKeyboardButton(text="👥 Group Join", url=f"https://t.me/{USER_GROUP_USERNAME.lstrip('@')}")])
    
    if buttons: 
        buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya", callback_data="check_join")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

def get_full_limit_keyboard():
    if not ALTERNATE_BOTS: return None
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(message: types.Message) -> Dict:
    """Helper to extract file info and metadata from a message."""
    caption = message.caption or ""
    info = {"file_id": None, "file_unique_id": None, "title": None, "imdb_id": None, "year": None}

    if message.video:
        info["file_id"] = message.video.file_id
        info["file_unique_id"] = message.video.file_unique_id
    elif message.document:
        info["file_id"] = message.document.file_id
        info["file_unique_id"] = message.document.file_unique_id
    else:
        return None

    lines = caption.splitlines()
    title = lines[0].strip() if lines else ""
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]): 
        title += " " + lines[1].strip()
    
    if title: 
        info["title"] = title
    
    imdb = re.search(r"(tt\d{7,})", caption)
    year = re.findall(r"\b(19[89]\d|20[0-2]\d)\b", caption)
    
    if imdb: info["imdb_id"] = imdb.group(1)
    if year: info["year"] = year[-1]
    
    if not info["title"]:
        filename = ""
        if message.video and message.video.file_name: filename = message.video.file_name
        elif message.document and message.document.file_name: filename = message.document.file_name
        
        parsed_info = parse_filename(filename)
        info["title"] = parsed_info["title"]
        if not info["year"]: info["year"] = parsed_info["year"]
        
    return info if info["file_id"] and info["title"] else None


def parse_filename(filename: str) -> Dict[str, str]:
    if not filename: return {"title": "Untitled", "year": None}
    year = None
    match_paren = re.search(r"\(((19[89]\d|20[0-3]\d))\)", filename)
    if match_paren:
        year = match_paren.group(1)
    else:
        matches_bare = re.findall(r"\b((19[89]\d|20[0-3]\d))\b", filename)
        if matches_bare:
            year = matches_bare[-1][0]
    title = os.path.splitext(filename)[0].strip()
    if year:
        title = re.sub(rf"(\s*\(?{year}\)?\s*)$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\[.*?\]", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\(.*?\)", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio)\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r'[._]', ' ', title).strip()
    title = re.sub(r"\s+", " ", title).strip()
    if not title:
        title = os.path.splitext(filename)[0].strip()
        title = re.sub(r"\[.*?\]", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r"\(.*?\)", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r'[._]', ' ', title).strip()
        title = re.sub(r"\s+", " ", title).strip()
    return {"title": title or "Untitled", "year": year}

def overflow_message(active_users: int) -> str: return f"⚠️ Server Capacity Reached ({active_users}/{CURRENT_CONC_LIMIT}). Nayi requests hold par hain.\nAlternate bots use karein:"

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    loop = asyncio.get_running_loop()
    while True:
        try:
            st = loop.time(); await asyncio.sleep(0.1); lag = loop.time() - st
            if lag > 0.5: logger.warning(f"⚠️ Event loop lag: {lag:.3f}s")
            await asyncio.sleep(60)
        except asyncio.CancelledError: logger.info("Event loop monitor stopped."); break
        except Exception as e: logger.error(f"Event loop monitor error: {e}", exc_info=True); await asyncio.sleep(120)

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor_task, executor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop(); loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized.")

    try:
        await db.init_db() # MongoDB
        logger.info("Database (MongoDB) initialization successful.")
    except Exception as e:
        logger.critical(f"FATAL: Database (MongoDB) initialization failed: {e}", exc_info=True)
        raise RuntimeError("MongoDB connection failed on startup.") from e

    try:
        await neondb.init_db() # NeonDB (Postgres)
        logger.info("Database (NeonDB/Postgres) initialization successful.")
    except Exception as e:
        logger.critical(f"FATAL: Database (NeonDB) initialization failed: {e}", exc_info=True)
        raise RuntimeError("NeonDB connection failed on startup.") from e

    try:
        ts_ok = await is_typesense_ready()
        if ts_ok: logger.info("Typesense initialization successful.")
        else: logger.critical("TYPESENSE INITIALIZATION FAILED. Search par retry karega.")
    except Exception as e: logger.critical(f"Error during Typesense initialization: {e}", exc_info=True)

    monitor_task = asyncio.create_task(monitor_event_loop()); logger.info("Event loop monitor started.")

    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            if not current_webhook or current_webhook.url != WEBHOOK_URL:
                 logger.info(f"Setting webhook to {WEBHOOK_URL}...")
                 await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types(), secret_token=(WEBHOOK_SECRET or None), drop_pending_updates=True)
                 logger.info("Webhook set.")
            else: logger.info("Webhook already set correctly.")
        except Exception as e: logger.error(f"Webhook setup error: {e}", exc_info=True)
    else: logger.warning("WEBHOOK_URL not set. Running without webhook.")

    logger.info("Application startup sequence complete.")
    yield
    logger.info("Application shutdown sequence starting...")
    # Cleanup (handled by shutdown_procedure)


app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK / HEALTH / CAPACITY ============
async def _process_update_safe(update_obj: Update):
    try: await dp.feed_update(bot=bot, update=update_obj)
    except Exception as e: logger.exception(f"Error processing update {update_obj.update_id}: {e}")

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token.")
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        telegram_update = Update(**update)
        background_tasks.add_task(_process_update_safe, telegram_update)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook could not parse update: {e}", exc_info=False)
        logger.debug(f"Failed update data: {update}")
        return {"ok": False, "error": f"Invalid update format: {e}"}

@app.get("/")
@app.get("/ping")
async def ping(): return {"status": "ok", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    global CURRENT_SEARCH_MODE
    db_check_task = db.is_ready() # Mongo check
    neondb_check_task = neondb.is_ready() # NeonDB check
    ts_check_task = is_typesense_ready() # Typesense check
    
    db_ok, neondb_ok, ts_ok = await asyncio.gather(db_check_task, neondb_check_task, ts_check_task)
    
    status_code = 200
    status_msg = "ok"
    
    if not db_ok:
        status_msg = "error_mongodb"
        status_code = 503
    elif not neondb_ok:
        status_msg = "error_neondb"
        status_code = 503
    elif not ts_ok and CURRENT_SEARCH_MODE == 'typesense':
        status_msg = "degraded_typesense_down"
        # Bot abhi bhi chalega (fallback ke kaaran), isliye 200 OK bhejte hain
    
    return {
        "status": status_msg,
        "database_mongo_connected": db_ok,
        "database_neon_connected": neondb_ok,
        "typesense_connected": ts_ok,
        "current_search_mode": CURRENT_SEARCH_MODE,
        "uptime": get_uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, status_code


async def ensure_capacity_or_inform(message_or_callback: types.Message | types.CallbackQuery) -> bool:
    user = message_or_callback.from_user
    if not user: return True
    target_chat_id = None
    if isinstance(message_or_callback, types.Message): target_chat_id = message_or_callback.chat.id
    elif isinstance(message_or_callback, types.CallbackQuery) and message_or_callback.message: target_chat_id = message_or_callback.message.chat.id
    
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))
    if user.id == ADMIN_USER_ID: return True
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=CURRENT_CONC_LIMIT + 1)
    if active >= CURRENT_CONC_LIMIT:
        logger.warning(f"Capacity reached: {active}/{CURRENT_CONC_LIMIT}. User {user.id} request held.")
        if target_chat_id:
            await safe_tg_call(bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()))
        if isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server busy, please use alternate bots or try again later.", show_alert=False))
        return False
    return True

# ============ BOT HANDLERS ============

def get_search_mode_display(mode: str, ts_ready: bool, neon_ready: bool, mongo_ready: bool) -> Tuple[str, str]:
    """Helper function to get status text for admin panel."""
    status_icon = "🟢"
    status_text = f"<b>{mode.capitalize()}</b>"
    
    if mode == 'typesense':
        if not ts_ready:
            status_icon = "❌"
            status_text += " (Down)"
        else:
            status_text += " (Fast)"
    elif mode == 'neondb':
        if not neon_ready:
            status_icon = "❌"
            status_text += " (Down)"
        else:
            status_text += " (FTS)"
    elif mode == 'mongodb':
        if not mongo_ready:
            status_icon = "❌"
            status_text += " (Down)"
        else:
            status_text += " (Fallback)"
            
    return status_icon, status_text


@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user = message.from_user
    if not user: return
    user_id = user.id
    bot_info = await safe_tg_call(bot.get_me(), timeout=5)
    bot_username = bot_info.username if bot_info else "Bot"
    await safe_db_call(db.add_user(user_id, user.username, user.first_name, user.last_name))

    # --- Admin Panel (Updated for Triple Engine) ---
    if user_id == ADMIN_USER_ID:
        # Check all services
        user_count_task = safe_db_call(db.get_user_count(), default=0)
        mongo_movie_count_task = safe_db_call(db.get_movie_count(), default=-1)
        neon_movie_count_task = safe_neondb_call(neondb.get_movie_count(), default=-1)
        concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        ts_ready_task = is_typesense_ready()
        neon_ready_task = neondb.is_ready()
        mongo_ready_task = db.is_ready()

        user_count, mongo_count_raw, neon_count_raw, concurrent_users, ts_ready, neon_ready, mongo_ready = await asyncio.gather(
            user_count_task, mongo_movie_count_task, neon_movie_count_task, concurrent_users_task,
            ts_ready_task, neon_ready_task, mongo_ready_task
        )

        mongo_count_str = f"{mongo_count_raw:,}" if mongo_count_raw >= 0 else "DB Error"
        neon_count_str = f"{neon_count_raw:,}" if neon_count_raw >= 0 else "DB Error"
        
        # Get current search mode status
        search_icon, search_text = get_search_mode_display(CURRENT_SEARCH_MODE, ts_ready, neon_ready, mongo_ready)
        
        # Get status for all engines
        ts_icon, ts_text = get_search_mode_display('typesense', ts_ready, neon_ready, mongo_ready)
        neon_icon, neon_text = get_search_mode_display('neondb', ts_ready, neon_ready, mongo_ready)
        mongo_icon, mongo_text = get_search_mode_display('mongodb', ts_ready, neon_ready, mongo_ready)

        admin_message = (
            f"👑 <b>Admin: @{bot_username}</b> (Triple-Engine)\n\n"
            f"<b>Status</b>\n"
            f"🟢 Active ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}\n"
            f"👥 Users (Mongo): {user_count:,}\n"
            f"🎬 Movies (Mongo): {mongo_count_str}\n"
            f"📚 Index (NeonDB): {neon_count_str}\n"
            f"⏰ Uptime: {get_uptime()}\n\n"
            f"<b>Search Engines</b>\n"
            f"{search_icon} Current Mode: <b>{search_text}</b>\n"
            f"-------------------\n"
            f"{ts_icon} Typesense: {ts_text}\n"
            f"{neon_icon} NeonDB: {neon_text}\n"
            f"{mongo_icon} MongoDB: {mongo_text}\n\n"
            f"<b>Commands</b>\n"
            f"/stats | /health\n"
            f"<b>/search_switch</b> (Cycle: TS -> Neon -> Mongo)\n"
            f"/broadcast (Reply)\n"
            f"/get_user `[user_id]`\n"
            f"/cleanup_users | /set_limit `N`\n\n"
            f"<b>Library Management (NeonDB)</b>\n"
            f"<b>/remove_library_duplicates `[100]`</b> ⭐️\n"
            f"<b>/backup_channel `[chat_id]`</b> ⭐️\n\n"
            f"<b>DB Sync / Maintenance</b>\n"
            f"<b>/sync_mongo_to_neon</b> ⚠️\n"
            f"<b>/sync_typesense</b> ⚠️ (Full Mongo->TS)\n"
            f"<b>/cleanup_mongo_duplicates</b> (IMDB ID)\n"
            f"<b>/cleanup_json</b> (Dead JSON data)\n"
            f"<b>/rebuild_index</b> (Mongo text index)\n\n"
            f"<b>⭐️ Migration:</b> Forward files from `LIBRARY_CHANNEL`."
        )
        await safe_tg_call(message.answer(admin_message))
        return

    # --- Regular User ---
    if not await ensure_capacity_or_inform(message): return
    
    is_member = await check_user_membership(user_id)
    join_markup = get_join_keyboard()
    
    if is_member:
        welcome_text = (
            f"🎬 Namaste <b>{user.first_name}</b>!\n"
            f"Movie search bot. Naam bhejein (galat spelling bhi chalegi).\n"
            f"Example: <code>Kantara 2022</code> ya <code>Kantra</code>\n\n"
            f"⚠️ Free server start hone mein 10-15 sec lag sakte hain agar inactive tha."
        )
        await safe_tg_call(message.answer(welcome_text, reply_markup=None))
    else:
        welcome_text = (
            f"🎬 Namaste <b>{user.first_name}</b>!\n"
            f"Movie search bot mein swagat hai.\n\n"
            f"Access ke liye, kripya neeche diye gaye Channel aur Group join karein, phir '✅ Maine Join Kar Liya' button dabayen."
        )
        if join_markup:
            await safe_tg_call(message.answer(welcome_text, reply_markup=join_markup))
        else:
            await safe_tg_call(message.answer("Configuration error: Join channels not set."))


@dp.message(Command("help"))
@handler_timeout(15)
async def help_command(message: types.Message):
    user = message.from_user
    if not user: return
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))
    help_text = (
        "❓ <b>Bot Ka Upyog</b>\n\n"
        "1.  <b>Instant Search:</b> Movie/Show ka naam seedha message mein bhejein.\n"
        "    Example: <code>Jawan</code>\n\n"
        "2.  <b>Typo Friendly:</b> Agar aap spelling galat likhte hain (e.g., <code>Mirjapur</code>), toh bhi bot search kar lega.\n\n"
        "3.  <b>Behtar Results:</b> Naam ke saath saal (year) jodein.\n"
        "    Example: <code>Pushpa 2021</code>\n\n"
        "⚠️ <b>Start Hone Mein Deri?</b>\n"
        "Yeh bot free server par hai. Agar 15 min use na ho, toh server 'so' jaata hai. Dobara /start karne par use 'jagne' mein 10-15 second lag sakte hain. Search hamesha fast rahegi."
    )
    await safe_tg_call(message.answer(help_text))


@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))
    
    await safe_tg_call(callback.answer("Verifying membership..."))
    
    if not await ensure_capacity_or_inform(callback): return

    is_member = await check_user_membership(user.id)
    join_markup = get_join_keyboard()

    if is_member:
        active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = (
            f"✅ Verification successful, <b>{user.first_name}</b>!\n\n"
            f"Ab aap movie search kar sakte hain - bas movie ka naam bhejein.\n\n"
            f"(Server Load: {active_users}/{CURRENT_CONC_LIMIT})"
        )
        edited = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        if not edited: 
            await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=None))
    else:
        await safe_tg_call(callback.answer("❌ Aapne channel/group join nahi kiya hai. Kripya join karke dubara '✅ Maine Join Kar Liya' button dabayen.", show_alert=True))
        
        if not callback.message.reply_markup or not callback.message.reply_markup.inline_keyboard:
             await safe_tg_call(callback.message.edit_reply_markup(reply_markup=join_markup))


# =======================================================
# +++++ "ULTIMATE STABLE" SEARCH HANDLER (WATERFALL) +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(10)
async def search_movie_handler(message: types.Message):
    global CURRENT_SEARCH_MODE
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message): return
    
    if user_id != ADMIN_USER_ID and not await check_user_membership(user_id):
        join_markup = get_join_keyboard()
        if join_markup:
            await safe_tg_call(message.answer(
                "❌ Search karne se pehle, kripya neeche diye gaye Channel aur Group join karein, phir '✅ Maine Join Kar Liya' button dabayen.",
                reply_markup=join_markup
            ))
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Query kam se kam 2 characters ki honi chahiye."))
        return

    # --- Naya "Waterfall" Logic ---
    search_coro = None
    engine_used = CURRENT_SEARCH_MODE # Admin ki choice se start karein
    search_engine_name = ""

    # 1. Try Admin's Choice (Default)
    if engine_used == 'typesense':
        if await is_typesense_ready():
            search_coro = typesense_search(original_query, limit=20)
            search_engine_name = "⚡️ Typesense (Fast)"
        else:
            logger.warning(f"Search: Typesense (Current Mode) is DOWN. Falling back to NeonDB.")
            engine_used = 'neondb' # Agle par fallback karein
    
    # 2. Try NeonDB (Fallback 1)
    if engine_used == 'neondb':
        if neondb.is_ready(): # Yeh async nahi hai
            search_coro = neondb.neondb_search(original_query, limit=20)
            search_engine_name = "📚 NeonDB (FTS)"
        else:
            logger.warning(f"Search: NeonDB is DOWN. Falling back to MongoDB.")
            engine_used = 'mongodb' # Aakhri fallback
            
    # 3. Try MongoDB (Fallback 2 - Aakhri umeed)
    if engine_used == 'mongodb':
        if await db.is_ready():
            search_coro = db.mongo_search_internal(original_query, limit=20)
            search_engine_name = "🗄️ MongoDB (Fallback)"
        else:
            # Agar Mongo bhi down hai, toh bot poori tarah down hai
            logger.critical("Search: ALL DATABASES ARE DOWN. Cannot perform search.")
            await safe_tg_call(message.answer("❌ Server Error: Search service abhi available nahi hai. Please try again later."))
            return

    searching_msg = await safe_tg_call(message.answer(f"<b>{original_query}</b> search ho raha hai...\n(Using: {search_engine_name})"))
    if not searching_msg: return

    # Coroutine ko execute karein
    search_results = await search_coro

    if not search_results:
        await safe_tg_call(searching_msg.edit_text(f"🥲 Maaf kijiye, '<b>{original_query}</b>' ke liye kuch nahi mila."))
        return

    buttons = []
    max_buttons = 15
    for movie in search_results[:max_buttons]:
        # Dono DBs (Neon/Mongo) ab 'imdb_id', 'title', 'year' return karte hain
        display_title = movie["title"][:50] + '...' if len(movie["title"]) > 50 else movie["title"]
        year_str = f" ({movie['year']})" if movie.get('year') else ""
        buttons.append([InlineKeyboardButton(text=f"{display_title}{year_str}", callback_data=f"get_{movie['imdb_id']}")])

    result_count_text = f"{len(search_results)}" if len(search_results) <= max_buttons else f"{max_buttons}+"
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> - {result_count_text} results mile:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(15)
async def get_movie_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))
    await safe_tg_call(callback.answer("File bheji ja rahi hai..."))
    if not await ensure_capacity_or_inform(callback): return
    
    imdb_id = callback.data.split("_", 1)[1]
    # Movie data hamesha MongoDB se hi nikalta hai (Search kisi bhi DB se ho)
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie database mein nahi mili. Ho sakta hai remove ho gayi ho."))
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN NOTE: Movie <code>{imdb_id}</code> search mein hai par DB mein nahi. Please run /sync_typesense to fix."))
        return

    await safe_tg_call(callback.message.edit_text(f"✅ Preparing to send <b>{movie['title']}</b>..."))
    success = False; error_detail = "Unknown error"
    try:
        is_valid_forward = movie.get("channel_id") and movie.get("channel_id") != 0 and \
                           movie.get("message_id") and movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER
        if is_valid_forward:
            logger.debug(f"Attempting forward for {imdb_id} from {movie['channel_id']}:{movie['message_id']}")
            fwd_result = await safe_tg_call(bot.forward_message(
                chat_id=user.id,
                from_chat_id=int(movie["channel_id"]),
                message_id=movie["message_id"],
            ), timeout=LONG_TG_OP_TIMEOUT)
            if fwd_result: success = True
            elif fwd_result is False: error_detail = "Bot blocked or chat not found."
            else: error_detail = "Forwarding failed (timeout or API error)."
        else:
            error_detail = "Cannot forward (invalid channel/message ID)."
        
        if not success:
            logger.info(f"Forward failed or skipped ({error_detail}), falling back to send_document for {imdb_id} using file_id.")
            if not movie.get("file_id"):
                 error_detail = "File ID missing, cannot send document."
            else:
                send_result = await safe_tg_call(bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'N/A'})"
                ), timeout=LONG_TG_OP_TIMEOUT)
                if send_result: success = True
                elif send_result is False: error_detail += " (Bot blocked/Chat not found on send_doc)"
                else: error_detail += " (Sending document by file_id failed)"
    except Exception as e:
        error_detail = f"Unexpected error during send/forward: {e}"
        logger.error(f"Exception during send/forward for {imdb_id}: {e}", exc_info=True)

    if not success:
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: <b>{movie['title']}</b> ko nahi bhej paya.\nReason: {error_detail}{admin_hint}"
        await safe_tg_call(bot.send_message(user.id, error_text))
        try: await safe_tg_call(callback.message.edit_text(f"❌ Failed to send <b>{movie['title']}</b>."))
        except: pass


# =======================================================
# +++++ DUAL DB MIGRATION/INDEXING +++++
# =======================================================
@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message):
    """
    Jab admin LIBRARY_CHANNEL se file bot ko FORWARD karta hai,
    toh yeh unhe DONO database (Mongo aur Neon) mein add/update kar deta hai.
    """
    if not message.forward_from_chat or message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0:
            await safe_tg_call(message.answer("❌ Migration Error: `LIBRARY_CHANNEL_ID` set nahi hai."))
        else:
            await safe_tg_call(message.answer(f"Migration ke liye, files ko seedha apne `LIBRARY_CHANNEL` (ID: `{LIBRARY_CHANNEL_ID}`) se forward karein."))
        return

    info = extract_movie_info(message)
    if not info:
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption/File se info parse nahi kar paya.")
        await safe_tg_call(message.answer(f"❌ Migration Skipped: MessageID `{message.forward_from_message_id}` ka caption/file parse nahi kar paya."))
        return

    file_id = info["file_id"]
    file_unique_id = info["file_unique_id"]
    message_id = message.forward_from_message_id
    channel_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{message_id}"
    title = info["title"]
    year = info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    db_res_task = safe_db_call(db.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    neondb_res_task = safe_neondb_call(neondb.add_movie(message_id, channel_id, file_id, file_unique_id, imdb_id, title))
    ts_data = {'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val}
    ts_res_task = typesense_add_movie(ts_data)
    
    db_res, neondb_res, ts_res = await asyncio.gather(db_res_task, neondb_res_task, ts_res_task)

    db_map = {True: "✅ Mongo Added", "updated": "✅ Mongo Updated", "duplicate": "ℹ️ Mongo Skipped", False: "❌ Mongo Error"}
    db_stat = db_map.get(db_res, "❌ Mongo Error")
    
    neon_map = {True: "✅ Neon Added", False: "❌ Neon Error/Duplicate"}
    neon_stat = neon_map.get(neondb_res, "❌ Neon Error/Duplicate")
    
    ts_stat = "✅ TS Synced" if ts_res else "❌ TS Error"

    await safe_tg_call(message.answer(f"<b>{title}</b>\n{db_stat}\n{neon_stat}\n{ts_stat}"))


# --- ADMIN COMMANDS ---

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    
    user_count_task = safe_db_call(db.get_user_count(), default=0)
    mongo_movie_count_task = safe_db_call(db.get_movie_count(), default=-1)
    neon_movie_count_task = safe_neondb_call(neondb.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    ts_ready_task = is_typesense_ready()
    neon_ready_task = neondb.is_ready()
    mongo_ready_task = db.is_ready()
    
    user_count, mongo_count_raw, neon_count_raw, concurrent_users, ts_ready, neon_ready, mongo_ready = await asyncio.gather(
        user_count_task, mongo_movie_count_task, neon_movie_count_task, concurrent_users_task,
        ts_ready_task, neon_ready_task, mongo_ready_task
    )
    
    mongo_count_str = f"{mongo_count_raw:,}" if mongo_count_raw >= 0 else "DB Error"
    neon_count_str = f"{neon_count_raw:,}" if neon_count_raw >= 0 else "DB Error"
    
    search_icon, search_text = get_search_mode_display(CURRENT_SEARCH_MODE, ts_ready, neon_ready, mongo_ready)
    
    stats_msg = (
        f"📊 Stats (Triple-Engine)\n"
        f"🟢 Active({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}\n"
        f"👥 Users (Mongo): {user_count:,}\n"
        f"🎬 Movies (Mongo): {mongo_count_str}\n"
        f"📚 Index (NeonDB): {neon_count_str}\n"
        f"⏰ Uptime: {get_uptime()}\n\n"
        f"<b>Search Status</b>\n"
        f"{search_icon} Current Mode: <b>{search_text}</b>"
    )
    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("search_switch"), AdminFilter())
@handler_timeout(10)
async def search_switch_command(message: types.Message):
    global CURRENT_SEARCH_MODE, SEARCH_MODES
    
    try:
        current_index = SEARCH_MODES.index(CURRENT_SEARCH_MODE)
    except ValueError:
        current_index = 0 # Agar list se bahar hai toh reset karein
    
    # Agle available service ko dhoondein
    for i in range(1, len(SEARCH_MODES) + 1):
        new_mode = SEARCH_MODES[(current_index + i) % len(SEARCH_MODES)]
        
        if new_mode == 'typesense' and await is_typesense_ready():
            CURRENT_SEARCH_MODE = new_mode
            await safe_tg_call(message.answer("✅ Search mode set to ⚡️ <b>Typesense (Fast)</b>."))
            return
        elif new_mode == 'neondb' and neondb.is_ready():
            CURRENT_SEARCH_MODE = new_mode
            await safe_tg_call(message.answer("✅ Search mode set to 📚 <b>NeonDB (FTS)</b>."))
            return
        elif new_mode == 'mongodb' and await db.is_ready():
            CURRENT_SEARCH_MODE = new_mode
            await safe_tg_call(message.answer("✅ Search mode set to 🗄️ <b>MongoDB (Fallback)</b>."))
            return
            
    # Agar loop poora ho gaya (sab down hain)
    await safe_tg_call(message.answer("❌ All search services are down! Switch failed."))


@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(1800)
async def broadcast_command(message: types.Message):
    if not message.reply_to_message: await safe_tg_call(message.answer("❌ Reply to msg.")); return
    users = await safe_db_call(db.get_all_users(), timeout=60, default=[])
    if not users: await safe_tg_call(message.answer("❌ No users.")); return
    total = len(users); s, f = 0, 0; msg = await safe_tg_call(message.answer(f"📤 Broadcasting to {total:,}..."))
    st = datetime.now(timezone.utc)
    tasks = []
    processed_count = 0
    
    broadcast_semaphore = asyncio.Semaphore(25)
    
    async def send_to_user(uid):
        nonlocal s, f
        async with broadcast_semaphore:
            res = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=5)
            if res: s += 1
            elif res is False: f += 1; await safe_db_call(db.deactivate_user(uid))
            else: f += 1
            await asyncio.sleep(0.04)
            
    for i, uid in enumerate(users):
        tasks.append(send_to_user(uid))
        processed_count += 1
        now = datetime.now(timezone.utc)
        
        if processed_count % 100 == 0 or (now - st).total_seconds() > 10 or processed_count == total:
            if tasks:
                await asyncio.gather(*tasks)
                tasks = []
            if msg:
                try: await safe_tg_call(msg.edit_text(f"📤 Progress: {processed_count}/{total}\n✅ Sent: {s:,} | ❌ Failed: {f:,}"))
                except TelegramBadRequest: pass
            st = now

    txt = f"✅ Broadcast Done!\nSent: {s:,}\nFailed/Blocked: {f:,}\nTotal: {total:,}"
    if msg: await safe_tg_call(msg.edit_text(txt))
    else: await safe_tg_call(message.answer(txt))


@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120)
async def cleanup_users_command(message: types.Message):
    msg = await safe_tg_call(message.answer("🧹 Cleaning inactive users (>30 days)..."))
    removed = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=90, default=0)
    new_count = await safe_db_call(db.get_user_count(), default=0)
    txt = f"✅ Cleanup done!\nDeactivated: {removed:,}\nNow Active: {new_count:,}"
    if msg: await safe_tg_call(msg.edit_text(txt))
    else: await safe_tg_call(message.answer(txt))

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(20)
async def add_movie_command(message: types.Message):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Reply to file: `/add_movie imdb|title|year`"))
        return
        
    info = extract_movie_info(message.reply_to_message)
    if not info:
        await safe_tg_call(message.answer("❌ Reply to a valid video/document file."))
        return
        
    try:
        parts = message.text.split("|", 2)
        imdb_id_arg = parts[0].split(" ", 1)[1].strip()
        title_arg = parts[1].strip()
        year_arg = parts[2].strip() if len(parts) > 2 else None
        
        imdb_id = imdb_id_arg or info["imdb_id"] or f"auto_{message.reply_to_message.message_id}"
        title = title_arg or info["title"]
        year = year_arg or info["year"]
        
        if not imdb_id or not title: raise ValueError("Missing IMDB ID or Title")

        rpl = message.reply_to_message
        file_id = info["file_id"]
        file_unique_id = info["file_unique_id"]
        msg_id = rpl.message_id
        chan_id = rpl.chat.id
        
    except Exception as e: 
        await safe_tg_call(message.answer(f"❌ Format Error: {e}\nUse: `/add_movie imdb|title|year` (Reply to file)"))
        return
    
    msg = await safe_tg_call(message.answer(f"⏳ Processing '<b>{title}</b>' for Dual DB..."))
    clean_title_val = clean_text_for_search(title)
    
    db_res_task = safe_db_call(db.add_movie(imdb_id, title, year, file_id, msg_id, chan_id, clean_title_val, file_unique_id))
    neondb_res_task = safe_neondb_call(neondb.add_movie(msg_id, chan_id, file_id, file_unique_id, imdb_id, title))
    ts_data = {'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val}
    ts_res_task = typesense_add_movie(ts_data)
    
    db_res, neondb_res, ts_res = await asyncio.gather(db_res_task, neondb_res_task, ts_res_task)

    db_map = {True: "✅ Mongo Added", "updated": "✅ Mongo Updated", "duplicate": "ℹ️ Mongo Skipped", False: "❌ Mongo Error"}
    db_stat = db_map.get(db_res, "❌ Mongo Error")
    neon_map = {True: "✅ Neon Added", False: "ℹ️ Neon Duplicate/Error"}
    neon_stat = neon_map.get(neondb_res, "ℹ️ Neon Duplicate/Error")
    ts_stat = "✅ TS Synced" if ts_res else "❌ TS Error"
    
    txt = f"<b>{title}</b>\n{db_stat}\n{neon_stat}\n{ts_stat}"
    if msg: await safe_tg_call(msg.edit_text(txt))
    else: await safe_tg_call(message.answer(txt))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split(maxsplit=1);
    if len(args) < 2: await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID")); return
    imdb_id = args[1].strip(); msg = await safe_tg_call(message.answer(f"⏳ Removing <code>{imdb_id}</code> from Mongo & Typesense..."))
    
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id));
    db_del_task = db.remove_movie_by_imdb(imdb_id)
    ts_del_task = typesense_remove_movie(imdb_id)
    
    db_del, ts_del = await asyncio.gather(db_del_task, ts_del_task)
    
    db_stat = f"✅ DB Removed '{movie['title'] if movie else imdb_id}'." if db_del else ("ℹ️ DB Not found." if not movie else "❌ DB Error removing.")
    ts_stat = "✅ Typesense Removed." if ts_del else "ℹ️ Typesense Not Found or Error."
    
    txt = f"{db_stat}\n{ts_stat}\n\n(Note: Yeh NeonDB se delete nahi karta hai)";
    if msg: await safe_tg_call(msg.edit_text(txt))
    else: await safe_tg_call(message.answer(txt))


@dp.message(Command("cleanup_json"), AdminFilter())
@handler_timeout(300)
async def cleanup_json_command(message: types.Message):
    msg = await safe_tg_call(message.answer("⏳ Invalid JSON entries (`json_...`) ko MongoDB se delete kiya ja raha hai..."))
    if not msg: return
    
    deleted_count = await safe_db_call(db.remove_json_imports(), timeout=240, default=0)
    
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ DB se {deleted_count:,} 'json_' entries remove kiye.\n⏳ Ab Typesense ko sync kiya ja raha hai..."))
        await sync_typesense_command(message, from_user_id=message.from_user.id)
    else:
        await safe_tg_call(msg.edit_text("✅ Koi 'json_...' entry database mein nahi mili. Sab saaf hai."))


@dp.message(Command("sync_typesense"), AdminFilter())
@handler_timeout(1800)
async def sync_typesense_command(message: types.Message, from_user_id: int = None):
    admin_id = from_user_id or message.from_user.id
    
    if not await is_typesense_ready():
        await safe_tg_call(bot.send_message(admin_id, "❌ Typesense is not connected. Sync failed."))
        return
        
    msg = None
    if not from_user_id: 
        msg = await safe_tg_call(message.answer("⚠️ Full Typesense Sync Started...\n⏳ Fetching all movies from MongoDB..."))
        if not msg: return
    
    try:
        all_movies_db = await safe_db_call(db.get_all_movies_for_sync(), timeout=300)
        if all_movies_db is None:
            if msg: await safe_tg_call(msg.edit_text("❌ Error fetching movies from MongoDB. Sync cancelled."))
            else: await safe_tg_call(bot.send_message(admin_id, "❌ Error fetching movies from MongoDB. Sync cancelled."))
            return
            
        db_count = len(all_movies_db)
        sync_msg_text = f"✅ Fetched {db_count:,} movies from Mongo.\n⏳ Syncing to Typesense (replace all)..."
        
        if msg: await safe_tg_call(msg.edit_text(sync_msg_text))
        else: await safe_tg_call(bot.send_message(admin_id, sync_msg_text))
        
        success, total_uploaded = await typesense_sync_data(all_movies_db)
        
        final_text = f"✅ Sync Complete! {total_uploaded:,} records replaced in Typesense." if success else "❌ Sync Failed! Check logs for details."
        
        if msg: await safe_tg_call(msg.edit_text(final_text))
        else: await safe_tg_call(bot.send_message(admin_id, final_text))
        
    except Exception as e:
        logger.error(f"Unexpected error during /sync_typesense: {e}", exc_info=True)
        if msg: await safe_tg_call(msg.edit_text(f"❌ Sync Command Error: {e}"))
        else: await safe_tg_call(bot.send_message(admin_id, f"❌ Sync Command Error: {e}"))


@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300)
async def rebuild_index_command(message: types.Message):
    msg = await safe_tg_call(message.answer("🔧 Rebuilding `clean_title` and text index in MongoDB... (DB Only)"))
    if not msg: return
    updated, total = await safe_db_call(db.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    await safe_db_call(db.create_mongo_text_index())
    
    result_text = f"✅ DB Reindex done: Updated {updated:,} missing clean_titles. Total: {total:,}. MongoDB text index (re)created."
    if msg: await safe_tg_call(msg.edit_text(result_text))
    else: await safe_tg_call(message.answer(result_text))


@dp.message(Command("cleanup_mongo_duplicates"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_duplicates_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    try:
        batch_size = int(args[1]) if len(args) > 1 else 100
        if not (10 <= batch_size <= 1000):
            raise ValueError("Batch size must be between 10 and 1000")
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ Error: {e}\nUse: /cleanup_mongo_duplicates [batch_size (10-1000)]\nDefault: 100"))
        return

    msg = await safe_tg_call(message.answer(f"⏳ Searching for duplicates in MongoDB (by imdb_id) and removing up to {batch_size} entries..."))
    if not msg: return
    
    deleted_count, duplicates_found = await safe_db_call(
        db.cleanup_mongo_duplicates(batch_limit=batch_size), 
        timeout=240, 
        default=(0, 0)
    )
    
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ Mongo Batch complete.\nDeleted: {deleted_count}\nTotal duplicates found (in first pass): {duplicates_found}\n\n⚠️ Run /sync_typesense to update search index."))
    elif duplicates_found > 0:
        await safe_tg_call(msg.edit_text(f"ℹ️ Found {duplicates_found} duplicates, but batch limit was reached. Run command again."))
    else:
        await safe_tg_call(msg.edit_text("✅ No duplicates found in MongoDB (based on imdb_id)."))


@dp.message(Command("remove_library_duplicates"), AdminFilter())
@handler_timeout(1800)
async def remove_library_duplicates_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    try:
        batch_size = int(args[1]) if len(args) > 1 else 100
        if not (10 <= batch_size <= 2000):
            raise ValueError("Batch size must be between 10 and 2000")
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ Error: {e}\nUse: /remove_library_duplicates [batch_size (10-2000)]\nDefault: 100"))
        return

    msg = await safe_tg_call(message.answer(f"⏳ Finding duplicate files in NeonDB index... (Keeps newest, deletes {batch_size} oldest)"))
    if not msg: return
    
    result = await safe_neondb_call(
        neondb.find_and_delete_duplicates(batch_limit=batch_size), 
        timeout=300, 
        default=None
    )
    
    if result is None:
        await safe_tg_call(msg.edit_text("❌ Error finding duplicates in NeonDB. Check logs."))
        return
        
    messages_to_delete, total_found = result
    
    if not messages_to_delete:
        await safe_tg_call(msg.edit_text("✅ No duplicates found in NeonDB index."))
        return

    await safe_tg_call(msg.edit_text(f"Found {total_found} total duplicates.\nDeleting batch of {len(messages_to_delete)} messages from channel... (This may take time)"))
    
    delete_semaphore = asyncio.Semaphore(10)
    deleted_count = 0
    failed_count = 0
    
    async def delete_message_task(msg_id, chat_id):
        nonlocal deleted_count, failed_count
        async with delete_semaphore:
            try:
                await asyncio.sleep(0.1)
                await bot.delete_message(chat_id, msg_id)
                deleted_count += 1
                return True
            except Exception as e:
                logger.warning(f"Failed to delete message {msg_id} from {chat_id}: {e}")
                failed_count += 1
                return False

    tasks = [delete_message_task(msg_id, chat_id) for msg_id, chat_id in messages_to_delete]
    await asyncio.gather(*tasks)
    
    await safe_tg_call(msg.edit_text(f"✅ Library Cleanup Done!\nDeleted from channel: {deleted_count}\nFailed: {failed_count}\n(NeonDB index is now clean)"))


@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(3600)
async def backup_channel_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    target_chat_id_str = BACKUP_CHANNEL_ID
    if len(args) > 1:
        target_chat_id_str = args[1].strip()
        if not target_chat_id_str.startswith(("-", "@")):
            await safe_tg_call(message.answer("❌ Invalid target Chat ID. Must be a number (e.g., -100...) or username (@...)."))
            return
            
    msg = await safe_tg_call(message.answer(f"⏳ Fetching unique movie list from NeonDB for backup..."))
    if not msg: return
    
    movies_to_backup = await safe_neondb_call(neondb.get_unique_movies_for_backup(), timeout=300, default=[])
    
    if not movies_to_backup:
        await safe_tg_call(msg.edit_text("❌ No unique movies found in NeonDB index to backup."))
        return

    total_movies = len(movies_to_backup)
    await safe_tg_call(msg.edit_text(f"Found {total_movies:,} unique movies.\nStarting backup to `{target_chat_id_str}`... (Rate: ~20/sec)"))
    
    forward_semaphore = asyncio.Semaphore(20)
    success_count = 0
    failed_count = 0
    
    async def forward_message_task(msg_id, from_chat_id):
        nonlocal success_count, failed_count
        async with forward_semaphore:
            try:
                await asyncio.sleep(0.05)
                await bot.forward_message(
                    chat_id=target_chat_id_str,
                    from_chat_id=from_chat_id,
                    message_id=msg_id
                )
                success_count += 1
                return True
            except Exception as e:
                logger.warning(f"Failed to forward message {msg_id} from {from_chat_id}: {e}")
                failed_count += 1
                return False

    tasks = [forward_message_task(msg_id, chat_id) for msg_id, chat_id in movies_to_backup]
    
    batch_size = 200
    for i in range(0, total_movies, batch_size):
        batch_tasks = tasks[i:i+batch_size]
        await asyncio.gather(*batch_tasks)
        
        await safe_tg_call(msg.edit_text(
            f"⏳ Backup Progress: {i+len(batch_tasks)}/{total_movies}\n"
            f"✅ Success: {success_count}\n"
            f"❌ Failed: {failed_count}"
        ))

    await safe_tg_call(msg.edit_text(
        f"✅ Backup Complete!\n"
        f"Total Unique Movies: {total_movies}\n"
        f"✅ Forwarded: {success_count}\n"
        f"❌ Failed: {failed_count}"
    ))


@dp.message(Command("sync_mongo_to_neon"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_to_neon_command(message: types.Message):
    msg = await safe_tg_call(message.answer("⏳ Fetching all data from MongoDB... (This may take time)"))
    if not msg: return
    
    mongo_movies = await safe_db_call(db.get_all_movies_for_neon_sync(), timeout=300, default=[])
    
    if not mongo_movies:
        await safe_tg_call(msg.edit_text("❌ No movies found in MongoDB to sync."))
        return
        
    if 'file_unique_id' not in mongo_movies[0]:
        await safe_tg_call(msg.edit_text("❌ Error: MongoDB data mein `file_unique_id` nahi hai. Sync nahi ho sakta. Pehle data migrate/re-index karein."))
        return

    await safe_tg_call(msg.edit_text(f"Found {len(mongo_movies):,} movies in MongoDB.\n⏳ Syncing to NeonDB... (Uses ON CONFLICT DO NOTHING)"))
    
    inserted_count = await safe_neondb_call(neondb.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    
    await safe_tg_call(msg.edit_text(f"✅ Sync Complete!\n{len(mongo_movies):,} Mongo records processed.\n{inserted_count:,} new records inserted into NeonDB."))


@dp.message(Command("get_user"), AdminFilter())
@handler_timeout(15)
async def get_user_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer("❌ Use: /get_user [user_id]"))
        return
    
    try: user_id_to_check = int(args[1])
    except ValueError: await safe_tg_call(message.answer("❌ Invalid User ID.")); return

    msg = await safe_tg_call(message.answer(f"⏳ Fetching info for user <code>{user_id_to_check}</code>..."))
    if not msg: return
    
    user_info = await safe_db_call(db.get_user_info(user_id_to_check), default=None)
    
    if not user_info:
        await safe_tg_call(msg.edit_text(f"❌ User <code>{user_id_to_check}</code> not found in database."))
        return
    
    joined = user_info.get('joined_date', datetime.min.replace(tzinfo=timezone.utc)).strftime("%Y-%m-%d %H:%M")
    last_active = user_info.get('last_active', datetime.min.replace(tzinfo=timezone.utc)).strftime("%Y-%m-%d %H:%M")
    is_active_str = "🟢 Active" if user_info.get('is_active', False) else "🔴 Inactive"
    
    info_text = (
        f"<b>User Info: <code>{user_info.get('user_id')}</code></b>\n"
        f"Username: @{user_info.get('username', 'N/A')}\n"
        f"Name: {user_info.get('first_name', '')} {user_info.get('last_name', '')}\n"
        f"Status: {is_active_str}\n"
        f"Joined: {joined} UTC\n"
        f"Last Active: {last_active} UTC"
    )
    await safe_tg_call(msg.edit_text(info_text))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT; args = message.text.split();
    if len(args)<2 or not args[1].isdigit(): await safe_tg_call(message.answer(f"Use: /set_limit N (Current: {CURRENT_CONC_LIMIT})")); return
    try:
        val = int(args[1]); assert 5 <= val <= 200
        CURRENT_CONC_LIMIT = val; await safe_tg_call(message.answer(f"✅ Concurrency limit set to {CURRENT_CONC_LIMIT}")); logger.info(f"Concurrency limit changed to {CURRENT_CONC_LIMIT} by admin.")
    except (ValueError, AssertionError): await safe_tg_call(message.answer("❌ Limit must be a number between 5 and 200."))

# --- AUTO INDEXING (DUAL DB) ---
@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID or LIBRARY_CHANNEL_ID == 0: return
    
    info = extract_movie_info(message)
    if not info:
        if message.caption: logger.warning(f"Auto-Index Skip (MsgID {message.message_id}): Could not parse title/info from caption: '{message.caption[:50]}...'")
        return

    file_id = info["file_id"]
    file_unique_id = info["file_unique_id"]
    
    imdb_id = info.get("imdb_id") or f"auto_{message.message_id}"
    title = info["title"]
    year = info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: '{title}'):"
    
    db_res_task = safe_db_call(db.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    neondb_res_task = safe_neondb_call(neondb.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title))
    ts_data = {'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val}
    ts_res_task = typesense_add_movie(ts_data)
    
    db_res, neondb_res, ts_res = await asyncio.gather(db_res_task, neondb_res_task, ts_res_task)

    if db_res in [True, "updated"]: logger.info(f"{log_prefix} Mongo OK.")
    else: logger.error(f"{log_prefix} Mongo FAILED.")
    
    if neondb_res: logger.info(f"{log_prefix} NeonDB OK.")
    else: logger.error(f"{log_prefix} NeonDB FAILED/Duplicate.")
        
    if ts_res: logger.info(f"{log_prefix} Typesense OK.")
    else: logger.error(f"{log_prefix} Typesense FAILED.")


# --- ERROR HANDLER ---
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.exception(f"Unhandled error during update processing: {exception}", exc_info=True)
    target_chat_id = None
    callback_query = None
    if update.message: target_chat_id = update.message.chat.id
    elif update.callback_query:
        callback_query = update.callback_query
        if callback_query.message: target_chat_id = callback_query.message.chat.id
    error_message = "❗️ An unexpected error occurred. Please try again later."
    if target_chat_id:
        try: await bot.send_message(target_chat_id, error_message)
        except Exception as notify_err: logger.error(f"Failed to notify user about error: {notify_err}")
    if callback_query:
        try: await callback_query.answer("Error processing request.", show_alert=False)
        except Exception as cb_err: logger.error(f"Failed to answer callback query during error handling: {cb_err}")

# --- Main Execution (for local testing) ---
async def main():
    logger.info("Bot starting in polling mode (for local testing)...")
    try:
        await db.init_db()
        await neondb.init_db()
        await is_typesense_ready()
    except Exception as init_err:
        logger.critical(f"Initialization failed in main(): {init_err}", exc_info=True)
        return
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    await shutdown_procedure(asyncio.get_running_loop())


if __name__ == "__main__":
    logger.warning("Running bot directly using __main__. Uvicorn/FastAPI is recommended for deployment.")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
    except Exception as e:
        logger.critical(f"Bot failed to run: {e}", exc_info=True)
