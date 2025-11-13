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
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.client.default import DefaultBotProperties

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Database Imports ---
from database import Database
# CHANGE 2: Typesense Removed, Secondary MongoDB Added
from secondary_db import SecondaryDB 
# --- NEONDB Import ---
from neondb import NeonDB


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
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING) 

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))

# @THEGREATMOVIESL9 -> thegreatmoviesl9
# @MOVIEMAZASU -> MOVIEMAZASU (Capital 'U')
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "thegreatmoviesl9").replace("@", "")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "MOVIEMAZASU").replace("@", "")

DATABASE_URL = os.getenv("DATABASE_URL") 
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL") 
# CHANGE 2: New ENV for Secondary DB
SECONDARY_DATABASE_URL = os.getenv("SECONDARY_DATABASE_URL") 

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "")
ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

# --- Triple-Engine Search (Revised) ---
# CHANGE 2: 'typesense' replaced with 'secondary'
SEARCH_MODES = ["secondary", "neondb", "mongodb"]
CURRENT_SEARCH_MODE_INDEX = 0 # Default is Secondary DB
CURRENT_SEARCH_MODE = SEARCH_MODES[CURRENT_SEARCH_MODE_INDEX]
# --- End Triple-Engine Search ---

# ============ TIMEOUTS ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 10
TG_OP_TIMEOUT = 5

# ============ SEMAPHORE ============
DB_SEMAPHORE = asyncio.Semaphore(10)
# --- Naye Semaphores (Rate Limit se bachne ke liye) ---
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_FORWARD_SEMAPHORE = asyncio.Semaphore(15)

# --- Critical Configuration Checks ---
if not BOT_TOKEN:
    logger.critical("Missing BOT_TOKEN environment variable! Exiting.")
    raise SystemExit(1)
if not DATABASE_URL:
    logger.critical("Missing DATABASE_URL (MongoDB Atlas) environment variable! Exiting.")
    raise SystemExit(1)
if not NEON_DATABASE_URL:
    logger.critical("Missing NEON_DATABASE_URL (Postgres) environment variable! Exiting.")
    raise SystemExit(1)
# Check for Secondary DB
if not SECONDARY_DATABASE_URL:
    logger.warning("Missing SECONDARY_DATABASE_URL! Secondary DB features will fail.")
    
if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID is not set. Admin commands will not work.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID is not set. Auto-indexing and Migration will not work.")
if not JOIN_CHANNEL_USERNAME and not USER_GROUP_USERNAME:
    logger.warning("--- NO JOIN_CHANNEL_USERNAME or USER_GROUP_USERNAME set. Membership check is fully SKIPPED. ---")


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
    # CHANGE 2: Init Secondary DB
    sec_db = SecondaryDB(SECONDARY_DATABASE_URL)
    logger.info("Database objects (Mongo Primary, Mongo Secondary, Neon) created.")
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
        if sec_db and sec_db.client:
            sec_db.client.close()
            logger.info("Secondary MongoDB client connection closed.")
        if neondb:
            await neondb.close()
            logger.info("NeonDB (Postgres) pool closed.")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")
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
        logger.error(f"DB timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore = None):
    """Telegram call ke liye safe wrapper, optional semaphore ke saath."""
    try:
        if semaphore:
            async with semaphore:
                await asyncio.sleep(0.5) # Thoda rate limit
                return await asyncio.wait_for(coro, timeout=timeout)
        else:
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError: 
        logger.warning(f"TG timeout: {getattr(coro, '__name__', 'unknown_coro')}"); return None
    except (TelegramAPIError, TelegramBadRequest) as e:
        if "bot was blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
            logger.info(f"TG: Bot blocked or user deactivated."); return False
        elif "chat not found" in str(e).lower() or "peer_id_invalid" in str(e).lower():
            logger.info(f"TG: Chat not found or Peer ID invalid."); return False
        elif "message is not modified" in str(e).lower():
            logger.debug(f"TG: Message not modified."); return None
        elif "message to delete not found" in str(e).lower():
            logger.debug(f"TG: Message to delete not found."); return None
        elif "too many requests" in str(e).lower():
            logger.warning(f"TG: FLOOD WAIT (Too Many Requests). {e}")
            await asyncio.sleep(10) # Flood wait par 10 sec wait karein
            return None
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

# --- "ULTIMATE STABLE" MEMBERSHIP CHECK ---
async def check_user_membership(user_id: int) -> bool:
    """
    Checks user membership based on configured channels/groups.
    - If both are set, checks both.
    - If one is set, checks one.
    - If none are set, returns True.
    """
    check_channel = bool(JOIN_CHANNEL_USERNAME)
    check_group = bool(USER_GROUP_USERNAME)

    # Agar koi bhi check set nahi hai, toh access de dein
    if not check_channel and not check_group:
        return True

    try:
        tasks_to_run = []
        if check_channel:
            tasks_to_run.append(safe_tg_call(
                bot.get_chat_member(chat_id=f"@{JOIN_CHANNEL_USERNAME}", user_id=user_id),
                timeout=5
            ))
        if check_group:
            tasks_to_run.append(safe_tg_call(
                bot.get_chat_member(chat_id=f"@{USER_GROUP_USERNAME}", user_id=user_id),
                timeout=5
            ))

        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
        
        valid_statuses = {"member", "administrator", "creator"}
        
        # Default True (paas) hai agar check nahi kiya gaya
        is_in_channel = True 
        is_in_group = True

        result_index = 0
        if check_channel:
            channel_member = results[result_index]
            is_in_channel = isinstance(channel_member, types.ChatMember) and channel_member.status in valid_statuses
            if channel_member in [False, None]:
                    logger.warning(f"Membership check failed for Channel @{JOIN_CHANNEL_USERNAME}. Check bot admin rights or username.")
            result_index += 1
        
        if check_group:
            group_member = results[result_index]
            is_in_group = isinstance(group_member, types.ChatMember) and group_member.status in valid_statuses
            if group_member in [False, None]:
                    logger.warning(f"Membership check failed for Group @{USER_GROUP_USERNAME}. Check bot admin rights or username.")

        # User ko sabhi *configured* channels/groups mein hona zaroori hai
        return is_in_channel and is_in_group

    except Exception as e:
            if not isinstance(e, (TelegramBadRequest, TelegramAPIError)):
                logger.error(f"Unexpected error during membership check for {user_id}: {e}", exc_info=True)
            else:
                logger.info(f"Membership check API error for {user_id}: {e}")
            return False # Error par fail karein
# --- END STABLE MEMBERSHIP CHECK ---


def get_join_keyboard():
    buttons = []
    # CHANGE 3: Better Icons
    if JOIN_CHANNEL_USERNAME: buttons.append([InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")])
    if USER_GROUP_USERNAME: buttons.append([InlineKeyboardButton(text="👥 Join Group", url=f"https://t.me/{USER_GROUP_USERNAME}")])
    
    if buttons: buttons.append([InlineKeyboardButton(text="✅ Joined", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

def get_full_limit_keyboard():
    if not ALTERNATE_BOTS: return None
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

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

def overflow_message(active_users: int) -> str: return f"⚠️ Server Capacity Reached ({active_users}/{CURRENT_CONC_LIMIT}).\nPlease use alternate bots:"

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

    # --- MongoDB Init ---
    try:
        await db.init_db()
        logger.info("Database (MongoDB) initialization successful.")
    except Exception as e:
        logger.critical(f"FATAL: Database (MongoDB) initialization failed: {e}", exc_info=True)
        raise RuntimeError("MongoDB connection failed on startup.") from e

    # --- NeonDB (Postgres) Init ---
    try:
        await neondb.init_db()
        logger.info("Database (NeonDB/Postgres) initialization successful.")
    except Exception as e:
        logger.critical(f"FATAL: Database (NeonDB/Postgres) initialization failed: {e}", exc_info=True)
        raise RuntimeError("NeonDB/Postgres connection failed on startup.") from e

    # --- Secondary DB Init (CHANGE 2) ---
    try:
        sec_ok = await sec_db._connect()
        if sec_ok: logger.info("Secondary MongoDB initialization successful.")
        else: logger.warning("SECONDARY MONGODB INITIALIZATION FAILED. Search will degrade.")
    except Exception as e: logger.critical(f"Error during Secondary DB initialization: {e}", exc_info=True)

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
    db_check_task = safe_db_call(db.is_ready(), default=False)
    neondb_check_task = safe_db_call(neondb.is_ready(), default=False)
    sec_check_task = safe_db_call(sec_db.is_ready(), default=False) # Check Secondary
    
    db_ok, neondb_ok, sec_ok = await asyncio.gather(
        db_check_task, neondb_check_task, sec_check_task
    )

    status_code = 200
    status_msg = "ok"
    
    if CURRENT_SEARCH_MODE == "secondary" and not sec_ok:
        status_msg = "degraded_secondary"
    elif CURRENT_SEARCH_MODE == "neondb" and not neondb_ok:
        status_msg = "degraded_neondb"
    elif not db_ok:
        status_msg = "error_db"
        status_code = 503
        
    return {
        "status": status_msg,
        "database_mongo_connected": db_ok,
        "database_neon_connected": neondb_ok,
        "secondary_db_connected": sec_ok,
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

@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user = message.from_user
    if not user: return
    user_id = user.id
    bot_info = await safe_tg_call(bot.get_me(), timeout=5)
    bot_username = bot_info.username if bot_info else "Bot"
    await safe_db_call(db.add_user(user_id, user.username, user.first_name, user.last_name))

    # --- Admin Panel (Triple DB) ---
    if user_id == ADMIN_USER_ID:
        user_count_task = safe_db_call(db.get_user_count(), default=0)
        mongo_count_task = safe_db_call(db.get_movie_count(), default=-1)
        neon_count_task = safe_db_call(neondb.get_movie_count(), default=-1)
        concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        # Connection status checks
        sec_ready_task = safe_db_call(sec_db.is_ready(), default=False)
        neon_ready_task = safe_db_call(neondb.is_ready(), default=False)
        mongo_ready_task = safe_db_call(db.is_ready(), default=False)

        user_count, mongo_count_raw, neon_count_raw, concurrent_users, sec_ready, neon_ready, mongo_ready = await asyncio.gather(
            user_count_task, mongo_count_task, neon_count_task, concurrent_users_task,
            sec_ready_task, neon_ready_task, mongo_ready_task
        )
        
        def status_icon(is_ok): return "🟢" if is_ok else "❌"

        mongo_count_str = f"{mongo_count_raw:,}" if mongo_count_raw >= 0 else "Error"
        neon_count_str = f"{neon_count_raw:,}" if neon_count_raw >= 0 else "Error"
        
        search_status = f"⚡️ {CURRENT_SEARCH_MODE.capitalize()}"
        if (CURRENT_SEARCH_MODE == 'secondary' and not sec_ready) or \
           (CURRENT_SEARCH_MODE == 'neondb' and not neon_ready) or \
           (CURRENT_SEARCH_MODE == 'mongodb' and not mongo_ready):
            search_status += " (⚠️ Failing over...)"


        admin_message = (
            f"🛡 <b>Admin: @{bot_username}</b> (Triple-DB)\n\n"
            f"<b>Status</b>\n"
            f"🟢 Active ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}\n"
            f"👥 Users (Mongo): {user_count:,}\n"
            f"🎬 Movies (Mongo): {mongo_count_str}\n"
            f"🗂️ Index (Neon): {neon_count_str}\n"
            f"⏰ Uptime: {get_uptime()}\n\n"
            f"<b>Connections</b>\n"
            f"{status_icon(mongo_ready)} MongoDB (Primary)\n"
            f"{status_icon(sec_ready)} MongoDB (Secondary)\n"
            f"{status_icon(neon_ready)} NeonDB (Postgres)\n\n"
            f"<b>Search Mode: {search_status}</b>\n"
            f"/search_switch (Toggle Search Engine)\n\n"
            f"<b>Commands</b>\n"
            f"/stats | /health | /get_user `ID`\n"
            f"/broadcast (Reply)\n"
            f"/set_limit `N` (5-200)\n\n"
            f"<b>Data & Indexing</b>\n"
            f"<b>/remove_library_duplicates</b> ⚠️ (NeonDB)\n"
            f"<b>/backup_channel</b> 🚀 (NeonDB)\n"
            f"<b>/sync_mongo_to_neon</b> 🔄 (Mongo->Neon)\n"
            f"<b>/sync_secondary</b> 🔄 (Mongo->Secondary)\n"
            f"<b>/cleanup_mongo_duplicates</b> (Mongo DB)\n"
            f"/rebuild_index (Mongo DB)\n"
            f"/cleanup_users (Inactive >30d)\n\n"
            f"<b>⭐️ Migration:</b> Forward files from `LIBRARY_CHANNEL` (Admin only)."
        )
        await safe_tg_call(message.answer(admin_message))
        return

    # --- Regular User (CHANGE 3: Professional UI) ---
    if not await ensure_capacity_or_inform(message): return
    is_member = await check_user_membership(user_id) 
    join_markup = get_join_keyboard() 
    
    if is_member:
        welcome_text = (
            f"👋 Hello <b>{user.first_name}</b>!\n\n"
            f"🎬 <b>Looking for movies or series?</b>\n"
            f"Just type the name and I'll find it for you.\n\n"
            f"🔍 <i>Example:</i> <code>Avengers</code> or <code>Pushpa 2021</code>\n"
            f"✨ <i>Pro Tip:</i> Add the year for better results."
        )
        await safe_tg_call(message.answer(welcome_text, reply_markup=None))
    else: 
        welcome_text = (
            f"👋 <b>Welcome {user.first_name}!</b>\n\n"
            f"To use this bot, please join our channels below.\n"
            f"Once joined, click the <b>'✅ Joined'</b> button."
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
        "❓ <b>How to use</b>\n\n"
        "1.  <b>Instant Search:</b> Send the movie/show name.\n"
        "    Example: <code>Jawan</code>\n\n"
        "2.  <b>Smart Search:</b> Spelling mistakes are fine!\n"
        "    Example: <code>Mirjapur</code> works too.\n\n"
        "3.  <b>Better Results:</b> Add the year.\n"
        "    Example: <code>Iron Man 2008</code>\n\n"
        "⚠️ <b>Note:</b> Server might take 10s to wake up if inactive."
    )
    await safe_tg_call(message.answer(help_text))


@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))
    await safe_tg_call(callback.answer("Verifying..."))
    if not await ensure_capacity_or_inform(callback): return

    is_member = await check_user_membership(user.id)
    join_markup = get_join_keyboard()

    if is_member:
        active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = (
            f"✅ <b>Access Granted!</b>\n\n"
            f"You can now search for movies.\n"
            f"Just type the name (e.g., <code>Pathaan</code>)."
        )
        edited = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        if not edited: await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=None))
    else:
        await safe_tg_call(callback.answer("⚠️ You haven't joined the channels yet!", show_alert=True))
        if callback.message and (not callback.message.reply_markup or not callback.message.reply_markup.inline_keyboard):
             if callback.message.text: 
                 await safe_tg_call(callback.message.edit_reply_markup(reply_markup=join_markup))


# =======================================================
# +++++ UNIVERSAL SEARCH HANDLER (Triple-Engine Waterfall) +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(15)
async def search_movie_handler(message: types.Message):
    global CURRENT_SEARCH_MODE, CURRENT_SEARCH_MODE_INDEX
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message): return
    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Please type at least 2 characters."))
        return

    searching_msg = await safe_tg_call(message.answer(f"🔎 Searching for <b>{original_query}</b>..."))
    if not searching_msg: return

    search_results = None
    search_engine_used = ""

    # --- Search Engine Waterfall (High Availability) ---
    # 1. Preferred engine se try karein
    preferred_mode = CURRENT_SEARCH_MODE
    logger.info(f"User {user_id} searching. Preferred engine: {preferred_mode}")
    
    if preferred_mode == "secondary":
        if await safe_db_call(sec_db.is_ready(), default=False):
            search_results = await sec_db.search_movies(original_query, limit=20)
            search_engine_used = "Fast DB"
    elif preferred_mode == "neondb":
        if await safe_db_call(neondb.is_ready(), default=False):
            search_results = await safe_db_call(neondb.neondb_search(original_query, limit=20), default=[])
            search_engine_used = "NeonDB Index"
    elif preferred_mode == "mongodb":
        if await safe_db_call(db.is_ready(), default=False):
            search_results = await safe_db_call(db.mongo_search_internal(original_query, limit=20), default=[])
            search_engine_used = "Primary DB"

    # 2. Agar fail hua, toh auto-failover karein
    if not search_results:
        logger.warning(f"Preferred engine '{preferred_mode}' failed/empty. Trying failovers...")
        
        # Try Secondary (Fastest)
        if preferred_mode != "secondary" and await safe_db_call(sec_db.is_ready(), default=False):
            search_results = await sec_db.search_movies(original_query, limit=20)
            search_engine_used = "Fast DB (Fallback)"
        
        # Try NeonDB
        elif not search_results and preferred_mode != "neondb" and await safe_db_call(neondb.is_ready(), default=False):
            search_results = await safe_db_call(neondb.neondb_search(original_query, limit=20), default=[])
            search_engine_used = "NeonDB (Fallback)"
            
        # Try MongoDB (Last Resort)
        elif not search_results and preferred_mode != "mongodb" and await safe_db_call(db.is_ready(), default=False):
            search_results = await safe_db_call(db.mongo_search_internal(original_query, limit=20), default=[])
            search_engine_used = "Primary DB (Fallback)"

    # --- End Search Waterfall ---

    if not search_results:
        await safe_tg_call(searching_msg.edit_text(f"❌ No results found for '<b>{original_query}</b>'.\nTry checking the spelling."))
        return

    buttons = []
    max_buttons = 15
    for movie in search_results[:max_buttons]:
        # CHANGE 3: Clean UI
        display_title = movie["title"][:50] + '...' if len(movie["title"]) > 50 else movie["title"]
        year_str = f" ({movie['year']})" if movie.get('year') and movie['year'] != 'N/A' else ""
        buttons.append([InlineKeyboardButton(text=f"🎬 {display_title}{year_str}", callback_data=f"get_{movie['imdb_id']}")])

    result_count_text = f"{len(search_results)}" if len(search_results) <= max_buttons else f"{max_buttons}+"
    await safe_tg_call(searching_msg.edit_text(
        f"✅ Found {result_count_text} results for <b>{original_query}</b>:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20) # Increased timeout for copy_message
async def get_movie_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))
    await safe_tg_call(callback.answer("📤 Sending file..."))
    if not await ensure_capacity_or_inform(callback): return
    
    imdb_id = callback.data.split("_", 1)[1]
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ File not found in database. It might have been deleted."))
        return

    # CHANGE 3: UI Update
    await safe_tg_call(callback.message.edit_text(f"📤 Sending <b>{movie['title']}</b>..."))
    success = False
    error_detail = "Unknown error"
    
    # --- CHANGE 1: COPY MODE (Forwarding -> Copy) ---
    # Copy message hides the "Forwarded from" tag.
    # Logic: Try copy_message -> If fail (e.g. bot not admin in source), try send_document (if file_id exists)
    
    try:
        if movie.get("channel_id") and movie.get("channel_id") != 0 and \
           movie.get("message_id") and movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER:
            
            # Attempt Copy
            copy_result = await safe_tg_call(bot.copy_message(
                chat_id=user.id,
                from_chat_id=int(movie["channel_id"]),
                message_id=movie["message_id"],
                caption=f"🎬 <b>{movie['title']}</b>\n\n🤖 {await bot.get_my_name()}", # New caption
                parse_mode=ParseMode.HTML
            ), timeout=TG_OP_TIMEOUT * 2)
            
            if copy_result: 
                success = True
            else:
                error_detail = "Copy failed (Restriction or Bot not Admin in Source)."
        else:
            error_detail = "Invalid Channel/Message ID."
            
        # Fallback: Send by File ID if Copy fails
        if not success and movie.get("file_id"):
            logger.info(f"Copy failed for {imdb_id}, falling back to send_document.")
            send_result = await safe_tg_call(bot.send_document(
                chat_id=user.id,
                document=movie["file_id"],
                caption=f"🎬 <b>{movie['title']}</b>"
            ), timeout=TG_OP_TIMEOUT * 4)
            if send_result: success = True
            else: error_detail += " & Send_Doc Failed."

    except Exception as e:
        error_detail = f"Exception: {e}"
        logger.error(f"Exception during copy/send for {imdb_id}: {e}", exc_info=True)

    if not success:
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: Could not send <b>{movie['title']}</b>.\nReason: {error_detail}{admin_hint}"
        await safe_tg_call(bot.send_message(user.id, error_text))
        try: await safe_tg_call(callback.message.edit_text(f"❌ Failed to send <b>{movie['title']}</b>."))
        except: pass
    else:
        # Optional: Clean up the "Sending..." message to keep chat clean
        try: await callback.message.delete()
        except: pass


# =======================================================
# +++++ MIGRATION HANDLER (Syncs to Secondary DB now) +++++
# =======================================================
@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message):
    if not message.forward_from_chat or message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0:
            await safe_tg_call(message.answer("❌ Migration Error: `LIBRARY_CHANNEL_ID` not set."))
        else:
            await safe_tg_call(message.answer(f"For Migration, forward files from `LIBRARY_CHANNEL` (ID: `{LIBRARY_CHANNEL_ID}`)."))
        return

    if not (message.video or message.document):
        logger.warning("Admin forwarded a non-file message, skipping migration.")
        return

    info = extract_movie_info(message.caption or "")
    if not info or not info.get("title"):
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption parse failed.")
        await safe_tg_call(message.answer(f"❌ Skipped: MsgID `{message.forward_from_message_id}` caption parse failed."))
        return

    file_data = message.video or message.document
    file_id = file_data.file_id
    file_unique_id = file_data.file_unique_id
    
    message_id = message.forward_from_message_id
    channel_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{message_id}"
    title = info["title"]
    year = info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    # 1. MongoDB
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    db_map = {True: "✅ Migrated (Added DB)", "updated": "✅ Migrated (Updated DB)", "duplicate": "ℹ️ Migrated (Skipped DB)", False: "❌ DB Error"}
    db_status = db_map.get(db_res, "❌ DB Error")

    # 2. NeonDB
    neon_res = await safe_db_call(neondb.add_movie(message_id, channel_id, file_id, file_unique_id, imdb_id, title))
    neon_status = "✅ Neon Synced" if neon_res else "❌ Neon Sync Fail"
    
    await safe_tg_call(message.answer(f"{db_status} | {neon_status}\n<b>{title}</b>"))
    
    # 3. Secondary DB (Instead of Typesense)
    if db_res in [True, "updated"]:
        ts_data = {'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val}
        ts_res = await sec_db.add_movie(ts_data)
        if not ts_res:
            await safe_tg_call(message.answer(f"❌ Secondary DB Sync Error for <b>{title}</b>"))


# --- ADMIN COMMANDS (Restored Full Code) ---

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    
    user_count_task = safe_db_call(db.get_user_count(), default=0)
    mongo_count_task = safe_db_call(db.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(neondb.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    sec_ready_task = safe_db_call(sec_db.is_ready(), default=False)
    neon_ready_task = safe_db_call(neondb.is_ready(), default=False)
    mongo_ready_task = safe_db_call(db.is_ready(), default=False)

    user_count, mongo_count_raw, neon_count_raw, concurrent_users, sec_ready, neon_ready, mongo_ready = await asyncio.gather(
        user_count_task, mongo_count_task, neon_count_task, concurrent_users_task,
        sec_ready_task, neon_ready_task, mongo_ready_task
    )
    
    def status_icon(is_ok): return "🟢" if is_ok else "❌"

    mongo_count_str = f"{mongo_count_raw:,}" if mongo_count_raw >= 0 else "Error"
    neon_count_str = f"{neon_count_raw:,}" if neon_count_raw >= 0 else "Error"
    
    search_status = f"⚡️ {CURRENT_SEARCH_MODE.capitalize()}"
    if (CURRENT_SEARCH_MODE == 'secondary' and not sec_ready) or \
        (CURRENT_SEARCH_MODE == 'neondb' and not neon_ready) or \
        (CURRENT_SEARCH_MODE == 'mongodb' and not mongo_ready):
        search_status += " (⚠️ Failing over...)"
        
    stats_msg = (
        f"📊 Stats (Triple-DB)\n"
        f"🟢 Active({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}\n"
        f"👥 Users: {user_count:,}\n"
        f"🎬 Movies: {mongo_count_str}\n"
        f"🗂️ Index: {neon_count_str}\n"
        f"⏰ Uptime: {get_uptime()}\n\n"
        f"<b>Connections</b>\n"
        f"{status_icon(mongo_ready)} Mongo (Pri)\n"
        f"{status_icon(sec_ready)} Mongo (Sec)\n"
        f"{status_icon(neon_ready)} NeonDB\n\n"
        f"<b>Search Mode: {search_status}</b>"
    )
    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("search_switch"), AdminFilter())
@handler_timeout(10)
async def search_switch_command(message: types.Message):
    global CURRENT_SEARCH_MODE, CURRENT_SEARCH_MODE_INDEX
    
    CURRENT_SEARCH_MODE_INDEX = (CURRENT_SEARCH_MODE_INDEX + 1) % len(SEARCH_MODES)
    CURRENT_SEARCH_MODE = SEARCH_MODES[CURRENT_SEARCH_MODE_INDEX]
    
    status = "⚠️ (Not Connected)"
    if CURRENT_SEARCH_MODE == "secondary":
        if await safe_db_call(sec_db.is_ready(), default=False): status = "🟢 (Ready)"
    elif CURRENT_SEARCH_MODE == "neondb":
        if await safe_db_call(neondb.is_ready(), default=False): status = "🟢 (Ready)"
    elif CURRENT_SEARCH_MODE == "mongodb":
        if await safe_db_call(db.is_ready(), default=False): status = "🟢 (Ready)"

    logger.info(f"Admin changed search mode to: {CURRENT_SEARCH_MODE}")
    await safe_tg_call(message.answer(f"✅ Search mode set to <b>{CURRENT_SEARCH_MODE.capitalize()}</b>.\nStatus: {status}"))


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
    async def send_to_user(uid):
        nonlocal s, f
        res = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=5)
        if res: s += 1
        elif res is False: f += 1; await safe_db_call(db.deactivate_user(uid))
        else: f += 1
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
            if processed_count < total: await asyncio.sleep(1)
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

@dp.message(Command("get_user"), AdminFilter())
@handler_timeout(10)
async def get_user_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer("❌ Use: /get_user `USER_ID`"))
        return
    
    user_id_to_find = int(args[1])
    user_data = await safe_db_call(db.get_user_info(user_id_to_find))
    
    if not user_data:
        await safe_tg_call(message.answer(f"❌ User <code>{user_id_to_find}</code> not found in database."))
        return
        
    user_text = (
        f"<b>User Info:</b> <code>{user_data.get('user_id')}</code>\n"
        f"<b>Username:</b> @{user_data.get('username') or 'N/A'}\n"
        f"<b>First Name:</b> {user_data.get('first_name') or 'N/A'}\n"
        f"<b>Last Name:</b> {user_data.get('last_name') or 'N/A'}\n"
        f"<b>Joined:</b> {user_data.get('joined_date')}\n"
        f"<b>Last Active:</b> {user_data.get('last_active')}\n"
        f"<b>Is Active:</b> {user_data.get('is_active', True)}"
    )
    await safe_tg_call(message.answer(user_text))


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800)
async def import_json_command(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.document: await safe_tg_call(message.answer("❌ Reply to .json file.")); return
    doc = message.reply_to_message.document;
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"): await safe_tg_call(message.answer("❌ Must be .json file.")); return
    msg = await safe_tg_call(message.answer(f"⏳ Downloading `{doc.file_name}`..."));
    if not msg: return
    try:
        file = await bot.get_file(doc.file_id)
        fio = io.BytesIO()
        await bot.download_file(file.file_path, fio)
        fio.seek(0)
        mlist = json.loads(fio.read().decode('utf-8'))
        assert isinstance(mlist, list)
        logger.info(f"JSON Downloaded: {doc.file_name}, Items: {len(mlist)}")
    except Exception as e: await safe_tg_call(msg.edit_text(f"❌ Download/Parse Error: {e}")); logger.exception("JSON download/parse error"); return
    
    total = len(mlist); a, u, s, fdb = 0, 0, 0, 0; sec_batch = []; neon_batch = []; st = datetime.now(timezone.utc)
    await safe_tg_call(msg.edit_text(f"⏳ Processing {total:,} items (DB)..."))
    
    for i, item in enumerate(mlist):
        try:
            fid = item.get("file_id")
            fname = item.get("title")
            if not fid or not fname: 
                s += 1; continue
            fid_str = str(fid)
            file_unique_id = item.get("file_unique_id") or fid_str 
            
            imdb = f"json_{hashlib.md5(fid_str.encode()).hexdigest()[:10]}"
            message_id = item.get("message_id") or AUTO_MESSAGE_ID_PLACEHOLDER
            channel_id = item.get("channel_id") or 0
            info = parse_filename(fname); 
            title = info["title"] or "Untitled"; 
            year = info["year"]
            clean_title_val = clean_text_for_search(title)
            
            # 1. Add to Mongo
            db_res = await safe_db_call(db.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id))
            
            # Prepare Batch for others
            ts_data = {'imdb_id': imdb, 'title': title, 'year': year, 'clean_title': clean_title_val}
            neon_data = (message_id, channel_id, fid_str, file_unique_id, imdb, title)

            if db_res is True: a += 1; sec_batch.append(ts_data); neon_batch.append(neon_data)
            elif db_res == "updated": u += 1; sec_batch.append(ts_data); neon_batch.append(neon_data)
            elif db_res == "duplicate": s += 1
            else: fdb += 1
        except Exception as e: fdb += 1; logger.error(f"Error processing JSON item {i+1}: {e}", exc_info=False)
        
        now = datetime.now(timezone.utc);
        if (i + 1) % 200 == 0 or (now - st).total_seconds() > 15 or (i+1) == total:
            try: await safe_tg_call(msg.edit_text(f"⏳ DB: {i+1}/{total:,} | ✅A:{a:,} 🔄U:{u:,} ↷S:{s:,} ❌F:{fdb:,}"))
            except TelegramBadRequest: pass
            st = now; await asyncio.sleep(0.05)
    
    db_sum = f"DB Done: ✅Added:{a:,} 🔄Updated:{u:,} ↷Skipped:{s:,} ❌Failed:{fdb:,}";
    await safe_tg_call(msg.edit_text(f"{db_sum}\n⏳ Syncing {len(sec_batch):,} items to Secondary DB & Neon..."))
    
    # --- Secondary DB Sync ---
    sec_stat = ""
    if sec_batch:
        sec_res = await sec_db.add_batch_movies(sec_batch)
        sec_stat = f"✅ Secondary DB: {len(sec_batch):,} synced" if sec_res else "❌ Secondary DB: FAILED"
    else: sec_stat = "ℹ️ Secondary DB: Nothing to sync."
    
    # --- NeonDB Sync ---
    neon_stat = ""
    if neon_batch:
        mongo_like_data = [{
            "message_id": d[0], "channel_id": d[1], "file_id": d[2], 
            "file_unique_id": d[3], "imdb_id": d[4], "title": d[5]
        } for d in neon_batch]
        
        neon_res_count = await safe_db_call(neondb.sync_from_mongo(mongo_like_data), default=0)
        neon_stat = f"✅ NeonDB: {neon_res_count:,} synced"
    else: neon_stat = "ℹ️ NeonDB: Nothing to sync."

    await safe_tg_call(msg.edit_text(f"✅ Import Complete!\n{db_sum}\n{sec_stat}\n{neon_stat}"))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split(maxsplit=1);
    if len(args) < 2: await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID")); return
    imdb_id = args[1].strip(); msg = await safe_tg_call(message.answer(f"⏳ Removing <code>{imdb_id}</code>..."))
    
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id));
    db_del = await safe_db_call(db.remove_movie_by_imdb(imdb_id))
    db_stat = f"✅ DB Removed '{movie['title'] if movie else imdb_id}'." if db_del else ("ℹ️ DB Not found." if not movie else "❌ DB Error removing.")
    
    # Remove from Secondary DB
    sec_del = await sec_db.remove_movie(imdb_id)
    sec_stat = "✅ Secondary DB Removed." if sec_del else "ℹ️ Secondary Not Found or Error."
    
    txt = f"{db_stat}\n{sec_stat}";
    if msg: await safe_tg_call(msg.edit_text(txt))
    else: await safe_tg_call(message.answer(txt))


@dp.message(Command("cleanup_mongo_duplicates"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_duplicates_command(message: types.Message):
    msg = await safe_tg_call(message.answer("⏳ MongoDB mein `imdb_id` duplicates dhoondhe ja rahe hain (Batch: 100)..."))
    
    deleted_count, duplicates_found = await safe_db_call(db.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ {deleted_count} duplicate entries Mongo se delete kiye.\n"
                                        f"ℹ️ Abhi bhi {max(0, duplicates_found - deleted_count)} duplicates baaki hain. Command dobara chalayein."))
    else:
        await safe_tg_call(msg.edit_text("✅ MongoDB mein `imdb_id` duplicates nahi mile."))


@dp.message(Command("remove_library_duplicates"), AdminFilter())
@handler_timeout(3600) 
async def remove_library_duplicates_command(message: types.Message):
    msg = await safe_tg_call(message.answer("⏳ NeonDB se `file_unique_id` duplicates dhoondhe ja rahe hain... (Batch: 100)"))
    if not msg: return
    
    messages_to_delete, total_duplicates = await safe_db_call(neondb.find_and_delete_duplicates(batch_limit=100), default=([], 0))
    
    if not messages_to_delete:
        await safe_tg_call(msg.edit_text("✅ Library mein koi duplicate files nahi mili."))
        return
        
    await safe_tg_call(msg.edit_text(f"✅ {total_duplicates} duplicates mile.\n⏳ Ab {len(messages_to_delete)} files ko channel se delete kiya ja raha hai..."))
    
    deleted_count = 0
    failed_count = 0
    tasks = []
    
    async def delete_message(msg_id, chat_id):
        nonlocal deleted_count, failed_count
        res = await safe_tg_call(
            bot.delete_message(chat_id=chat_id, message_id=msg_id),
            semaphore=TELEGRAM_DELETE_SEMAPHORE
        )
        if res or res is None: deleted_count += 1
        else: failed_count += 1

    for msg_id, chat_id in messages_to_delete:
        tasks.append(delete_message(msg_id, chat_id))
        
    await asyncio.gather(*tasks) 
    
    await safe_tg_call(msg.edit_text(f"✅ Cleanup Done!\n"
                                    f"🗑️ Deleted: {deleted_count}\n"
                                    f"❌ Failed: {failed_count}\n"
                                    f"ℹ️ Left: {max(0, total_duplicates - deleted_count)}"))


@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(7200) 
async def backup_channel_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Use: /backup_channel `TARGET_CHANNEL`"))
        return
        
    target_channel = args[1].strip()
    msg = await safe_tg_call(message.answer(f"⏳ NeonDB se unique files fetch ki ja rahi hai..."))
    if not msg: return
    
    unique_files = await safe_db_call(neondb.get_unique_movies_for_backup(), default=[])
    
    if not unique_files:
        await safe_tg_call(msg.edit_text("❌ NeonDB mein backup ke liye koi files nahi mili."))
        return
        
    total_files = len(unique_files)
    await safe_tg_call(msg.edit_text(f"✅ {total_files:,} unique files found.\n🚀 Forwarding to {target_channel}..."))
    
    forwarded_count = 0
    failed_count = 0
    tasks = []
    
    async def forward_file(msg_id, chat_id):
        nonlocal forwarded_count, failed_count
        res = await safe_tg_call(
            bot.forward_message(chat_id=target_channel, from_chat_id=chat_id, message_id=msg_id),
            timeout=TG_OP_TIMEOUT * 2,
            semaphore=TELEGRAM_FORWARD_SEMAPHORE
        )
        if res: forwarded_count += 1
        else: failed_count += 1

    for i, (msg_id, chat_id) in enumerate(unique_files):
        tasks.append(forward_file(msg_id, chat_id))
        
        if (i + 1) % 50 == 0 or (i + 1) == total_files:
            await asyncio.gather(*tasks)
            tasks = []
            try:
                await safe_tg_call(msg.edit_text(
                    f"🚀 Progress: {(i+1)}/{total_files}\n"
                    f"✅ Forwarded: {forwarded_count}\n"
                    f"❌ Failed: {failed_count}"
                ))
            except TelegramBadRequest: pass 
            
    await safe_tg_call(msg.edit_text(f"✅ Backup Complete!\nFWD: {forwarded_count} | Fail: {failed_count}"))


@dp.message(Command("sync_mongo_to_neon"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_to_neon_command(message: types.Message):
    msg = await safe_tg_call(message.answer("⏳ Fetching from MongoDB..."))
    if not msg: return
    
    mongo_movies = await safe_db_call(db.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies:
        await safe_tg_call(msg.edit_text("❌ No movies found in MongoDB."))
        return
    
    await safe_tg_call(msg.edit_text(f"✅ Found {len(mongo_movies):,}. Syncing to NeonDB..."))
    processed_count = await safe_db_call(neondb.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    await safe_tg_call(msg.edit_text(f"✅ Sync complete! {processed_count:,} sent to NeonDB."))


@dp.message(Command("sync_secondary"), AdminFilter()) # Renamed from sync_typesense
@handler_timeout(1800)
async def sync_secondary_command(message: types.Message, from_user_id: int = None):
    admin_id = from_user_id or message.from_user.id
    
    if not await safe_db_call(sec_db.is_ready(), default=False):
        await safe_tg_call(bot.send_message(admin_id, "❌ Secondary DB not connected."))
        return
        
    msg = None
    if not from_user_id:
        msg = await safe_tg_call(message.answer("⚠️ Secondary DB Sync Started...\n⏳ Fetching from Primary DB..."))
        if not msg: return
    
    try:
        all_movies_db = await safe_db_call(db.get_all_movies_for_sync(), timeout=300)
        if all_movies_db is None:
            text = "❌ Error fetching movies from DB."
            if msg: await safe_tg_call(msg.edit_text(text))
            else: await safe_tg_call(bot.send_message(admin_id, text))
            return
            
        db_count = len(all_movies_db)
        text = f"✅ Fetched {db_count:,} movies.\n⏳ Syncing to Secondary DB..."
        if msg: await safe_tg_call(msg.edit_text(text))
        else: await safe_tg_call(bot.send_message(admin_id, text))
        
        # Clear first
        await sec_db.clear_all_data()
        
        # Batch Add
        success = await sec_db.add_batch_movies(all_movies_db)
        
        final_text = f"✅ Sync Complete! {db_count:,} records synced to Secondary DB." if success else "❌ Sync Failed!"
        if msg: await safe_tg_call(msg.edit_text(final_text))
        else: await safe_tg_call(bot.send_message(admin_id, final_text))
        
    except Exception as e:
        logger.error(f"Sync Error: {e}", exc_info=True)
        if msg: await safe_tg_call(msg.edit_text(f"❌ Error: {e}"))


@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300)
async def rebuild_index_command(message: types.Message):
    msg = await safe_tg_call(message.answer("🔧 Rebuilding `clean_title` and text index in MongoDB..."))
    if not msg: return
    updated, total = await safe_db_call(db.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    await safe_db_call(db.create_mongo_text_index())
    await safe_tg_call(msg.edit_text(f"✅ DB Reindex done: {updated:,} updated. Total: {total:,}."))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT; args = message.text.split();
    if len(args)<2 or not args[1].isdigit(): await safe_tg_call(message.answer(f"Use: /set_limit N (Current: {CURRENT_CONC_LIMIT})")); return
    try:
        val = int(args[1]); assert 5 <= val <= 200
        CURRENT_CONC_LIMIT = val; await safe_tg_call(message.answer(f"✅ Limit set to {CURRENT_CONC_LIMIT}")); 
    except: await safe_tg_call(message.answer("❌ Limit must be 5-200."))

# --- AUTO INDEXING (Updated for Secondary DB) ---
@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID or LIBRARY_CHANNEL_ID == 0: return
    if not (message.video or message.document): return
    info = extract_movie_info(message.caption or "");
    if not info or not info.get("title"): return

    file_data = message.video or message.document
    file_id = file_data.file_id
    file_unique_id = file_data.file_unique_id
    
    imdb_id = info.get("imdb_id") or f"auto_{message.message_id}"
    title=info["title"]; year=info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    # 1. MongoDB
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    
    # 2. NeonDB
    await safe_db_call(neondb.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title))

    # 3. Secondary DB (Real-time sync)
    if db_res in [True, "updated"]:
        ts_data = {'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val}
        await sec_db.add_movie(ts_data)
        logger.info(f"Auto-Indexed: {title}")


# --- ERROR HANDLER ---
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.exception(f"Unhandled error: {exception}", exc_info=True)
    try:
        if update.message: await bot.send_message(update.message.chat.id, "❗️ Unexpected error. Try again.")
        elif update.callback_query: await update.callback_query.answer("Error.", show_alert=False)
    except: pass

# --- Main Execution ---
async def main():
    try:
        await db.init_db()
        await neondb.init_db()
    except Exception as init_err:
        logger.critical(f"Initialization failed: {init_err}")
        return
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    await shutdown_procedure(asyncio.get_running_loop())


if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
    except Exception as e: logger.critical(f"Bot failed: {e}")
