# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import signal
import json
import hashlib
import concurrent.futures
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import List, Dict, Union, Optional
from functools import wraps

# --- Load dotenv FIRST ---
# Environment variables ko sabse pehle load karte hain
from dotenv import load_dotenv
load_dotenv()

# --- Uvloop activation ---
# Linux systems par fast performance ke liye uvloop use karte hain
try:
    import uvloop
    uvloop.install()
    logging.info("✅ Uvloop installed successfully.")
except ImportError:
    logging.info("ℹ️ Uvloop not found, using default asyncio event loop.")

# --- Aiogram & FastAPI Imports ---
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.client.default import DefaultBotProperties

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Database Imports ---
# Teeno databases ko import kar rahe hain
from database import Database
from neondb import NeonDB
from secondary_db import SecondaryDB

# ============ LOGGING SETUP ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bot")

# Reduce log noise from libraries
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)

# ============ CONFIGURATION & ENV CHECK ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))

# Channels/Groups Configuration
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "thegreatmoviesl9").replace("@", "")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "MOVIEMAZASU").replace("@", "")

# Database URLs
DATABASE_URL = os.getenv("DATABASE_URL") 
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL") 
SECONDARY_DATABASE_URL = os.getenv("SECONDARY_DATABASE_URL")

# Webhook & Server Config
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "mysecret")

# Limits
DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

# Alternate Bots List
ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "")
ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

# --- Triple-Engine Search Config ---
# Order: Secondary -> NeonDB -> MongoDB
SEARCH_MODES = ["secondary", "neondb", "mongodb"]
CURRENT_SEARCH_MODE_INDEX = 0 
CURRENT_SEARCH_MODE = SEARCH_MODES[CURRENT_SEARCH_MODE_INDEX]

# Timeouts & Semaphores
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 10
TG_OP_TIMEOUT = 5

# Rate limiting ke liye Semaphores
DB_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(20)
TELEGRAM_FORWARD_SEMAPHORE = asyncio.Semaphore(20)

# --- Helpers ---
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

# Critical Checks
if not BOT_TOKEN:
    logger.critical("❌ BOT_TOKEN missing! Exiting.")
    raise SystemExit(1)
if not DATABASE_URL:
    logger.critical("❌ DATABASE_URL missing! Exiting.")
    raise SystemExit(1)
if not NEON_DATABASE_URL:
    logger.critical("❌ NEON_DATABASE_URL missing! Exiting.")
    raise SystemExit(1)

# ============ FUNCTIONS & UTILS ============

def clean_text_for_search(text: str) -> str:
    """
    ADVANCED CLEANING FUNCTION
    - Converts 'Season 1' -> 's01' for webseries matching
    - Converts 'Episode 5' -> 'e05'
    - Removes special characters
    """
    if not text: return ""
    text = text.lower()
    
    # 1. Web Series Normalization (Critical for matching S01E01 format)
    # Matches: "season 1", "season 01", "s 1", "s01" -> "s01"
    text = re.sub(r'\b(season|s)\s*(\d+)\b', lambda m: f"s{m.group(2).zfill(2)}", text)
    
    # Matches: "episode 1", "e 1", "ep 1" -> "e01"
    text = re.sub(r'\b(episode|ep|e)\s*(\d+)\b', lambda m: f"e{m.group(2).zfill(2)}", text)
    
    # 2. General Cleanup
    text = re.sub(r"[^a-z0-9\s]+", " ", text) # Remove special chars
    text = re.sub(r"\s+", " ", text).strip()   # Remove extra spaces
    
    return text

def parse_filename(filename: str) -> Dict[str, str]:
    """
    Extracts Title and Year from a filename string.
    Used during JSON import or complex caption parsing.
    """
    if not filename: return {"title": "Untitled", "year": None}
    year = None
    
    # Try to find year in brackets (2024)
    match_paren = re.search(r"\(((19[89]\d|20[0-3]\d))\)", filename)
    if match_paren:
        year = match_paren.group(1)
    else:
        # Try to find bare year 2024
        matches_bare = re.findall(r"\b((19[89]\d|20[0-3]\d))\b", filename)
        if matches_bare:
            year = matches_bare[-1][0]
            
    # Remove extension
    title = os.path.splitext(filename)[0].strip()
    
    # Remove year from title if present at end
    if year:
        title = re.sub(rf"(\s*\(?{year}\)?\s*)$", "", title, flags=re.IGNORECASE).strip()
        
    # Remove standard junk tags
    title = re.sub(r"\[.*?\]|\(.*?\)", "", title)
    title = re.sub(r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio|esubs)\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r'[._]', ' ', title).strip()
    title = re.sub(r"\s+", " ", title).strip()
    
    return {"title": title or "Untitled", "year": year}

def extract_movie_info(caption: str):
    """
    Parses Telegram caption to find Title, Year and IMDB ID.
    """
    if not caption: return None
    info = {}
    lines = caption.splitlines()
    title = lines[0].strip() if lines else ""
    
    # Check second line for "Season X" info to append to title
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]):
        title += " " + lines[1].strip()
        
    if title: info["title"] = title
    
    # Extract IMDB ID (tt1234567)
    imdb = re.search(r"(tt\d{7,})", caption)
    if imdb: info["imdb_id"] = imdb.group(1)
    
    # Extract Year
    year = re.findall(r"\b(19[89]\d|20[0-2]\d)\b", caption)
    if year: info["year"] = year[-1]
    
    return info if "title" in info else None

def build_webhook_url() -> str:
    """Constructs the webhook URL based on environment variables."""
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        webhook_path = f"/bot/{BOT_TOKEN}"
        # Fix double /bot paths
        if base.endswith('/bot'): base = base.rsplit('/bot', 1)[0]
        elif base.endswith('/bot/'): base = base.rsplit('/bot/', 1)[0]
        final_url = f"{base}{webhook_path}"
        logger.info(f"🔗 Webhook URL generated: {final_url}")
        return final_url
    return ""

WEBHOOK_URL = build_webhook_url()

# ============ INITIALIZATION ============
try:
    # Bot init
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    # Database init
    db = Database(DATABASE_URL)
    neondb = NeonDB(NEON_DATABASE_URL)
    # Secondary DB is optional but recommended
    secondary_db = SecondaryDB(SECONDARY_DATABASE_URL) if SECONDARY_DATABASE_URL else None
    
    logger.info("✅ Bot and Database objects initialized successfully.")
except Exception as e:
    logger.critical(f"❌ Initialization Failed: {e}", exc_info=True)
    raise SystemExit("Init Failed")

start_time = datetime.now(timezone.utc)
monitor_task = None
executor = None

# ============ LIFECYCLE & SHUTDOWN ============

async def shutdown_procedure(loop):
    """Gracefully closes all connections."""
    logger.info("🛑 Initiating graceful shutdown...")
    
    # Cancel background tasks
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        
    # Delete Webhook
    if WEBHOOK_URL:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted.")
        except Exception as e: logger.error(f"Webhook delete error: {e}")
        
    # Close Bot Session
    try:
        if bot.session: await bot.session.close()
    except Exception as e: logger.error(f"Bot session close error: {e}")
    
    # Close Executor
    if executor:
        executor.shutdown(wait=True, cancel_futures=False)
        
    # Close Databases
    try:
        if db and db.client: db.client.close()
        if neondb: await neondb.close()
        if secondary_db: secondary_db.close()
        logger.info("✅ All database connections closed.")
    except Exception as e: logger.error(f"DB close error: {e}")

def handle_shutdown_signal(signum, frame):
    logger.info(f"Received signal {signum}. Shutting down...")
    try:
        loop = asyncio.get_running_loop()
        asyncio.ensure_future(shutdown_procedure(loop), loop=loop)
    except Exception: pass

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)

# ============ DECORATORS & MIDDLEWARE ============

def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    """Decorator to prevent handlers from getting stuck."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"⏱️ Handler {func.__name__} timed out.")
                return None
            except Exception as e:
                logger.exception(f"⚠️ Handler {func.__name__} error: {e}")
                return None
        return wrapper
    return decorator

async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """Executes a DB call with a timeout and semaphore."""
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB Timeout: {coro.__name__ if hasattr(coro, '__name__') else 'Unknown'}")
        return default
    except Exception as e:
        logger.error(f"DB Error: {e}")
        return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore = None):
    """Executes a Telegram API call safely."""
    try:
        if semaphore:
            async with semaphore:
                await asyncio.sleep(0.5) # Rate limiting
                return await asyncio.wait_for(coro, timeout=timeout)
        else:
            return await asyncio.wait_for(coro, timeout=timeout)
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError) as e:
        if "flood" in str(e).lower():
            logger.warning(f"FloodWait: Sleeping for 5s...")
            await asyncio.sleep(5)
        return None
    except Exception as e:
        logger.error(f"TG Error: {e}")
        return None

# ============ FILTERS ============

class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

# ============ MEMBERSHIP & CAPACITY CHECKS ============

async def check_user_membership(user_id: int) -> bool:
    """Checks if user has joined required channels."""
    check_channel = bool(JOIN_CHANNEL_USERNAME)
    check_group = bool(USER_GROUP_USERNAME)
    
    # If no channels configured, everyone is allowed
    if not check_channel and not check_group: return True

    try:
        tasks = []
        if check_channel:
            tasks.append(safe_tg_call(bot.get_chat_member(chat_id=f"@{JOIN_CHANNEL_USERNAME}", user_id=user_id), timeout=5))
        if check_group:
            tasks.append(safe_tg_call(bot.get_chat_member(chat_id=f"@{USER_GROUP_USERNAME}", user_id=user_id), timeout=5))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_statuses = {"member", "administrator", "creator"}
        is_member = True
        
        idx = 0
        if check_channel:
            res = results[idx]
            is_member = is_member and (isinstance(res, types.ChatMember) and res.status in valid_statuses)
            idx += 1
            
        if check_group:
            res = results[idx]
            is_member = is_member and (isinstance(res, types.ChatMember) and res.status in valid_statuses)
            
        return is_member
    except Exception as e:
        logger.error(f"Membership check error for {user_id}: {e}")
        return False

def get_join_keyboard():
    """Generates Join Channel/Group buttons."""
    buttons = []
    if JOIN_CHANNEL_USERNAME:
        buttons.append([InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")])
    if USER_GROUP_USERNAME:
        buttons.append([InlineKeyboardButton(text="👥 Join Group", url=f"https://t.me/{USER_GROUP_USERNAME}")])
    
    if buttons:
        buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya", callback_data="check_join")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    return None

def get_full_limit_keyboard():
    if not ALTERNATE_BOTS: return None
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def ensure_capacity_or_inform(message_or_callback: Union[types.Message, types.CallbackQuery]) -> bool:
    """Checks concurrent user limit."""
    user = message_or_callback.from_user
    if not user: return True
    
    # Always add/update user stats
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))
    
    if user.id == ADMIN_USER_ID: return True
    
    active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    if active_users >= CURRENT_CONC_LIMIT:
        text = f"⚠️ Server Capacity Reached ({active_users}/{CURRENT_CONC_LIMIT}).\nPlease try again in 5 minutes or use alternate bots."
        markup = get_full_limit_keyboard()
        
        if isinstance(message_or_callback, types.Message):
            await safe_tg_call(message_or_callback.answer(text, reply_markup=markup))
        elif isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server Busy! Try later.", show_alert=True))
            
        return False
    return True

def get_uptime() -> str:
    delta = datetime.now(timezone.utc) - start_time
    total_seconds = int(delta.total_seconds())
    days, r = divmod(total_seconds, 86400)
    hours, r = divmod(r, 3600)
    minutes, seconds = divmod(r, 60)
    return f"{days}d {hours}h {minutes}m"

# ============ FASTAPI APP SETUP ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global monitor_task, executor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    asyncio.get_running_loop().set_default_executor(executor)

    await db.init_db()
    await neondb.init_db()
    if secondary_db: await secondary_db.init_db()

    monitor_task = asyncio.create_task(monitor_event_loop())

    if WEBHOOK_URL:
        try:
            await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types(), secret_token=WEBHOOK_SECRET, drop_pending_updates=True)
        except Exception as e:
            logger.error(f"Webhook setup failed: {e}")

    logger.info("🚀 Application Started!")
    yield
    # Shutdown logic in shutdown_procedure

app = FastAPI(lifespan=lifespan)

# Loop Monitor
async def monitor_event_loop():
    loop = asyncio.get_running_loop()
    while True:
        try:
            st = loop.time()
            await asyncio.sleep(0.1)
            lag = loop.time() - st
            if lag > 0.5: logger.warning(f"⚠️ Event loop lag: {lag:.3f}s")
            await asyncio.sleep(60)
        except asyncio.CancelledError: break
        except: await asyncio.sleep(120)

# Webhook Endpoint
@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Secret")
    try:
        background_tasks.add_task(dp.feed_update, bot=bot, update=Update(**update))
        return {"ok": True}
    except Exception:
        return {"ok": False}

@app.get("/health")
async def health_check():
    db_ok = await safe_db_call(db.is_ready(), default=False)
    neondb_ok = await safe_db_call(neondb.is_ready(), default=False)
    return {
        "status": "ok" if db_ok and neondb_ok else "degraded",
        "mongo": db_ok,
        "neon": neondb_ok,
        "secondary": await secondary_db.is_ready() if secondary_db else "Disabled",
        "uptime": get_uptime()
    }

@app.get("/")
async def root(): return {"status": "running", "bot": "MovieSearchBot"}

# ============ BOT COMMAND HANDLERS ============

@dp.message(CommandStart())
@handler_timeout(20)
async def start_command(message: types.Message):
    user = message.from_user
    if not user: return
    
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))

    # --- ADMIN WELCOME ---
    if user.id == ADMIN_USER_ID:
        user_count = await safe_db_call(db.get_user_count(), default=0)
        mongo_count = await safe_db_call(db.get_movie_count(), default=-1)
        neon_count = await safe_db_call(neondb.get_movie_count(), default=-1)
        active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        admin_msg = (
            f"👑 <b>Admin Control Panel</b>\n\n"
            f"<b>📊 Statistics:</b>\n"
            f"• Active Users ({ACTIVE_WINDOW_MINUTES}m): {active}/{CURRENT_CONC_LIMIT}\n"
            f"• Total Users: {user_count:,}\n"
            f"• Movies (Mongo): {mongo_count:,}\n"
            f"• Movies (Neon): {neon_count:,}\n"
            f"• Uptime: {get_uptime()}\n\n"
            f"<b>⚙️ System Status:</b>\n"
            f"• Search Mode: <b>{CURRENT_SEARCH_MODE.upper()}</b>\n"
            f"• MongoDB: {'🟢' if await safe_db_call(db.is_ready()) else '🔴'}\n"
            f"• NeonDB: {'🟢' if await safe_db_call(neondb.is_ready()) else '🔴'}\n"
            f"• Secondary: {'🟢' if secondary_db and await secondary_db.is_ready() else '⚪️'}\n\n"
            f"<b>🛠 Commands:</b>\n"
            f"/search_switch - Toggle Search Engine\n"
            f"/broadcast - Reply to msg to broadcast\n"
            f"/import_json - Reply to .json file\n"
            f"/sync_mongo_to_neon - Full Sync\n"
            f"/backup_channel [ID] - Backup unique files\n"
            f"/remove_library_duplicates - Delete dups from channel\n"
            f"/cleanup_users - Remove inactive users"
        )
        await safe_tg_call(message.answer(admin_msg))
        return

    # --- USER WELCOME ---
    if not await ensure_capacity_or_inform(message): return
    
    is_member = await check_user_membership(user.id)
    
    if is_member:
        # Professional User UI
        welcome_text = (
            f"👋 Namaste <b>{user.first_name}</b>!\n\n"
            f"🎬 <b>Welcome to Movie Search Bot</b>\n"
            f"Main yahan aapki favorite movies aur web series dhoondne mein madad karunga.\n\n"
            f"🔍 <b>Kaise Search Karein?</b>\n"
            f"Bas movie ya series ka naam likhkar bhejein.\n"
            f"Example: <code>Mirzapur Season 2</code> ya <code>Avengers</code>\n\n"
            f"🚀 <b>Features:</b>\n"
            f"• Fast Results\n"
            f"• Spelling Correction (Typo Friendly)\n"
            f"• Smart Search (Season/Episode)"
        )
        await safe_tg_call(message.answer(welcome_text))
    else:
        welcome_text = (
            f"👋 Namaste <b>{user.first_name}</b>!\n\n"
            f"⚠️ Bot use karne ke liye aapko humare channels join karne honge.\n"
            f"Uske baad '✅ Maine Join Kar Liya' button par click karein."
        )
        await safe_tg_call(message.answer(welcome_text, reply_markup=get_join_keyboard()))

@dp.callback_query(F.data == "check_join")
@handler_timeout(15)
async def check_join_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not await ensure_capacity_or_inform(callback): return
    
    is_member = await check_user_membership(user.id)
    
    if is_member:
        await safe_tg_call(callback.message.edit_text(
            f"✅ <b>Verification Successful!</b>\n\n"
            f"Ab aap koi bhi movie search kar sakte hain.\n"
            f"Try karein: <code>Stree 2</code>"
        ))
    else:
        await safe_tg_call(callback.answer("❌ Aapne abhi tak channels join nahi kiye hain!", show_alert=True))
        # Refresh buttons just in case
        await safe_tg_call(callback.message.edit_reply_markup(reply_markup=get_join_keyboard()))

# =======================================================
# +++++ SEARCH LOGIC (TRIPLE ENGINE + WEBSERIES) +++++
# =======================================================

@dp.message(F.text & ~F.text.startswith("/"))
@handler_timeout(20)
async def search_handler(message: types.Message):
    global CURRENT_SEARCH_MODE
    user = message.from_user
    if not await ensure_capacity_or_inform(message): return

    original_query = message.text.strip()
    
    # 1. Advanced Cleaning (S01/E01 conversion happens here)
    query = clean_text_for_search(original_query)
    
    if len(query) < 2:
        await safe_tg_call(message.answer("⚠️ Query bahut chhoti hai. Kam se kam 2 letters likhein."))
        return

    search_msg = await safe_tg_call(message.answer(f"🔍 <b>'{original_query}'</b> dhoond raha hu..."))
    if not search_msg: return

    results = []
    used_engine = ""

    # --- STRATEGY 1: Secondary DB (Fastest) ---
    if CURRENT_SEARCH_MODE == "secondary" and secondary_db and await secondary_db.is_ready():
        results = await secondary_db.search_movies(query)
        used_engine = "Secondary"
    
    # --- STRATEGY 2: NeonDB (Best for Typos & Fuzzy) ---
    # Falls back here if Secondary is empty/disabled/failed
    if not results and neondb and await safe_db_call(neondb.is_ready()):
        # NeonDB expects original text for Trigram match, but cleaned for TSVector
        # Our neondb.py handles this.
        results = await safe_db_call(neondb.neondb_search(original_query, limit=20), default=[])
        used_engine = "NeonDB (Fuzzy)"

    # --- STRATEGY 3: MongoDB (Deep Search Fallback) ---
    # Final fallback
    if not results and db and await safe_db_call(db.is_ready()):
        results = await safe_db_call(db.mongo_search_internal(original_query, limit=20), default=[])
        used_engine = "MongoDB"

    # Results Processing
    if not results:
        await safe_tg_call(search_msg.edit_text(
            f"❌ <b>'{original_query}'</b> nahi mili.\n\n"
            f"Suggestions:\n"
            f"1. Spelling check karein.\n"
            f"2. Year hata kar dekhein.\n"
            f"3. 'Season 1' ki jagah 'S1' likhein."
        ))
        return

    # Formatting Results
    buttons = []
    for movie in results[:15]: # Show max 15
        title_display = movie['title']
        if len(title_display) > 50: title_display = title_display[:47] + "..."
        
        year_str = f" ({movie['year']})" if movie.get('year') else ""
        btn_text = f"{title_display}{year_str}"
        
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"get_{movie['imdb_id']}")])

    footer_text = f"Found {len(results)} results | Engine: {used_engine}"
    
    await safe_tg_call(search_msg.edit_text(
        f"✅ <b>Result for: {original_query}</b>\n👇 Apni file select karein:\n\nAttempting match for: <i>{query}</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    ))

@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20)
async def get_movie_callback(callback: types.CallbackQuery):
    user = callback.from_user
    imdb_id = callback.data.split("_", 1)[1]
    
    if not await ensure_capacity_or_inform(callback): return
    
    await safe_tg_call(callback.answer("🚀 File bheji ja rahi hai..."))
    
    # Fetch Movie Details
    # 1. Try Primary DB
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    
    # 2. If missing in Primary, try Secondary (Redundancy)
    if not movie and secondary_db:
        movie = await secondary_db.get_movie(imdb_id)
        
    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Error: Yeh movie database se remove ho chuki hai."))
        return

    # --- SENDING LOGIC (COPY MODE FIX) ---
    try:
        sent = False
        
        # Priority 1: Copy Message (Preserves Caption)
        if movie.get("channel_id") and movie.get("message_id") and movie["message_id"] != AUTO_MESSAGE_ID_PLACEHOLDER:
            try:
                # CRITICAL FIX: Not passing 'caption' argument preserves the original caption.
                await bot.copy_message(
                    chat_id=user.id,
                    from_chat_id=int(movie["channel_id"]),
                    message_id=int(movie["message_id"])
                )
                sent = True
            except Exception as copy_err:
                logger.warning(f"Copy failed for {imdb_id}: {copy_err}")
                
        # Priority 2: Send by File ID (Fallback)
        if not sent and movie.get("file_id"):
            try:
                caption_text = f"🎬 <b>{movie['title']}</b>"
                if movie.get('year'): caption_text += f" ({movie['year']})"
                
                await bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=caption_text
                )
                sent = True
            except Exception as doc_err:
                logger.error(f"Send Doc failed for {imdb_id}: {doc_err}")

        if not sent:
            await safe_tg_call(callback.message.answer("❌ File send karne mein error aaya. Shayad source channel se delete ho gayi hai."))

    except Exception as e:
        logger.error(f"Critical error in get_movie_callback: {e}")
        await safe_tg_call(callback.message.answer("❌ Technical Error."))

# =======================================================
# +++++ ADMIN & MIGRATION HANDLERS +++++
# =======================================================

@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(30)
async def migration_handler(message: types.Message):
    """
    Handles migration/adding movies by forwarding from Library Channel.
    Adds to ALL 3 Databases.
    """
    # Validate Source
    if message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0:
            await message.answer("⚠️ Library Channel ID set nahi hai.")
        return

    # Validate Content
    if not (message.video or message.document):
        return

    # Parse Info
    info = extract_movie_info(message.caption or "")
    if not info or not info.get("title"):
        await message.answer(f"❌ Caption Parse Fail: {message.message_id}")
        return

    # Prepare Data
    file_data = message.video or message.document
    file_id = file_data.file_id
    file_unique_id = file_data.file_unique_id
    
    msg_id = message.forward_from_message_id
    chat_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{msg_id}"
    title = info["title"]
    year = info.get("year")
    
    # Clean title for searching
    clean_title = clean_text_for_search(title)

    # 1. Add to MongoDB (Primary)
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, msg_id, chat_id, clean_title, file_unique_id))
    
    # 2. Add to NeonDB (Search Index)
    neon_res = await safe_db_call(neondb.add_movie(msg_id, chat_id, file_id, file_unique_id, imdb_id, title, year))
    
    # 3. Add to SecondaryDB (Redundancy)
    if secondary_db:
        await secondary_db.add_movie(imdb_id, title, year, msg_id, chat_id, file_id, file_unique_id, clean_title)

    status_text = "✅ Added" if db_res is True else "🔄 Updated" if db_res == "updated" else "⚠️ Duplicate"
    await message.answer(f"{status_text}: <b>{title}</b>")

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600)
async def broadcast_command(message: types.Message):
    """Broadcasts a message to all active users."""
    if not message.reply_to_message:
        await message.answer("❌ Jis message ko broadcast karna hai uspe reply karein.")
        return

    status_msg = await message.answer("⏳ Fetching users list...")
    users = await db.get_all_users()
    
    if not users:
        await status_msg.edit_text("❌ No active users found.")
        return
        
    total = len(users)
    await status_msg.edit_text(f"📤 Broadcasting to {total} users...")
    
    success = 0
    failed = 0
    blocked = 0
    
    start_time = datetime.now()
    
    for i, user_id in enumerate(users):
        try:
            await message.reply_to_message.copy_to(chat_id=user_id)
            success += 1
        except TelegramForbiddenError:
            blocked += 1
            await db.deactivate_user(user_id)
        except Exception as e:
            failed += 1
            logger.debug(f"Broadcast fail for {user_id}: {e}")
            
        # Update status every 200 users
        if i > 0 and i % 200 == 0:
            try:
                await status_msg.edit_text(f"📤 Progress: {i}/{total}\n✅: {success} | 🚫: {blocked} | ❌: {failed}")
            except: pass
            await asyncio.sleep(1) # Prevent flood
            
    duration = datetime.now() - start_time
    await status_msg.edit_text(
        f"✅ <b>Broadcast Complete</b>\n"
        f"⏱ Time: {duration}\n"
        f"👥 Total: {total}\n"
        f"✅ Success: {success}\n"
        f"🚫 Blocked/Deleted: {blocked}\n"
        f"❌ Failed: {failed}"
    )

@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(3600)
async def import_json_command(message: types.Message):
    """Imports movies from a JSON file."""
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.answer("❌ Reply to a .json file.")
        return

    status_msg = await message.answer("⏳ Downloading file...")
    
    try:
        file_id = message.reply_to_message.document.file_id
        file_info = await bot.get_file(file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        
        data = json.load(downloaded_file)
        
        if not isinstance(data, list):
            await status_msg.edit_text("❌ Invalid JSON format. Expected a list.")
            return
            
        total = len(data)
        await status_msg.edit_text(f"⏳ Processing {total} items... (This will take time)")
        
        added = 0
        updated = 0
        
        # Batch list for Neon/Secondary
        batch_data = []
        
        for item in data:
            try:
                fid = item.get('file_id')
                if not fid: continue
                
                # Heuristic parsing
                raw_title = item.get('title', 'Untitled')
                parsed = parse_filename(raw_title)
                
                title = parsed['title']
                year = parsed['year']
                imdb = item.get('imdb_id') or f"json_{hashlib.md5(fid.encode()).hexdigest()[:10]}"
                mid = item.get('message_id', AUTO_MESSAGE_ID_PLACEHOLDER)
                cid = item.get('channel_id', 0)
                fuid = item.get('file_unique_id') or fid
                
                clean = clean_text_for_search(title)
                
                # Add to Primary
                res = await db.add_movie(imdb, title, year, fid, mid, cid, clean, fuid)
                if res is True: added += 1
                elif res == "updated": updated += 1
                
                # Add to batch
                batch_data.append({
                    "imdb_id": imdb, "title": title, "year": year,
                    "message_id": mid, "channel_id": cid,
                    "file_id": fid, "file_unique_id": fuid,
                    "clean_title": clean
                })
                
            except Exception: pass
            
        # Sync Batch to Neon
        if batch_data:
            await status_msg.edit_text(f"⏳ DB Done. Syncing {len(batch_data)} to NeonDB...")
            await neondb.sync_from_mongo(batch_data)
            
            if secondary_db:
                await status_msg.edit_text(f"⏳ Syncing to SecondaryDB...")
                await secondary_db.add_batch_movies(batch_data)

        await status_msg.edit_text(f"✅ Import Complete!\nTotal: {total}\nAdded: {added}\nUpdated: {updated}")
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")
        logger.error(f"JSON Import Error: {e}", exc_info=True)

@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(7200)
async def backup_channel_command(message: types.Message):
    """Backs up unique files to another channel."""
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Usage: /backup_channel [Target_Channel_ID/Username]")
        return
    
    target_id = args[1]
    status_msg = await message.answer("⏳ Fetching unique files list from NeonDB...")
    
    unique_files = await neondb.get_unique_movies_for_backup()
    total = len(unique_files)
    
    if not total:
        await status_msg.edit_text("❌ No files found to backup.")
        return
        
    await status_msg.edit_text(f"🚀 Backing up {total} files to {target_id}...")
    
    success = 0
    failed = 0
    
    for i, (mid, cid) in enumerate(unique_files):
        try:
            await bot.forward_message(chat_id=target_id, from_chat_id=cid, message_id=mid)
            success += 1
            await asyncio.sleep(1) # Conservative rate limit
        except Exception as e:
            failed += 1
            logger.debug(f"Backup fail for msg {mid}: {e}")
            
        if i > 0 and i % 50 == 0:
            try: await status_msg.edit_text(f"🚀 Progress: {i}/{total}\n✅: {success} ❌: {failed}")
            except: pass
            
    await status_msg.edit_text(f"✅ Backup Complete!\nTotal: {total}\nSuccess: {success}\nFailed: {failed}")

@dp.message(Command("sync_mongo_to_neon"), AdminFilter())
async def sync_mongo_to_neon_command(message: types.Message):
    """Syncs all data from MongoDB to NeonDB."""
    msg = await message.answer("⏳ Fetching all movies from MongoDB...")
    movies = await db.get_all_movies_for_neon_sync()
    
    if not movies:
        await msg.edit_text("❌ MongoDB is empty.")
        return
        
    await msg.edit_text(f"⏳ Syncing {len(movies)} movies to NeonDB...")
    count = await neondb.sync_from_mongo(movies)
    
    await msg.edit_text(f"✅ Sync Complete! {count} records synced.")

@dp.message(Command("remove_library_duplicates"), AdminFilter())
async def remove_library_duplicates_command(message: types.Message):
    """Deletes duplicate files from the Telegram Channel based on file_unique_id."""
    msg = await message.answer("⏳ Analyzing duplicates in NeonDB...")
    
    duplicates, total_count = await neondb.find_and_delete_duplicates(batch_limit=1000)
    
    if not duplicates:
        await msg.edit_text("✅ No duplicates found.")
        return
        
    await msg.edit_text(f"🗑 Found {total_count} duplicates. Deleting from channel...")
    
    deleted = 0
    failed = 0
    
    for mid, cid in duplicates:
        try:
            await bot.delete_message(chat_id=cid, message_id=mid)
            deleted += 1
            await asyncio.sleep(0.1) # Rate limit
        except Exception:
            failed += 1
            
    await msg.edit_text(f"✅ Cleanup Done.\nDeleted: {deleted}\nFailed: {failed}")

@dp.message(Command("cleanup_mongo_duplicates"), AdminFilter())
async def cleanup_mongo_duplicates_command(message: types.Message):
    msg = await message.answer("⏳ Cleaning MongoDB duplicates...")
    deleted, found = await db.cleanup_mongo_duplicates()
    await msg.edit_text(f"✅ Removed {deleted} duplicate entries from MongoDB.")

@dp.message(Command("cleanup_users"), AdminFilter())
async def cleanup_users_command(message: types.Message):
    msg = await message.answer("⏳ Cleaning inactive users...")
    count = await db.cleanup_inactive_users(days=30)
    await msg.edit_text(f"✅ Deactivated {count} users inactive for >30 days.")

@dp.message(Command("remove_dead_movie"), AdminFilter())
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Usage: /remove_dead_movie IMDB_ID")
        return
    
    imdb = args[1]
    await db.remove_movie_by_imdb(imdb)
    # We rely on sync to clean Neon, or let it stick as it doesn't hurt
    await message.answer(f"✅ Removed {imdb} from MongoDB.")

@dp.message(Command("search_switch"), AdminFilter())
async def search_switch_command(message: types.Message):
    global CURRENT_SEARCH_MODE, CURRENT_SEARCH_MODE_INDEX
    CURRENT_SEARCH_MODE_INDEX = (CURRENT_SEARCH_MODE_INDEX + 1) % len(SEARCH_MODES)
    CURRENT_SEARCH_MODE = SEARCH_MODES[CURRENT_SEARCH_MODE_INDEX]
    
    await message.answer(f"🔄 Search Mode switched to: <b>{CURRENT_SEARCH_MODE.upper()}</b>")

@dp.message(Command("stats"), AdminFilter())
async def stats_command(message: types.Message):
    await start_command(message) # Re-use admin panel logic

@dp.message(Command("get_user"), AdminFilter())
async def get_user_command(message: types.Message):
    try:
        uid = int(message.text.split()[1])
        info = await db.get_user_info(uid)
        await message.answer(f"👤 <b>User Info:</b>\n{json.dumps(info, default=str, indent=2)}")
    except:
        await message.answer("❌ User not found or invalid format.")

@dp.message(Command("set_limit"), AdminFilter())
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT
    try:
        val = int(message.text.split()[1])
        CURRENT_CONC_LIMIT = val
        await message.answer(f"✅ Concurrency Limit set to: {val}")
    except:
        await message.answer("❌ Invalid number.")

# ============ AUTO-INDEXING ============

@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    """Automatically indexes files posted in the Library Channel."""
    if message.chat.id != LIBRARY_CHANNEL_ID: return
    if not (message.video or message.document): return
    
    info = extract_movie_info(message.caption or "")
    if not info or not info.get("title"): return
    
    # Data Preparation
    file_data = message.video or message.document
    fid = file_data.file_id
    fuid = file_data.file_unique_id
    mid = message.message_id
    cid = message.chat.id
    imdb = info.get("imdb_id") or f"auto_{mid}"
    title = info["title"]
    year = info.get("year")
    clean = clean_text_for_search(title)
    
    # 1. Mongo
    await safe_db_call(db.add_movie(imdb, title, year, fid, mid, cid, clean, fuid))
    
    # 2. Neon
    await safe_db_call(neondb.add_movie(mid, cid, fid, fuid, imdb, title, year))
    
    # 3. Secondary
    if secondary_db:
        await secondary_db.add_movie(imdb, title, year, mid, cid, fid, fuid, clean)
        
    logger.info(f"➕ Auto-Indexed: {title}")

# ============ ERROR HANDLER ============

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    """Global Error Handler."""
    logger.exception(f"🚨 Unhandled Exception: {exception}")
    return True

# ============ MAIN EXECUTION ============

async def main():
    """Entry point."""
    logger.info("🚀 Starting Bot...")
    
    # Init DBs
    try:
        await db.init_db()
        await neondb.init_db()
        if secondary_db: await secondary_db.init_db()
    except Exception as e:
        logger.critical(f"DB Init failed: {e}")
        return

    # Start Monitor
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    
    # Start Polling
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
