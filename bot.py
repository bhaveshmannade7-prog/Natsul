# bot.py
# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import signal
import json
import hashlib
import random 
import uuid # Naya: Unique IDs ke liye
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import List, Dict, Callable, Any
from functools import wraps, partial
import concurrent.futures

# --- Load dotenv FIRST ---
from dotenv import load_dotenv
load_dotenv()

# --- NEW IMPORTS ---
# FIXED: name 'TELEGRAM_DELETE_SEMAPHORE' instead of typo 'TELEGRAM_DELETE_SEMAP_RE'
from core_utils import safe_tg_call, safe_db_call, DB_SEMAPHORE, TELEGRAM_DELETE_SEMAPHORE, TELEGRAM_COPY_SEMAPHORE, TELEGRAM_BROADCAST_SEMAPHORE, WEBHOOK_SEMAPHORE, TG_OP_TIMEOUT, DB_OP_TIMEOUT
from redis_cache import redis_cache, RedisCacheLayer
from queue_wrapper import priority_queue, PriorityQueueWrapper, QUEUE_CONCURRENCY, PRIORITY_ADMIN
from smart_watchdog import SmartWatchdog, WATCHDOG_ENABLED 

# --- NEW FEATURE IMPORTS ---
from ad_manager import send_sponsor_ad
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
# CRITICAL FIX: StateFilter use karna zaroori hai conflicts ke liye
from aiogram.filters import StateFilter
# --- END NEW IMPORTS ---

# --- NAYA FUZZY SEARCH IMPORT ---
try:
    from rapidfuzz import process, fuzz
except ImportError:
    logging.critical("--- rapidfuzz library nahi mili! ---")
    logging.critical("Kripya install karein: pip install rapidfuzz")
    raise SystemExit("Missing dependency: rapidfuzz")

# --- Uvloop activation ---
try:
    import uvloop
    uvloop.install()
    logging.info("Uvloop (fast asyncio) install ho gaya.")
except ImportError:
    logging.info("Uvloop nahi mila, default asyncio event loop istemal hoga.")

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramRetryAfter
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Database Imports ---
from database import Database
from neondb import NeonDB
ADMIN_ACTIVE_TASKS = {} 
# ============ LOGGING SETUP ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("bot")

logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)
logging.getLogger("fastapi").setLevel(logging.WARNING)


# ============ CONFIGURATION ============

# --- NEW: FSM STATES FOR ADS ---
class AdStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_btn_text = State()
    waiting_for_btn_url = State()

# --- FIX: Centralized Cleanup Utility (For ENV input robustness) ---
def clean_tg_identifier(identifier: str) -> str:
    if not identifier: return ""
    # Step 1: Remove https://t.me/ prefixes
    identifier = re.sub(r'https?://t\.me/', '', identifier, flags=re.IGNORECASE)
    # Step 2: Remove leading @ sign
    return identifier.lstrip('@')
# --- END FIX ---

# --- NAYA FIX: Minimal Join Logic Cleaner (RULE D) ---
def get_clean_username_only(identifier: str) -> str | None:
    if not identifier: return None
    # Remove URL prefixes and @ sign
    identifier = re.sub(r'https?://t\.me/', '', identifier, flags=re.IGNORECASE)
    clean_id = identifier.lstrip('@').strip()
    # Check if numeric ID (private chat ID)
    if clean_id.isdigit() or (clean_id.startswith('-') and clean_id[1:].isdigit()):
        return None # Return None for numeric IDs as they can't be used in t.me/
    return clean_id if clean_id else None
# --- END NAYA FIX ---


try:
    BOT_TOKEN = os.environ["BOT_TOKEN"]
    
    # --- 3 DB Connections ---
    DATABASE_URL_PRIMARY = os.environ["DATABASE_URL_PRIMARY"]
    DATABASE_URL_FALLBACK = os.environ["DATABASE_URL_FALLBACK"]
    NEON_DATABASE_URL = os.environ["NEON_DATABASE_URL"]
    
    # --- NEW: Redis URL ---
    REDIS_URL = os.getenv("REDIS_URL")
    
    # Using your Admin ID as default fallback
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "7263519581"))
    LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))

    # FIX: Input ko yahan clean karke store karein
    JOIN_CHANNEL_USERNAME = clean_tg_identifier(os.getenv("JOIN_CHANNEL_USERNAME", "thegreatmoviesl9"))
    USER_GROUP_USERNAME = clean_tg_identifier(os.getenv("USER_GROUP_USERNAME", "MOVIEMAZASU"))

    RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
    PUBLIC_URL = os.getenv("PUBLIC_URL")
    WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

    DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
    ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
    
    # Gunicorn worker coordination config
    WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT", "120"))
    
    ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "")
    ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

except KeyError as e:
    logger.critical(f"--- MISSING ENVIRONMENT VARIABLE: {e} ---")
    logger.critical("Bot band ho raha hai. Kripya apni .env file / Render secrets check karein.")
    raise SystemExit(f"Missing env var: {e}")
except ValueError as e:
    logger.critical(f"--- INVALID ENVIRONMENT VARIABLE: {e} ---")
    raise SystemExit(f"Invalid env var: {e}")

CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

# --- NAYA SEARCH LOGIC ---
logger.info("Search Logic: Intent Engine V6 (Smart Tokenization + Word Presence)")


if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID set nahi hai. Admin commands kaam nahi karenge.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID set nahi hai. Auto-indexing aur Migration kaam nahi karenge.")
if not JOIN_CHANNEL_USERNAME and not USER_GROUP_USERNAME:
    logger.warning("--- KOI JOIN CHECK SET NAHI HAI. Membership check skip ho jayega. ---")


# ============ TIMEOUTS & SEMAPHORES (Now in core_utils) ============
HANDLER_TIMEOUT = 15 
# TG_OP_TIMEOUT is imported from core_utils
# DB_OP_TIMEOUT is imported from core_utils

# ============ NEW: BACKGROUND TASK WRAPPER (FREEZE FIX) ============
# ============ NEW: BACKGROUND TASK WRAPPER (FREEZE FIX + CANCEL SUPPORT) ============
async def run_in_background(task_func, message: types.Message, *args, **kwargs):
    """
    Prevents Bot Freeze, Supports Cancellation, and Handles Locking.
    """
    db_primary = kwargs.get('db_primary')
    user_id = message.from_user.id
    
    # Check if admin already has a running task
    if user_id in ADMIN_ACTIVE_TASKS and not ADMIN_ACTIVE_TASKS[user_id].done():
         await message.answer("⚠️ **Task Already Running**\nYou have an active task. Use /cancel to stop it first.")
         return

    lock_name = f"task_lock_{task_func.__name__}"
    
    try:
        # Acquire Lock
        lock_acquired = await safe_db_call(db_primary.acquire_cross_process_lock(lock_name, 3600), default=False)
        if not lock_acquired:
            await message.answer("⚠️ **System Busy**\nAnother admin is running this command. Please wait.")
            return

        status_msg = await message.answer("⚙️ **Background Task Started.**\nMonitor progress below. Use /cancel to stop.")
        
        async def task_wrapper():
            try:
                # Add watchdog timeout (1 hour max)
                await asyncio.wait_for(task_func(message, status_msg, *args, **kwargs), timeout=3600)
            except asyncio.CancelledError:
                logger.warning(f"Task {task_func.__name__} cancelled by user.")
                await safe_tg_call(status_msg.edit_text("🛑 **Task Forcefully Stopped** by Admin."))
                raise # Re-raise to ensure cleanup
            except asyncio.TimeoutError:
                await safe_tg_call(status_msg.edit_text("❌ **TASK TIMEOUT**: System limit reached."))
            except Exception as e:
                logger.exception(f"Background task crash: {e}")
                await safe_tg_call(status_msg.edit_text(f"❌ **TASK CRASHED**: {str(e)[:100]}"))
            finally:
                # Cleanup Lock and Registry
                await safe_db_call(db_primary.release_cross_process_lock(lock_name))
                if user_id in ADMIN_ACTIVE_TASKS:
                    del ADMIN_ACTIVE_TASKS[user_id]
                logger.info(f"Task {task_func.__name__} finished/cancelled.")

        # Create Task and Register it
        task = asyncio.create_task(task_wrapper())
        ADMIN_ACTIVE_TASKS[user_id] = task
        
    except Exception as e:
        logger.error(f"Background launch error: {e}")
        await safe_db_call(db_primary.release_cross_process_lock(lock_name))

# ============ NEW: SHORTLINK REDIRECT LOGIC ============
async def get_shortened_link(long_url, db: Database):
    """Generates monetized link from Admin Settings."""
    api_url = await db.get_config("shortlink_api", "https://shareus.io/api?api=KEY&url={url}")
    # FIX: Robust formatting to prevent revenue loss (Bug #14)
    try:
        return api_url.format(url=long_url)
    except KeyError:
        # Fallback: Agar {url} placeholder missing hai to append kar do
        return f"{api_url}&url={long_url}"
    except Exception:
        # Worst case: Original URL return karo taaki user block na ho
        return long_url

# ============ WEBHOOK URL ============
def build_webhook_url() -> str:
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        webhook_path = f"/bot/{BOT_TOKEN}"
        if base.endswith('/bot'): base = base.rsplit('/bot', 1)[0]
        elif base.endswith('/bot/'): base = base.rsplit('/bot/', 1)[0]
        final_url = f"{base}{webhook_path}"
        logger.info(f"Webhook URL set kiya gaya: {final_url}")
        return final_url
    logger.warning("RENDER_EXTERNAL_URL ya PUBLIC_URL nahi mila. Webhook set nahi ho sakta.")
    return ""

WEBHOOK_URL = build_webhook_url()

# ============ BOT INITIALIZATION ============

class BotManager:
    """Multi-Bot (Token) instances ko manage karta hai।"""
    def __init__(self, main_token: str, alternate_tokens: List[str]):
        # Main bot instance (already created)
        self.main_bot = None 
        
        # All tokens in a hashable list
        self.all_tokens = [main_token] + alternate_tokens
        self.bots: Dict[str, Bot] = {}
        
    def add_main_bot(self, main_bot_instance: Bot):
        self.main_bot = main_bot_instance
        self.bots[main_bot_instance.token] = main_bot_instance
        
        # Alternate bots ko initialize karein
        for token in self.all_tokens:
            if token != self.main_bot.token and token not in self.bots:
                 self.bots[token] = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
                 logger.info(f"Alternate Bot instance for {token[:4]}... initialize ho gaya।")

    def get_bot_by_token(self, token: str) -> Bot:
        """Webhook se aaye token ke hisaab se bot instance return karein।"""
        return self.bots.get(token, self.main_bot) 
        
    def get_all_bots(self) -> List[Bot]:
        return list(self.bots.values())

# Global Bot Manager
bot_manager = BotManager(BOT_TOKEN, ALTERNATE_BOTS)

try:
    # Existing bot (Main bot instance)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    # --- NEW: Add main bot to manager ---
    bot_manager.add_main_bot(bot)
    # --- END NEW ---
    
    storage = MemoryStorage()
    
    # --- 3 Database Objects ---
    # FIX: db_primary ko pehle initialize karein taaki NeonDB use kar sake
    db_primary = Database(DATABASE_URL_PRIMARY) 
    
    # FIX: NeonDB ko db_primary instance pass karein (Cross-process lock ke liye)
    db_neon = NeonDB(NEON_DATABASE_URL, db_primary_instance=db_primary) 
    
    db_fallback = Database(DATABASE_URL_FALLBACK)
    
    # --- Dependency Injection ---
    dp = Dispatcher(
        storage=storage, 
        db_primary=db_primary, 
        db_fallback=db_fallback, 
        db_neon=db_neon,
        redis_cache=redis_cache # Naya: Redis cache inject karein
    )
    # Store start time on dispatcher for watchdog use
    dp.start_time = datetime.now(timezone.utc)
    
    logger.info("Bot, Dispatcher, aur 3 Database objects (M+M+N) initialize ho gaye.")
    logger.info(f"Multi-Bot Manager mein {len(bot_manager.all_tokens)} tokens configured hain।")
except Exception as e:
    logger.critical(f"Bot/Dispatcher initialize nahi ho paya: {e}", exc_info=True)
    raise SystemExit(f"Bot initialization fail. Error: {e}")

start_time = datetime.now(timezone.utc)
monitor_task: asyncio.Task | None = None
executor: concurrent.futures.ThreadPoolExecutor | None = None
# --- NEW: Watchdog Instance ---
watchdog: SmartWatchdog | None = None 
# --- END NEW ---
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

# --- NAYA FUZZY CACHE (Dict[str, Dict] format) ---
fuzzy_movie_cache: Dict[str, Dict] = {}
FUZZY_CACHE_LOCK = asyncio.Lock()

# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure():
    logger.info("Graceful shutdown shuru ho raha hai...")
    
    # --- NEW: Stop Watchdog ---
    if watchdog:
        watchdog.stop()
    # --- END NEW ---
    
    # --- NEW: Stop Queue Workers ---
    await priority_queue.stop_workers()
    # --- END NEW ---
    
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try: await asyncio.wait_for(monitor_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError): pass
            
    # --- NEW: Delete webhooks for all bots and close sessions ---
    tasks = []
    for bot_instance in bot_manager.get_all_bots():
        if WEBHOOK_URL:
            # Har bot ke liye webhook delete karein (Rate-limit se bachne ke liye safe_tg_call use karein)
            tasks.append(safe_tg_call(bot_instance.delete_webhook(drop_pending_updates=True)))
        if bot_instance.session:
            tasks.append(safe_tg_call(bot_instance.session.close()))
    
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"{len(tasks)} cleanup tasks (webhooks/sessions) done।")
    # --- END NEW ---
            
    try: await dp.storage.close()
    except Exception as e: logger.error(f"Dispatcher storage close karte waqt error: {e}")
        
    if executor:
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("ThreadPoolExecutor shutdown ho gaya.")
        
    # --- NEW: Close Redis Connection ---
    await redis_cache.close()
    # --- END NEW ---
        
    try:
        if db_primary and db_primary.client:
            # Motor client.close() is synchronous
            db_primary.client.close()
            logger.info("MongoDB (Primary) client connection close ho gaya.")
        if db_fallback and db_fallback.client:
            # Motor client.close() is synchronous
            db_fallback.client.close()
            logger.info("MongoDB (Fallback) client connection close ho gaya.")
        if db_neon:
            await db_neon.close()
    except Exception as e:
        logger.error(f"Database connections close karte waqt error: {e}")
        
    logger.info("Graceful shutdown poora hua.")


def setup_signal_handlers():
    loop = asyncio.get_running_loop()
    def handle_signal(signum):
        logger.info(f"Signal {signum} mila. Shutdown shuru...")
        asyncio.create_task(shutdown_procedure())
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal, sig)
    logger.info("Signal handlers (SIGTERM, SIGINT) set ho गए.")


# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} {timeout}s ke baad time out ho gaya.")
                target_chat_id = None
                callback_query: types.CallbackQuery | None = None
                if args:
                    if isinstance(args[0], types.Message):
                        target_chat_id = args[0].chat.id
                    elif isinstance(args[0], types.CallbackQuery):
                        callback_query = args[0]
                        target_chat_id = callback_query.message.chat.id if callback_query.message else None
                if target_chat_id:
                    try: 
                        # UI Enhancement: Friendly Timeout Message
                        timeout_text = "⏳ **REQUEST TIMEOUT**\n━━━━━━━━━━━━━━\nThe server is taking longer than expected. Please wait a moment and try again. 🔄"
                        current_bot = kwargs.get('bot') or bot
                        await current_bot.send_message(target_chat_id, timeout_text)
                    except Exception: pass
                if callback_query:
                    try: await callback_query.answer("⚠️ Timeout: Server Busy", show_alert=False)
                    except Exception: pass
            except Exception as e:
                logger.exception(f"Handler {func.__name__} mein error: {e}")
        return wrapper
    return decorator

# ============ FILTERS & HELPER FUNCTIONS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

# --- NAYA FEATURE 2: Ban Check Filter ---
class BannedFilter(BaseFilter):
    async def __call__(self, message: types.Message, db_primary: Database) -> bool:
        user = message.from_user
        if not user or user.id == ADMIN_USER_ID:
            return False # Admin cannot be banned, non-user messages skip
        
        # is_user_banned is an async method in database.py
        is_banned = await safe_db_call(db_primary.is_user_banned(user.id), default=False)
        
        if is_banned:
            logger.warning(f"Banned user {user.id} tried to use bot.")
            try:
                # UI Enhancement: Ban message
                ban_text = "🚫 **ACCESS DENIED**\n━━━━━━━━━━━━━━\nYou have been restricted from using this service.\n🔒 Contact Support for appeals."
                await safe_tg_call(
                    message.answer(ban_text),
                    semaphore=TELEGRAM_COPY_SEMAPHORE
                )
            except Exception:
                pass
            return True # Filter matches, handler should be skipped
        return False # Filter does not match, proceed to handler
# --- END NAYA FEATURE 2 ---

def get_uptime() -> str:
    # FIX: dp.start_time use karein
    delta = datetime.now(timezone.utc) - dp.start_time; total_seconds = int(delta.total_seconds())
    days, r = divmod(total_seconds, 86400); hours, r = divmod(r, 3600); minutes, seconds = divmod(r, 60)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int, current_bot: Bot) -> bool:
    # Function signature update: Ab bot instance ko parameter mein pass kar rahe hain
    check_channel = bool(JOIN_CHANNEL_USERNAME)
    check_group = bool(USER_GROUP_USERNAME)
    if not check_channel and not check_group: return True
    
    # FIX: Chat ID ko pehle check karein (Input Robustness)
    chat_identifier_channel = JOIN_CHANNEL_USERNAME
    chat_identifier_group = USER_GROUP_USERNAME
    
    # Logic to handle potential numeric chat IDs (e.g., -1001234567890)
    def normalize_chat_id(identifier):
        if not identifier: return None
        # Step 1: Remove URL prefixes and @ sign
        identifier = re.sub(r'https?://t\.me/', '', identifier, flags=re.IGNORECASE)
        identifier = identifier.lstrip('@')
        
        # Step 2: Check if numeric/negative numeric
        if identifier and (identifier.startswith('-') and identifier[1:].isdigit() or identifier.isdigit()):
            # Numeric IDs ko asali API call ke liye int mein convert karein
            return int(identifier)
        
        # Step 3: If not numeric, use as @username (API call ke liye @ sign zaroori hai)
        return f"@{identifier}" if identifier else None
        
    chat_id_channel = normalize_chat_id(chat_identifier_channel)
    chat_id_group = normalize_chat_id(chat_identifier_group)
    
    
    try:
        tasks_to_run = []
        if chat_id_channel:
            tasks_to_run.append(safe_tg_call(current_bot.get_chat_member(chat_id=chat_id_channel, user_id=user_id), timeout=5))
        if chat_id_group:
            tasks_to_run.append(safe_tg_call(current_bot.get_chat_member(chat_id=chat_id_group, user_id=user_id), timeout=5))
            
        results = await asyncio.gather(*tasks_to_run)
        valid_statuses = {"member", "administrator", "creator"}
        is_in_channel = True; is_in_group = True; result_index = 0
        
        if chat_id_channel:
            channel_member = results[result_index]
            is_in_channel = isinstance(channel_member, types.ChatMember) and channel_member.status in valid_statuses
            # Agar TelegramAPIError ya Chat not found aata hai, to channel_member False ho jata hai
            if channel_member in [False, None]: logger.warning(f"Membership check fail (Channel {chat_identifier_channel}).")
            result_index += 1
            
        if chat_id_group:
            group_member = results[result_index]
            is_in_group = isinstance(group_member, types.ChatMember) and group_member.status in valid_statuses
            if group_member in [False, None]: logger.warning(f"Membership check fail (Group {chat_identifier_group}).")

        return is_in_channel and is_in_group
    except Exception as e:
        # Predict future error: Bot admin nahi hai ya chat ID invalid hai
        if not isinstance(e, (TelegramBadRequest, TelegramAPIError)): logger.error(f"Membership check mein error {user_id}: {e}", exc_info=True)
        else: logger.info(f"Membership check API error {user_id}: {e}")
        return False

# UI Enhancement: Redesign get_join_keyboard
def get_join_keyboard() -> InlineKeyboardMarkup | None:
    buttons = []
    
    def is_numeric_id_string(identifier):
        if not identifier: return False
        return identifier.isdigit() or (identifier.startswith('-') and identifier[1:].isdigit())
        
    def get_clean_identifier(identifier):
        if not identifier: return None
        identifier = re.sub(r'https?://t\.me/', '', identifier, flags=re.IGNORECASE)
        return identifier.lstrip('@')
        
    # Button Labels Redesigned
    if JOIN_CHANNEL_USERNAME:
        clean_name = get_clean_identifier(JOIN_CHANNEL_USERNAME)
        label = "📢 Join Official Channel" if not is_numeric_id_string(clean_name) else "📢 Join Private Channel"
        
        if is_numeric_id_string(clean_name):
             buttons.append([InlineKeyboardButton(text=label, callback_data="no_url_join")])
        else:
             buttons.append([InlineKeyboardButton(text=label, url=f"https://t.me/{clean_name}")])

    if USER_GROUP_USERNAME:
        clean_name = get_clean_identifier(USER_GROUP_USERNAME)
        label = "👥 Join Community Group" if not is_numeric_id_string(clean_name) else "👥 Join Private Group"
        
        if is_numeric_id_string(clean_name):
             buttons.append([InlineKeyboardButton(text=label, callback_data="no_url_join")])
        else:
             buttons.append([InlineKeyboardButton(text=label, url=f"https://t.me/{clean_name}")])
             
    # Verification button always present if any join check is configured
    if JOIN_CHANNEL_USERNAME or USER_GROUP_USERNAME: 
        # UI Enhancement: Primary action button
        buttons.append([InlineKeyboardButton(text="✅ Verify Membership", callback_data="check_join")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    return None

def get_full_limit_keyboard() -> InlineKeyboardMarkup | None:
    if not ALTERNATE_BOTS: return None
    # UI Enhancement: Premium alternative bot button
    buttons = [[InlineKeyboardButton(text=f"🚀 Use Fast Mirror: @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- CLEANING LOGIC (Unchanged) ---
def clean_text_for_search(text: str) -> str:
    """Strict cleaning for Search Index (Used for Fuzzy Cache Keys and Exact Match Anchor)."""
    if not text: return ""
    text = text.lower()
    # Separators ko space se badle (DB clean logic se synchronize)
    text = re.sub(r"[._\-]+", " ", text) 
    # FIX: Better regex to only remove S01/Season 1 constructs (Bug #15)
    text = re.sub(r"\b(s|season)\s*\d{1,2}(?!\d)", " ", text)
    # Sirf a-z, 0-9, aur space rakhein
    text = re.sub(r"[^a-z0-9\s]+", "", text) 
    # Extra spaces hatayein
    text = re.sub(r"\s+", " ", text).strip() 
    return text

def clean_text_for_fuzzy(text: str) -> str:
    # FIX: Unified cleaning logic using the main search cleaner (Bug #17)
    return clean_text_for_search(text)

def extract_movie_info(caption: str | None) -> Dict[str, str] | None:
    if not caption: return None
    info = {}; lines = caption.splitlines(); title = lines[0].strip() if lines else ""
        # FIX: Regex ko case-insensitive aur flexible banaya
    if len(lines) > 1 and re.search(r"^\s*(s|season)\s*\d{1,2}", lines[1], flags=re.IGNORECASE): 
        title += " " + lines[1].strip()

    if title: info["title"] = title
    imdb_match = re.search(r"(tt\d{7,})", caption);
    if imdb_match: info["imdb_id"] = imdb_match.group(1)
    year_match = re.findall(r"\b(19[89]\d|20[0-2]\d)\b", caption)
    if year_match: info["year"] = year_match[-1]
    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str | None]:
    if not filename: return {"title": "Untitled", "year": None}
    year = None
    match_paren = re.search(r"\(((19[89]\d|20[0-3]\d))\)", filename)
    if match_paren: year = match_paren.group(1)
    else:
        matches_bare = re.findall(r"\b((19[89]\d|20[0-3]\d))\b", filename)
        if matches_bare: year = matches_bare[-1][0]
    
    title = os.path.splitext(filename)[0].strip()
    if year: title = re.sub(rf"(\s*\(?{year}\)?\s*)$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\[.*?\]", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\(.*?\)", "", title, flags=re.IGNORECASE)
    common_tags = r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio|esub|full hd)\b"
    title = re.sub(common_tags, "", title, flags=re.IGNORECASE)
    title = re.sub(r'[._]', ' ', title).strip()
    title = re.sub(r"\s+", " ", title).strip()
    
    if not title:
        title = os.path.splitext(filename)[0].strip()
        title = re.sub(r"\[.*?\]", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r"\(.*?\)", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r'[._]', ' ', title).strip()
        title = re.sub(r"\s+", " ", title).strip()
        
    return {"title": title or "Untitled", "year": year}

# UI Enhancement: Overflow message redesigned
def overflow_message(active_users: int) -> str:
    return (
        f"🚦 **SYSTEM OVERLOAD ALERT**\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ **High Traffic Detected**\n"
        f"Current Load: {active_users}/{CURRENT_CONC_LIMIT} active users.\n\n"
        f"🛡️ The system is prioritizing stability. Please try your request again in 30 seconds.\n"
        f"✨ *Thank you for your patience.*"
    )

# --- NEW: AUTO DELETE HELPER (ENGLISH + HINGLISH) ---
async def schedule_auto_delete(bot: Bot, chat_id: int, file_message_id: int, warning_message_id: int, delay: int = 120):
    """
    Schedules deletion of File AND Warning Message.
    After deletion, notifies user in both English and Hinglish.
    Default Delay: 120 seconds (2 Minutes).
    """
    # Wait for delay
    await asyncio.sleep(delay)
    
    # 1. Delete Movie File
    try:
        await safe_tg_call(
            bot.delete_message(chat_id=chat_id, message_id=file_message_id),
            semaphore=TELEGRAM_DELETE_SEMAPHORE
        )
    except Exception as e:
        logger.warning(f"Auto-Delete File Fail (Chat {chat_id}): {e}")

    # 2. Delete Warning Message (Jo button wala msg tha)
    try:
        await safe_tg_call(
            bot.delete_message(chat_id=chat_id, message_id=warning_message_id),
            semaphore=TELEGRAM_DELETE_SEMAPHORE
        )
    except Exception:
        pass

    # 3. Send "Deleted" Notification (DUAL LANGUAGE)
    # FIX: Message in both English & Hinglish for better UX
    try:
        delete_notify_text = (
            "🗑️ **File Deleted**\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🇺🇸 **System:** This file has been auto-deleted for security reasons.\n"
            "🇮🇳 **Notice:** Security reasons ki wajah se ye file delete kar di gayi hai.\n\n"
            "💡 **Solution:** Search for the movie name again to get a new link.\n"
            "💡 **Upay:** Movie wapas pane ke liye bas uska naam dobara search karein."
        )
        await safe_tg_call(
            bot.send_message(chat_id, delete_notify_text)
        )
        logger.info(f"✅ Deleted notification sent to {chat_id}")
    except Exception as e:
        logger.error(f"❌ Failed to send deleted notification to {chat_id}: {e}")
# --- END NEW ---
# ============ EVENT LOOP MONITOR (Unchanged) ============
async def monitor_event_loop():
    loop = asyncio.get_running_loop()
    while True:
        try:
            start_time = loop.time()
            # Rule: DO NOT add ANY blocking I/O or long waits.
            await asyncio.sleep(1)
            lag = (loop.time() - start_time) - 1
            if lag > 0.5: logger.warning(f"⚠️ Event loop lag detect hua: {lag:.3f}s")
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.info("Event loop monitor band ho raha hai."); break
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}", exc_info=True); await asyncio.sleep(120)

# ============ NAYA FUZZY CACHE FUNCTIONS (Unchanged) ============
async def load_fuzzy_cache(db: Database):
    """Mongo/Redis se movie titles fetch k k करके in-memory fuzzy cache banata hai।"""
    global fuzzy_movie_cache
    async with FUZZY_CACHE_LOCK:
        logger.info("In-Memory Fuzzy Cache load ho raha hai (Redis > Mongo se)...")
        try:
            # get_all_movies_for_fuzzy_cache is an async method in database.py
            movies_list = await safe_db_call(db.get_all_movies_for_fuzzy_cache(), timeout=300, default=[])
            temp_cache = {}
            
            if movies_list:
                for movie_dict in movies_list:
                    orig_clean = movie_dict.get('clean_title', '')
                    if orig_clean:
                         # FIX: Store list of movies to prevent shadowing (Bug #6)
                         if orig_clean not in temp_cache:
                             temp_cache[orig_clean] = []
                         temp_cache[orig_clean].append(movie_dict)
                
                fuzzy_movie_cache = temp_cache
                logger.info(f"✅ In-Memory Fuzzy Cache {len(fuzzy_movie_cache):,} unique titles ke saath loaded.")
            else:
                logger.error("Fuzzy cache load nahi ho paya (Redis/Mongo se koi data nahi mila).")
                fuzzy_movie_cache = {}
        except Exception as e:
            logger.error(f"Fuzzy cache load karte waqt error: {e}", exc_info=True)
            fuzzy_movie_cache = {}

# ==================================================
# +++++ V7 ULTRA INTENT ENGINE (Google-Like) +++++
# ==================================================

def get_smart_match_score_v7(query_tokens: List[str], target_clean: str, query_year: str = None, target_year: str = None) -> int:
    """
    V7 Ultra Engine:
    10-Point Logic System for 'Google-Like' Accuracy without high CPU usage.
    """
    if not query_tokens or not target_clean: return 0
    
    score = 0
    # Create Tight Strings (Spaces removed) for typo tolerance (e.g., 'ironman' == 'iron man')
    query_str_tight = "".join(query_tokens).lower()
    target_str_tight = re.sub(r'\s+', '', target_clean).lower()
    target_tokens = target_clean.split()
    
    # --- LOGIC 1: EXACT TIGHT MATCH (Highest Priority) ---
    if query_str_tight == target_str_tight:
        return 2000 # Instant Winner
        
    # --- LOGIC 2: STARTS WITH (Prefix Bonus) ---
    # Example: "Ava" matches "Avatar" better than "The Ava..."
    if target_clean.startswith(query_tokens[0]):
        score += 150
    elif target_str_tight.startswith(query_str_tight):
        score += 100

    # --- LOGIC 3: YEAR BOOSTING (Critical for Accuracy) ---
    # If user typed '2023' and movie is '2023', massive boost.
    if query_year and target_year:
        if query_year == target_year:
            score += 300
    
    # --- LOGIC 4: WORD PRESENCE & WHOLE WORD MATCH ---
    matched_words = 0
    for q_token in query_tokens:
        if len(q_token) < 2: continue 
        # Check if token exists as a WHOLE word in target
        if any(q_token == t_token for t_token in target_tokens):
            score += 60 # Whole word bonus (High)
            matched_words += 1
        # Check if token exists as Substring
        elif q_token in target_clean:
            score += 30 # Substring bonus (Medium)
            matched_words += 1
            
    # Full Query Match Bonus
    if matched_words == len([t for t in query_tokens if len(t) >= 2]):
        score += 100

    # --- LOGIC 5: WORD ORDER CORRECTNESS ---
    # "Iron Man" (Correct) vs "Man Iron" (Incorrect)
    try:
        last_idx = -1
        order_score = 0
        for q_token in query_tokens:
            curr_idx = target_clean.find(q_token)
            if curr_idx > last_idx:
                order_score += 20
                last_idx = curr_idx
        score += order_score
    except: pass

    # --- LOGIC 6: ACRONYM/INITIALS MATCH ---
    # Handles "KGF" -> "K.G.F" or "DDLJ"
    if len(query_tokens) == 1 and len(query_str_tight) > 2:
        # Check if first letters of target match query
        initials = "".join([t[0] for t in target_tokens if t])
        if query_str_tight == initials:
            score += 250

    # --- LOGIC 7: SEQUENCE MATCH (Your Legacy Logic - Preserved) ---
    # Checks character-by-character sequence
    last_idx = -1
    broken = False
    for char in query_str_tight:
        found_idx = target_str_tight.find(char, last_idx + 1)
        if found_idx == -1:
            broken = True
            break
        last_idx = found_idx
    
    if not broken:
        score += 100 # Sequence found
        
        # --- LOGIC 8: DENSITY SCORE (Coverage) ---
        # "Avengers" matches "The Avengers" (High Density) better than "Avengers Age of Ultron..." (Low Density)
        density = len(query_str_tight) / len(target_str_tight)
        score += int(density * 100) # Max 100 bonus

    # --- LOGIC 9: AESTHETIC PENALTY ---
    # Penalize if query is tiny and matches a huge title (prevent false positives)
    if len(query_str_tight) < 4 and len(target_str_tight) > 30:
        score -= 50
        
    # --- LOGIC 10: ROMAN NUMERAL INTELLIGENCE (Basic) ---
    # If query has '2', boost titles with 'II'
    if '2' in query_tokens and 'ii' in target_tokens: score += 50
    if '3' in query_tokens and 'iii' in target_tokens: score += 50

    return score

def python_fuzzy_search(query: str, limit: int = 10, **kwargs) -> List[Dict]:
    """
    V7 Ultra Search Handler:
    Integrates Intent Engine V7 with RapidFuzz for Google-like precision.
    """
    # 1. Thread Safety Snapshot
    current_cache = kwargs.get('cache_snapshot') or fuzzy_movie_cache
    if not current_cache:
        return []

    try:
        # --- INTELLIGENT QUERY PARSING ---
        # Extract Year from Query if present (e.g., "Jawan 2023")
        query_year = None
        year_match = re.search(r"\b(19[7-9]\d|20[0-2]\d)\b", query)
        if year_match:
            query_year = year_match.group(1)
        
        q_fuzzy = clean_text_for_fuzzy(query) 
        q_anchor = clean_text_for_search(query) 
        
        if not q_fuzzy or not q_anchor: return []
        
        query_tokens = [t for t in q_anchor.split() if t]
        candidates = []
        seen_imdb = set()
        
        # --- 1. EXACT MATCH ANCHOR (Confirmation) ---
        anchor_keys = [q_anchor]
        if q_anchor.startswith('the '): anchor_keys.append(q_anchor[4:]) 
        else: anchor_keys.append('the ' + q_anchor) 

        for key in set(anchor_keys):
            if key in current_cache:
                movies_list = current_cache[key]
                if isinstance(movies_list, dict): movies_list = [movies_list]

                for data in movies_list:
                    if data['imdb_id'] not in seen_imdb:
                         candidates.append({
                            'imdb_id': data['imdb_id'],
                            'title': data['title'],
                            'year': data.get('year'),
                            'score': 2000, # MAX SCORE
                            'match_type': 'exact_anchor'
                         })
                         seen_imdb.add(data['imdb_id'])
        
        # --- 2. RAPIDFUZZ BROAD FETCH (Keeping 800 Limit as requested) ---
        all_titles = list(current_cache.keys())
        
        # CPU Optimization: Only scan if exact match didn't fill the page
        if len(candidates) < limit:
            pre_filtered = process.extract(
                q_fuzzy, 
                all_titles, 
                limit=800, # Keeping your request
                scorer=fuzz.WRatio, 
                score_cutoff=35 
            )
            
            # --- 3. V7 ENGINE RE-RANKING ---
            for clean_title_key, fuzz_score, _ in pre_filtered:
                movies_list = current_cache.get(clean_title_key)
                if not movies_list: continue
                if isinstance(movies_list, dict): movies_list = [movies_list]

                for data in movies_list:
                    if data['imdb_id'] in seen_imdb: continue
                    
                    target_year = data.get('year')
                    
                    # CALL V7 ENGINE
                    intent_score = get_smart_match_score_v7(query_tokens, clean_title_key, query_year, target_year)
                    
                    final_score = 0
                    match_type = "fuzzy"
                    
                    # Hybrid Scoring Formula
                    if fuzz_score >= 90:
                        final_score = 900 + intent_score
                        match_type = "high_fuzzy"
                    else:
                        # Base fuzz score + V7 Intelligence
                        final_score = fuzz_score + intent_score
                        match_type = "intent_v7"

                    candidates.append({
                        'imdb_id': data['imdb_id'],
                        'title': data['title'],
                        'year': target_year,
                        'score': final_score,
                        'match_type': match_type
                    })
                    seen_imdb.add(data['imdb_id'])

        # 4. Final Sort
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        return candidates[:limit]
        
    except Exception as e:
        logger.error(f"python_fuzzy_search V7 mein error: {e}", exc_info=True)
        return []
# ============ LIFESPAN MANAGEMENT (FastAPI) (F.I.X.E.D.) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor_task, executor, watchdog
    logger.info("Application startup shuru ho raha hai...")
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop(); loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialize ho gaya.")

    # --- NEW: Redis Init (Free-Tier Optimization) ---
    await redis_cache.init_cache()
    # --- END NEW ---
    
    # MongoDB 1 (Primary)
    db1_success = False
    try:
        # F.I.X: init_db coroutine ko execute karein aur safe_db_call se wrap karein
        # safe_db_call ab connection error ko False mein badal dega.
        db1_success = await safe_db_call(db_primary.init_db(), timeout=60, default=False) 

        if db1_success:
             logger.info("Database 1 (MongoDB Primary) initialization safal.")
        else:
             # F.I.X: agar safe_db_call False lautaata hai (connection fail/timeout), to crash karein
             logger.critical("FATAL: Database 1 (MongoDB Primary) initialize nahi ho paya (connection failed or timed out).")
             await shutdown_procedure()
             # Yahan koi 'from e' nahi hai, kyunki e ko safe_db_call mein handle kar liya gaya hai
             raise RuntimeError("MongoDB 1 connection fail (startup).")
    except Exception as e:
        # F.I.X: agar upar ka try block mein koi unknown exception aata hai (rare), to crash karein
        logger.critical(f"FATAL: Database 1 (MongoDB Primary) initialize nahi ho paya: {e}", exc_info=True)
        await shutdown_procedure()
        raise RuntimeError("MongoDB 1 connection fail (startup).") from e

    # MongoDB 2 (Fallback)
    try:
        # F.I.X: init_db coroutine ko execute karein aur safe_db_call se wrap karein
        db_fallback_success = await safe_db_call(db_fallback.init_db(), default=False)
        if db_fallback_success:
             logger.info("Database 2 (MongoDB Fallback) initialization safal.")
        else:
             logger.warning("Database 2 (MongoDB Fallback) initialization failed. Bot degraded mode mein chalega.")
    except Exception as e:
        logger.warning(f"Database 2 (MongoDB Fallback) initialize karte waqt error: {e}")

    # NeonDB 3
    try:
        # NeonDB init is a pure ASYNC method (asyncpg)
        await db_neon.init_db() 
        logger.info("Database 3 (NeonDB/Postgres Backup) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database 3 (NeonDB/Postgres Backup) initialize nahi ho paya: {e}", exc_info=True)
        await shutdown_procedure()
        raise RuntimeError("NeonDB/Postgres connection fail (startup).") from e


    # --- NAYA: Fuzzy Cache Load Karein (ab Redis/Mongo se) ---
    await load_fuzzy_cache(db_primary)

    # --- NEW: Start Priority Queue Workers ---
    db_objects_for_queue = {
        'db_primary': db_primary,
        'db_fallback': db_fallback,
        'db_neon': db_neon,
        'redis_cache': redis_cache,
        'admin_id': ADMIN_USER_ID
    }
    priority_queue.start_workers(bot, dp, db_objects_for_queue)
    logger.info(f"Priority Queue with {QUEUE_CONCURRENCY} workers start ho gaya।")
    # --- END NEW ---

    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor start ho gaya.")

    # --- NEW: Start Watchdog (Rule: Only ADD new layers/wrappers) ---
    if WATCHDOG_ENABLED:
         db_objects_for_watchdog = {
             'db_primary': db_primary,
             'db_neon': db_neon,
             'redis_cache': redis_cache,
         }
         # Watchdog ko DP ke baaki objects pass karein
         watchdog = SmartWatchdog(bot, dp, db_objects_for_watchdog)
         watchdog.start()
         logger.warning("Smart Watchdog initialized and running.")
    # --- END NEW ---
    
    # --- FIX: Webhook calls ko Throttle karna (Multi-Token Flood Control) ---
    
    # NAYA FIX: Webhook initialization lock check
    WEBHOOK_INIT_LOCK_NAME = "global_webhook_set_lock"
    
    # check_if_lock_exists is an async method in database.py
    is_webhook_already_set = await safe_db_call(db_primary.check_if_lock_exists(WEBHOOK_INIT_LOCK_NAME), default=False)

    if not is_webhook_already_set:
        logger.warning("Webhook initialization lock nahi mila. Setting up webhooks...")
        
        # acquire_cross_process_lock is an async method in database.py
        lock_acquired = await safe_db_call(db_primary.acquire_cross_process_lock(WEBHOOK_INIT_LOCK_NAME, 300), default=False)
        
        if lock_acquired:
            try:
                webhook_tasks = []
                for bot_instance in bot_manager.get_all_bots():
                    token = bot_instance.token
                    webhook_url_for_token = build_webhook_url().replace(BOT_TOKEN, token) 
                    
                    if webhook_url_for_token:
                        # Webhook setting function
                        async def set_webhook_safely(bot_instance: Bot, url: str):
                            async with WEBHOOK_SEMAPHORE:
                                # Guaranteed delay for setting webhook on any token (FLOOD WAIT mitigation)
                                await asyncio.sleep(1.0) 
                                try:
                                    current_webhook = await safe_tg_call(bot_instance.get_webhook_info())
                                    is_webhook_set = current_webhook and current_webhook.url == url
            
                                    if not is_webhook_set:
                                         logger.info(f"Webhook set kiya ja raha hai for {token[:4]}...: {url}")
                                         result = await safe_tg_call(
                                             bot_instance.set_webhook(
                                                 url=url,
                                                 allowed_updates=dp.resolve_used_update_types(),
                                                 secret_token=(WEBHOOK_SECRET or None),
                                                 drop_pending_updates=True
                                             )
                                         )
                                         if result:
                                             logger.info(f"Webhook set ho gaya for {token[:4]}...।")
                                         else:
                                             logger.error(f"Webhook setup fail for {token[:4]}... after retries.")
                                    else:
                                         logger.info(f"Webhook pehle se sahi set hai for {token[:4]}...।")
                                except Exception as e:
                                    logger.error(f"Webhook setup mein critical error for {token[:4]}...: {e}", exc_info=True)
                        
                        webhook_tasks.append(set_webhook_safely(bot_instance, webhook_url_for_token))

                if webhook_tasks:
                    # Sabhi set_webhook tasks ko chalao
                    await asyncio.gather(*webhook_tasks) 
                
            finally:
                # release_cross_process_lock is an async method in database.py
                await safe_db_call(db_primary.release_cross_process_lock(WEBHOOK_INIT_LOCK_NAME))
                logger.warning("✅ Global Webhook Lock released.")
        else:
            logger.warning("Webhook Lock acquire nahi ho paya. Assuming another process is handling it or system load is too high.")
    else:
        logger.info("Webhook initialization lock exists. Skipping set_webhook procedure in this worker.")
    # --- END FIX ---

    setup_signal_handlers()
    logger.info("Application startup poora hua. Bot taiyar hai.")
    yield
    logger.info("Application shutdown sequence shuru ho raha hai...")
    await shutdown_procedure()
    logger.info("Application shutdown poora hua.")


app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK / HEALTHCHECK ROUTES (Unchanged) ============

@app.post(f"/bot/{{token}}")
async def bot_webhook(token: str, update: dict, background_tasks: BackgroundTasks, request: Request):
        # FIX: Strict Secret Verification
    secret_header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if WEBHOOK_SECRET and secret_header != WEBHOOK_SECRET:
        logger.critical(f"⛔ SECURITY ALERT: Invalid Webhook Secret! Got: {secret_header}")
        raise HTTPException(status_code=403, detail="Forbidden: Security Verification Failed")
        
    # --- NEW: Bot Manager se Bot Instance select karein (Multi-Token) ---
    bot_instance = bot_manager.get_bot_by_token(token)
    if bot_instance.token != token:
        logger.warning(f"Invalid token {token[:4]}... received।")
        raise HTTPException(status_code=404, detail="Not Found: Invalid Bot Token")
    # --- END NEW ---

    try:
        telegram_update = Update(**update)
        
        # --- NEW: BackgroundTasks hata kar PriorityQueue mein submit karein (Non-Blocking) ---
        db_objects_for_queue = {
            'db_primary': db_primary,
            'db_fallback': db_fallback,
            'db_neon': db_neon,
            'redis_cache': redis_cache, 
            'admin_id': ADMIN_USER_ID
        }
        priority_queue.submit(telegram_update, bot_instance, db_objects_for_queue)
        # --- END NEW ---
        
        return {"ok": True, "token_received": token[:4] + "..."}
    except Exception as e:
        logger.error(f"Webhook update parse/submit nahi kar paya: {e}", exc_info=False)
        logger.debug(f"Failed update data: {update}")
        return {"ok": False, "error": f"Invalid update format: {e}"}

@app.get("/")
@app.get("/ping")
async def ping():
    return {"status": "ok", "uptime": get_uptime(), "queue_size": priority_queue._queue.qsize()}

@app.get("/health")
async def health_check():
    # All check methods are async methods in database.py/neondb.py
    db_primary_ok_task = safe_db_call(db_primary.is_ready(), default=False)
    db_fallback_ok_task = safe_db_call(db_fallback.is_ready(), default=False)
    db_neon_ok_task = safe_db_call(db_neon.is_ready(), default=False)
    
    # FIX: redis_cache.is_ready() sync hai, ise gather se bahar call karein
    redis_ok = redis_cache.is_ready()

    db_primary_ok, db_fallback_ok, neon_ok = await asyncio.gather(
        db_primary_ok_task, db_fallback_ok_task, db_neon_ok_task
    )
    
    status_code = 200
    status_msg = "ok"
    
    if not db_primary_ok:
        status_msg = "error_mongodb_primary_connection"
        status_code = 503
    elif not redis_ok:
        status_msg = "degraded_redis_connection"
    elif not db_fallback_ok:
        status_msg = "degraded_mongodb_fallback_connection"
    elif not neon_ok:
        status_msg = "degraded_neondb_connection"
    
    return {
        "status": status_msg,
        "database_mongo_primary_connected": db_primary_ok,
        "database_mongo_fallback_connected": db_fallback_ok,
        "database_neon_connected": neon_ok,
        "cache_redis_connected": redis_ok, # Redis status
        "search_logic": "Hybrid (Smart Tokenization + Word Presence)",
        "fuzzy_cache_size": len(fuzzy_movie_cache),
        "queue_size": priority_queue._queue.qsize(), # Queue size
        "uptime": get_uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, status_code

# ============ USER CAPACITY CHECK (Unchanged logic, updated messages) ============

async def ensure_capacity_or_inform(
    message_or_callback: types.Message | types.CallbackQuery,
    db_primary: Database,
    current_bot: Bot, # Naya: Bot instance pass karein
    redis_cache: RedisCacheLayer 
) -> bool:
    user = message_or_callback.from_user
    if not user: return True
    
    target_chat_id = None
    if isinstance(message_or_callback, types.Message):
        target_chat_id = message_or_callback.chat.id
    elif isinstance(message_or_callback, types.CallbackQuery) and message_or_callback.message:
        target_chat_id = message_or_callback.message.chat.id
    
    # add_user is an async method in database.py
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    if user.id == ADMIN_USER_ID: 
        return True
        
    # get_concurrent_user_count is an async method in database.py
    active = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    if active >= CURRENT_CONC_LIMIT:
        logger.warning(f"Capacity full: {active}/{CURRENT_CONC_LIMIT}. User {user.id} ki request hold par.")
        
        # Admin commands ko overflow message se skip karein (High Priority)
        is_command = (
             isinstance(message_or_callback, types.Message) and 
             message_or_callback.text and 
             message_or_callback.text.startswith('/')
        )
        is_admin_action = user.id == ADMIN_USER_ID

        if not is_command and not is_admin_action and target_chat_id:
            # UI Enhancement: Use redesigned overflow message
            await safe_tg_call(
                current_bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()),
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
        if isinstance(message_or_callback, types.CallbackQuery):
            # UI Enhancement: Use friendly callback answer
            await safe_tg_call(message_or_callback.answer("⚠️ System Busy: High Load. Try again momentarily. 🟡", show_alert=False))
        return False
        
    return True

# ============ USER COMMANDS AND HANDLERS ============
# --- GLOBAL TASK REGISTRY ---
# Admin ke active tasks ko store karne ke liye
ADMIN_ACTIVE_TASKS: Dict[int, asyncio.Task] = {}

@dp.message(Command("cancel"), StateFilter("*"))
async def cancel_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    canceled_any = False

    # 1. FSM State Cancel
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        canceled_any = True
    
    # 2. Background Task Cancel (Universal Cancel)
    if user_id in ADMIN_ACTIVE_TASKS:
        task = ADMIN_ACTIVE_TASKS[user_id]
        if not task.done():
            task.cancel() # Task ko stop signal bhejo
            canceled_any = True
            logger.info(f"Admin {user_id} ne task cancel kiya.")
        # Registry se remove hum done_callback me karenge, par safety ke liye yahan bhi try kar sakte hain
        del ADMIN_ACTIVE_TASKS[user_id]

    if canceled_any:
        await message.answer("🚫 **Process Cancelled.**\nStopped active tasks and cleared states.")
    else:
        await message.answer("ℹ️ **Nothing to cancel.** No active tasks found.")

@dp.message(CommandStart(), BannedFilter())
async def banned_start_command_stub(message: types.Message):
    pass

# UI Enhancement & CRITICAL BUG FIX (The logic that caused /stats to trigger /start for admin is removed)
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    user = message.from_user
    if not user: return
    user_id = user.id
        # FIX: Security Check for Banned Users
    is_banned = await safe_db_call(db_primary.is_user_banned(user_id), default=False)
    if is_banned and user_id != ADMIN_USER_ID:
        await message.answer("🚫 **Access Denied**: You are banned.")
        return

    # --- FEATURE B: MONETIZATION TOKEN CATCH ---
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("unlock_"):
        token = args[1].split("_")[1]
        token_doc = await db_primary.verify_unlock_token(token, user_id)
        if token_doc:
            # --- BILINGUAL SUCCESS MESSAGE ---
            success_msg = (
                "✅ **DOWNLOAD UNLOCKED / अनलॉक हो गया!**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "🇺🇸 **Success!** Delivering your file. You now have a **24-hour direct pass**.\n"
                "🇮🇳 **सफल!** आपकी फाइल भेजी जा रही है। अब आपको **24 घंटे तक** कोई शॉर्टलिंक नहीं दिखेga।"
            )
            await message.answer(success_msg)
            
            # --- SET 24 HOUR PASS IN REDIS ---
            if redis_cache.is_ready():
                # 86400 seconds = 24 hours
                await redis_cache.set(f"sl_pass:{user_id}", "active", ttl=86400)
            
            asyncio.create_task(db_primary.track_event("shortlink_success"))
            
            # --- MOVIE DELIVERY LOGIC ---
            imdb_id = token_doc["imdb_id"]
            movie = await safe_db_call(db_primary.get_movie_by_imdb(imdb_id))
            
            if movie and movie.get("channel_id") and movie.get("message_id"):
                res = await safe_tg_call(bot.copy_message(user_id, int(movie["channel_id"]), movie["message_id"]), semaphore=TELEGRAM_COPY_SEMAPHORE)
                if res:
                    # Auto-delete schedule
                    asyncio.create_task(schedule_auto_delete(bot, user_id, res.message_id, message.message_id))
                    # Sponsor ad trigger
                    asyncio.create_task(send_sponsor_ad(user_id, bot, db_primary, redis_cache))
            else: # <--- Ab yeh bilkul sahi aligned hai
                await message.answer("⚠️ **Content Unavailable**: File data is corrupted.")
            return
        else:
            await message.answer("❌ **Verification Failed!**\nToken is invalid or expired. Please search again and use the new link.")
            return
    # --- END MONETIZATION CATCH ---

        # --- ADMIN WELCOME LOGIC (NEW) ---
    if user_id == ADMIN_USER_ID:
        admin_text = (
            f"🕶️ **SYSTEM COMMAND CENTER**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👋 **Greetings, Administrator.**\n\n"
            f"🚀 **System Status:** ONLINE\n"
            f"📡 **Network:** STABLE\n"
            f"🛡️ **Security:** ACTIVE\n\n"
            f"Tap the console button to view live metrics."
        )
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Open Live Dashboard", callback_data="admin_stats_cmd")]
        ])
        # FIX: Indentation correct kiya hai (return hata diya hai)
        await safe_tg_call(message.answer(admin_text, reply_markup=admin_kb), semaphore=TELEGRAM_COPY_SEMAPHORE)
    # --- END ADMIN WELCOME LOGIC ---

    if not await ensure_capacity_or_inform(message, db_primary, bot, redis_cache):
        return
        
    is_member = await check_user_membership(user.id, bot)
    join_markup = get_join_keyboard()
    
    if is_member:
        # UI Enhancement: Cinematic Welcome Banner (Start UI)
        welcome_text = (
            f"🎬 **THE CINEMATIC ARCHIVE** 🍿\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👋 Welcome back, <b>{user.first_name}</b>.\n"
            f"Your gateway to the ultimate movie collection is active.\n\n"
            f"📥 **HOW TO SEARCH**\n"
            f"Simply type the **Movie Name** below.\n"
            f"• <i>Example:</i> <code>Avengers</code>\n"
            f"• <i>Smart Search:</i> Typos are auto-corrected.\n\n"
            f"🚀 **Ready? Start typing...**"
        )
        
        # UI Enhancement: App-like main menu buttons
        main_menu = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💡 Search Tips & Tricks", callback_data="help_cmd"),
            ],
            [
                InlineKeyboardButton(text="📢 Official Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}" if JOIN_CHANNEL_USERNAME else "https://t.me/telegram"),
                InlineKeyboardButton(text="🆘 Support Hub", callback_data="support_cmd"),
            ]
        ])
        
        await safe_tg_call(message.answer(welcome_text, reply_markup=main_menu), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        # UI Enhancement: Join Check Screen Text
        welcome_text = (
            f"🔒 **AUTHENTICATION REQUIRED**\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"To access the full Cinematic Database, please verify your membership.\n\n"
            f"1️⃣ **Join the channels** using the buttons below.\n"
            f"2️⃣ Tap **Verify Membership** to unlock access.\n\n"
            f"<i>Access is free and instant.</i>"
        )
        if join_markup:
            await safe_tg_call(message.answer(welcome_text, reply_markup=join_markup), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            logger.error("User ne start kiya par koi JOIN_CHANNEL/GROUP set nahi hai.")
            await safe_tg_call(message.answer("⚠️ Configuration Error: Please contact Admin."), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("help"), BannedFilter())
@handler_timeout(10)
async def help_command(message: types.Message, bot: Bot, db_primary: Database, redis_cache: RedisCacheLayer):
    user = message.from_user
    if not user: return
    # add_user is an async method in database.py
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    # UI Enhancement: Aesthetic "How to Use" screen
    help_text = (
        "📚 **SEARCH PROTOCOLS**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "🔎 **Basic Search**\n"
        "Type the title directly. No commands needed.\n"
        "• <code>Jawan</code>\n"
        "• <code>Inception</code>\n\n"
        "🧠 **Smart & Fuzzy Logic**\n"
        "Our engine handles spelling mistakes automatically.\n"
        "• <code>Avegers</code> → <b>Avengers</b>\n\n"
        "🎯 **Precision Search**\n"
        "Add the year to filter results instantly.\n"
        "• <code>Pathaan 2023</code>\n\n"
        "⚡ **Pro Tip:** If the bot is waking up, the first search takes ~10s. Subsequent searches are instant."
    )
    
    # UI Enhancement: Add a return button for continuity
    back_button = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data="start_cmd")]
    ])
    
    await safe_tg_call(message.answer(help_text, reply_markup=back_button), semaphore=TELEGRAM_COPY_SEMAPHORE)

# UI Enhancement: Handle help_cmd callback to show help text
@dp.callback_query(F.data == "help_cmd")
@handler_timeout(10)
async def help_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, redis_cache: RedisCacheLayer):
    await safe_tg_call(callback.answer("Opening Guide..."))
    user = callback.from_user
    
    help_text = (
        "📚 **SEARCH PROTOCOLS**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "🔎 **Basic Search**\n"
        "Type the title directly. No commands needed.\n"
        "• <code>Jawan</code>\n"
        "• <code>Inception</code>\n\n"
        "🧠 **Smart & Fuzzy Logic**\n"
        "Our engine handles spelling mistakes automatically.\n"
        "• <code>Avegers</code> → <b>Avengers</b>\n\n"
        "🎯 **Precision Search**\n"
        "Add the year to filter results instantly.\n"
        "• <code>Pathaan 2023</code>\n\n"
        "⚡ **Pro Tip:** If the bot is waking up, the first search takes ~10s. Subsequent searches are instant."
    )
    
    back_button = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data="start_cmd")]
    ])
    
    try:
        await safe_tg_call(callback.message.edit_text(help_text, reply_markup=back_button))
    except Exception:
        await safe_tg_call(bot.send_message(user.id, help_text, reply_markup=back_button), semaphore=TELEGRAM_COPY_SEMAPHORE)

# UI Enhancement: UNIQUE Support Handler
@dp.callback_query(F.data == "support_cmd")
@handler_timeout(10)
async def support_callback(callback: types.CallbackQuery, bot: Bot):
    await safe_tg_call(callback.answer("Opening Support Hub..."))
    
    support_text = (
        "🆘 **SUPPORT CENTER**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "Need assistance? We are here to help.\n\n"
        "🔹 **Common Issues**\n"
        "• File not opening? Try updating your Telegram app.\n"
        "• Search not working? Check spelling or try adding the year.\n\n"
        "🔹 **Contact Admin**\n"
        "For broken links or specific requests, please contact the main admin.\n\n"
        "<i>To return, tap the button below.</i>"
    )
    
    back_button = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data="start_cmd")]
    ])
    
    try:
        await safe_tg_call(callback.message.edit_text(support_text, reply_markup=back_button))
    except Exception:
        pass


# UI Enhancement: Handle start_cmd callback to return to home
@dp.callback_query(F.data == "start_cmd")
@handler_timeout(15)
async def start_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    await safe_tg_call(callback.answer("Home..."))
    # Re-use the logic from start_command
    user = callback.from_user
    if not user: return

    if not await ensure_capacity_or_inform(callback, db_primary, bot, redis_cache):
        return
        
    is_member = await check_user_membership(user.id, bot)
    join_markup = get_join_keyboard()
    
    if is_member:
        welcome_text = (
            f"🎬 **THE CINEMATIC ARCHIVE** 🍿\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"👋 Welcome back, <b>{user.first_name}</b>.\n"
            f"Your gateway to the ultimate movie collection is active.\n\n"
            f"📥 **HOW TO SEARCH**\n"
            f"Simply type the **Movie Name** below.\n"
            f"• <i>Example:</i> <code>Avengers</code>\n"
            f"• <i>Smart Search:</i> Typos are auto-corrected.\n\n"
            f"🚀 **Ready? Start typing...**"
        )
        
        main_menu = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💡 Search Tips & Tricks", callback_data="help_cmd"),
            ],
            [
                InlineKeyboardButton(text="📢 Official Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}" if JOIN_CHANNEL_USERNAME else "https://t.me/telegram"),
                InlineKeyboardButton(text="🆘 Support Hub", callback_data="support_cmd"),
            ]
        ])
        
        try:
            await safe_tg_call(callback.message.edit_text(welcome_text, reply_markup=main_menu))
        except Exception:
            await safe_tg_call(bot.send_message(user.id, welcome_text, reply_markup=main_menu), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        welcome_text = (
            f"🔒 **AUTHENTICATION REQUIRED**\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"To access the full Cinematic Database, please verify your membership.\n\n"
            f"1️⃣ **Join the channels** using the buttons below.\n"
            f"2️⃣ Tap **Verify Membership** to unlock access.\n\n"
            f"<i>Access is free and instant.</i>"
        )
        if join_markup:
            try:
                await safe_tg_call(callback.message.edit_text(welcome_text, reply_markup=join_markup))
            except Exception:
                await safe_tg_call(bot.send_message(user.id, welcome_text, reply_markup=join_markup), semaphore=TELEGRAM_COPY_SEMAPHORE)

@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, redis_cache: RedisCacheLayer):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))

    # is_user_banned is an async method in database.py
    is_banned = await safe_db_call(db_primary.is_user_banned(user.id), default=False)
    if is_banned:
        await safe_tg_call(callback.answer("❌ Access Denied: You are restricted from this service.", show_alert=True))
        return
        
    await safe_tg_call(callback.answer("Verifying Membership... 🔄"))
    
    if not await ensure_capacity_or_inform(callback, db_primary, bot, redis_cache):
        return

    is_member = await check_user_membership(user.id, bot)
    
    if is_member:
        # get_concurrent_user_count is an async method in database.py
        active_users = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        # UI Enhancement: Success message
        success_text = (
            f"✅ **VERIFICATION COMPLETE**\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Access Granted. Welcome, <b>{user.first_name}</b>!\n\n"
            f"🔍 **Start Searching Now**\n"
            f"Type any movie title to begin.\n"
            f"<i>Live Traffic: {active_users}/{CURRENT_CONC_LIMIT} Users</i>"
        )
        
        # Re-display main menu for convenience
        main_menu = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💡 Search Tips & Tricks", callback_data="help_cmd"),
            ],
            [
                InlineKeyboardButton(text="📢 Official Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}" if JOIN_CHANNEL_USERNAME else "https://t.me/telegram"),
                InlineKeyboardButton(text="🆘 Support Hub", callback_data="support_cmd"),
            ]
        ])
        
        try:
            await safe_tg_call(callback.message.edit_text(success_text, reply_markup=main_menu))
        except Exception:
            await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=main_menu), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        # UI Enhancement: Failure message
        await safe_tg_call(callback.answer("❌ Verification Failed: Please join all required channels first.", show_alert=True))
        join_markup = get_join_keyboard()
        if callback.message and (not callback.message.reply_markup or not callback.message.reply_markup.inline_keyboard):
             if callback.message.text and join_markup:
                 await safe_tg_call(callback.message.edit_reply_markup(reply_markup=join_markup))

@dp.callback_query(F.data == "no_url_join")
@handler_timeout(5)
async def no_url_join_callback(callback: types.CallbackQuery):
    # UI Enhancement: More polished private link notice
    await safe_tg_call(callback.answer("🔒 Private Access: Please wait for the Admin to approve the link. Tap 'Verify' once joined.", show_alert=True))


# =======================================================
# +++++ BOT HANDLERS: NAYA HYBRID SEARCH LOGIC +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"), BannedFilter())
async def banned_search_movie_handler_stub(message: types.Message): pass

# SMART FIX: StateFilter(None) ensured search only triggers outside FSM flow
@dp.message(
    StateFilter(None), 
    F.text, 
    ~F.text.startswith("/"), 
    (F.chat.type == "private")
)
@handler_timeout(20)
async def search_movie_handler(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message, db_primary, bot, redis_cache):
        return
            # --- FIX START: Force Join Check Before Search ---
    # Agar user member nahi hai, toh search rok do aur Join buttons dikhao
    is_member = await check_user_membership(user.id, bot)
    if not is_member:
        join_markup = get_join_keyboard()
        if join_markup:
            join_text = (
                f"🔒 **UNLOCK SEARCH ACCESS**\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ You must join our channels to search for movies.\n\n"
                f"1️⃣ **Join the channels** below.\n"
                f"2️⃣ Tap **Verify Membership** to unlock."
            )
            await safe_tg_call(message.answer(join_text, reply_markup=join_markup), semaphore=TELEGRAM_COPY_SEMAPHORE)
            return
    # --- FIX END ---
    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("⚠️ **Query too short.** Please enter at least 2 characters."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    clean_query = clean_text_for_search(original_query) # "tere ishq me"
    if not clean_query:
        await safe_tg_call(message.answer("⚠️ **Invalid Query.** Please try a different title."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    if not fuzzy_movie_cache:
        logger.error(f"FATAL: User {user_id} ne search kiya, lekin fuzzy cache khaali hai!")
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(message.answer("⚠️ ADMIN WARNING: Cache Empty. Run /reload_fuzzy_cache."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            # UI Enhancement: Soft initialization error
            await safe_tg_call(message.answer("🔄 **System Initializing...**\nPlease wait 30 seconds and try again."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # UI Enhancement: Polished searching message
    searching_msg = await safe_tg_call(message.answer(f"📡 **Scanning Database...**\nLooking for: <i>{original_query}</i>"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not searching_msg: return

    # --- SAVE QUERY FOR REFRESH LOGIC (Redis) ---
    if redis_cache.is_ready():
         # Store for 10 minutes to allow refresh
         await redis_cache.set(f"last_query:{user_id}", original_query, ttl=600)

    # --- NAYA HYBRID SEARCH (Smart Sequence > Fuzzy) ---
    
    logger.info(f"User {user_id} searching for '{clean_query}'")
    
    # Rule: python_fuzzy_search is CPU-bound (rapidfuzz), run it in ThreadPoolExecutor.
    loop = asyncio.get_running_loop()
    # python_fuzzy_search is a plain function, so it runs in executor
        # FIX: Pass Cache Snapshot to Thread
    cache_snapshot = fuzzy_movie_cache.copy() # Shallow copy is enough and fast
    fuzzy_hits_task = loop.run_in_executor(executor, partial(python_fuzzy_search, cache_snapshot=cache_snapshot), original_query, 45)
  
    try:
        fuzzy_hits_raw = await fuzzy_hits_task
    except Exception as e:
        logger.error(f"Search Executor Failed: {e}")
        fuzzy_hits_raw = []

    unique_movies = {}
    for movie in fuzzy_hits_raw:
        # Exact match (score 1001) ko pehle hi dal denge
        if movie.get('imdb_id') and movie['imdb_id'] not in unique_movies:
            unique_movies[movie['imdb_id']] = movie

    # --- End Hybrid Search ---

    if not unique_movies:
        # UI Enhancement: No results message
        await safe_tg_call(searching_msg.edit_text(f"✖️ **NO RESULTS FOUND**\n━━━━━━━━━━━━━━\nWe couldn't find matches for '<b>{original_query}</b>'.\n\n🔹 Check spelling.\n🔹 Try a shorter keyword."))
        return

    buttons = []
    max_buttons = 15
    
    final_results = list(unique_movies.values())
    # Priority Sort
    final_results.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    # Slice for first page
    page_results = final_results[:max_buttons]
    
    for movie in page_results:
        # UI Enhancement: Shorten title & add premium icons
        display_title = movie["title"][:35] + '...' if len(movie["title"]) > 35 else movie["title"]
        year_str = f" ({movie.get('year')})" if movie.get('year') else ""
        
        icon = "🎬" # Default
        if movie.get('match_type') == 'exact_anchor':
            icon = "🎯" # Exact Match Anchor
        elif 'score' in movie and movie['score'] >= 900: # High Fuzzy
            icon = "🥇" 
        elif 'score' in movie and movie['score'] >= 500: # Intent matches (Smart Sequence + Word Presence)
            icon = "⚡" 
        elif 'score' in movie and movie['score'] > 75: # High Fuzzy score
            icon = "⭐"
        
        buttons.append([InlineKeyboardButton(text=f"{icon} {display_title}{year_str}", callback_data=f"get_{movie['imdb_id']}")])

    # --- REFRESH BUTTON LOGIC ---
    if len(final_results) > max_buttons:
        # User requested refresh limit: 2 times per day.
        # We start offset at 15.
        buttons.append([InlineKeyboardButton(text="🔄 Refresh Results (Next Page)", callback_data="refresh_search:15")])

    result_count = len(final_results)
    result_count_text = f"{result_count}" if result_count <= 45 else "45+"
    
    # UI Enhancement: Premium header for results
    await safe_tg_call(searching_msg.edit_text(
        f"✨ **SEARCH COMPLETE**\n━━━━━━━━━━━━━━\nFound **{result_count_text}** matches for '<b>{original_query}</b>':\n👇 <i>Tap to download</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))

# --- NEW: REFRESH SEARCH CALLBACK ---
@dp.callback_query(F.data.startswith("refresh_search:"))
@handler_timeout(10)
async def refresh_search_callback(callback: types.CallbackQuery, bot: Bot, redis_cache: RedisCacheLayer):
    user = callback.from_user
    if not user: return
    
    try:
        offset = int(callback.data.split(":")[1])
    except:
        offset = 15

    # 1. Rate Limit Check (2 per day)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    limit_key = f"refresh_limit:{user.id}:{today}"
    
    current_count = 0
    if redis_cache.is_ready():
        cached_count = await redis_cache.get(limit_key)
        if cached_count:
            # FIX: Safe casting for Redis data (Bug #16)
            try:
                current_count = int(float(cached_count))
            except (ValueError, TypeError):
                current_count = 0
            
    MAX_REFRESH_LIMIT = 20
    # FIX: Strict limit check for non-admins
    if current_count >= MAX_REFRESH_LIMIT and user.id != ADMIN_USER_ID:
        await safe_tg_call(callback.answer("⚠️ Daily Search Limit Reached. Please try again tomorrow.", show_alert=True))
        return

    # 2. Get Last Query
    last_query = None
    if redis_cache.is_ready():
        last_query = await redis_cache.get(f"last_query:{user.id}")
        
    if not last_query:
        await safe_tg_call(callback.answer("⚠️ Session Expired: Please search again.", show_alert=True))
        return

    await safe_tg_call(callback.answer(f"Fetching more results... ({2 - current_count} left)"))

    # 3. Perform Search again
    loop = asyncio.get_running_loop()
    # python_fuzzy_search is CPU-bound, run in executor
    fuzzy_hits_raw = await loop.run_in_executor(executor, python_fuzzy_search, last_query, 45)
    
    if not fuzzy_hits_raw:
        await safe_tg_call(callback.answer("No more results found.", show_alert=True))
        return

    unique_movies = {}
    for movie in fuzzy_hits_raw:
        if movie.get('imdb_id') and movie['imdb_id'] not in unique_movies:
            unique_movies[movie['imdb_id']] = movie
            
    final_results = list(unique_movies.values())
    final_results.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    # 4. Slice Results based on offset
    end_offset = offset + 15
    page_results = final_results[offset:end_offset]
    
    if not page_results:
        await safe_tg_call(callback.answer("No more extra results to show.", show_alert=True))
        return

    # 5. Build Buttons
    buttons = []
    for movie in page_results:
        display_title = movie["title"][:35] + '...' if len(movie["title"]) > 35 else movie["title"]
        year_str = f" ({movie.get('year')})" if movie.get('year') else ""
        icon = "🎬"
        
        if movie.get('match_type') == 'exact_anchor':
            icon = "🎯"
        elif 'score' in movie and movie['score'] >= 900:
            icon = "🥇"
        elif 'score' in movie and movie['score'] >= 500:
            icon = "⚡"
            
        buttons.append([InlineKeyboardButton(text=f"{icon} {display_title}{year_str}", callback_data=f"get_{movie['imdb_id']}")])

    # Add next refresh button if more exist and limit not reached (check against limit - 1 because we are about to incr)
    if len(final_results) > end_offset and current_count < 1:
         buttons.append([InlineKeyboardButton(text=f"🔄 Refresh Results ({1 - current_count} left)", callback_data=f"refresh_search:{end_offset}")])

    # 6. Update Rate Limit in Redis
    if redis_cache.is_ready():
        await redis_cache.incr(limit_key)
        await redis_cache.expire(limit_key, 86400)

    # 7. Edit Message
    await safe_tg_call(callback.message.edit_text(
        f"✨ **REFRESHED RESULTS**\n━━━━━━━━━━━━━━\nShowing results {offset+1}-{offset+len(page_results)} for '<b>{last_query}</b>':",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    ))
# --- END NEW CALLBACK ---


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20)
async def get_movie_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, db_fallback: Database, redis_cache: RedisCacheLayer):
    user = callback.from_user
    if not user: 
        await safe_tg_call(callback.answer("Error: User not found."))
        return
    
    # is_user_banned is an async method in database.py
    is_banned = await safe_db_call(db_primary.is_user_banned(user.id), default=False)
    if is_banned:
        await safe_tg_call(callback.answer("❌ Access Denied: You are restricted.", show_alert=True))
        return
        
    await safe_tg_call(callback.answer("📥 Retrieving Content..."))
        # --- FIX START: Force Join Check on Download Button ---
    is_member = await check_user_membership(user.id, bot)
    if not is_member:
        join_markup = get_join_keyboard()
        if join_markup:
            # Agar member nahi hai, toh wahi message edit karke join button dikhao
            join_text = (
                f"🔒 **ACCESS DENIED**\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ You must join our channels to download this file.\n\n"
                f"👇 Join below and tap **Verify Membership**."
            )
            try:
                await safe_tg_call(callback.message.edit_text(join_text, reply_markup=join_markup))
            except Exception:
                await safe_tg_call(bot.send_message(user.id, join_text, reply_markup=join_markup), semaphore=TELEGRAM_COPY_SEMAPHORE)
            return
    # --- FIX END ---
    
    if not await ensure_capacity_or_inform(callback, db_primary, bot, redis_cache):
        return

    imdb_id = callback.data.split("_", 1)[1]
    
    # --- FIXED SHORTLINK WITH 24H COOLDOWN & BILINGUAL TEXT ---
    shortlink_enabled = await db_primary.get_config("shortlink_enabled", False)
    shortlink_api = await db_primary.get_config("shortlink_api", None)
    
    # Check if user already has a 24H pass in Redis
    has_pass = False
    if redis_cache.is_ready():
        has_pass = await redis_cache.get(f"sl_pass:{user.id}")

    # Agar system ON hai aur user ke paas pass NAHI hai
    if shortlink_enabled and shortlink_api and not has_pass and user.id != ADMIN_USER_ID:
        # Create unique token
        token = await db_primary.create_unlock_token(user.id, imdb_id)
        bot_user = (await bot.get_me()).username
        unlock_url = f"https://t.me/{bot_user}?start=unlock_{token}"
        
        # Shorten link
        monetized_link = await get_shortened_link(unlock_url, db_primary)
        
        unlock_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔓 UNLOCK ALL FILES (24H)", url=monetized_link)
        ]])
        
        bilingual_locked_text = (
            "🔐 **DOWNLOAD LOCKED / डाउनलोड लॉक है**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🇺🇸 Complete **one** shortlink to unlock **unlimited downloads** for 24 hours!\n"
            "🇮🇳 **24 घंटे** के लिए **अनलिमिटेड डाउनलोड** अनलॉक करने के लिए बस एक शॉर्टलिंक पूरा करें।\n\n"
            "✅ **Benefits / फायदे:**\n"
            "• No more links for 24 hours (24 घंटे तक कोई लिंक नहीं)\n"
            "• Instant direct files (सीधी फाइलें मिलेंगी)\n\n"
            "👇 **Tap 'Unlock' to start** / नीचे 'Unlock' पर क्लिक करें"
        )
        
        await callback.message.edit_text(text=bilingual_locked_text, reply_markup=unlock_kb)
        asyncio.create_task(db_primary.track_event("shortlink_attempt"))
        return
    # --- END SHORTLINK WRAPPER ---

    # get_movie_by_imdb is an async method in database.py
    movie = await safe_db_call(db_primary.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)
    if not movie:
        logger.warning(f"Movie {imdb_id} not found in db_primary, checking db_fallback...")
        # get_movie_by_imdb is an async method in database.py
        movie = await safe_db_call(db_fallback.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        # UI Enhancement: Movie Not Found message
        await safe_tg_call(callback.message.edit_text("❌ **CONTENT UNAVAILABLE**\nThis title has been removed from the library."))
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN: Movie <code>{imdb_id}</code> missing from DBs."))
        return
        
    success = False; error_detail = "System Failure"
    # To track message ID for auto-delete
    sent_msg_id = None
    
    try:
        is_valid_for_copy = all([
            movie.get("channel_id"), movie.get("channel_id") != 0,
            movie.get("message_id"), movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER
        ])
        
        if is_valid_for_copy:
            # COPY MESSAGE returns a MessageId object (not Message)
            copy_result = await safe_tg_call(
                bot.copy_message(
                    chat_id=user.id,
                    from_chat_id=int(movie["channel_id"]),
                    message_id=movie["message_id"],
                    caption=None 
                ), 
                timeout=TG_OP_TIMEOUT * 2,
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
            if copy_result: 
                success = True
                sent_msg_id = copy_result.message_id
            elif copy_result is False: error_detail = "Bot Blocked / Chat Not Found"
            else: error_detail = "Source File Inaccessible"
        
        if not success:
            logger.info(f"Copy fail ({error_detail}), ab send_document (file_id) try...")
            if not movie.get("file_id"):
                 error_detail = "Missing File ID"
            else:
                # SEND DOCUMENT returns a Message object
                send_result = await safe_tg_call(bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=None # Caption nahi chahiye
                ), 
                timeout=TG_OP_TIMEOUT * 4,
                semaphore=TELEGRAM_COPY_SEMAPHORE
                )
                if send_result: 
                    success = True
                    sent_msg_id = send_result.message_id
                elif send_result is False: error_detail += " (Bot Blocked)"
                else: error_detail += " (ID Send Failed)"
                    
    except Exception as e:
        error_detail = f"Unknown Error: {e}"
        logger.error(f"Exception during send/copy {imdb_id}: {e}", exc_info=True)
    if success and sent_msg_id:
        # --- AUTO DELETE SCHEDULE ---
        # Warning Msg ID wo message hai jisme button tha (callback.message)
        warning_msg_id = callback.message.message_id
        
        # 2 minutes = 120 seconds
        asyncio.create_task(schedule_auto_delete(bot, user.id, sent_msg_id, warning_msg_id, delay=120))
        
        # UI Enhancement: Success message with DUAL LANGUAGE WARNING
        success_text = (
            f"🎉 **CONTENT DELIVERED**\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"✅ '<b>{movie['title']}</b>' has been sent.\n\n"
            f"⚠️ **AUTO-DELETE WARNING / जरुरी सुचना**\n"
            f"🇺🇸 This file will **self-destruct in 2 minutes** to protect the channel.\n"
            f"🇮🇳 Channel safety ke liye ye file **2 minute me delete** ho jayegi.\n\n"
            f"🔥 **FORWARD to 'Saved Messages' NOW!**\n"
            f"🔥 **Delete hone se pehle ise Forward kar lein!**"
        )
        try:
            await safe_tg_call(callback.message.edit_text(success_text))
        except Exception:
            pass 
        
        # --- FEATURE A: POST-DOWNLOAD AD DELIVERY ---
        asyncio.create_task(send_sponsor_ad(user.id, bot, db_primary, redis_cache))

    else:
        # UI Enhancement: Error message (gentle)
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = (
            f"⚠️ **DELIVERY FAILED**\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"We could not send '<b>{movie['title']}</b>'.\n"
            f"Reason: {error_detail}{admin_hint}\n\n"
            f"Please try again later."
        )
        try:
            await safe_tg_call(callback.message.edit_text(error_text))
        except Exception:
            await safe_tg_call(bot.send_message(user.id, error_text), semaphore=TELEGRAM_COPY_SEMAPHORE)

# =======================================================
# +++++ BOT HANDLERS: ADMIN COMMANDS +++++
# =======================================================

@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    if not message.forward_from_chat or message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0: await safe_tg_call(message.answer("❌ **Configuration Error**: `LIBRARY_CHANNEL_ID` not set."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        else: await safe_tg_call(message.answer(f"❌ **Invalid Source**: Forward from Library Channel (ID: `{LIBRARY_CHANNEL_ID}`) only."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    if not (message.video or message.document): return

        # --- FIX START: Filename Fallback for Migration ---
    info = extract_movie_info(message.caption or "")
    
    if not info or not info.get("title"):
        # Fallback to filename
        file_obj = message.video or message.document
        if file_obj and hasattr(file_obj, 'file_name') and file_obj.file_name:
            parsed_meta = parse_filename(file_obj.file_name)
            if parsed_meta.get("title"):
                info = parsed_meta

    if not info or not info.get("title"):
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption/Filename parse nahi kar paya.")
        await safe_tg_call(message.answer(f"❌ **Parse Error**: Caption missing/invalid for MsgID `{message.forward_from_message_id}`."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    # --- FIX END ---

    file_data = message.video or message.document

    file_data = message.video or message.document
    file_id = file_data.file_id; file_unique_id = file_data.file_unique_id
    message_id = message.forward_from_message_id
    channel_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{message_id}"
    title = info["title"]; year = info.get("year")
    
    clean_title_val = clean_text_for_search(title)
    
    # add_movie is an async method in database.py
    db1_task = safe_db_call(db_primary.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    db2_task = safe_db_call(db_fallback.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    # db_neon.add_movie is an async method in neondb.py
    neon_task = safe_db_call(db_neon.add_movie(message_id, channel_id, file_id, file_unique_id, imdb_id, title))
    
    db1_res, db2_res, neon_res = await asyncio.gather(db1_task, db2_task, neon_task)
    
    def get_status(res):
        return "✨ Added" if res is True else ("🔄 Updated" if res == "updated" else ("ℹ️ Skipped" if res == "duplicate" else "❌ FAILED"))

    db1_status = get_status(db1_res)
    db2_status = get_status(db2_res)
    neon_status = "✅ Synced" if neon_res else "❌ FAILED"
    
    if db1_res is True:
        # Fuzzy Cache ko update karein
        async with FUZZY_CACHE_LOCK:
            if clean_title_val not in fuzzy_movie_cache:
                movie_data = {
                    "imdb_id": imdb_id,
                    "title": title,
                    "year": year,
                    "clean_title": clean_title_val
                }
                fuzzy_movie_cache[clean_title_val] = movie_data
                # --- NEW: Update Redis Cache asynchronously (future-proofing) ---
                if redis_cache.is_ready():
                    # Non-blocking background task (Rule 3)
                    asyncio.create_task(redis_cache.set(f"movie_title_{clean_title_val}", json.dumps(movie_data), ttl=86400))
                # --- END NEW ---

    # UI Enhancement: Migration result format
    result_text = (
        f"📥 **MIGRATION REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎬 **Title:** <b>{title}</b>\n"
        f"🆔 **ID:** <code>{imdb_id}</code>\n\n"
        f"**Database Sync Status**\n"
        f"🔹 Primary Node: {db1_status}\n"
        f"🔹 Fallback Node: {db2_status}\n"
        f"🔹 Neon Index: {neon_status}"
    )
    
    await safe_tg_call(message.answer(result_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


# --- REPLACEMENT CODE FOR auto_index_handler ---
@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    # 1. Debug Logging
    # logger.info(f"📥 CHANNEL POST DETECTED | Chat ID: {message.chat.id} | Type: {message.content_type}")

    # 2. ID Check
    if message.chat.id != LIBRARY_CHANNEL_ID or LIBRARY_CHANNEL_ID == 0:
        return

    # 3. Media Check
    file_obj = message.video or message.document or message.audio
    if not file_obj:
        return
        
    # 4. Smart Info Extraction
    info = extract_movie_info(message.caption or "")
    
    # Filename Fallback
    if not info or not info.get("title"):
        filename = getattr(file_obj, "file_name", None)
        if not filename and message.video:
             filename = "Unknown Video"

        if filename:
            parsed_meta = parse_filename(filename)
            if parsed_meta.get("title") and parsed_meta["title"] != "Untitled":
                info = parsed_meta
                if not info.get("title"): info = None

    # 5. Final Valid Data Check
    if not info or not info.get("title"):
        logger.warning(f"❌ Auto-Index FAILED: MsgID {message.message_id} - Title extract nahi kar paya.")
        return

    # 6. Data Preparation
    file_id = file_obj.file_id
    file_unique_id = file_obj.file_unique_id
    
    # --- FIX START: DUPLICATE PREVENTION LOGIC ---
    # Agar Info me IMDb ID hai to wo use karo, nahi to File Unique ID use karo.
    # Message ID use mat karo, wo har baar change ho jata hai.
    if info.get("imdb_id"):
        imdb_id = info["imdb_id"]
    else:
        # "file_" prefix lagaya taaki conflict na ho
        imdb_id = f"file_{file_unique_id}"
    # --- FIX END ---

    title = info["title"]
    year = info.get("year")
    
    log_prefix = f"✅ Auto-Index (Title: '{title}'):"
    clean_title_val = clean_text_for_search(title)
    
    # 7. Database Operations
    db1_task = safe_db_call(db_primary.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    db2_task = safe_db_call(db_fallback.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    neon_task = safe_db_call(db_neon.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title))
    
    async def run_tasks():
        res = await db1_task
        await db2_task
        await neon_task
        
        # Result check: Agar "duplicate" ya "updated" hai to log karo
        if res == "duplicate":
            logger.info(f"{log_prefix} Skipped (Duplicate File).")
        elif res == "updated":
            logger.info(f"{log_prefix} Updated Existing Entry.")
        elif res is True: 
            # Sirf nayi movie ke liye cache update karo
            async with FUZZY_CACHE_LOCK:
                if clean_title_val not in fuzzy_movie_cache:
                    movie_data = {
                        "imdb_id": imdb_id,
                        "title": title,
                        "year": year,
                        "clean_title": clean_title_val
                    }
                    fuzzy_movie_cache[clean_title_val] = movie_data
                    if redis_cache.is_ready():
                         asyncio.create_task(redis_cache.set(f"movie_title_{clean_title_val}", json.dumps(movie_data), ttl=86400))
            logger.info(f"{log_prefix} New Movie Added & Cached.")
    
    asyncio.create_task(run_tasks())
@dp.message(Command("stats"), AdminFilter())
@handler_timeout(20)
# --- NEW: Centralized Dashboard Generator (Prevents Code Duplication) ---
async def generate_admin_dashboard(db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    """
    Generates the text and markup for the admin dashboard.
    Fetches: User counts, Movie counts, System Health, Monetization Status, Ads Stats.
    """
    # 1. Database Health & Counts
    user_count_task = safe_db_call(db_primary.get_user_count(), default=0)
    mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=0)
    mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=0)
    neon_count_task = safe_db_call(db_neon.get_movie_count(), default=0)
    concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    # 2. Connection Checks
    mongo_1_ready_task = safe_db_call(db_primary.is_ready(), default=False)
    mongo_2_ready_task = safe_db_call(db_fallback.is_ready(), default=False)
    neon_ready_task = safe_db_call(db_neon.is_ready(), default=False)
    
    # 3. Monetization & Ads Data (NEW ADDITION)
    shortlink_status_task = safe_db_call(db_primary.get_config("shortlink_enabled", False), default=False)
    shortlink_api_task = safe_db_call(db_primary.get_config("shortlink_api", "Not Set"), default="Not Set")
    # Assuming 'ads' collection exists as used in /listads
    active_ads_task = safe_db_call(db_primary.ads.count_documents({}), default=0)

    # 4. Gather All Data concurrently
    (
        user_count, m1_count, m2_count, neon_count, active_users,
        m1_ok, m2_ok, neon_ok,
        sl_enabled, sl_api, ads_count
    ) = await asyncio.gather(
        user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task,
        mongo_1_ready_task, mongo_2_ready_task, neon_ready_task,
        shortlink_status_task, shortlink_api_task, active_ads_task
    )

    redis_ok = redis_cache.is_ready()

    # 5. Icons & Formatting
    def status_icon(is_ok): return "🟢 Online" if is_ok else "🔴 Offline"
    def cache_icon(is_ok): return "🟢 Active" if is_ok else "🟠 Degraded"
    
    # Search Engine Logic Status
    search_status = "⚡ Hybrid (Smart Sequence)"
    if not m1_ok: search_status = "⚠️ Degraded (Primary DB Down)"
    if len(fuzzy_movie_cache) == 0: search_status = "⚠️ Cache Empty (Run /reload...)"

    # Shortlink Domain Extraction
    sl_domain = "N/A"
    if sl_api and "http" in sl_api:
        try:
            sl_domain = sl_api.split("/")[2]
        except:
            sl_domain = "Custom API"

    monetization_icon = "🟢 ON" if sl_enabled else "🔴 OFF"

    # 6. Final Dashboard Text
    dashboard_text = (
        f"🛡️ *COMMANDER DASHBOARD V2*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*💰 MONETIZATION & ADS*\n"
        f"• *Shortlink System:* {monetization_icon}\n"
        f"• *Provider:* {sl_domain}\n"
        f"• *Active Campaigns (Ads):* {ads_count}\n\n"

        f"*🖥️ INFRASTRUCTURE*\n"
        f"• *Primary Node (M1):* {status_icon(m1_ok)} | 📂 {m1_count:,}\n"
        f"• *Fallback Node (M2):* {status_icon(m2_ok)} | 📂 {m2_count:,}\n"
        f"• *Neon Backup:* {status_icon(neon_ok)} | 📂 {neon_count:,}\n"
        f"• *Redis Cache:* {cache_icon(redis_ok)}\n\n"
        
        f"*🚦 TRAFFIC & USAGE*\n"
        f"• *Total Users:* {user_count:,}\n"
        f"• *Active Now (5m):* {active_users:,} / {CURRENT_CONC_LIMIT}\n"
        f"• *Queue Load:* {priority_queue._queue.qsize()} tasks\n"
        f"• *Search Engine:* {search_status}\n"
        f"• *Memory Cache:* {len(fuzzy_movie_cache):,} titles\n"
        f"• *Uptime:* {get_uptime()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    
    # Refresh Button
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh Stats", callback_data="admin_stats_cmd")],
        [InlineKeyboardButton(text="🛠 Open Command Hub", callback_data="admin_panel_open")]
    ])
    
    return dashboard_text, keyboard
async def stats_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    # UI: Working state
    loading_msg = await safe_tg_call(message.answer("📊 *Analysing System Metrics...*"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not loading_msg: return

    try:
        # Generate Dashboard using the central function
        text, reply_markup = await generate_admin_dashboard(db_primary, db_fallback, db_neon, redis_cache)
        await safe_tg_call(loading_msg.edit_text(text, reply_markup=reply_markup))
    except Exception as e:
        logger.error(f"Stats generation failed: {e}")
        await safe_tg_call(loading_msg.edit_text(f"❌ *Stats Error*: {e}"))

# --- NEW: Callback Handler for Admin Stats Button ---
@dp.callback_query(F.data == "admin_stats_cmd", AdminFilter())
@handler_timeout(20)
async def admin_stats_callback(callback: types.CallbackQuery, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    # Quietly acknowledge the click
    await safe_tg_call(callback.answer("Refreshing Dashboard... 🔄"))
    
    try:
        # Reuse the SAME function (Centralized Dashboard Logic)
        text, reply_markup = await generate_admin_dashboard(db_primary, db_fallback, db_neon, redis_cache)
        
        # Only edit if content changed to avoid Telegram errors
        if callback.message.text != text:
            await safe_tg_call(callback.message.edit_text(text, reply_markup=reply_markup))
        else:
            await safe_tg_call(callback.answer("✅ Already up to date!"))
            
    except Exception as e:
        logger.error(f"Stats callback failed: {e}")
        await safe_tg_call(callback.answer("❌ Stats Error", show_alert=True))
# --- END NEW ---

# UI Enhancement: DEDICATED ADMIN PANEL COMMAND HUB
@dp.message(Command("admin_panel"), AdminFilter())
@handler_timeout(10)
async def admin_panel_command(message: types.Message):
    await show_admin_panel(message)

@dp.callback_query(F.data == "admin_panel_open", AdminFilter())
@handler_timeout(10)
async def admin_panel_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("Opening Command Hub..."))
    await show_admin_panel(callback.message, is_edit=True)

async def show_admin_panel(message: types.Message, is_edit: bool = False):
    panel_text = (
        "🛠 **ADMIN COMMAND HUB**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Select a command from the list below to copy.\n\n"
        
        "👥 **USER MANAGEMENT**\n"
        "• <code>/stats</code> - View Server Dashboard\n"
        "• <code>/get_user ID</code> - View User Profile\n"
        "• <code>/export_users</code> - Download User DB\n"
        "• <code>/ban ID</code> | <code>/unban ID</code> - Access Control\n"
        "• <code>/broadcast</code> - Reply to message to send\n"
        "• <code>/cleanup_users</code> - Remove inactive users\n\n"
        
        "📂 **DATA SYNC & IMPORT**\n"
        "• <code>/import_json</code> - Reply to JSON file\n"
        "• <code>/backup_channel ID</code> - Copy all files to channel\n"
        "• <code>/sync_mongo_1_to_2</code> - Sync M1 → M2\n"
        "• <code>/sync_mongo_1_to_neon</code> - Sync M1 → Neon\n\n"
        
        "🔧 **MAINTENANCE & REPAIR**\n"
        "• <code>/check_db</code> - Diagnostics\n"
        "• <code>/reload_fuzzy_cache</code> - Refresh Search Index\n"
        "• <code>/cleanup_titles</code> - Remove @usernames/links from titles\n"
        "• <code>/rebuild_clean_titles_m1</code> - Fix M1 Index\n"
        "• <code>/rebuild_clean_titles_m2</code> - Fix M2 Index\n"
        "• <code>/rebuild_neon_vectors</code> - Fix Neon Vectors\n"
        "• <code>/force_rebuild_m1</code> - Deep Rebuild M1 (Slow)\n"
        "• <code>/remove_dead_movie ID</code> - Delete Movie\n"
        "• <code>/remove_library_duplicates</code> - Fix Channel Dupes\n"
        "• <code>/cleanup_mongo_1</code> | <code>/cleanup_mongo_2</code> - Fix DB Dupes\n\n"
        
        "⚙️ **NEW: ADS & MONETIZATION**\n"
        "• <code>/addad</code> - Add Sponsor Ads\n"
        "• <code>/listads</code> - Manage Ads\n"
        "• <code>/setshort ON/OFF LINK</code> - Monetize Config\n"
        "• <code>/clearads</code> - Delete All Ads\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    
    if is_edit:
        await safe_tg_call(message.edit_text(panel_text))
    else:
        await safe_tg_call(message.answer(panel_text), semaphore=TELEGRAM_COPY_SEMAPHORE)

@dp.message(Command("search_switch"), AdminFilter())
@handler_timeout(10)
async def search_switch_command(message: types.Message):
    # UI Enhancement: Improved deprecation message
    dep_text = "ℹ️ **DEPRECATED**\nThe bot now runs on the permanent **Hybrid Smart-Sequence Engine**. No switch needed."
    await safe_tg_call(message.answer(dep_text), semaphore=TELEGRAM_COPY_SEMAPHORE)
    # --- NEW: Cancel Handler for FSM ---
# Ye handler zaroori hai taaki agar Admin /addad command use karke 
@dp.message(Command("clearlocks"), AdminFilter())
async def clear_locks_command(message: types.Message, db_primary: Database):
    # Sabhi locks ko force release karein
    locks = ["task_lock_backup_task", "task_lock_sync_mongo_1_to_2_command", "global_webhook_set_lock"]
    for lock in locks:
        await safe_db_call(db_primary.release_cross_process_lock(lock))
    
    # Global dictionary clear karein
    ADMIN_ACTIVE_TASKS.clear()
    
    await message.answer("✅ **System Locks Cleared!**\nAb aap naye commands chala sakte hain.")
# ==========================================
# FEATURE: ADS ADMIN HANDLERS
# ==========================================

@dp.message(Command("addad"), AdminFilter())
async def cmd_add_ad(message: types.Message, state: FSMContext):
    await message.answer("📝 **AD STEP 1**: Send the Ad Text (Markdown supported).")
    await state.set_state(AdStates.waiting_for_text)

@dp.message(AdStates.waiting_for_text)
async def ad_text_rcv(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer("🔘 **AD STEP 2**: Send Button Label (e.g., 'Check Now') or send /skip.")
    await state.set_state(AdStates.waiting_for_btn_text)

@dp.message(AdStates.waiting_for_btn_text)
async def ad_btn_label_rcv(message: types.Message, state: FSMContext, db_primary: Database):
    if message.text == "/skip":
        data = await state.get_data()
        ad_id = await db_primary.add_ad(data['text'])
        await message.answer(f"✅ **Ad Saved!** ID: `{ad_id}`")
        # SMART FIX: Clear state after saving
        await state.clear()
        return
    await state.update_data(btn_text=message.text)
    await message.answer("🔗 **AD STEP 3**: Send the Button URL.")
    await state.set_state(AdStates.waiting_for_btn_url)

@dp.message(AdStates.waiting_for_btn_url)
async def ad_url_rcv(message: types.Message, state: FSMContext, db_primary: Database):
    data = await state.get_data()
    ad_id = await db_primary.add_ad(data['text'], data['btn_text'], message.text)
    await message.answer(f"✅ **Ad Saved with Button!** ID: `{ad_id}`")
    # SMART FIX: Clear state after saving
    await state.clear()

@dp.message(Command("listads"), AdminFilter())
async def list_ads(message: types.Message, db_primary: Database):
    cursor = db_primary.ads.find()
    ads = await cursor.to_list(length=100)
    if not ads:
        return await message.answer("No ads found.")
    
    text = "📋 **BOT SPONSORS**\n\n"
    for a in ads:
        status = "🟢" if a['status'] else "🔴"
        text += f"{status} ID: `{a['ad_id']}` | Views: {a['views']}\n"
    
    await message.answer(text)

# NEW FEATURE: Clear All Ads (Admin Only)
@dp.message(Command("clearads"), AdminFilter())
async def clear_ads_cmd(message: types.Message, db_primary: Database):
    deleted_count = await db_primary.clear_all_ads()
    if deleted_count > 0:
         await message.answer(f"🗑️ **Ads Cleared!** `{deleted_count}` ads removed from database.")
    else:
         await message.answer("ℹ️ **Database Clean.** No ads found to delete.")

@dp.message(Command("setshort"), AdminFilter())
@dp.message(Command("setshort"), AdminFilter())
async def set_shortlink_cmd(message: types.Message, db_primary: Database):
    args = message.text.split(maxsplit=2)
    
    if len(args) < 2:
        usage = (
            "🛠 **SHORTLINK CONFIGURATION / शॉर्टलिंक सेटअप**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔹 **Enable:** `/setshort ON` (चालू करें)\n"
            "🔹 **Disable:** `/setshort OFF` (बंद करें)\n"
            "🔹 **Set API:** `/setshort LINK <url>` (API लिंक सेट करें)\n\n"
            "⚠️ **Note:** URL mein `{url}` hona zaroori hai."
        )
        return await message.answer(usage)

    cmd_type = args[1].upper()

    if cmd_type == "ON":
        # Database mein Boolean True save karein
        await db_primary.update_config("shortlink_enabled", True)
        await message.answer("✅ **Monetization Enabled!**\nUsers will now see shortlinks once every 24h.")

    elif cmd_type == "OFF":
        # Database mein Boolean False save karein
        await db_primary.update_config("shortlink_enabled", False)
        await message.answer("❌ **Monetization Disabled!**\nDirect downloads are now active.")

    elif cmd_type == "LINK":
        if len(args) < 3:
            return await message.answer("⚠️ **Error:** API URL missing! / यूआरएल गायब है।")
        
        new_api = args[2].strip()
        if "{url}" not in new_api:
            return await message.answer("❌ **Invalid API:** `{url}` placeholder is missing!")
        
        await db_primary.update_config("shortlink_api", new_api)
        await message.answer(f"🚀 **Shortlink API Updated!**\nURL: `{new_api}`")
    else:
        await message.answer("❌ **Invalid Option!** Use `ON`, `OFF`, or `LINK`.")

# ==========================================
# PROBLEM FIX: SYNC COMMAND WRAPPERS
# ==========================================

@dp.message(Command("sync_mongo_1_to_2"), AdminFilter())
async def sync_m12_freeze_fix(message: types.Message, db_primary: Database, db_fallback: Database):
    await run_in_background(sync_mongo_1_to_2_command, message, db_primary=db_primary, db_fallback=db_fallback)

@dp.message(Command("force_rebuild_m1"), AdminFilter())
async def force_rebuild_freeze_fix(message: types.Message, db_primary: Database):
    await run_in_background(force_rebuild_m1_command, message, db_primary=db_primary)

@dp.message(Command("sync_mongo_1_to_neon"), AdminFilter())
async def sync_neon_freeze_fix(message: types.Message, db_primary: Database, db_neon: NeonDB):
    await run_in_background(sync_mongo_1_to_neon_command, message, db_primary=db_primary, db_neon=db_neon)

@dp.message(Command("remove_library_duplicates"), AdminFilter())
async def rem_dupes_freeze_fix(message: types.Message, db_primary: Database, db_neon: NeonDB):
    await run_in_background(remove_library_duplicates_command, message, db_primary=db_primary, db_neon=db_neon)

# =======================================================
# +++++ ORIGINAL BOT HANDLERS PRESERVED +++++
# =======================================================

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600)
async def broadcast_command(message: types.Message, db_primary: Database):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("⚠️ **Broadcast Error**: Reply to a message to broadcast."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        
    # --- FIX START: Error yahan tha ---
    # 'get_all_users_cursor' aapke DB me nahi hai, isliye direct 'get_all_users' use karenge
    # Ye function list return karega
    users = await safe_db_call(db_primary.get_all_users(), default=[]) 
    
    if not users:
        await safe_tg_call(message.answer("⚠️ **Broadcast Error**: No users found."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        
    total = len(users)
    msg = await safe_tg_call(message.answer(f"📢 **Initializing Broadcast**\nTarget: {total:,} users..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    start_broadcast_time = datetime.now(timezone.utc)
    success_count, failed_count = 0, 0
    tasks = []
    
    async def send_to_user(user_id: int):
        nonlocal success_count, failed_count
        res = await safe_tg_call(message.reply_to_message.copy_to(user_id), timeout=10, semaphore=TELEGRAM_BROADCAST_SEMAPHORE)
        if res: success_count += 1
        elif res is False:
            failed_count += 1
            # deactivate_user is an async method in database.py
            await safe_db_call(db_primary.deactivate_user(user_id))
        else: failed_count += 1

    last_update_time = start_broadcast_time
    
    # --- FIX Loop Logic: Handle both Dictionary and Integer ---
    for i, user_data in enumerate(users):
        # Agar user_data dict hai (e.g. {'user_id': 123}), to ID nikalo
        # Agar seedha int hai (e.g. 123), to waisa hi use karo
        target_id = user_data if isinstance(user_data, int) else user_data.get('user_id')
        
        if not target_id: continue

        tasks.append(send_to_user(target_id))
        processed_count = i + 1
        now = datetime.now(timezone.utc)
        
        # Batch processing (Har 100 users ya 15 seconds me execute karo)
        if processed_count % 100 == 0 or (now - last_update_time).total_seconds() > 15 or processed_count == total:
            await asyncio.gather(*tasks)
            tasks = []
            elapsed = (now - start_broadcast_time).total_seconds()
            speed = processed_count / elapsed if elapsed > 0 else 0
            try:
                # UI Enhancement: Broadcast progress update
                await safe_tg_call(msg.edit_text(
                    f"📢 **BROADCASTING**\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"**Progress:** {processed_count:,} / {total:,}\n"
                    f"**Success:** ✅ {success_count:,}\n"
                    f"**Failed:** ❌ {failed_count:,}\n"
                    f"**Speed:** {speed:.1f} users/sec"
                ))
            except TelegramBadRequest: pass
            last_update_time = now
            
    final_text = (f"✅ **BROADCAST FINISHED**\n"
                  f"━━━━━━━━━━━━━━━━━━\n"
                  f"**Delivered:** {success_count:,}\n"
                  f"**Failed/Blocked:** {failed_count:,}\n"
                  f"**Total Reach:** {total:,}")
    await safe_tg_call(msg.edit_text(final_text))


@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120)
async def cleanup_users_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🧹 **User Cleanup**: Removing inactive users (>30 days)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # cleanup_inactive_users is an async method in database.py
    removed = await safe_db_call(db_primary.cleanup_inactive_users(days=30), timeout=90, default=0)
    # get_user_count is an async method in database.py
    new_count = await safe_db_call(db_primary.get_user_count(), default=0)
    txt = f"✅ **Cleanup Complete**\n\n🗑️ **Removed:** {removed:,} users.\n👥 **Current Active:** {new_count:,} users."
    await safe_tg_call(msg.edit_text(txt))


@dp.message(Command("get_user"), AdminFilter())
@handler_timeout(10)
async def get_user_command(message: types.Message, db_primary: Database):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer("⚠️ **Usage**: /get_user `USER_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    user_id_to_find = int(args[1])
    # get_user_info is an async method in database.py
    user_data = await safe_db_call(db_primary.get_user_info(user_id_to_find))
    if not user_data:
        await safe_tg_call(message.answer(f"❌ User <code>{user_id_to_find}</code> not found."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    def format_dt(dt): return dt.strftime('%Y-%m-%d %H:%M:%S UTC') if dt else 'N/A'
    user_text = (
        f"👤 **USER PROFILE**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 **ID:** <code>{user_data.get('user_id')}</code>\n"
        f"🏷️ **Username:** @{user_data.get('username') or 'N/A'}\n"
        f"👤 **Name:** {user_data.get('first_name') or 'N/A'} {user_data.get('last_name') or ''}\n"
        f"🔋 **Status:** {'✅ Active' if user_data.get('is_active', True) else '❌ Inactive'}\n"
        f"🚫 **Banned:** {'YES' if user_data.get('is_banned', False) else 'No'}\n"
        f"📅 **Joined:** {format_dt(user_data.get('joined_date'))}\n"
        f"🕒 **Last Seen:** {format_dt(user_data.get('last_active'))}"
    )
    await safe_tg_call(message.answer(user_text), semaphore=TELEGRAM_COPY_SEMAPHORE)

# --- NAYA FEATURE 1: Export Users Command (Unchanged logic, updated message) ---
@dp.message(Command("export_users"), AdminFilter())
@handler_timeout(60)
async def export_users_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("📦 **Exporting Data**: Fetching user database..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # get_all_user_details is an async method in database.py
    user_data_list = await safe_db_call(db_primary.get_all_user_details(), timeout=50, default=[])
    
    if not user_data_list:
        await safe_tg_call(msg.edit_text("❌ **Export Failed**: No data found.")); return
        
    loop = asyncio.get_running_loop()
    try:
        # JSON dump is CPU bound, run in executor
        json_bytes = await loop.run_in_executor(executor, lambda: json.dumps(user_data_list, indent=2).encode('utf-8'))
    except Exception as e:
        logger.exception("JSON serialization error for user export")
        await safe_tg_call(msg.edit_text(f"❌ **Export Error**: {e}")); return
        
    file_name = f"users_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        # UI Enhancement: Export message
        await safe_tg_call(
            message.answer_document(
                BufferedInputFile(json_bytes, filename=file_name),
                caption=f"✅ **Export Ready**: **{len(user_data_list):,}** active user records."
            ),
            semaphore=TELEGRAM_COPY_SEMAPHORE
        )
        await safe_tg_call(msg.delete())
    except Exception as e:
        logger.error(f"Failed to send exported file: {e}", exc_info=False)
        await safe_tg_call(msg.edit_text(f"❌ **Delivery Error**: {e}"))
# --- END NAYA FEATURE 1 ---

# --- NAYA FEATURE 2: Ban/Unban Commands (Unchanged logic, updated message) ---
async def _get_target_user_id(message: types.Message) -> int | None:
    args = message.text.split(maxsplit=1)
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        if target_id != message.from_user.id:
            return target_id
    elif len(args) > 1 and args[1].isdigit():
        return int(args[1])
    return None

@dp.message(Command("ban"), AdminFilter())
@handler_timeout(10)
async def ban_user_command(message: types.Message, db_primary: Database):
    target_id = await _get_target_user_id(message)
    if target_id is None:
        await safe_tg_call(message.answer("⚠️ **Usage**: /ban `USER_ID` or reply to user."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    
    if target_id == ADMIN_USER_ID:
        await safe_tg_call(message.answer("🛡️ **Error**: Cannot ban Admin."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        
    text_parts = message.text.split(maxsplit=2)
    reason = None
    if len(text_parts) > 2:
        reason = text_parts[2]
    elif len(text_parts) == 2 and not text_parts[1].isdigit():
        reason = text_parts[1]
    
    if not reason:
         reason = "Admin decision."

    # ban_user is an async method in database.py
    banned = await safe_db_call(db_primary.ban_user(target_id, reason))
    
    if banned:
        await safe_tg_call(message.answer(f"🚫 **BANNED**: User <code>{target_id}</code>.\nReason: {reason}"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        await safe_tg_call(message.answer(f"❌ **Error**: Could not ban <code>{target_id}</code>."), semaphore=TELEGRAM_COPY_SEMAPHORE)

@dp.message(Command("unban"), AdminFilter())
@handler_timeout(10)
async def unban_user_command(message: types.Message, db_primary: Database):
    target_id = await _get_target_user_id(message)
    if target_id is None:
        await safe_tg_call(message.answer("⚠️ **Usage**: /unban `USER_ID` or reply to user."), semaphore=TELEGRAM_COPY_SEMAPHORE); return

    # unban_user is an async method in database.py
    unbanned = await safe_db_call(db_primary.unban_user(target_id))
    
    if unbanned:
        await safe_tg_call(message.answer(f"✅ **UNBANNED**: Access restored for <code>{target_id}</code>."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        await safe_tg_call(message.answer(f"❌ **Error**: User <code>{target_id}</code> not found in ban list."), semaphore=TELEGRAM_COPY_SEMAPHORE)
# --- END NAYA FEATURE 2 ---


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800)
async def import_json_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    if not message.reply_to_message or not message.reply_to_message.document:
        await safe_tg_call(message.answer("⚠️ **Import Error**: Reply to a `.json` file."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    doc = message.reply_to_message.document
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"):
        await safe_tg_call(message.answer("⚠️ **Format Error**: Only `.json` files supported."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        
    msg = await safe_tg_call(message.answer(f"📥 **Downloading**: `{doc.file_name}`..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    try:
        file = await bot.get_file(doc.file_id);
        if file.file_path is None: await safe_tg_call(msg.edit_text(f"❌ **Error**: Path missing.")); return
        
        # --- FIX START: Download & Size Check ---
        fio = io.BytesIO()
        await bot.download_file(file.file_path, fio)

        # File size check karein
        fio.seek(0, os.SEEK_END)
        file_size = fio.tell()
        fio.seek(0)
        
        # 30MB Limit check
        if file_size > 30 * 1024 * 1024:
            await safe_tg_call(msg.edit_text("❌ **File Too Large**: Max limit is 30MB for JSON imports."))
            return
        # --- FIX END ---

        # JSON parsing is CPU bound, run in executor
        mlist = await loop.run_in_executor(executor, lambda: json.loads(fio.read().decode('utf-8')))

        assert isinstance(mlist, list)
    except Exception as e:
        await safe_tg_call(msg.edit_text(f"❌ **Parse Error**: {e}")); logger.exception("JSON download/parse error"); return
    
    total = len(mlist); s, f = 0, 0
    await safe_tg_call(msg.edit_text(f"⏳ **Processing**: Importing **{total:,}** items..."))
    start_import_time = datetime.now(timezone.utc)
    
    db1_tasks, db2_tasks, neon_tasks = [], [], []
    
    for i, item in enumerate(mlist):
        processed_count = i + 1
        try:
            fid = item.get("file_id"); fname = item.get("title")
            if not fid or not fname: s += 1; continue
            
            fid_str = str(fid); file_unique_id = item.get("file_unique_id") or fid_str 
            # FIX: Use UUID to prevent ID collisions (Bug #7)
            imdb = f"json_{uuid.uuid5(uuid.NAMESPACE_DNS, fid_str).hex[:12]}"
            message_id = item.get("message_id") or AUTO_MESSAGE_ID_PLACEHOLDER

            channel_id = item.get("channel_id") or 0
            
            info = parse_filename(fname); 
            title = info["title"] or "Untitled"; 
            year = info["year"]
            
            clean_title_val = clean_text_for_search(title)

            # add_movie is an async method in database.py
            db1_tasks.append(safe_db_call(db_primary.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id)))
            db2_tasks.append(safe_db_call(db_fallback.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id)))
            # db_neon.add_movie is an async method in neondb.py
            neon_tasks.append(safe_db_call(db_neon.add_movie(message_id, channel_id, fid_str, file_unique_id, imdb, title)))
            
        except Exception as e: f += 1; logger.error(f"Error processing JSON item {i+1}: {e}", exc_info=False)
        
        now = datetime.now(timezone.utc)
        if processed_count % 100 == 0 or (now - start_import_time).total_seconds() > 10 or processed_count == total:
            # Await all gathered tasks for progress update
            await asyncio.gather(
                *db1_tasks,
                *db2_tasks,
                *neon_tasks
            )
            db1_tasks, db2_tasks, neon_tasks = [], [], []
            try: 
                # UI Enhancement: Import progress update
                await safe_tg_call(msg.edit_text(f"📥 **Importing...**\nProgress: {processed_count:,}/{total:,}\nSkipped: {s:,} | Failed: {f:,}"))
            except TelegramBadRequest: pass
            last_update_time = now
            
    # UI Enhancement: Final import status
    await safe_tg_call(msg.edit_text(f"✅ **IMPORT SUCCESSFUL**\n\n**Processed:** {total-s-f:,}\n**Skipped:** {s:,}\n**Failed:** {f:,}"))
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("🧠 **Search Index Updated**"))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    args = message.text.split(maxsplit=1)
    if len(args) < 2: await safe_tg_call(message.answer("⚠️ **Usage**: /remove_dead_movie `IMDB_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    imdb_id = args[1].strip()
    msg = await safe_tg_call(message.answer(f"🗑️ **Deleting**: <code>{imdb_id}</code>..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # remove_movie_by_imdb is an async method in database.py/neondb.py
    db1_task = safe_db_call(db_primary.remove_movie_by_imdb(imdb_id))
    db2_task = safe_db_call(db_fallback.remove_movie_by_imdb(imdb_id))
    neon_task = safe_db_call(db_neon.remove_movie_by_imdb(imdb_id))
    
    db1_del, db2_del, neon_del = await asyncio.gather(db1_task, db2_task, neon_task)
    
    if db1_del:
        async with FUZZY_CACHE_LOCK:
            global fuzzy_movie_cache
            key_to_delete = None
            for key, movie_dict in fuzzy_movie_cache.items():
                if movie_dict['imdb_id'] == imdb_id:
                    key_to_delete = key
                    break
            if key_to_delete and key_to_delete in fuzzy_movie_cache:
                del fuzzy_movie_cache[key_to_delete]
    
    db1_stat = "✅ M1" if db1_del else "❌ M1"
    db2_stat = "✅ M2" if db2_del else "❌ M2"
    neon_stat = "✅ Neon" if neon_del else "❌ Neon"
    
    await safe_tg_call(msg.edit_text(f"🗑️ **Deletion Report** (<code>{imdb_id}</code>):\n\n{db1_stat} | {db2_stat} | {neon_stat}\n\nSearch index updated."))


@dp.message(Command("cleanup_mongo_1"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🧹 **M1 Cleanup**: Finding duplicates..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # cleanup_mongo_duplicates is an async method in database.py
    deleted_count, duplicates_found = await safe_db_call(db_primary.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ **M1 Cleaned**\nDeleted: {deleted_count}\nRemaining: {max(0, duplicates_found - deleted_count)}"))
        await load_fuzzy_cache(db_primary)
    else:
        await safe_tg_call(msg.edit_text("✅ **M1 Clean**: No duplicates found."))

@dp.message(Command("cleanup_mongo_2"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_2_command(message: types.Message, db_fallback: Database):
    msg = await safe_tg_call(message.answer("🧹 **M2 Cleanup**: Finding duplicates..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # cleanup_mongo_duplicates is an async method in database.py
    deleted_count, duplicates_found = await safe_db_call(db_fallback.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ **M2 Cleaned**\nDeleted: {deleted_count}\nRemaining: {max(0, duplicates_found - deleted_count)}"))
    else:
        await safe_tg_call(msg.edit_text("✅ **M2 Clean**: No duplicates found."))


@dp.message(Command("remove_library_duplicates"), AdminFilter())
@handler_timeout(3600)
async def remove_library_duplicates_command(message: types.Message, status_msg: types.Message, db_primary: Database, db_neon: NeonDB):
    # This function is now correctly wrapped and called as a background task.
    await safe_tg_call(status_msg.edit_text("🧹 **Library Cleanup**: Scanning NeonDB for duplicates..."))
    
    # find_and_delete_duplicates is an async method in neondb.py
    messages_to_delete, total_duplicates = await safe_db_call(db_neon.find_and_delete_duplicates(batch_limit=100), default=([], 0))
    
    if not messages_to_delete:
        await safe_tg_call(status_msg.edit_text("✅ **Library Clean**: No duplicates found."))
        return
        
    await safe_tg_call(status_msg.edit_text(f"⚠️ **Duplicates Found**: {total_duplicates}\n🗑️ Deleting **{len(messages_to_delete)}** messages..."))
    
    deleted_count, failed_count = 0, 0
    tasks = []
    
    async def delete_message(msg_id: int, chat_id: int):
        nonlocal deleted_count, failed_count
        res = await safe_tg_call(bot.delete_message(chat_id=chat_id, message_id=msg_id), semaphore=TELEGRAM_DELETE_SEMAPHORE)
        if res or res is None: deleted_count += 1
        else: failed_count += 1

    for msg_id, chat_id in messages_to_delete:
        tasks.append(delete_message(msg_id, chat_id))
        
    await asyncio.gather(*tasks)
    
    await safe_tg_call(status_msg.edit_text(
        f"✅ **Cleanup Report**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🗑️ Deleted: {deleted_count}\n"
        f"❌ Failed: {failed_count}\n"
        f"⚠️ Remaining: {max(0, total_duplicates - deleted_count)}\n\n"
        f"ℹ️ Run again to continue cleaning."
    ))
@dp.message(Command("sync_mongo_1_to_neon"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_neon_command(message: types.Message, status_msg: types.Message, db_primary: Database, db_neon: NeonDB):
    # This is called via run_in_background
    await safe_tg_call(status_msg.edit_text("🔄 **Syncing M1 → Neon**..."))
    
    # get_all_movies_for_neon_sync is an async method in database.py
    mongo_movies = await safe_db_call(db_primary.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies:
        await safe_tg_call(status_msg.edit_text("❌ **Sync Failed**: No data in M1.")); return
    
    await safe_tg_call(status_msg.edit_text(f"✅ **Data Ready**: {len(mongo_movies):,} movies.\n🔄 Uploading to Neon..."))
    # sync_from_mongo is an async method in neondb.py
    processed_count = await safe_db_call(db_neon.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    await safe_tg_call(status_msg.edit_text(f"✅ **Sync Complete**\nProcessed: {processed_count:,} records."))

@dp.message(Command("sync_mongo_1_to_2"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_2_command(message: types.Message, status_msg: types.Message, db_primary: Database, db_fallback: Database):
    # This is called via run_in_background
    await safe_tg_call(status_msg.edit_text("🔄 **Syncing M1 → M2**..."))
        
    await safe_tg_call(status_msg.edit_text(f"⏳ **Fetching M1 Data**..."))
    # get_all_movies_for_neon_sync is an async method in database.py
    mongo_movies_full = await safe_db_call(db_primary.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies_full:
        await safe_tg_call(status_msg.edit_text("❌ **Sync Failed**: No data in M1.")); return
        
    total_movies = len(mongo_movies_full) # F.I.X: Total count set
    await safe_tg_call(status_msg.edit_text(f"✅ **Found**: {total_movies:,} movies.\n🔄 Syncing to M2..."))
    
    processed_count = 0
    all_sync_tasks = [] 
    BATCH_SIZE = 200 # Progress update ka interval
    
    for movie in mongo_movies_full:
        processed_count += 1
        
        # F.I.X: task ko safe_db_call से बनाएं
        task = safe_db_call(db_fallback.add_movie(
            imdb_id=movie.get('imdb_id'),
            title=movie.get('title'),
            year=None, 
            file_id=movie.get('file_id'),
            message_id=movie.get('message_id'),
            channel_id=movie.get('channel_id'),
            # clean_text_for_search is a sync function, so it runs fine before the db call
            clean_title=clean_text_for_search(movie.get('title')),
            file_unique_id=movie.get('file_unique_id') or movie.get('file_id')
        ))
        all_sync_tasks.append(task)
        
        # F.I.X: Progress अपडेट ko niyantrit tareeke se bhejein
        if processed_count % BATCH_SIZE == 0:
            # Await current batch to avoid memory pressure and event loop blocking (BUG-3)
            await asyncio.gather(*all_sync_tasks)
            all_sync_tasks = []
            try:
                 await safe_tg_call(status_msg.edit_text(f"🔄 **Syncing M1 → M2...**\nProgress: {processed_count:,} / {total_movies:,}"))
            except TelegramBadRequest: pass
            
    # Final batch
    if all_sync_tasks:
        await asyncio.gather(*all_sync_tasks)

    final_text = f"✅ **Sync Complete**\nProcessed: {processed_count:,} records."
    await safe_tg_call(status_msg.edit_text(final_text))


@dp.message(Command("rebuild_clean_titles_m1"), AdminFilter())
@handler_timeout(300)
async def rebuild_clean_titles_m1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🛠 **Rebuilding M1 Index**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # rebuild_clean_titles is an async method in database.py
    updated, total = await safe_db_call(db_primary.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    # create_mongo_text_index is an async method in database.py
    await safe_db_call(db_primary.create_mongo_text_index())
    await safe_tg_call(msg.edit_text(f"✅ **Rebuild Done**\nFixed: {updated:,} / {total:,}"))
    
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("🧠 **Cache Reloaded**"))

@dp.message(Command("force_rebuild_m1"), AdminFilter())
@handler_timeout(1800) # Timeout ko 30 minute tak badhaya gaya
async def force_rebuild_m1_command(message: types.Message, status_msg: types.Message, db_primary: Database):
    # This is called via run_in_background
    await safe_tg_call(status_msg.edit_text("⚠️ **FORCE REBUILDING M1**\nStarting database scan..."))
    
    # --- FIX: Progress Callback Function with Flood/Edit Protection ---
    async def progress_callback(processed_count: int, total_count: int):
        # Flood limit se bachne ke liye chhota pause
        await asyncio.sleep(0.5) 
        
        try:
            await safe_tg_call(
                bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=status_msg.message_id,
                    text=f"⚠️ **FORCE REBUILDING M1**\n"
                         f"━━━━━━━━━━━━━━━━━━━━\n"
                         f"⏳ **Progress**: {processed_count:,} / {total_count:,} records processed.\n"
                         f"Please wait. This may take several minutes."
                ),
                semaphore=TELEGRAM_COPY_SEMAPHORE 
            )
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                 logger.error(f"Error editing progress message: {e}")
        except Exception as e:
            logger.warning(f"Unknown error updating rebuild progress: {e}")
            
    # STEP 2: Run the Heavy DB Task
    updated, total = await safe_db_call(
        db_primary.force_rebuild_all_clean_titles(
            clean_text_for_search, 
            progress_callback=progress_callback
        ), 
        timeout=1740, 
        default=(0,0)
    )
    
    # STEP 3: Final Index Rebuild and Message
    await safe_db_call(db_primary.create_mongo_text_index()) 
    
    final_text = (
        f"✅ **Force Rebuild Complete**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Overwritten: {updated:,} / {total:,} records.\n"
        f"🧠 **Search Index Updated**"
    )
    
    await safe_tg_call(status_msg.edit_text(final_text))
    await load_fuzzy_cache(db_primary)


@dp.message(Command("rebuild_clean_titles_m2"), AdminFilter())
@handler_timeout(300)
async def rebuild_clean_titles_m2_command(message: types.Message, db_fallback: Database):
    msg = await safe_tg_call(message.answer("🛠 **Rebuilding M2 Index**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # rebuild_clean_titles is an async method in database.py
    updated, total = await safe_db_call(db_fallback.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    # create_mongo_text_index is an async method in database.py
    await safe_db_call(db_fallback.create_mongo_text_index()) 
    await safe_tg_call(msg.edit_text(f"✅ **Rebuild Done**\nFixed: {updated:,} / {total:,}"))


@dp.message(Command("cleanup_titles"), AdminFilter())
@handler_timeout(60)
async def cleanup_titles_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("🧹 **Title Cleanup**: Removing unwanted links and usernames (M1 & M2)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    # Cleanup in M1
    updated_m1, total_m1 = await safe_db_call(db_primary.cleanup_movie_titles(), timeout=240, default=(0,0))
    
    # Cleanup in M2
    updated_m2, total_m2 = await safe_db_call(db_fallback.cleanup_movie_titles(), timeout=240, default=(0,0))

    if updated_m1 > 0 or updated_m2 > 0:
        await load_fuzzy_cache(db_primary) # M1 se Cache reload karein
        
        await safe_tg_call(msg.edit_text(
            f"✅ **Title Cleanup Complete**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"**Primary (M1)**: {updated_m1:,} titles cleaned/rebuilt (Total: {total_m1:,})\n"
            f"**Fallback (M2)**: {updated_m2:,} titles cleaned/rebuilt (Total: {total_m2:,})\n"
            f"🧠 **Search Index Updated**"
        ))
    else:
        await safe_tg_call(msg.edit_text("✅ **Title Cleanup**: No links/usernames found that needed cleaning in M1/M2."))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT; args = message.text.split()
    if len(args)<2 or not args[1].isdigit(): 
        await safe_tg_call(message.answer(f"⚠️ **Usage**: /set_limit N (Current: {CURRENT_CONC_LIMIT})"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    try:
        val = int(args[1]); assert 5 <= val <= 5000 
        CURRENT_CONC_LIMIT = val
        await safe_tg_call(message.answer(f"✅ **Limit Updated**: {CURRENT_CONC_LIMIT} concurrent users."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        logger.info(f"Concurrency limit admin ne {CURRENT_CONC_LIMIT} kar diya hai।")
    except (ValueError, AssertionError): 
        await safe_tg_call(message.answer("❌ **Error**: Must be between 5-5000."), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("rebuild_neon_vectors"), AdminFilter())
@handler_timeout(600)
async def rebuild_neon_vectors_command(message: types.Message, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("🛠 **Rebuilding Neon Vectors**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # rebuild_fts_vectors is an async method in neondb.py
    updated_count = await safe_db_call(db_neon.rebuild_fts_vectors(), timeout=540, default=-1)
    if updated_count >= 0:
        await safe_tg_call(msg.edit_text(f"✅ **Rebuild Done**\nUpdated: {updated_count:,} records."))
    else:
        await safe_tg_call(msg.edit_text("❌ **Failed**: Error during rebuild."))


@dp.message(Command("reload_fuzzy_cache"), AdminFilter())
@handler_timeout(300)
async def reload_fuzzy_cache_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🧠 **Reloading Cache**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer(f"✅ **Reloaded**\nSize: {len(fuzzy_movie_cache):,} titles."))


@dp.message(Command("check_db"), AdminFilter())
@handler_timeout(15)
async def check_db_command(message: types.Message, db_primary: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    msg = await safe_tg_call(message.answer("🕵️‍♂️ **Running Diagnostics**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    # check_mongo_clean_title is an async method in database.py
    mongo_check_task = safe_db_call(db_primary.check_mongo_clean_title(), default={"title": "Error", "clean_title": "Mongo check failed"})
    # check_neon_clean_title is an async method in neondb.py
    neon_check_task = safe_db_call(db_neon.check_neon_clean_title(), default={"title": "Error", "clean_title": "Neon check failed"})
    
    fuzzy_cache_check = {"title": "N/A", "clean_title": "--- EMPTY (Run /reload_fuzzy_cache) ---"}
    if fuzzy_movie_cache:
        try:
            first_key = next(iter(fuzzy_movie_cache))
            sample = fuzzy_movie_cache[first_key]
            fuzzy_cache_check = {"title": sample.get('title'), "clean_title": sample.get('clean_title')}
        except StopIteration:
            pass
        except Exception as e:
            fuzzy_cache_check = {"title": "Cache Error", "clean_title": str(e)}

    redis_status = "🔴 Offline"
    if redis_cache.is_ready():
        redis_status = "🟢 Online"

    mongo_res, neon_res = await asyncio.gather(mongo_check_task, neon_check_task)

    if mongo_res is None: mongo_res = {"title": "Error", "clean_title": "DB not ready"}
    if neon_res is None: neon_res = {"title": "Error", "clean_title": "DB not ready"}

    # UI Enhancement: Diagnostics Report
    reply_text = (
        f"🔬 **DIAGNOSTICS REPORT**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"**M1 (Primary)**\n"
        f"• Title: <code>{mongo_res.get('title')}</code>\n"
        f"• Index: <code>{mongo_res.get('clean_title')}</code>\n\n"
        f"**Redis Cache**\n"
        f"• Status: {redis_status}\n"
        f"• Size: {len(fuzzy_movie_cache):,} titles\n\n"
        f"**Neon (Backup)**\n"
        f"• Title: <code>{neon_res.get('title')}</code>\n"
        f"• Index: <code>{neon_res.get('clean_title')}</code>"
    )
    await safe_tg_call(msg.edit_text(reply_text))


# ============ ERROR HANDLER ============

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    if isinstance(exception, asyncio.TimeoutError):
        logger.warning(f"Error handler ne pakda asyncio.TimeoutError: {exception}")
        return
        
    logger.exception(f"--- UNHANDLED ERROR ---: {exception}", exc_info=True)
    
    target_chat_id = None; callback_query = None
    if update.message: target_chat_id = update.message.chat.id
    elif update.callback_query:
        callback_query = update.callback_query
        if callback_query.message: target_chat_id = callback_query.message.chat.id
            
    # UI Enhancement: Friendly, standardized error message
    error_message = "⚠️ **System Error**\nAn unexpected issue occurred. We are working on it. Please try again shortly. 🛡️"
    if target_chat_id:
        try: 
            await bot.send_message(target_chat_id, error_message)
        except Exception as notify_err: 
            logger.error(f"User ko error notify karne mein bhi error: {notify_err}")
    if callback_query:
        try: 
            await callback_query.answer("⚠️ System Error: Check chat.", show_alert=True)
        except Exception as cb_err: 
            logger.error(f"Error callback answer karne mein error: {cb_err}")

# ============ LOCAL POLLING (Testing ke liye) ============
async def main_polling():
    logger.info("Bot polling mode mein start ho raha hai (local testing)...")
    try:
        # Redis init
        await redis_cache.init_cache()
        
        db_primary_success = await safe_db_call(db_primary.init_db(), default=False)
        db_fallback_success = await safe_db_call(db_fallback.init_db(), default=False)
        
        if not db_primary_success:
            raise RuntimeError("Database 1 connection failed on startup.")
            
        await db_neon.init_db()
        await load_fuzzy_cache(db_primary) 
    except Exception as init_err:
        logger.critical(f"Local main() mein DB init fail: {init_err}", exc_info=True); return

    await bot.delete_webhook(drop_pending_updates=True)
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    setup_signal_handlers()

    # --- NEW: Start Priority Queue Workers for Polling ---
    db_objects_for_queue = {
        'db_primary': db_primary,
        'db_fallback': db_fallback,
        'db_neon': db_neon,
        'redis_cache': redis_cache,
        'admin_id': ADMIN_USER_ID
    }
    priority_queue.start_workers(bot, dp, db_objects_for_queue)
    # --- END NEW ---

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            db_primary=db_primary,
            db_fallback=db_fallback,
            db_neon=db_neon,
            redis_cache=redis_cache 
        )
    finally:
        await shutdown_procedure()

if __name__ == "__main__":
    logger.warning("Bot ko seedha __main__ se run kiya ja raha hai. Deployment ke liye Uvicorn/FastAPI ka istemal karein।")
    if not WEBHOOK_URL:
        try: 
            executor_for_main = concurrent.futures.ThreadPoolExecutor(max_workers=10)
            loop_for_main = asyncio.get_event_loop()
            loop_for_main.set_default_executor(executor_for_main)
            
            asyncio.run(main_polling())
            
            executor_for_main.shutdown(wait=True, cancel_futures=False)
            
        except (KeyboardInterrupt, SystemExit): 
            logger.info("Bot polling band kar raha hai।")
    else:
        logger.error("WEBHOOK_URL set hai. Local polling nahi chalega।")
        logger.error("Run karne ke liye: uvicorn bot:app --host 0.0.0.0 --port 8000")
