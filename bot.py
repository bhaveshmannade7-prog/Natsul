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
import uuid # Naya: Unique IDs ke liye
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import List, Dict, Callable, Any
from functools import wraps
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
async def run_in_background(task_func, message: types.Message, *args, **kwargs):
    """
    Prevents Bot Freeze by running heavy sync logic as a background task.
    MAIN SOLUTION FOR PROBLEM:Worker Freeze.
    """
    try:
        msg = await message.answer("⚙️ **Background Task Started.**\nBot responsive rahega. Task progress monitor karein.")
        # Logic to run heavy task without blocking current worker
        asyncio.create_task(task_func(message, msg, *args, **kwargs))
    except Exception as e:
        logger.error(f"Background launch error: {e}")

# ============ NEW: SHORTLINK REDIRECT LOGIC ============
async def get_shortened_link(long_url, db: Database):
    """Generates monetized link from Admin Settings."""
    api_url = await db.get_config("shortlink_api", "https://shareus.io/api?api=KEY&url={url}")
    # Return formatted shortlink
    return api_url.format(url=long_url)

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
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    # Sirf a-z, 0-9, aur space rakhein
    text = re.sub(r"[^a-z0-9\s]+", "", text) 
    # Extra spaces hatayein
    text = re.sub(r"\s+", " ", text).strip() 
    return text

def clean_text_for_fuzzy(text: str) -> str:
    """Fuzzy ke liye spaces rakhna better hai"""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    return text.strip()

def extract_movie_info(caption: str | None) -> Dict[str, str] | None:
    if not caption: return None
    info = {}; lines = caption.splitlines(); title = lines[0].strip() if lines else ""
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]): 
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

# --- NEW: AUTO DELETE HELPER ---
async def schedule_auto_delete(bot: Bot, chat_id: int, message_id: int, delay: int = 420):
    """Schedules message deletion after delay seconds (default 7 mins)."""
    await asyncio.sleep(delay)
    try:
        await safe_tg_call(
            bot.delete_message(chat_id=chat_id, message_id=message_id),
            semaphore=TELEGRAM_DELETE_SEMAPHORE
        )
    except Exception as e:
        logger.warning(f"Auto-delete failed for {chat_id}/{message_id}: {e}")
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
                         if orig_clean not in temp_cache:
                             temp_cache[orig_clean] = movie_dict
                fuzzy_movie_cache = temp_cache
                logger.info(f"✅ In-Memory Fuzzy Cache {len(fuzzy_movie_cache):,} unique titles ke saath loaded.")
            else:
                logger.error("Fuzzy cache load nahi ho paya (Redis/Mongo se koi data nahi mila).")
                fuzzy_movie_cache = {}
        except Exception as e:
            logger.error(f"Fuzzy cache load karte waqt error: {e}", exc_info=True)
            fuzzy_movie_cache = {}

#
# ==================================================
# +++++ NEW: SMART INTENT ENGINE V6 (Fixed and Improved) ++++++++++++++++
# ==================================================
#
def get_smart_match_score(query_tokens: List[str], target_clean: str) -> int:
    """
    Intent Engine V6:
    1. Word Presence: Checks if key query tokens exist in the target (Big Bonus).
    2. Sequence Match: Original V5 sequence check (Small Bonus).
    3. Aesthetic Penalty: Penalizes short queries matching long titles too closely.
    """
    if not query_tokens or not target_clean: return 0
    
    score = 0
    query_str_tight = "".join(query_tokens)
    target_str_tight = re.sub(r'\s+', '', target_clean)
    
    # --- RULE 1: WORD PRESENCE (Most Important: V6 Upgrade) ---
    target_tokens = target_clean.split()
    matched_words = 0
    
    # Query ke har token ko title mein dhoondho
    for q_token in query_tokens:
        if len(q_token) < 2: continue # Ignore single letters (a, an, the)
        
        # Simple containment check: token target mein hai ya nahi
        if q_token in target_tokens or any(q_token in t_token for t_token in target_tokens):
            matched_words += 1
            
    if matched_words == len([t for t in query_tokens if len(t) >= 2]):
        score += 250 # Full match (e.g., 'me next' in 'meet me next christmas')
    elif matched_words > 0:
        score += 100 * (matched_words / len([t for t in query_tokens if len(t) >= 2])) # Partial match
        
    # --- RULE 2: STRICT SEQUENCE CHECK (Original V5 Logic) ---
    last_idx = -1
    broken = False
    for char in query_str_tight:
        found_idx = target_str_tight.find(char, last_idx + 1)
        if found_idx == -1:
            broken = True
            break
        last_idx = found_idx
    
    if not broken:
        score += 150 # Sequence bonus
        
        # Tie-breaker: Length Difference (smaller diff = better)
        len_diff = abs(len(target_str_tight) - len(query_str_tight))
        score -= min(len_diff * 5, 50) # Moderate penalty max 50
    
    # --- RULE 3: AESTHETIC PENALTY (V6 Anti-False Positive) ---
    # Agar query choti hai (length < 8) lekin bahut zyaada words match ho rahe hain
    if len(query_str_tight) < 8 and (score > 100):
        # Long target title ko penalize karein
        if len(target_str_tight) > 20:
             score -= 50
    
    return score

def python_fuzzy_search(query: str, limit: int = 10) -> List[Dict]:
    """
    Smart V6 Search: Hybrid approach with Intent Engine V6 Re-Ranking.
    - Low DB/CPU consumption guaranteed by using in-memory fuzzy_movie_cache.
    """
    if not fuzzy_movie_cache:
        return []

    try:
        # 1. Clean inputs
        q_fuzzy = clean_text_for_fuzzy(query) 
        q_anchor = clean_text_for_search(query) 
        
        if not q_fuzzy or not q_anchor: return []
        
        # Tokenization (for Rule 1)
        query_tokens = [t for t in q_anchor.split() if t]

        candidates = []
        seen_imdb = set()
        
        # --- 1. EXACT MATCH ANCHOR (Score 1001) ---
        anchor_keys = [q_anchor]
        if q_anchor.startswith('the '):
             anchor_keys.append(q_anchor[4:]) 
        else:
             anchor_keys.append('the ' + q_anchor) 

        for key in set(anchor_keys):
            if key in fuzzy_movie_cache:
                data = fuzzy_movie_cache[key]
                if data['imdb_id'] not in seen_imdb:
                     candidates.append({
                        'imdb_id': data['imdb_id'],
                        'title': data['title'],
                        'year': data.get('year'),
                        'score': 1001, # Highest Priority Score
                        'match_type': 'exact_anchor'
                     })
                     seen_imdb.add(data['imdb_id'])
                     logger.debug(f"🎯 Exact Anchor Match: {data['title']}")
        # --- END 1 ---


        # 2. RAPIDFUZZ BROAD FETCH (Limit 1000 - CPU-bound work)
        all_titles = list(fuzzy_movie_cache.keys())
        
        # Process.extract ko WRatio se chalao
        pre_filtered = process.extract(
            q_fuzzy, 
            all_titles, 
            limit=1000,  # Max 1000 items
            scorer=fuzz.WRatio, 
            score_cutoff=30 # Minimum required similarity
        )
        
        # 3. INTENT ENGINE V6 RE-RANKING
        for clean_title_key, fuzz_score, _ in pre_filtered:
            data = fuzzy_movie_cache.get(clean_title_key)
            if not data or data['imdb_id'] in seen_imdb: continue
            
            t_clean_key = clean_title_key 
            
            # Smart Score Calculation (V6 Logic)
            intent_score = get_smart_match_score(query_tokens, t_clean_key)
            
            final_score = 0
            match_type = "fuzzy"
            
            if fuzz_score >= 95:
                # High Fuzzy (near perfect) ko 900+ score do
                final_score = 900 + intent_score
                match_type = "high_fuzzy"
            elif intent_score > 50: 
                # Intent matches (word presence/sequence) ko 500-750 score do
                final_score = 500 + intent_score 
                match_type = "intent"
            else:
                # Default WRatio score
                final_score = fuzz_score

            candidates.append({
                'imdb_id': data['imdb_id'],
                'title': data['title'],
                'year': data.get('year'),
                'score': final_score,
                'match_type': match_type
            })
            seen_imdb.add(data['imdb_id'])


        # 4. Final Sort & Deduplicate
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # Final deduplication (though done above, this ensures safety)
        unique_candidates = []
        final_seen_imdb = set()
        for c in candidates:
            if c['imdb_id'] not in final_seen_imdb:
                unique_candidates.append(c)
                final_seen_imdb.add(c['imdb_id'])
        
        return unique_candidates[:limit]
        
    except Exception as e:
        logger.error(f"python_fuzzy_search mein error: {e}", exc_info=True)
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
        db1_success = await safe_db_call(db_primary.init_db(), default=False) 
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
             logger.warning("Database 2 (MongoDB Fallback) initialization safal. Bot degraded mode mein chalega.")
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
    logger.info(f"Priority Queue with {QUEUE_CONCURRENCY} workers start ho गया।")
    # --- END NEW ---

    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor start ho गया.")

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
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token mila.")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid Secret Token")
        
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
    
    # FIX: redis_cache.is_ready() sync है, इसे gather से बाहर call करें
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
        "search_logic": "Hybrid (Smart Sequence > Fuzzy)",
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
@dp.message(CommandStart(), BannedFilter())
async def banned_start_command_stub(message: types.Message):
    pass

# UI Enhancement & CRITICAL BUG FIX (The logic that caused /stats to trigger /start for admin is removed)
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(
    message: types.Message,
    bot: Bot,
    db_primary: Database,
    db_fallback: Database,
    db_neon: NeonDB,
    redis_cache: RedisCacheLayer,
    state: FSMContext
):
    await state.clear()  # 🔥 FSM RESET
    user = message.from_user
    if not user: return
    user_id = user.id

    # --- FEATURE B: MONETIZATION TOKEN CATCH ---
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("unlock_"):
        token = args[1].split("_")[1]
        token_doc = await db_primary.verify_unlock_token(token, user_id)
        if token_doc:
            await message.answer("✅ **Download Unlocked!** Delivering your file now...")
            asyncio.create_task(db_primary.track_event("shortlink_success"))
            # Normal delivery logic from token info
            imdb_id = token_doc["imdb_id"]
            movie = await safe_db_call(db_primary.get_movie_by_imdb(imdb_id))
            if movie:
                 # Execute copy
                 res = await safe_tg_call(bot.copy_message(user_id, int(movie["channel_id"]), movie["message_id"]), semaphore=TELEGRAM_COPY_SEMAPHORE)
                 if res:
                      asyncio.create_task(schedule_auto_delete(bot, user_id, res.message_id))
                      # Trigger Post-Download Ad (Isolated Task)
                      asyncio.create_task(send_sponsor_ad(user_id, bot, db_primary, redis_cache))
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
        await safe_tg_call(message.answer(admin_text, reply_markup=admin_kb), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
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
@dp.message(
    F.text
    & ~F.text.startswith("/")
    & (F.chat.type == "private")
    & ~F.state.in_(AdStates)   # 🔥 IMPORTANT FIX
)
@handler_timeout(20)
async def search_movie_handler(
    message: types.Message,
    bot: Bot,
    db_primary: Database,
    db_fallback: Database,
    db_neon: NeonDB,
    redis_cache: RedisCacheLayer,
    state: FSMContext
):
    # ⚠️ Yahan state.clear() MAT lagana
    pass
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message, db_primary, bot, redis_cache):
        return
        
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
    fuzzy_hits_task = loop.run_in_executor(executor, python_fuzzy_search, original_query, 45) 
    
    fuzzy_hits_raw = await fuzzy_hits_task
    if fuzzy_hits_raw is None: fuzzy_hits_raw = []

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
            current_count = int(cached_count)
            
    if current_count >= 2:
        await safe_tg_call(callback.answer("⚠️ Limit Reached: You can only refresh results 2 times per day.", show_alert=True))
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
    
    if not await ensure_capacity_or_inform(callback, db_primary, bot, redis_cache):
        return

    imdb_id = callback.data.split("_", 1)[1]
    
    # --- NEW: SHORTLINK MONETIZATION WRAPPER ---
    is_shortlink_active = await db_primary.get_config("shortlink_status", "off") == "on"
    if is_shortlink_active and user.id != ADMIN_USER_ID:
        # 1. Create Token
        token = await db_primary.create_unlock_token(user.id, imdb_id)
        # 2. Build Redirect URL
        bot_user = (await bot.get_me()).username
        unlock_url = f"https://t.me/{bot_user}?start=unlock_{token}"
        # 3. Monetize
        monetized_link = await get_shortened_link(unlock_url, db_primary)
        
        unlock_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔓 UNLOCK DOWNLOAD", url=monetized_link)
        ]])
        
        await callback.message.edit_text(
            "🔐 **DOWNLOAD LOCKED**\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "To keep this service free, please complete one shortlink to unlock the file.\n\n"
            "1️⃣ Tap 'Unlock' below.\n"
            "2️⃣ Complete verification.\n"
            "3️⃣ You will be redirected back.",
            reply_markup=unlock_kb
        )
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
        # 7 minutes = 420 seconds
        asyncio.create_task(schedule_auto_delete(bot, user.id, sent_msg_id, delay=420))
        
        # UI Enhancement: Success message with WARNING
        success_text = (
            f"🎉 **CONTENT DELIVERED**\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"✅ '<b>{movie['title']}</b>' has been sent.\n\n"
            f"⚠️ **COPYRIGHT ALERT: AUTO-DELETE**\n"
            f"To protect the channel, this file will **self-destruct in 7 minutes**.\n\n"
            f"🔥 **FORWARD IT to your 'Saved Messages' NOW!**"
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

    info = extract_movie_info(message.caption or "") 
    if not info or not info.get("title"):
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption parse nahi kar paya.")
        await safe_tg_call(message.answer(f"❌ **Parse Error**: Caption missing/invalid for MsgID `{message.forward_from_message_id}`."), semaphore=TELEGRAM_COPY_SEMAPHORE); return

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


@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    if message.chat.id != LIBRARY_CHANNEL_ID or LIBRARY_CHANNEL_ID == 0: return
    if not (message.video or message.document): return
        
    info = extract_movie_info(message.caption or "") 
    if not info or not info.get("title"):
        if message.caption: logger.warning(f"Auto-Index Skip (MsgID {message.message_id}): Caption parse nahi kar paya: '{message.caption[:50]}...'")
        else: logger.warning(f"Auto-Index Skip (MsgID {message.message_id}): Koi caption nahi.")
        return

    file_data = message.video or message.document
    file_id = file_data.file_id; file_unique_id = file_data.file_unique_id
    imdb_id = info.get("imdb_id") or f"auto_{message_id}"
    title = info["title"]; year = info.get("year")
    
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: '{title}'):"
    clean_title_val = clean_text_for_search(title)
    
    # db.add_movie is an async method in database.py
    db1_task = safe_db_call(db_primary.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    db2_task = safe_db_call(db_fallback.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    # db_neon.add_movie is an async method in neondb.py
    neon_task = safe_db_call(db_neon.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title))
    
    async def run_tasks():
        res = await db1_task
        await db2_task
        await neon_task
        if res is True: # Agar movie nayi thi
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
                         asyncio.create_task(redis_cache.set(f"movie_title_{clean_title_val}", json.dumps(movie_data), ttl=86400))
                    # --- END NEW ---
            logger.info(f"{log_prefix} Fuzzy cache mein add ho gayi।")
    
    asyncio.create_task(run_tasks())
    
    logger.info(f"{log_prefix} Teeno DBs ko sync ke liye bhej diya.")

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    # UI Enhancement: Custom working message
    msg = await safe_tg_call(message.answer("📊 **Connecting to Dashboard...**"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    # All check methods are async methods in database.py/neondb.py
    user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
    mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
    mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    mongo_1_ready_task = safe_db_call(db_primary.is_ready(), default=False)
    mongo_2_ready_task = safe_db_call(db_fallback.is_ready(), default=False)
    neon_ready_task = safe_db_call(db_neon.is_ready(), default=False)
    
    redis_ready = redis_cache.is_ready()

    user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users, mongo_1_ready, mongo_2_ready, neon_ready = await asyncio.gather(
        user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task, mongo_1_ready_task, mongo_2_ready_task, neon_ready_task
    )
    
    # UI Enhancement: Status Indicators
    def node_status_icon(is_ok): 
        return "🟢 Online" if is_ok else "🔴 Offline"

    def cache_status_icon(is_ok): 
        return "🟢 Active" if is_ok else "🟠 Degraded"

    def count_str(c): return f"{c:,}" if c >= 0 else "N/A"

    search_status = f"⚡ Hybrid (Smart V6)"
    if not mongo_1_ready: search_status = "🟠 Degraded (M1 Down)"
    if len(fuzzy_movie_cache) == 0: search_status = "🟠 Degraded (No Cache)"
    
    # UI Enhancement: Premium Dashboard
    stats_text = (
        f"👑 **ADMIN DASHBOARD** 👑\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"**🖥️ SERVER NODES**\n"
        f"• Node 1 (Mongo Primary): {node_status_icon(mongo_1_ready)}\n"
        f"• Node 2 (Mongo Fallback): {node_status_icon(mongo_2_ready)}\n"
        f"• Node 3 (Neon Backup): {node_status_icon(neon_ready)}\n"
        f"• Node 4 (Redis Cache): {cache_status_icon(redis_ready)}\n\n"
        
        f"**📊 LIVE METRICS**\n"
        f"• **Active Users:** {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
        f"• **Server Uptime:** {get_uptime()}\n"
        f"• **Queue Load:** {priority_queue._queue.qsize()} tasks\n"
        f"• **Search Engine:** {search_status}\n"
        f"• **Fuzzy Cache:** {len(fuzzy_movie_cache):,} titles\n\n"
        
        f"**📂 DATA VOLUME**\n"
        f"• **Total Users:** {count_str(user_count)}\n"
        f"• **M1 Movies:** {count_str(mongo_1_count)}\n"
        f"• **M2 Movies:** {count_str(mongo_2_count)}\n"
        f"• **Neon Vectors:** {count_str(neon_count)}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    
    # UI Enhancement: Link to Command Hub
    panel_btn = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛠 Open Command Hub", callback_data="admin_panel_open")]])
    await safe_tg_call(msg.edit_text(stats_text, reply_markup=panel_btn))

# --- NEW: Callback Handler for Admin Stats Button ---
@dp.callback_query(F.data == "admin_stats_cmd", AdminFilter())
@handler_timeout(15)
async def admin_stats_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    await safe_tg_call(callback.answer("Accessing Dashboard..."))
    
    # Re-using stats generation logic here for the callback
    # We edit the message instead of sending a new one
    
    # All check methods are async methods in database.py/neondb.py
    user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
    mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
    mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    mongo_1_ready_task = safe_db_call(db_primary.is_ready(), default=False)
    mongo_2_ready_task = safe_db_call(db_fallback.is_ready(), default=False)
    neon_ready_task = safe_db_call(db_neon.is_ready(), default=False)
    
    redis_ready = redis_cache.is_ready()
    
    user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users, mongo_1_ready, mongo_2_ready, neon_ready = await asyncio.gather(
        user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task, mongo_1_ready_task, mongo_2_ready_task, neon_ready_task
    )
    
    def node_status_icon(is_ok): return "🟢 Online" if is_ok else "🔴 Offline"
    def cache_status_icon(is_ok): return "🟢 Active" if is_ok else "🟠 Degraded"
    def count_str(c): return f"{c:,}" if c >= 0 else "N/A"

    search_status = f"⚡ Hybrid (Smart V6)"
    if not mongo_1_ready: search_status = "🟠 Degraded (M1 Down)"
    if len(fuzzy_movie_cache) == 0: search_status = "🟠 Degraded (No Cache)"
    
    stats_text = (
        f"👑 **ADMIN DASHBOARD** 👑\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"**🖥️ SERVER NODES**\n"
        f"• Node 1 (Mongo Primary): {node_status_icon(mongo_1_ready)}\n"
        f"• Node 2 (Mongo Fallback): {node_status_icon(mongo_2_ready)}\n"
        f"• Node 3 (Neon Backup): {node_status_icon(neon_ready)}\n"
        f"• Node 4 (Redis Cache): {cache_status_icon(redis_ready)}\n\n"
        
        f"**📊 LIVE METRICS**\n"
        f"• **Active Users:** {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
        f"• **Server Uptime:** {get_uptime()}\n"
        f"• **Queue Load:** {priority_queue._queue.qsize()} tasks\n"
        f"• **Search Engine:** {search_status}\n"
        f"• **Fuzzy Cache:** {len(fuzzy_movie_cache):,} titles\n\n"
        
        f"**📂 DATA VOLUME**\n"
        f"• **Total Users:** {count_str(user_count)}\n"
        f"• **M1 Movies:** {count_str(mongo_1_count)}\n"
        f"• **M2 Movies:** {count_str(mongo_2_count)}\n"
        f"• **Neon Vectors:** {count_str(neon_count)}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    
    panel_btn = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛠 Open Command Hub", callback_data="admin_panel_open")]])
    await safe_tg_call(callback.message.edit_text(stats_text, reply_markup=panel_btn))
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
async def ad_btn_label_rcv(message: types.Message, state: FSMContext):
    if message.text == "/skip":
        data = await state.get_data()
        ad_id = await db_primary.add_ad(data['text'])
        await message.answer(f"✅ **Ad Saved!** ID: `{ad_id}`")
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

@dp.message(Command("setshort"), AdminFilter())
async def set_shortlink_cmd(message: types.Message, db_primary: Database):
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("⚠️ **Usage**: /setshort `ON/OFF` `[URL]`")
    
    status = args[1].lower()
    await db_primary.update_config("shortlink_status", status)
    if len(args) > 2:
        await db_primary.update_config("shortlink_api", args[2])
    
    await message.answer(f"✅ Monetization is now **{status.upper()}**.")

# ==========================================
# PROBLEM FIX: SYNC COMMAND WRAPPERS
# ==========================================

@dp.message(Command("sync_mongo_1_to_2"), AdminFilter())
async def sync_m12_freeze_fix(message: types.Message, db_primary: Database, db_fallback: Database):
    await run_in_background(sync_mongo_1_to_2_command, message, db_primary, db_fallback)

@dp.message(Command("force_rebuild_m1"), AdminFilter())
async def force_rebuild_freeze_fix(message: types.Message, db_primary: Database):
    await run_in_background(force_rebuild_all_clean_titles, message, db_primary)

@dp.message(Command("sync_mongo_1_to_neon"), AdminFilter())
async def sync_neon_freeze_fix(message: types.Message, db_primary: Database, db_neon: NeonDB):
    await run_in_background(sync_mongo_1_to_neon_command, message, db_primary, db_neon)

@dp.message(Command("remove_library_duplicates"), AdminFilter())
async def rem_dupes_freeze_fix(message: types.Message, db_neon: NeonDB):
    await run_in_background(remove_library_duplicates_command, message, db_neon)

# =======================================================
# +++++ ORIGINAL BOT HANDLERS PRESERVED +++++
# =======================================================

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600)
async def broadcast_command(message: types.Message, db_primary: Database):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("⚠️ **Broadcast Error**: Reply to a message to broadcast."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    # get_all_users is an async method in database.py
    users = await safe_db_call(db_primary.get_all_users(), timeout=60, default=[])
    if not users:
        await safe_tg_call(message.answer("⚠️ **Broadcast Error**: No users found."), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        
    total = len(users); msg = await safe_tg_call(message.answer(f"📢 **Initializing Broadcast**\nTarget: {total:,} users..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
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
    for i, user_id in enumerate(users):
        tasks.append(send_to_user(user_id))
        processed_count = i + 1
        now = datetime.now(timezone.utc)
        if processed_count % 100 == 0 or (now - last_update_time).total_seconds() > 15 or processed_count == total:
            await asyncio.gather(*tasks); tasks = []
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
        fio = io.BytesIO(); 
        await bot.download_file(file.file_path, fio); fio.seek(0)
        loop = asyncio.get_running_loop()
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
            imdb = f"json_{hashlib.md5(fid_str.encode()).hexdigest()[:10]}"
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
async def remove_library_duplicates_command(message: types.Message, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("🧹 **Library Cleanup**: Scanning NeonDB for duplicates..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # find_and_delete_duplicates is an async method in neondb.py
    messages_to_delete, total_duplicates = await safe_db_call(db_neon.find_and_delete_duplicates(batch_limit=100), default=([], 0))
    
    if not messages_to_delete:
        await safe_tg_call(msg.edit_text("✅ **Library Clean**: No duplicates found."))
        return
        
    await safe_tg_call(msg.edit_text(f"⚠️ **Duplicates Found**: {total_duplicates}\n🗑️ Deleting **{len(messages_to_delete)}** messages..."))
    
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
    
    await safe_tg_call(msg.edit_text(
        f"✅ **Cleanup Report**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🗑️ Deleted: {deleted_count}\n"
        f"❌ Failed: {failed_count}\n"
        f"⚠️ Remaining: {max(0, total_duplicates - deleted_count)}\n\n"
        f"ℹ️ Run again to continue cleaning."
    ))


@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(7200)
async def backup_channel_command(message: types.Message, db_neon: NeonDB):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer("⚠️ **Usage**: /backup_channel `ID_OR_USERNAME`"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    target_channel = args[1].strip()
    try:
        if not (target_channel.startswith("@") or target_channel.startswith("-100")):
             raise ValueError("Invalid format.")
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ **Error**: Invalid target format."), semaphore=TELEGRAM_COPY_SEMAPHORE); return

    msg = await safe_tg_call(message.answer("⏳ **Backup**: Fetching unique files..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # get_unique_movies_for_backup is an async method in neondb.py
    unique_files = await safe_db_call(db_neon.get_unique_movies_for_backup(), default=[])
    if not unique_files:
        await safe_tg_call(msg.edit_text("❌ **Failed**: No files found.")); return
        
    total_files = len(unique_files)
    await safe_tg_call(msg.edit_text(f"✅ **Found**: {total_files:,} files.\n🚀 Copying to `{target_channel}`..."))
    
    copied_count, failed_count = 0, 0
    tasks = []
    
    async def copy_file(msg_id: int, chat_id: int):
        nonlocal copied_count, failed_count
        res = await safe_tg_call(
            bot.copy_message(chat_id=target_channel, from_chat_id=chat_id, message_id=msg_id),
            timeout=TG_OP_TIMEOUT * 2, semaphore=TELEGRAM_COPY_SEMAPHORE
        )
        if res: copied_count += 1
        else: failed_count += 1
        await asyncio.sleep(1.0) 

    for i, (msg_id, chat_id) in enumerate(unique_files):
        tasks.append(copy_file(msg_id, chat_id))
        if (i + 1) % 50 == 0 or (i + 1) == total_files:
            await asyncio.gather(*tasks); tasks = []
            try: 
                # UI Enhancement: Backup progress update
                await safe_tg_call(msg.edit_text(f"🚀 **Backup Progress**\nCompleted: {(i+1):,} / {total_files:,}\n✅ {copied_count} | ❌ {failed_count}"))
            except TelegramBadRequest: pass
            
    # UI Enhancement: Final backup status
    await safe_tg_call(msg.edit_text(f"🎉 **BACKUP FINISHED**\n\n**Total:** {total_files:,}\n**Success:** {copied_count}\n**Failed:** {failed_count}"))


@dp.message(Command("sync_mongo_1_to_neon"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_neon_command(message: types.Message, db_primary: Database, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("🔄 **Syncing M1 → Neon**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # get_all_movies_for_neon_sync is an async method in database.py
    mongo_movies = await safe_db_call(db_primary.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies:
        await safe_tg_call(msg.edit_text("❌ **Sync Failed**: No data in M1.")); return
    
    await safe_tg_call(msg.edit_text(f"✅ **Data Ready**: {len(mongo_movies):,} movies.\n🔄 Uploading to Neon..."))
    # sync_from_mongo is an async method in neondb.py
    processed_count = await safe_db_call(db_neon.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    await safe_tg_call(msg.edit_text(f"✅ **Sync Complete**\nProcessed: {processed_count:,} records."))

@dp.message(Command("sync_mongo_1_to_2"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_2_command(message: types.Message, db_primary: Database, db_fallback: Database):
    msg = await safe_tg_call(message.answer("🔄 **Syncing M1 → M2**..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
        
    await safe_tg_call(msg.edit_text(f"⏳ **Fetching M1 Data**..."))
    # get_all_movies_for_neon_sync is an async method in database.py
    mongo_movies_full = await safe_db_call(db_primary.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies_full:
        await safe_tg_call(msg.edit_text("❌ **Sync Failed**: No data in M1.")); return
        
    total_movies = len(mongo_movies_full) # F.I.X: Total count set
    await safe_tg_call(msg.edit_text(f"✅ **Found**: {total_movies:,} movies.\n🔄 Syncing to M2..."))
    
    processed_count = 0
    # F.I.X: Tasks list (ab final gather ke liye)
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
        
        # F.I.X: Progress अपडेट को नियंत्रित तरीके से भेजें
        if processed_count % BATCH_SIZE == 0:
            try:
                 # Progress update ko await karna zaroori hai
                 await safe_tg_call(msg.edit_text(f"🔄 **Syncing M1 → M2...**\nProgress: {processed_count:,} / {total_movies:,}"))
            except TelegramBadRequest: pass
            
    # F.I.X: सभी tasks को एक साथ चलाएँ और await करें
    await safe_tg_call(msg.edit_text(f"⏳ **Finalizing Sync**...\nProcessing final {len(all_sync_tasks):,} tasks."))
    
    # F.I.X: MAIN BLOCKING AWAIT
    await asyncio.gather(*all_sync_tasks) # सारे tasks एक साथ थ्रेडपूल में चलेंगे

    # F.I.X: Final Message Retry Logic
    final_text = f"✅ **Sync Complete**\nProcessed: {processed_count:,} records."
    
    for attempt in range(3):
        result = await safe_tg_call(msg.edit_text(final_text), timeout=10)
        if result:
            logger.info(f"Sync complete message sent successfully on attempt {attempt + 1}.")
            break
        logger.warning(f"Failed to send final sync message on attempt {attempt + 1}. Retrying in 5s.")
        await asyncio.sleep(5)
    else:
        logger.error("Failed to send final sync complete message after 3 attempts.")


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
@handler_timeout(1800) # Timeout को 30 मिनट तक बढ़ाया गया
async def force_rebuild_m1_command(message: types.Message, bot: Bot, db_primary: Database):
    # STEP 1: Initial Message
    msg = await safe_tg_call(message.answer("⚠️ **FORCE REBUILDING M1**\nStarting database scan...", semaphore=TELEGRAM_COPY_SEMAPHORE))
    if not msg: return
    
    # --- FIX: Progress Callback Function with Flood/Edit Protection ---
    async def progress_callback(processed_count: int, total_count: int):
        # Flood limit से बचने के लिए छोटा पॉज
        await asyncio.sleep(0.5) 
        
        try:
            await safe_tg_call(
                bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=msg.message_id,
                    text=f"⚠️ **FORCE REBUILDING M1**\n"
                         f"━━━━━━━━━━━━━━━━━━━━\n"
                         f"⏳ **Progress**: {processed_count:,} / {total_count:,} records processed.\n"
                         f"Please wait. This may take several minutes."
                ),
                # Telegram API rate limit से बचने के लिए COPY_SEMAPHORE का उपयोग
                semaphore=TELEGRAM_COPY_SEMAPHORE 
            )
        except TelegramRetryAfter as e:
            logger.warning(f"Telegram rate limited during progress update. Waiting {e.retry_after}s.")
            # यहां हम इंतज़ार नहीं करेंगे क्योंकि DB का काम चल raha hai, bas skip karenge.
            pass 
        except TelegramBadRequest as e:
            # Message not modified error ignore karein
            if "message is not modified" not in str(e):
                 logger.error(f"Error editing progress message: {e}")
        except Exception as e:
            logger.warning(f"Unknown error updating rebuild progress: {e}")
            
    # STEP 2: Run the Heavy DB Task
    # force_rebuild_all_clean_titles is an async method in database.py
    updated, total = await safe_db_call(
        db_primary.force_rebuild_all_clean_titles(
            clean_text_for_search, # aapka sync cleaning function
            progress_callback=progress_callback
        ), 
        timeout=1740, # 29 minutes
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
    
    # Final message update (Retry to guarantee success)
    try:
        await safe_tg_call(msg.edit_text(final_text))
    except Exception:
        # agar edit fail ho jaye, to naya message bhejein
        await safe_tg_call(message.answer(final_text), semaphore=TELEGRAM_COPY_SEMAPHORE)
    
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("🧠 **Cache Reloaded**"))


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
    # cleanup_movie_titles is a new async method in database.py
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


# ==========================================
# ADMIN ADS MANAGEMENT HANDLERS
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
async def ad_btn_label_rcv(message: types.Message, state: FSMContext):
    if message.text == "/skip":
        data = await state.get_data()
        ad_id = await db_primary.add_ad(data['text'])
        await message.answer(f"✅ **Ad Saved!** ID: `{ad_id}`")
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

@dp.message(Command("setshort"), AdminFilter())
async def set_shortlink_cmd(message: types.Message, db_primary: Database):
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("⚠️ **Usage**: /setshort `ON/OFF` `[URL]`")
    
    status = args[1].lower()
    await db_primary.update_config("shortlink_status", status)
    if len(args) > 2:
        await db_primary.update_config("shortlink_api", args[2])
    
    await message.answer(f"✅ Monetization is now **{status.upper()}**.")


# ============ ERROR HANDLER ============

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    if isinstance(exception, asyncio.TimeoutError):
        logger.warning(f"Error handler ne ek unhandled TimeoutError pakda: {exception}")
        return
        
    logger.exception(f"--- UNHANDLED ERROR ---: {exception}", exc_info=True)
    logger.error(f"Update jo fail hua: {update.model_dump_json(indent=2, exclude_none=True)}")

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
        
        # F.I.X: init_db coroutines ko execute karein aur safe_db_call se wrap karein
        db_primary_success = await safe_db_call(db_primary.init_db(), default=False)
        db_fallback_success = await safe_db_call(db_fallback.init_db(), default=False)
        
        if not db_primary_success:
            raise RuntimeError("Database 1 connection failed on startup.")
            
        await db_neon.init_db()
        # Fuzzy cache load karein (ab Redis fallback ke saath)
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
            redis_cache=redis_cache # Redis inject karein
        )
    finally:
        await shutdown_procedure()

if __name__ == "__main__":
    logger.warning("Bot ko seedha __main__ se run kiya ja raha hai. Deployment ke liye Uvicorn/FastAPI ka istemal karein।")
    if not WEBHOOK_URL:
        try: 
            # Executor ko manually setup karein
            executor_for_main = concurrent.futures.ThreadPoolExecutor(max_workers=10)
            loop_for_main = asyncio.get_event_loop()
            loop_for_main.set_default_executor(executor_for_main)
            
            asyncio.run(main_polling())
            
            # Shutdown executor
            executor_for_main.shutdown(wait=True, cancel_futures=False)
            
        except (KeyboardInterrupt, SystemExit): 
            logger.info("Bot polling band kar raha hai।")
    else:
        logger.error("WEBHOOK_URL set hai. Local polling nahi chalega।")
        logger.error("Run karne ke liye: uvicorn bot:app --host 0.0.0.0 --port 8000")
