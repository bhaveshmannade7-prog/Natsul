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
from typing import List, Dict, Callable, Any
from functools import wraps
import concurrent.futures

# --- Load dotenv FIRST ---
from dotenv import load_dotenv
load_dotenv()

# --- NEW IMPORTS ---
# FIX: Core utils se import karna circular dependency ko todega
from core_utils import safe_tg_call, safe_db_call, DB_SEMAPHORE, TELEGRAM_DELETE_SEMAPHORE, TELEGRAM_COPY_SEMAPHORE, TELEGRAM_BROADCAST_SEMAPHORE, WEBHOOK_SEMAPHORE, TG_OP_TIMEOUT, DB_OP_TIMEOUT
from redis_cache import redis_cache, RedisCacheLayer
from queue_wrapper import priority_queue, PriorityQueueWrapper, QUEUE_CONCURRENCY, PRIORITY_ADMIN
from smart_watchdog import SmartWatchdog, WATCHDOG_ENABLED # NAYA WATCHDOG IMPORT
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

    JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "thegreatmoviesl9").replace("@", "")
    USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "MOVIEMAZASU").replace("@", "")

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
logger.info("Search Logic: Intent Engine V5 (Anchors + Sequence + Context)")


if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID set nahi hai. Admin commands kaam nahi karenge.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID set nahi hai. Auto-indexing aur Migration kaam nahi karenge.")
if not JOIN_CHANNEL_USERNAME and not USER_GROUP_USERNAME:
    logger.warning("--- KOI JOIN CHECK SET NAHI HAI. Membership check skip ho jayega. ---")


# ============ TIMEOUTS & SEMAPHORES ============
# Semaphores are now defined and imported from core_utils.py
HANDLER_TIMEOUT = 15 
# TG_OP_TIMEOUT is imported from core_utils
# DB_OP_TIMEOUT is imported from core_utils


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

# ============ BOT & DB INITIALIZATION ============

# --- NEW: Multi-Bot Manager Class (Remains the same) ---
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
# --- END Multi-Bot Manager ---

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
            db_primary.client.close()
            logger.info("MongoDB (Primary) client connection close ho gaya.")
        if db_fallback and db_fallback.client:
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
                await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
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
                        # Yahaan hum bot instance ko kwargs se nikal rahe hain, jo DP se pass hoga
                        current_bot = kwargs.get('bot') or bot
                        await current_bot.send_message(target_chat_id, "⚠️ Server request time out ho gayi. Kripya dobara koshish karein।")
                    except Exception: pass
                if callback_query:
                    try: await callback_query.answer("Timeout", show_alert=False)
                    except Exception: pass
            except Exception as e:
                logger.exception(f"Handler {func.__name__} mein error: {e}")
        return wrapper
    return decorator

# ============ SAFE API CALL WRAPPERS (Core Utils mein move ho gaye, yahan sirf reference) ============
# We use the imported safe_db_call and safe_tg_call from core_utils now.


# ============ FILTERS & HELPER FUNCTIONS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

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
    
    # FIX: Chat ID ko pehle check karein
    chat_identifier_channel = JOIN_CHANNEL_USERNAME
    chat_identifier_group = USER_GROUP_USERNAME
    
    # Logic to handle potential numeric chat IDs (e.g., -1001234567890)
    def normalize_chat_id(identifier):
        if identifier and (identifier.startswith('-') and identifier[1:].isdigit() or identifier.isdigit()):
            return int(identifier)
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

def get_join_keyboard() -> InlineKeyboardMarkup | None:
    buttons = []
    # FIX: Buttons mein sirf username hi dikhega, chahe background me ID ho
    if JOIN_CHANNEL_USERNAME: buttons.append([InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https.t.me/{JOIN_CHANNEL_USERNAME.lstrip('@')}")])
    if USER_GROUP_USERNAME: buttons.append([InlineKeyboardButton(text="👥 Group Join Karein", url=f"https.t.me/{USER_GROUP_USERNAME.lstrip('@')}")])
    if buttons: buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya (Verify)", callback_data="check_join")]); return InlineKeyboardMarkup(inline_keyboard=buttons)
    return None

def get_full_limit_keyboard() -> InlineKeyboardMarkup | None:
    if not ALTERNATE_BOTS: return None
    buttons = [[InlineKeyboardButton(text=f"🚀 Dusra Bot @{b}", url=f"https.t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- CLEANING LOGIC ---
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

def overflow_message(active_users: int) -> str:
    return (f"⚠️ Server par load zyada hai ({active_users}/{CURRENT_CONC_LIMIT}).\n"
            f"Aapki request abhi hold par hai. Kripya thodi देर baad try karein ya neeche diye gaye alternate bots ka istemal karein:")

# ============ EVENT LOOP MONITOR ============
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

# ============ NAYA FUZZY CACHE FUNCTIONS ============
async def load_fuzzy_cache(db: Database):
    """Mongo/Redis se movie titles fetch k करके in-memory fuzzy cache banata hai।"""
    global fuzzy_movie_cache
    async with FUZZY_CACHE_LOCK:
        logger.info("In-Memory Fuzzy Cache load ho raha hai (Redis > Mongo se)...")
        try:
            # db.get_all_movies_for_fuzzy_cache mein ab Redis fallback logic hai
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
# +++++ NEW: SMART INTENT ENGINE V5 ++++++++++++++++
# ==================================================
#
def get_smart_match_score(query: str, target: str) -> int:
    """
    Intent Engine V5:
    1. Anchors: Start AND End letters must match (Critical for Katra->Kantara vs Kesari).
    2. Sequence: All characters of query must exist in target.
    3. Context: Handle "The Avengers" logic.
    """
    if not query or not target: return 0
    
    score = 0
    
    # Handle "The " prefix in target for comparison
    target_core = target
    if target.startswith("the "):
        target_core = target[4:]
        if not target_core: target_core = target # Handle if title was just "The"
    
    # --- RULE 1: START ANCHOR (Start must match) ---
    if query[0] == target_core[0]:
        score += 50
    
    # --- RULE 2: END ANCHOR (End must match - Fixes Katra vs Kesari) ---
    # Katra (ends a) == Kantara (ends a) -> +50
    # Katra (ends a) != Kesari (ends i) -> 0
    if query[-1] == target[-1]:
        score += 50
        
    # --- RULE 3: STRICT SEQUENCE CHECK ---
    # Verify if "aveger" fits inside "avengers"
    last_idx = -1
    broken = False
    
    # Query ko bilkul spaces ke bina clean karein sirf sequence check ke liye
    q_tight_seq = re.sub(r'\s+', '', query)
    
    # Target ko bhi sirf sequence check ke liye tight karein
    t_tight_seq = re.sub(r'\s+', '', target)
    
    for char in q_tight_seq:
        found_idx = t_tight_seq.find(char, last_idx + 1)
        if found_idx == -1:
            broken = True
            break
        last_idx = found_idx
    
    if not broken:
        score += 150 # Big Bonus for valid sequence
        
        # Tie-breaker: Length Difference
        len_diff = abs(len(t_tight_seq) - len(q_tight_seq))
        score -= (len_diff * 3) # Moderate penalty
    
    return score

def python_fuzzy_search(query: str, limit: int = 10) -> List[Dict]:
    """
    Smart V5 Search with Exact Match Anchor:
    1. Check for 100% Exact Match in Clean Titles (Anchor Score: 1001).
    2. RapidFuzz (1000 items) -> Intent Engine V5 Re-Rank.
    """
    if not fuzzy_movie_cache:
        return []

    try:
        # 1. Clean inputs
        q_fuzzy = clean_text_for_fuzzy(query) 
        q_anchor = clean_text_for_search(query) 
        
        if not q_fuzzy or not q_anchor: return []

        candidates = []
        seen_imdb = set()
        
        # --- FIX: ULTRA-HIGH PRIORITY EXACT MATCH ANCHOR (Score 1001) ---
        
        # Keys to check for 100% match
        anchor_keys = [q_anchor]
        if q_anchor.startswith('the '):
             anchor_keys.append(q_anchor[4:]) # e.g., 'avengers' agar query 'the avengers' thi
        else:
             # Agar query 'avengers' thi, to 'the avengers' check karo
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
        # --- END FIX ---


        # 2. RAPIDFUZZ BROAD FETCH (Limit 1000)
        all_titles = list(fuzzy_movie_cache.keys())
        
        pre_filtered = process.extract(
            q_fuzzy, 
            all_titles, 
            limit=1000,  
            scorer=fuzz.WRatio, 
            score_cutoff=30
        )
        
        # 3. INTENT ENGINE V5 RE-RANKING
        for clean_title_key, fuzz_score, _ in pre_filtered:
            data = fuzzy_movie_cache.get(clean_title_key)
            if not data or data['imdb_id'] in seen_imdb: continue
            
            t_clean_key = clean_title_key 
            
            # Smart Score Calculation 
            intent_score = get_smart_match_score(q_anchor, t_clean_key)
            
            final_score = 0
            match_type = "fuzzy"
            
            if intent_score > 50: 
                # Intent matches get score 501 to 750 (higher than raw fuzzy 30-100)
                final_score = 500 + intent_score 
                match_type = "intent"
            else:
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

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
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

    # --- 3 DB Init ---
    # MongoDB 1 ko pehle init karein taki NeonDB lock ke liye use ho
    try:
        await db_primary.init_db()
        logger.info("Database 1 (MongoDB Primary) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database 1 (MongoDB Primary) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("MongoDB 1 connection fail (startup).") from e

    try:
        await db_fallback.init_db()
        logger.info("Database 2 (MongoDB Fallback) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database 2 (MongoDB Fallback) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("MongoDB 2 connection fail (startup).") from e

    try:
        # NeonDB init mein ab concurrency lock hai (FIX for InternalServerError)
        await db_neon.init_db()
        logger.info("Database 3 (NeonDB/Postgres Backup) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database 3 (NeonDB/Postgres Backup) initialize nahi ho paya: {e}", exc_info=True)
        # Runtime Error raised for Gunicorn worker exit
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
    
    # FIX for AttributeError: check_if_lock_exists ko ab call kar sakte hain.
    is_webhook_already_set = await db_primary.check_if_lock_exists(WEBHOOK_INIT_LOCK_NAME)

    if not is_webhook_already_set:
        logger.warning("Webhook initialization lock nahi mila. Setting up webhooks...")
        
        # Lock acquire karne ki koshish (timeout 300 seconds)
        lock_acquired = await db_primary.acquire_cross_process_lock(WEBHOOK_INIT_LOCK_NAME, 300)
        
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
                # Lock release karna bahut zaroori hai!
                await db_primary.release_cross_process_lock(WEBHOOK_INIT_LOCK_NAME)
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

# ============ WEBHOOK / HEALTHCHECK ROUTES ============

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
    # Teeno DBs ko parallel check karein
    db_primary_ok_task = safe_db_call(db_primary.is_ready(), default=False)
    db_fallback_ok_task = safe_db_call(db_fallback.is_ready(), default=False)
    neon_ok_task = safe_db_call(db_neon.is_ready(), default=False)
    
    # FIX: redis_cache.is_ready() sync है, इसे gather से बाहर call करें
    redis_ok = redis_cache.is_ready()

    db_primary_ok, db_fallback_ok, neon_ok = await asyncio.gather(
        db_primary_ok_task, db_fallback_ok_task, neon_ok_task
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

# ============ USER CAPACITY CHECK ============

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
    
    # db_primary.add_user mein ab Redis hook hai (high-traffic optimization)
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    if user.id == ADMIN_USER_ID: 
        return True
        
    # get_concurrent_user_count pehle Redis check karega, phir Mongo (CPU reduction)
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
            await safe_tg_call(
                current_bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()),
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
        if isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server busy, kripya thodi देर baad try karein।", show_alert=True))
        return False
        
    return True

# ============ USER COMMANDS AND HANDLERS (Remains the same) ============
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    user = message.from_user
    if not user: return
    user_id = user.id

    # --- NAYA: Admin Panel ---
    if user_id == ADMIN_USER_ID:
        user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
        mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
        mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
        neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
        concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        mongo_1_ready_task = safe_db_call(db_primary.is_ready(), default=False)
        mongo_2_ready_task = safe_db_call(db_fallback.is_ready(), default=False)
        neon_ready_task = safe_db_call(db_neon.is_ready(), default=False)
        
        # --- FIX: redis_cache.is_ready() sync है, इसे gather से बाहर call करें ---
        redis_ready = redis_cache.is_ready()
        
        user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users, mongo_1_ready, mongo_2_ready, neon_ready = await asyncio.gather(
            user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task, mongo_1_ready_task, mongo_2_ready_task, neon_ready_task
        )
        # --- END FIX ---
        
        def status_icon(is_ok): return "🟢" if is_ok else "❌"
        def count_str(c): return f"{c:,}" if c >= 0 else "Error"

        search_status = f"⚡️ Hybrid (Smart Sequence > Fuzzy)"
        if not mongo_1_ready: search_status += " (⚠️ Mongo Down)"
        if len(fuzzy_movie_cache) == 0: search_status += " (⚠️ Fuzzy Cache Khaali Hai!)"

        admin_message = (
            f"🖥️ <b>Bot Control Matrix (M+M+N+R Architecture)</b>\n\n"
            f"<b>📊 Live System Telemetry</b>\n"
            f"  - ⚡️ Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
            f"  - 👤 User Records (M1): {count_str(user_count)}\n"
            f"  - 🎬 Primary Node (M1): {count_str(mongo_1_count)} docs\n"
            f"  - 🗂️ Fallback Node (M2): {count_str(mongo_2_count)} docs\n"
            f"  - 🗄️ Backup Index (Neon): {count_str(neon_count)} vectors\n"
            f"  - ⏱️ System Uptime: {get_uptime()}\n"
            f"  - 🔄 Queue Size: {priority_queue._queue.qsize()} (Workers: {QUEUE_CONCURRENCY})\n\n"
            f"<b>📡 Node Connectivity (Latency Check)</b>\n"
            f"  - {status_icon(mongo_1_ready)} Node 1: Mongo-Primary (Data + Exact Search)\n"
            f"  - {status_icon(mongo_2_ready)} Node 2: Mongo-Fallback (Data Replica)\n"
            f"  - {status_icon(neon_ready)} Node 3: Neon-Backup (File Index)\n"
            f"  - {status_icon(redis_ready)} Node 4: Redis Cache (Throttle/Fuzzy Storage)\n\n"
            f"<b>⚙️ Search Configuration (FIXED)</b>\n"
            f"  - <b>Default Engine: {search_status}</b>\n"
            f"  - 🧠 Fuzzy Cache: {len(fuzzy_movie_cache):,} titles loaded.\n"
            f"  - <b>/reload_fuzzy_cache</b> (Cache ko zabardasti reload karein)\n\n"
            f"<b>🔩 Core Operations</b>\n"
            f"  - /stats | /health | /get_user `ID`\n"
            f"  - /broadcast (Reply to message)\n"
            f"  - /set_limit `N` (Set active user throttle)\n\n"
            f"<b>🩺 Data Diagnostics</b>\n"
            f"  - <b>/check_db</b> (Check karein ki search data loaded hai ya nahi)\n\n"
            f"<b>🔂 Data Integrity & Sync (ZAROORI)</b>\n"
            f"  - <b>/rebuild_clean_titles_m1</b> (Mongo1 data fix karein)\n"
            f"  - <b>/force_rebuild_m1</b> ⚠️ (ZABARDASTI M1 data fix karein)\n"
            f"  - <b>/rebuild_neon_vectors</b> ⚠️ (Neon backup data fix karein)\n"
            f"  - <b>/sync_mongo_1_to_2</b> (Sync M1 → M2)\n"
            f"  - <b>/sync_mongo_1_to_neon</b> (Sync M1 → Neon)\n"
            f"  - /cleanup_mongo_1 | /cleanup_mongo_2\n\n"
            f"<b>🗃️ Library Management</b>\n"
            f"  - <b>/remove_library_duplicates</b> ⚠️ (De-duplicate Files)\n"
            f"  - <b>/backup_channel</b> 🚀 (Backup unique files)\n"
        )
        await safe_tg_call(message.answer(admin_message), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # --- NAYA: Regular User ---
    if not await ensure_capacity_or_inform(message, db_primary, bot, redis_cache):
        return
        
    is_member = await check_user_membership(user.id, bot)
    join_markup = get_join_keyboard()
    
    if is_member:
        welcome_text = (
            f"👋 Namaste <b>{user.first_name}</b>!\n\n"
            f"Movie search bot mein aapka swagat hai. Main aapke liye library se filmein dhoondh sakta hoon.\n\n"
            f"Bas movie ka naam bhejein. Aap galat spelling (typo) bhi try kar sakte hain.\n"
            f"Example: <code>Kantara</code> ya <code>Avengers</code>"
        )
        await safe_tg_call(message.answer(welcome_text, reply_markup=None), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        welcome_text = (
            f"🎬 Namaste <b>{user.first_name}</b>!\n"
            f"Movie search bot mein swagat hai.\n\n"
            f"Bot ko istemal karne ke liye, kripya neeche diye gaye Channel aur Group join karein, phir '✅ Maine Join Kar Liya' button dabayen।"
        )
        if join_markup:
            await safe_tg_call(message.answer(welcome_text, reply_markup=join_markup), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            logger.error("User ne start kiya par koi JOIN_CHANNEL/GROUP set nahi hai.")
            await safe_tg_call(message.answer("Configuration Error: Admin ne join channels set nahi kiye hain।"), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("help"))
@handler_timeout(10)
async def help_command(message: types.Message, db_primary: Database, bot: Bot, redis_cache: RedisCacheLayer):
    user = message.from_user
    if not user: return
    # db_primary.add_user mein ab Redis hook hai
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    help_text = (
        "❓ <b>Bot Ka Istemal Kaise Karein</b>\n\n"
        "<b>1. Seedha Search:</b>\n"
        "   Movie ya Show ka naam seedha message mein bhejein.\n"
        "   Example: <code>Jawan</code>\n\n"
        "<b>2. Galat Spelling (Typo):</b>\n"
        "   Agar aap spelling galat likhte hain (e.g., <code>Avegers</code> ya <code>Sabdam</code>), toh bhi bot search kar lega.\n\n"
        "<b>3. Behtar Results Ke Liye:</b>\n"
        "   Naam ke saath saal (year) jodein.\n"
        "   Example: Example: <code>Pathaan 2023</code>\n\n"
        "---\n"
        "⚠️ <b>Server Start Hone Mein Deri?</b>\n"
        "Yeh bot free server par hai. Agar 15 min use na ho, toh server 'so' (sleep) jaata hai. Dobara /start karne par use 'jagne' (wake up) mein 10-15 second lag sakte hain. Ek baar jaagne ke baad, search hamesha fast rahegi।"
    )
    await safe_tg_call(message.answer(help_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, redis_cache: RedisCacheLayer):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User nahi mila."))
        
    await safe_tg_call(callback.answer("Checking..."))
    
    if not await ensure_capacity_or_inform(callback, db_primary, bot, redis_cache):
        return

    is_member = await check_user_membership(user.id, bot)
    
    if is_member:
        # get_concurrent_user_count pehle Redis check karega, phir Mongo
        active_users = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = (
            f"✅ Verification safal, <b>{user.first_name}</b>!\n\n"
            f"Ab aap movie search kar sakte hain - bas movie ka naam bhejein.\n\n"
            f"(Server Load: {active_users}/{CURRENT_CONC_LIMIT})"
        )
        try:
            # Rule: DO NOT rewrite existing working feature
            await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        except Exception:
            await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=None), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        await safe_tg_call(callback.answer("❌ Aapne Channel/Group join nahi kiya hai. Kripya join karke dobara try karein।", show_alert=True))
        join_markup = get_join_keyboard()
        if callback.message and (not callback.message.reply_markup or not callback.message.reply_markup.inline_keyboard):
             if callback.message.text and join_markup:
                 await safe_tg_call(callback.message.edit_reply_markup(reply_markup=join_markup))

# =======================================================
# +++++ BOT HANDLERS: NAYA HYBRID SEARCH LOGIC +++++
# =======================================================

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20)
async def search_movie_handler(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message, db_primary, bot, redis_cache):
        return
        
    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Query bahut chhoti hai. Kam se kam 2 characters likhein।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    clean_query = clean_text_for_search(original_query) # "katra"
    if not clean_query:
        await safe_tg_call(message.answer("🤔 Query mein search karne laayak kuch nahi mila।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    if not fuzzy_movie_cache:
        logger.error(f"FATAL: User {user_id} ne search kiya, lekin fuzzy cache khaali hai!")
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(message.answer("⚠️ ADMIN WARNING: Fuzzy cache khaali hai. /reload_fuzzy_cache chalayein।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            await safe_tg_call(message.answer("⚠️ Bot abhi initialize ho raha hai, kripya 30 second baad try karein।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # Predict future error: Bot blocked ya message delete
    searching_msg = await safe_tg_call(message.answer(f"🔎 <b>{original_query}</b> search ho raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not searching_msg: return

    # --- NAYA HYBRID SEARCH (Smart Sequence > Fuzzy) ---
    
    logger.info(f"User {user_id} searching for '{clean_query}'")
    
    # Rule: DO NOT add ANY blocking I/O or long waits. Use ThreadPoolExecutor.
    loop = asyncio.get_running_loop()
    # python_fuzzy_search ab original_query leta hai
    fuzzy_hits_task = loop.run_in_executor(executor, python_fuzzy_search, original_query, 15) 
    
    fuzzy_hits_raw = await fuzzy_hits_task
    if fuzzy_hits_raw is None: fuzzy_hits_raw = []

    unique_movies = {}
    for movie in fuzzy_hits_raw:
        # Exact match (score 1001) ko pehle hi dal denge
        if movie.get('imdb_id') and movie['imdb_id'] not in unique_movies:
            unique_movies[movie['imdb_id']] = movie

    # --- End Hybrid Search ---

    if not unique_movies:
        await safe_tg_call(searching_msg.edit_text(f"🥲 Maaf kijiye, '<b>{original_query}</b>' ke liye kuch nahi mila।"))
        return

    buttons = []
    max_buttons = 15
    
    final_results = list(unique_movies.values())
    
    # Priority Sort: Use the new 'score' key
    final_results.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    for movie in final_results[:max_buttons]:
        display_title = movie["title"][:50] + '...' if len(movie["title"]) > 50 else movie["title"]
        year_str = f" ({movie.get('year')})" if movie.get('year') else ""
        
        # Debugging: Show score icon
        score_icon = ""
        if movie.get('match_type') == 'exact_anchor':
            score_icon = "🎯" # Exact Match Anchor
        elif 'score' in movie and movie['score'] > 200:
            score_icon = "⚡️" # Smart Sequence match
        
        buttons.append([InlineKeyboardButton(text=f"🎬 {display_title}{year_str} {score_icon}", callback_data=f"get_{movie['imdb_id']}")])

    result_count = len(final_results)
    result_count_text = f"{result_count}" if result_count <= max_buttons else f"{max_buttons}+"
    
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> - {result_count_text} results mile:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20)
async def get_movie_callback(callback: types.CallbackQuery, bot: Bot, db_primary: Database, db_fallback: Database, redis_cache: RedisCacheLayer):
    user = callback.from_user
    if not user: 
        await safe_tg_call(callback.answer("Error: User nahi mila."))
        return
        
    await safe_tg_call(callback.answer("✅ File nikali ja rahi hai..."))
    
    if not await ensure_capacity_or_inform(callback, db_primary, bot, redis_cache):
        return

    imdb_id = callback.data.split("_", 1)[1]
    
    movie = await safe_db_call(db_primary.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)
    if not movie:
        logger.warning(f"Movie {imdb_id} not found in db_primary, checking db_fallback...")
        movie = await safe_db_call(db_fallback.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie database mein nahi mili. Ho sakta hai remove ho gayi ho।"))
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN NOTE: Movie <code>{imdb_id}</code> search mein hai par DBs mein nahi. Please run sync commands।"))
        return
        
    success = False; error_detail = "Unknown error"
    
    try:
        is_valid_for_copy = all([
            movie.get("channel_id"), movie.get("channel_id") != 0,
            movie.get("message_id"), movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER
        ])
        
        if is_valid_for_copy:
            # Predict future error: Telegram flood limit
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
            if copy_result: success = True
            elif copy_result is False: error_detail = "Bot blocked ya chat not found."
            else: error_detail = "Copying fail (timeout, ya message channel se delete ho gaya)."
        else:
            error_detail = "Cannot copy (invalid channel/message ID)."
        
        if not success:
            logger.info(f"Copy fail ({error_detail}), ab send_document (file_id) try...")
            if not movie.get("file_id"):
                 error_detail = "File ID missing, document nahi bhej sakte."
            else:
                send_result = await safe_tg_call(bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=None # Caption nahi chahiye
                ), 
                timeout=TG_OP_TIMEOUT * 4,
                semaphore=TELEGRAM_COPY_SEMAPHORE
                )
                if send_result: success = True
                elif send_result is False: error_detail += " (Bot blocked/Chat not found)"
                else: error_detail += " (Sending document by file_id failed)"
                    
    except Exception as e:
        error_detail = f"File bhejte waqt anjaani error: {e}"
        logger.error(f"Exception during send/copy {imdb_id}: {e}", exc_info=True)

    if success:
        try:
            await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> bhej di gayi hai!"))
        except Exception:
            pass 
    else:
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: <b>{movie['title']}</b> ko nahi bhej paya.\nKaaran: {error_detail}{admin_hint}"
        try:
            await safe_tg_call(callback.message.edit_text(error_text))
        except Exception:
            await safe_tg_call(bot.send_message(user.id, error_text), semaphore=TELEGRAM_COPY_SEMAPHORE)

# =======================================================
# +++++ BOT HANDLERS: ADMIN MIGRATION (FIXED for clean_title) +++++
# =======================================================

@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message, bot: Bot, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    if not message.forward_from_chat or message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0: await safe_tg_call(message.answer("❌ Migration Error: `LIBRARY_CHANNEL_ID` set nahi hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else: await safe_tg_call(message.answer(f"Migration ke liye, files ko seedha apne `LIBRARY_CHANNEL` (ID: `{LIBRARY_CHANNEL_ID}`) se forward karein।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    if not (message.video or message.document): return

    info = extract_movie_info(message.caption or "") 
    if not info or not info.get("title"):
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption parse nahi kar paya.")
        await safe_tg_call(message.answer(f"❌ Migration Skipped: MessageID `{message.forward_from_message_id}` ka caption parse nahi kar paya।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    file_data = message.video or message.document
    file_id = file_data.file_id; file_unique_id = file_data.file_unique_id
    message_id = message.forward_from_message_id
    channel_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{message_id}"
    title = info["title"]; year = info.get("year")
    
    clean_title_val = clean_text_for_search(title)
    
    # Rule: DO NOT convert async to sync or add ANY blocking I/O or long waits.
    db1_task = safe_db_call(db_primary.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    db2_task = safe_db_call(db_fallback.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    neon_task = safe_db_call(db_neon.add_movie(message_id, channel_id, file_id, file_unique_id, imdb_id, title))
    
    db1_res, db2_res, neon_res = await asyncio.gather(db1_task, db2_task, neon_task)
    
    def get_status(res):
        return "✅ Added" if res is True else ("🔄 Updated" if res == "updated" else ("ℹ️ Skipped" if res == "duplicate" else "❌ FAILED"))

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

    
    await safe_tg_call(message.answer(f"<b>{title}</b>\nDB1: {db1_status} | DB2: {db2_status} | Neon: {neon_status}"), semaphore=TELEGRAM_COPY_SEMAPHORE)


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
    imdb_id = info.get("imdb_id") or f"auto_{message.message_id}"
    title = info["title"]; year = info.get("year")
    
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: '{title}'):"
    clean_title_val = clean_text_for_search(title)
    
    db1_task = db_primary.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id)
    db2_task = db_fallback.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id)
    neon_task = db_neon.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title)
    
    async def run_tasks():
        res = await safe_db_call(db1_task)
        await safe_db_call(db2_task)
        await safe_db_call(neon_task)
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
    
    # Rule: DO NOT convert async to sync or add ANY blocking I/O
    asyncio.create_task(run_tasks())
    
    logger.info(f"{log_prefix} Teeno DBs ko sync ke liye bhej diya.")

# =======================================================
# +++++ BOT HANDLERS: ADMIN COMMANDS (3-DB LOGIC) +++++
# =======================================================

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    msg = await safe_tg_call(message.answer("📊 System Telemetry fetch ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
    mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
    mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    mongo_1_ready_task = safe_db_call(db_primary.is_ready(), default=False)
    mongo_2_ready_task = safe_db_call(db_fallback.is_ready(), default=False)
    neon_ready_task = safe_db_call(db_neon.is_ready(), default=False)
    
    # --- FIX: redis_cache.is_ready() sync है, इसे gather से बाहर call करें ---
    redis_ready = redis_cache.is_ready()
    
    user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users, mongo_1_ready, mongo_2_ready, neon_ready = await asyncio.gather(
        user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task, mongo_1_ready_task, mongo_2_ready_task, neon_ready_task
    )
    # --- END FIX ---
    
    def status_icon(is_ok): return "🟢" if is_ok else "❌"
    def count_str(c): return f"{c:,}" if c >= 0 else "Error"
        
    search_status = f"⚡️ Hybrid (Smart Sequence > Fuzzy)"
    if not mongo_1_ready: search_status += " (⚠️ M1 Down)"
    if len(fuzzy_movie_cache) == 0: search_status += " (⚠️ Fuzzy Cache Khaali Hai!)"
        
    stats_msg = (
        f"📊 <b>Bot System Stats (M+M+N+R)</b>\n\n"
        f"<b>Live Telemetry</b>\n"
        f"  - ⚡️ Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
        f"  - 👤 User Records (M1): {count_str(user_count)}\n"
        f"  - 🎬 Movies (M1): {count_str(mongo_1_count)} docs\n"
        f"  - 🗂️ Movies (M2): {count_str(mongo_2_count)} docs\n"
        f"  - 🗄️ Backup (Neon): {count_str(neon_count)} vectors\n"
        f"  - ⏱️ Uptime: {get_uptime()}\n"
        f"  - 🔄 Queue Size: {priority_queue._queue.qsize()} (Workers: {QUEUE_CONCURRENCY})\n\n"
        f"<b>Node Connectivity</b>\n"
        f"  - {status_icon(mongo_1_ready)} Node 1: Mongo-Primary (Exact Search)\n"
        f"  - {status_icon(mongo_2_ready)} Node 2: Mongo-Fallback (Data Replica)\n"
        f"  - {status_icon(neon_ready)} Node 3: Neon-Backup (File Index)\n"
        f"  - {status_icon(redis_ready)} Node 4: Redis Cache (Throttle/Fuzzy Storage)\n\n"
        f"<b>Search Logic</b>\n"
        f"  - 🧠 Fuzzy Cache: {len(fuzzy_movie_cache):,} titles loaded\n"
        f"  - <b>{search_status}</b>"
    )
    await safe_tg_call(msg.edit_text(stats_msg))


@dp.message(Command("search_switch"), AdminFilter())
@handler_timeout(10)
async def search_switch_command(message: types.Message):
    await safe_tg_call(message.answer("ℹ️ Search switch ki ab zaroorat nahi hai.\nBot ab hamesha <b>Hybrid</b> ka istemal karta hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600)
async def broadcast_command(message: types.Message, db_primary: Database):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Broadcast karne ke liye kisi message ko reply karein।"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    users = await safe_db_call(db_primary.get_all_users(), timeout=60, default=[])
    if not users:
        await safe_tg_call(message.answer("❌ Database (Mongo 1) mein koi active users nahi mile।"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
        
    total = len(users); msg = await safe_tg_call(message.answer(f"📤 Broadcast shuru... {total:,} users ko target kiya gaya।"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    start_broadcast_time = datetime.now(timezone.utc)
    success_count, failed_count = 0, 0
    tasks = []
    
    async def send_to_user(user_id: int):
        nonlocal success_count, failed_count
        # Predict future error: Bot blocked
        res = await safe_tg_call(message.reply_to_message.copy_to(user_id), timeout=10, semaphore=TELEGRAM_BROADCAST_SEMAPHORE)
        if res: success_count += 1
        elif res is False:
            failed_count += 1; await safe_db_call(db_primary.deactivate_user(user_id))
        else: failed_count += 1

    last_update_time = start_broadcast_time
    for i, user_id in enumerate(users):
        tasks.append(send_to_user(user_id))
        processed_count = i + 1
        now = datetime.now(timezone.utc)
        if processed_count % 100 == 0 or (now - last_update_time).total_seconds() > 15 or processed_count == total:
            # Rule: DO NOT add ANY blocking I/O or long waits.
            await asyncio.gather(*tasks); tasks = []
            elapsed = (now - start_broadcast_time).total_seconds()
            speed = processed_count / elapsed if elapsed > 0 else 0
            try:
                await safe_tg_call(msg.edit_text(
                    f"📤 Progress: {processed_count} / {total}\n\n"
                    f"✅ Safal: {success_count:,}\n❌ Fail/Block: {failed_count:,}\n"
                    f"⏱️ Speed: {speed:.1f} users/sec"
                ))
            except TelegramBadRequest: pass
            last_update_time = now
            
    final_text = (f"✅ Broadcast Poora Hua!\n\n"
                  f"Sent: {success_count:,}\nFailed/Blocked: {failed_count:,}\nTotal Users: {total:,}")
    await safe_tg_call(msg.edit_text(final_text))


@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120)
async def cleanup_users_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🧹 (Mongo 1) 30 din se purane inactive users ko clean kiya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    removed = await safe_db_call(db_primary.cleanup_inactive_users(days=30), timeout=90, default=0)
    new_count = await safe_db_call(db_primary.get_user_count(), default=0)
    txt = f"✅ (Mongo 1) Cleanup poora hua!\nDeactivated: {removed:,}\nAb Active: {new_count:,}"
    await safe_tg_call(msg.edit_text(txt))


@dp.message(Command("get_user"), AdminFilter())
@handler_timeout(10)
async def get_user_command(message: types.Message, db_primary: Database):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer("❌ Istemal: /get_user `USER_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    user_id_to_find = int(args[1])
    user_data = await safe_db_call(db_primary.get_user_info(user_id_to_find))
    if not user_data:
        await safe_tg_call(message.answer(f"❌ User <code>{user_id_to_find}</code> database (Mongo 1) mein nahi mila।"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    def format_dt(dt): return dt.strftime('%Y-%m-%d %H:%M:%S UTC') if dt else 'N/A'
    user_text = (
        f"<b>User Info:</b> <code>{user_data.get('user_id')}</code> (Mongo 1)\n"
        f"<b>Username:</b> @{user_data.get('username') or 'N/A'}\n"
        f"<b>First Name:</b> {user_data.get('first_name') or 'N/A'}\n"
        f"<b>Joined:</b> {format_dt(user_data.get('joined_date'))}\n"
        f"<b>Last Active:</b> {format_dt(user_data.get('last_active'))}\n"
        f"<b>Is Active:</b> {user_data.get('is_active', True)}"
    )
    await safe_tg_call(message.answer(user_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800)
async def import_json_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    if not message.reply_to_message or not message.reply_to_message.document:
        await safe_tg_call(message.answer("❌ Kripya .json file ko reply karke command dein।"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    doc = message.reply_to_message.document
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"):
        await safe_tg_call(message.answer("❌ Yeh .json file nahi hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    msg = await safe_tg_call(message.answer(f"⏳ `{doc.file_name}` download ho raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    try:
        file = await bot.get_file(doc.file_id);
        if file.file_path is None: await safe_tg_call(msg.edit_text(f"❌ File path nahi mila।")); return
        fio = io.BytesIO(); 
        # Rule: DO NOT add ANY blocking I/O. aiogram's download_file is async.
        await bot.download_file(file.file_path, fio); fio.seek(0)
        # JSON loading is CPU-bound, so use ThreadPoolExecutor
        loop = asyncio.get_running_loop()
        mlist = await loop.run_in_executor(executor, lambda: json.loads(fio.read().decode('utf-8')))
        assert isinstance(mlist, list)
    except Exception as e:
        await safe_tg_call(msg.edit_text(f"❌ Download/Parse Error: {e}")); logger.exception("JSON download/parse error"); return
    
    total = len(mlist); s, f = 0, 0
    await safe_tg_call(msg.edit_text(f"⏳ {total:,} items ko teeno DBs mein process kiya ja raha hai..."))
    start_import_time = datetime.now(timezone.utc)
    
    db1_tasks, db2_tasks, neon_tasks = [], [], []
    
    for i, item in enumerate(mlist):
        try:
            fid = item.get("file_id"); fname = item.get("title")
            if not fid or not fname: s += 1; continue
            fid_str = str(fid); file_unique_id = item.get("file_unique_id") or fid_str 
            # Predict future error: Hash collision. Using MD5 is fast but prone to collision.
            imdb = f"json_{hashlib.md5(fid_str.encode()).hexdigest()[:10]}"
            message_id = item.get("message_id") or AUTO_MESSAGE_ID_PLACEHOLDER
            channel_id = item.get("channel_id") or 0
            
            info = parse_filename(fname); 
            title = info["title"] or "Untitled"; 
            year = info["year"]
            
            clean_title_val = clean_text_for_search(title)

            db1_tasks.append(db_primary.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id))
            db2_tasks.append(db_fallback.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id))
            neon_tasks.append(db_neon.add_movie(message.message_id, channel_id, fid_str, file_unique_id, imdb, title))
            
        except Exception as e: f += 1; logger.error(f"Error processing JSON item {i+1}: {e}", exc_info=False)
        
        now = datetime.now(timezone.utc)
        if (i + 1) % 100 == 0 or (now - start_import_time).total_seconds() > 10 or (i+1) == total:
            await asyncio.gather(
                *[safe_db_call(task) for task in db1_tasks],
                *[safe_db_call(task) for task in db2_tasks],
                *[safe_db_call(task) for task in neon_tasks]
            )
            db1_tasks, db2_tasks, neon_tasks = [], [], []
            try: await safe_tg_call(msg.edit_text(f"⏳ Processed: {i+1}/{total:,} | Skipped: {s:,} | Failed: {f:,}"))
            except TelegramBadRequest: pass
            last_update_time = now
    
    await safe_tg_call(msg.edit_text(f"✅ Import Poora Hua!\nProcessed: {total-s-f:,} | Skipped: {s:,} | Failed: {f:,}"))
    # load_fuzzy_cache mein ab Redis hook hai
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("✅ Fuzzy cache import ke baad reload ho gaya hai।"))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    args = message.text.split(maxsplit=1)
    if len(args) < 2: await safe_tg_call(message.answer("❌ Istemal: /remove_dead_movie `IMDB_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    imdb_id = args[1].strip()
    msg = await safe_tg_call(message.answer(f"⏳ <code>{imdb_id}</code> ko teeno DBs se hataya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
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
    
    db1_stat = "✅ DB1" if db1_del else "❌ DB1"
    db2_stat = "✅ DB2" if db2_del else "❌ DB2"
    neon_stat = "✅ Neon" if neon_del else "❌ Neon"
    
    await safe_tg_call(msg.edit_text(f"Deletion status for <code>{imdb_id}</code>:\n{db1_stat} | {db2_stat} | {neon_stat}\n(Cache updated)"))


@dp.message(Command("cleanup_mongo_1"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 1) mein `imdb_id` duplicates dhoondhe ja rahe hain (Batch: 100)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # Rule: DO NOT convert async to sync or add ANY blocking I/O
    deleted_count, duplicates_found = await safe_db_call(db_primary.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ (Mongo 1) {deleted_count} duplicates delete kiye।\nℹ️ Baaki: {max(0, duplicates_found - deleted_count)}. Command dobara chalayein।"))
        await load_fuzzy_cache(db_primary) # Cache reload karein
    else:
        await safe_tg_call(msg.edit_text("✅ (Mongo 1) mein koi duplicates nahi mile।"))

@dp.message(Command("cleanup_mongo_2"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_2_command(message: types.Message, db_fallback: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 2) mein `imdb_id` duplicates dhoondhe ja rahe hain (Batch: 100)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    deleted_count, duplicates_found = await safe_db_call(db_fallback.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ (Mongo 2) {deleted_count} duplicates delete kiye।\nℹ️ Baaki: {max(0, duplicates_found - deleted_count)}. Command dobara chalayein।"))
    else:
        await safe_tg_call(msg.edit_text("✅ (Mongo 2) mein koi duplicates nahi mile।"))


@dp.message(Command("remove_library_duplicates"), AdminFilter())
@handler_timeout(3600)
async def remove_library_duplicates_command(message: types.Message, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("⏳ NeonDB se `file_unique_id` duplicates dhoondhe ja rahe hain... (Batch: 100)"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    messages_to_delete, total_duplicates = await safe_db_call(db_neon.find_and_delete_duplicates(batch_limit=100), default=([], 0))
    
    if not messages_to_delete:
        await safe_tg_call(msg.edit_text("✅ Library mein koi duplicate files nahi mili।"))
        return
        
    await safe_tg_call(msg.edit_text(f"✅ {total_duplicates} duplicates mile।\n⏳ Ab {len(messages_to_delete)} files ko channel se delete kiya ja raha hai..."))
    
    deleted_count, failed_count = 0, 0
    tasks = []
    
    async def delete_message(msg_id: int, chat_id: int):
        nonlocal deleted_count, failed_count
        # Predict future error: Channel se message delete ho gaya ho
        res = await safe_tg_call(bot.delete_message(chat_id=chat_id, message_id=msg_id), semaphore=TELEGRAM_DELETE_SEMAPHORE)
        if res or res is None: deleted_count += 1
        else: failed_count += 1

    for msg_id, chat_id in messages_to_delete:
        tasks.append(delete_message(msg_id, chat_id))
        
    await asyncio.gather(*tasks)
    
    await safe_tg_call(msg.edit_text(
        f"✅ Cleanup Done!\n"
        f"🗑️ Channel se Delete kiye: {deleted_count}\n"
        f"❌ Fail hue: {failed_count}\n"
        f"ℹ️ Baaki Duplicates (DB): {max(0, total_duplicates - deleted_count)}\n"
        f"⚠️ Abhi bhi {max(0, total_duplicates - deleted_count)} duplicates hain. Command dobara chalayein।\n"
        f"⚠️ Poora hone ke baad, Mongo DBs ko update karne ke liye sync commands chalayein।"
    ))


@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(7200)
async def backup_channel_command(message: types.Message, db_neon: NeonDB):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Istemal: /backup_channel `BACKUP_CHANNEL_ID_OR_USERNAME`"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    target_channel = args[1].strip()
    try:
        if not (target_channel.startswith("@") or target_channel.startswith("-100")):
             raise ValueError("Invalid target channel format.")
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ Error: {e}"), semaphore=TELEGRAM_COPY_SEMAPHORE); return

    msg = await safe_tg_call(message.answer("⏳ NeonDB se unique files ki list fetch ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    unique_files = await safe_db_call(db_neon.get_unique_movies_for_backup(), default=[])
    if not unique_files:
        await safe_tg_call(msg.edit_text("❌ NeonDB mein backup ke liye koi files nahi mili।")); return
        
    total_files = len(unique_files)
    await safe_tg_call(msg.edit_text(f"✅ {total_files:,} unique files mili।\n🚀 Ab {target_channel} par *copy* kiya ja raha hai..."))
    
    copied_count, failed_count = 0, 0
    tasks = []
    
    async def copy_file(msg_id: int, chat_id: int):
        nonlocal copied_count, failed_count
        # Predict future error: Channel se message delete ho gaya ho
        res = await safe_tg_call(
            bot.copy_message(chat_id=target_channel, from_chat_id=chat_id, message_id=msg_id),
            timeout=TG_OP_TIMEOUT * 2, semaphore=TELEGRAM_COPY_SEMAPHORE
        )
        if res: copied_count += 1
        else: failed_count += 1

    for i, (msg_id, chat_id) in enumerate(unique_files):
        tasks.append(copy_file(msg_id, chat_id))
        if (i + 1) % 50 == 0 or (i + 1) == total_files:
            await asyncio.gather(*tasks); tasks = []
            try: await safe_tg_call(msg.edit_text(f"🚀 Progress: {(i+1)} / {total_files}\n✅ Copied: {copied_count} | ❌ Failed: {failed_count}"))
            except TelegramBadRequest: pass
            await asyncio.sleep(1.0)
            
    await safe_tg_call(msg.edit_text(f"✅ Backup Poora Hua!\nTotal: {total_files} | Copied: {copied_count} | Failed: {failed_count}"))


@dp.message(Command("sync_mongo_1_to_neon"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_neon_command(message: types.Message, db_primary: Database, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 1) se sabhi movies fetch ki ja rahi hain..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    mongo_movies = await safe_db_call(db_primary.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies:
        await safe_tg_call(msg.edit_text("❌ (Mongo 1) mein sync ke liye koi movies nahi mili।")); return
    
    await safe_tg_call(msg.edit_text(f"✅ {len(mongo_movies):,} movies mili. Ab NeonDB (Backup) mein sync kiya ja raha hai..."))
    processed_count = await safe_db_call(db_neon.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    await safe_tg_call(msg.edit_text(f"✅ Sync (Mongo 1 → Neon) poora hua! {processed_count:,} movies process ki gayin।"))

@dp.message(Command("sync_mongo_1_to_2"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_2_command(message: types.Message, db_primary: Database, db_fallback: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 1) se sabhi movies fetch ki ja rahi hain..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
        
    await safe_tg_call(msg.edit_text(f"⏳ (Mongo 1) se data (full) fetch kiya ja raha hai..."))
    mongo_movies_full = await safe_db_call(db_primary.get_all_movies_for_neon_sync(), timeout=300)
    if not mongo_movies_full:
        await safe_tg_call(msg.edit_text("❌ (Mongo 1) mein sync ke liye koi movies nahi mili।")); return
        
    await safe_tg_call(msg.edit_text(f"✅ {len(mongo_movies_full):,} movies mili. Ab (Mongo 2) mein sync (bulk upsert) kiya ja raha hai..."))
    
    processed_count = 0
    tasks = []
    
    for movie in mongo_movies_full:
        processed_count += 1
        tasks.append(safe_db_call(db_fallback.add_movie(
            imdb_id=movie.get('imdb_id'),
            title=movie.get('title'),
            year=None, 
            file_id=movie.get('file_id'),
            message_id=movie.get('message_id'),
            channel_id=movie.get('channel_id'),
            clean_title=clean_text_for_search(movie.get('title')),
            file_unique_id=movie.get('file_unique_id') or movie.get('file_id')
        )))
        
        if len(tasks) >= 200:
            await asyncio.gather(*tasks)
            tasks = []
            
    if tasks:
        await asyncio.gather(*tasks)

    await safe_tg_call(msg.edit_text(f"✅ Sync (Mongo 1 → Mongo 2) poora hua! {processed_count:,} movies process (added/updated) ki gayin।"))


@dp.message(Command("rebuild_clean_titles_m1"), AdminFilter())
@handler_timeout(300)
async def rebuild_clean_titles_m1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 1) mein `clean_title` field ko rebuild kiya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    updated, total = await safe_db_call(db_primary.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    await safe_db_call(db_primary.create_mongo_text_index())
    await safe_tg_call(msg.edit_text(f"✅ (Mongo 1) Rebuild Done!\n{updated:,} titles fix kiye gaye. Total: {total:,}.\nText index rebuild ho gaya।"))
    
    # load_fuzzy_cache mein ab Redis hook hai
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("✅ Fuzzy cache bhi reload ho gaya hai।"))

@dp.message(Command("force_rebuild_m1"), AdminFilter())
@handler_timeout(900) 
async def force_rebuild_m1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("⚠️ <b>DANGER ZONE</b> ⚠️\n(Mongo 1) mein *sabhi* `clean_title` fields ko zabardasti rebuild kiya ja raha hai...\n\nIsmein 10-15 minute lag sakte hain. Kripya intezaar karein..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    updated, total = await safe_db_call(db_primary.force_rebuild_all_clean_titles(clean_text_for_search), timeout=840, default=(0,0))
    
    await safe_db_call(db_primary.create_mongo_text_index()) 
    await safe_tg_call(msg.edit_text(f"✅ (Mongo 1) FORCE REBUILD DONE!\n{updated:,} titles zabardasti fix kiye gaye. Total: {total:,}.\nText index rebuild ho gaya।"))
    
    # load_fuzzy_cache mein ab Redis hook hai
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("✅ Fuzzy cache bhi reload ho gaya hai।"))


@dp.message(Command("rebuild_clean_titles_m2"), AdminFilter())
@handler_timeout(300)
async def rebuild_clean_titles_m2_command(message: types.Message, db_fallback: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 2) mein `clean_title` field ko rebuild kiya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    updated, total = await safe_db_call(db_fallback.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    await safe_db_call(db_fallback.create_mongo_text_index()) 
    await safe_tg_call(msg.edit_text(f"✅ (Mongo 2) Rebuild Done!\n{updated:,} titles fix kiye gaye. Total: {total:,}.\nText index rebuild ho gaya।"))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT; args = message.text.split()
    if len(args)<2 or not args[1].isdigit(): await safe_tg_call(message.answer(f"Istemal: /set_limit N (Abhi: {CURRENT_CONC_LIMIT})"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    try:
        val = int(args[1]); assert 5 <= val <= 5000 
        CURRENT_CONC_LIMIT = val; await safe_tg_call(message.answer(f"✅ Concurrency limit ab {CURRENT_CONC_LIMIT} par set hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE); logger.info(f"Concurrency limit admin ne {CURRENT_CONC_LIMIT} kar diya hai।")
    except (ValueError, AssertionError): await safe_tg_call(message.answer("❌ Limit 5 aur 5000 ke beech एक number hona chahiye।"), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("rebuild_neon_vectors"), AdminFilter())
@handler_timeout(600)
async def rebuild_neon_vectors_command(message: types.Message, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("⏳ (NeonDB Backup) mein `clean_title` aur FTS Data ko rebuild kiya ja raha hai... (Jo NULL hain)"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    updated_count = await safe_db_call(db_neon.rebuild_fts_vectors(), timeout=540, default=-1)
    if updated_count >= 0:
        await safe_tg_call(msg.edit_text(f"✅ (NeonDB Backup) Rebuild Done!\n{updated_count:,} purane records ke FTS/CleanTitle vectors fix kiye gaye।"))
    else:
        await safe_tg_call(msg.edit_text("❌ (NeonDB Backup) FTS vectors ko rebuild karne mein error aaya।"))


@dp.message(Command("reload_fuzzy_cache"), AdminFilter())
@handler_timeout(300)
async def reload_fuzzy_cache_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🧠 In-Memory Fuzzy Cache ko Mongo/Redis se reload kiya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    # load_fuzzy_cache mein ab Redis hook hai
    await load_fuzzy_cache(db_primary)
    await safe_tg_call(message.answer("✅ Fuzzy cache bhi reload ho gaya hai।"))


@dp.message(Command("check_db"), AdminFilter())
@handler_timeout(15)
async def check_db_command(message: types.Message, db_primary: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    msg = await safe_tg_call(message.answer("🕵️‍♂️ Diagnostics run ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    mongo_check_task = safe_db_call(db_primary.check_mongo_clean_title(), default={"title": "Error", "clean_title": "Mongo check failed"})
    neon_check_task = safe_db_call(db_neon.check_neon_clean_title(), default={"title": "Error", "clean_title": "Neon check failed"})
    
    # --- NAYA: Fuzzy Cache ko bhi check karein ---
    fuzzy_cache_check = {"title": "N/A", "clean_title": "--- KHAALI HAI (Run /reload_fuzzy_cache) ---"}
    if fuzzy_movie_cache:
        try:
            # Cache se pehla item nikaalein
            first_key = next(iter(fuzzy_movie_cache))
            sample = fuzzy_movie_cache[first_key]
            fuzzy_cache_check = {"title": sample.get('title'), "clean_title": sample.get('clean_title')}
        except StopIteration:
            pass
        except Exception as e:
            fuzzy_cache_check = {"title": "Cache Error", "clean_title": str(e)}

    # --- NAYA: Redis Status ---
    redis_status = "❌ NOT READY"
    if redis_cache.is_ready():
        redis_status = "✅ READY"
    # --- END NAYA ---


    mongo_res, neon_res = await asyncio.gather(mongo_check_task, neon_check_task)

    if mongo_res is None: mongo_res = {"title": "Error", "clean_title": "DB not ready"}
    if neon_res is None: neon_res = {"title": "Error", "clean_title": "DB not ready"}

    reply_text = (
        f"<b>📊 Database `clean_title` Diagnostics</b>\n\n"
        f"<b>Node 1: Mongo (Exact Search)</b>\n"
        f"  - Original: <code>{mongo_res.get('title')}</code>\n"
        f"  - Cleaned: <code>{mongo_res.get('clean_title')}</code>\n\n"
        f"<b>Node 4: Redis Cache (Status: {redis_status})</b>\n"
        f"  - Cache: {len(fuzzy_movie_cache):,} titles loaded (In-memory)\n\n"
        f"<b>Cache: Python (Fuzzy Search)</b>\n"
        f"  - Original: <code>{fuzzy_cache_check.get('title')}</code>\n"
        f"  - Cleaned: <code>{fuzzy_cache_check.get('clean_title')}</code>\n\n"
        f"<b>Node 3: Neon (Backup Index)</b>\n"
        f"  - Original: <code>{neon_res.get('title')}</code>\n"
        f"  - Cleaned: <code>{neon_res.get('clean_title')}</code>\n\n"
        f"ℹ️ Agar 'Cleaned' field <b>'--- KHAALI HAI ---'</b> dikha raha hai, iska matlab aapko zaroori <b>/rebuild...</b> command dobara chalana hoga।"
    )
    await safe_tg_call(msg.edit_text(reply_text))


# =======================================================
# +++++ ERROR HANDLER +++++
# =======================================================

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
            
    error_message = "❗️ Ek anjaani error aa gayi hai. Team ko soochit kar diya gaya hai. Kripya thodi देर baad try karein।"
    if target_chat_id:
        try: await bot.send_message(target_chat_id, error_message)
        except Exception as notify_err: logger.error(f"User ko error notify karne mein bhi error: {notify_err}")
    if callback_query:
        try: await callback_query.answer("Error", show_alert=True)
        except Exception as cb_err: logger.error(f"Error callback answer karne mein error: {cb_err}")

# =======================================================
# +++++ LOCAL POLLING (Testing ke liye) +++++
# =======================================================
async def main_polling():
    logger.info("Bot polling mode mein start ho raha hai (local testing)...")
    try:
        # Redis init
        await redis_cache.init_cache()
        await db_primary.init_db()
        await db_fallback.init_db()
        await db_neon.init_db()
        # Fuzzy cache load karein (ab Redis fallback ke saath)
        await load_fuzzy_cache(db_primary) 
    except Exception as init_err:
        logger.critical(f"Local main() mein DB init fail: {init_err}", exc_info=True); return

    await bot.delete_webhook(drop_pending_updates=True)
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    setup_signal_handlers()

    # --- NEW: Start Queue Workers for Polling ---
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
        try: asyncio.run(main_polling())
        except (KeyboardInterrupt, SystemExit): logger.info("Bot polling band kar raha hai।")
    else:
        logger.error("WEBHOOK_URL set hai. Local polling nahi chalega।")
        logger.error("Run karne ke liye: uvicorn bot:app --host 0.0.0.0 --port 8000")
