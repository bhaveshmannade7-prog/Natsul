# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import signal
import json
import hashlib
from datetime import datetime, timezone # <--- FIX: `timezone` import karein
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

# --- Database aur Algolia Imports (AFTER dotenv and uvloop) ---
# Import the new MongoDB Database class
from database import Database
# Import the initialization function specifically
from algolia_client import initialize_algolia, is_algolia_ready, algolia_search, algolia_add_movie, algolia_add_batch_movies, algolia_remove_movie, algolia_sync_data, algolia_clear_index


# --- Helpers moved from old database.py ---
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

# --- FINAL FIX (v3): Smart clean_text_for_search ---
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing, preserving transliterations."""
    if not text: return ""
    
    # 1. Lowercase
    text = text.lower()
    
    # 2. Remove year patterns like (2023) or [1998]
    text = re.sub(r"[\(\[]\s*(\d{4})\s*[\)\]]", " ", text, flags=re.UNICODE)

    # 3. Remove square brackets and their content [Hindi], [1080p] etc.
    # Yeh aksar non-title tags hote hain.
    text = re.sub(r"\[.*?\]", " ", text, flags=re.UNICODE)
    
    # 4. Remove common keywords (jo ab brackets ke bahar ho sakte hain)
    text = re.sub(r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio|hevc)\b", " ", text, flags=re.IGNORECASE | re.UNICODE)

    # 5. Ab, sirf letters (Unicode), numbers, aur spaces rakhein.
    # Yeh bache hue parens '()' aur emojis '🌸' ko hata dega.
    # Yeh (Sakti) ko 'sakti' bana dega.
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    
    # 6. Remove "season" and "s01" type patterns
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text, flags=re.IGNORECASE | re.UNICODE)
    
    # 7. Normalize whitespace
    text = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip()
    
    logger.debug(f"Cleaned text: '{text}'") # Debugging ke liye
    return text
# --- End Helpers ---


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)-12s %(message)s")
logger = logging.getLogger("bot")
# Reduce log noise from libraries
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING) # For MongoDB driver
logging.getLogger("pymongo").setLevel(logging.WARNING) # For MongoDB driver
logging.getLogger("algoliasearch").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING) # Quiet access logs

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0")) # Default to 0 if not set
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0")) # Default to 0
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME") # No default needed, check later
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME") # No default needed, check later

DATABASE_URL = os.getenv("DATABASE_URL") # Yeh ab MongoDB Atlas connection string honi chahiye

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "") # Optional secret

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "") # Comma-separated list
ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

# ============ TIMEOUTS ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 10 # Slightly increased
TG_OP_TIMEOUT = 5 # Slightly increased

# ============ SEMAPHORE ============
DB_SEMAPHORE = asyncio.Semaphore(10) # Limit concurrent DB operations

# --- Critical Configuration Checks ---
if not BOT_TOKEN:
    logger.critical("Missing BOT_TOKEN environment variable! Exiting.")
    raise SystemExit(1)
if not DATABASE_URL:
    logger.critical("Missing DATABASE_URL (MongoDB Atlas connection string) environment variable! Exiting.")
    raise SystemExit(1)
if "postgresql" in DATABASE_URL or "postgres" in DATABASE_URL:
     logger.critical("DATABASE_URL looks like PostgreSQL, but this bot requires a MongoDB Atlas connection string! Exiting.")
     raise SystemExit(1)
if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID is not set or set to 0. Admin commands will not work.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID is not set or set to 0. Auto-indexing will not work.")
if not JOIN_CHANNEL_USERNAME or not USER_GROUP_USERNAME:
    logger.warning("JOIN_CHANNEL_USERNAME or USER_GROUP_USERNAME not set. Membership check might be skipped or fail.")

# --- Webhook URL ---
def build_webhook_url() -> str:
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        # Ensure correct path format
        webhook_path = f"/bot/{BOT_TOKEN}"
        # Prevent double slashes if base already ends with /bot/
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

# Initialize Database (can raise exception if connection fails)
try:
    db = Database(DATABASE_URL)
    logger.info("Database object created.")
except Exception as e:
    # This might catch errors in engine creation if Database.__init__ fails
    logger.critical(f"Failed to create Database object: {e}", exc_info=True)
    raise SystemExit("Database object creation failed.")


# --- FIX: DeprecationWarning ke liye `datetime.now(timezone.utc)` use karein ---
start_time = datetime.now(timezone.utc)

# Declare global vars for tasks/executor to be accessible in shutdown
monitor_task = None
executor = None

# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure(loop):
    """Graceful shutdown logic."""
    logger.info("Initiating graceful shutdown...")

    # 1. Cancel monitor task
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try: await asyncio.wait_for(monitor_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError): logger.warning("Monitor task cancellation timed out or already cancelled.")

    # 2. Delete Webhook (if set)
    if WEBHOOK_URL:
        try:
            # Drop pending updates on shutdown to avoid processing stale requests on restart
            delete_result = await bot.delete_webhook(drop_pending_updates=True)
            logger.info(f"Webhook delete result: {delete_result}")
        except Exception as e: logger.error(f"Webhook delete error during shutdown: {e}")

    # 3. Close Bot Session
    try:
        if bot.session: await bot.session.close()
        logger.info("Bot session closed.")
    except Exception as e: logger.error(f"Error closing bot session: {e}")

    # 4. Shutdown ThreadPoolExecutor
    if executor:
        # Give some time for running tasks, but cancel queued ones immediately
        executor.shutdown(wait=True, cancel_futures=False) # Wait=True might be better
        logger.info("ThreadPoolExecutor shut down.")

    # 5. Close MongoDB Client (new)
    try:
        if db and db.client:
            db.client.close() # motor uses .close() synchronously
            logger.info("MongoDB client connection closed.")
    except Exception as e:
        logger.error(f"Error closing MongoDB client: {e}")

    logger.info("Graceful shutdown completed.")


def handle_shutdown_signal(signum, frame):
    """Signal handler to initiate graceful shutdown."""
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    try:
        loop = asyncio.get_running_loop()
        # Schedule shutdown_procedure to run on the loop
        asyncio.ensure_future(shutdown_procedure(loop), loop=loop)
    except RuntimeError:
        logger.error("No running event loop found in signal handler.")
    except Exception as e:
        logger.error(f"Error scheduling shutdown from signal handler: {e}")

# Register signal handlers
signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                await asyncio.sleep(0) # Yield control
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} timed out after {timeout}s")
                target_chat_id = None; callback_query = None
                if args:
                    if isinstance(args[0], types.Message): target_chat_id = args[0].chat.id
                    elif isinstance(args[0], types.CallbackQuery): callback_query = args[0]; target_chat_id = callback_query.message.chat.id if callback_query.message else None
                if target_chat_id:
                    try: await bot.send_message(target_chat_id, "⚠️ Request timeout, please try again.")
                    except: pass # Ignore errors sending timeout message
                if callback_query:
                    try: await callback_query.answer("Timeout.", show_alert=False)
                    except: pass
            except Exception as e: logger.exception(f"Handler {func.__name__} error: {e}") # Log full traceback
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
         # Log full traceback for DB errors
         logger.error(f"DB error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         await db._handle_db_error(e); # Attempt to handle connection issues
         return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    try: return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError: logger.warning(f"TG timeout: {getattr(coro, '__name__', 'unknown_coro')}"); return None
    except (TelegramAPIError, TelegramBadRequest) as e:
        # Specific handling for common, non-critical errors
        if "bot was blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
            logger.info(f"TG: Bot blocked or user deactivated."); return False
        elif "chat not found" in str(e).lower() or "peer_id_invalid" in str(e).lower():
            logger.info(f"TG: Chat not found or Peer ID invalid."); return False
        elif "message is not modified" in str(e).lower():
            logger.debug(f"TG: Message not modified."); return None # Not really an error
        elif "message to delete not found" in str(e).lower():
            logger.debug(f"TG: Message to delete not found."); return None # Not really an error
        else: # Log other API/Bad Request errors as warnings
            logger.warning(f"TG Error: {e}"); return None
    except Exception as e:
        # Log unexpected errors with full traceback
        logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None


# ============ FILTERS & HELPERS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        # Ensure user object exists before accessing id
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    # FIX: Use timezone-aware datetime
    delta = datetime.now(timezone.utc) - start_time; total_seconds = int(delta.total_seconds())
    days, r = divmod(total_seconds, 86400); hours, r = divmod(r, 3600); minutes, seconds = divmod(r, 60)
    if days > 0: return f"{days}d{hours}h{minutes}m"
    if hours > 0: return f"{hours}h{minutes}m"
    return f"{minutes}m{seconds}s"

async def check_user_membership(user_id: int) -> bool:
    """Checks if user is member of required channels/groups. Returns True if check passes or is skipped."""
    if not JOIN_CHANNEL_USERNAME or not USER_GROUP_USERNAME:
        # logger.warning("Skipping membership check: Channel/Group username not configured.")
        return True # Skip check if not configured

    try:
        # Use asyncio.gather for concurrent checks
        channel_member_task = bot.get_chat_member(chat_id=f"@{JOIN_CHANNEL_USERNAME}", user_id=user_id)
        group_member_task = bot.get_chat_member(chat_id=f"@{USER_GROUP_USERNAME}", user_id=user_id)
        channel_member, group_member = await asyncio.gather(channel_member_task, group_member_task, return_exceptions=True)

        # Handle potential exceptions from gather
        valid_statuses = {"member", "administrator", "creator"}
        is_in_channel = isinstance(channel_member, types.ChatMember) and channel_member.status in valid_statuses
        is_in_group = isinstance(group_member, types.ChatMember) and group_member.status in valid_statuses

        return is_in_channel and is_in_group

    except Exception as e:
         if not isinstance(e, (TelegramBadRequest, TelegramAPIError)):
              logger.error(f"Unexpected error during membership check for {user_id}: {e}", exc_info=True)
         else:
             logger.info(f"Membership check API error for {user_id} (likely not member): {e}")
         return False # Assume not member if any check fails

def get_join_keyboard():
    buttons = []
    if JOIN_CHANNEL_USERNAME: buttons.append([InlineKeyboardButton(text="📢 Channel Join", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")])
    if USER_GROUP_USERNAME: buttons.append([InlineKeyboardButton(text="👥 Group Join", url=f"https://t.me/{USER_GROUP_USERNAME}")])
    if buttons: buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

def get_full_limit_keyboard():
    if not ALTERNATE_BOTS: return None
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str):
    """Auto-index se title, year, aur IMDB ID nikaalne ke liye."""
    if not caption: return None
    info = {}; 
    lines = caption.splitlines()
    
    # Title ko pehli line se lene ki koshish karein
    title_line = lines[0].strip() if lines else ""
    
    # Common channel/username tags ko title se hatayein (@Atoz_films4u)
    title_line = re.sub(r"(@\w+)", "", title_line).strip()
    
    # Emoji hatayein (saaf title ke liye)
    title_line = re.sub(r"[^\w\s\(\)\[\]\-]", "", title_line, flags=re.UNICODE).strip() # Sirf letters, numbers, spaces, aur brackets rakhein

    # Agar title line abhi bhi kharaab hai (e.g., sirf tags the), toh skip karein
    if not title_line:
        # Dusri line ko try karein, agar pehli kharaab thi
        title_line = lines[1].strip() if len(lines) > 1 else ""
        if not title_line: return None # Agar dono line kharaab hain, toh chhod dein
    
    # Year ko (YYYY) format mein dhoondein
    year_match = re.search(r"[\(\[]\s*(\d{4})\s*[\)\]]", title_line) # (YYYY) or [YYYY]
    year = None
    if year_match:
        year = year_match.group(1)
        # Title mein se (YYYY) ko hatayein, taaki clean_text behtar kaam kare
        title_line = title_line.replace(year_match.group(0), " ").strip()
        
    # Season pattern check karein
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]):
        title_line += " " + lines[1].strip()
        
    info["title"] = title_line # Yeh title (Sakti) jaise hisse ko bacha kar rakhega
    
    # IMDB ID poore caption mein dhoondein
    imdb = re.search(r"(tt\d{7,})", caption); 
    if imdb: info["imdb_id"] = imdb.group(1)
    
    # Year poore caption mein dhoondein (agar title se nahi mila)
    if not year:
        year_list = re.findall(r"\b(19[89]\d|20[0-2]\d)\b", caption) # 1980-2029
        if year_list: info["year"] = year_list[-1] # Aakhri waala year
    else:
        info["year"] = year

    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str]:
    """Parses a filename/title string (JSON import ke liye) to extract a clean title and year."""
    if not filename: return {"title": "Untitled", "year": None}
    
    title = filename
    year = None
    
    # 1. Extract year (YYYY)
    match_paren = re.search(r"[\(\[]\s*((19[89]\d|20[0-3]\d))\s*[\)\]]", title, flags=re.IGNORECASE) # (YYYY) or [YYYY]
    if match_paren:
        year = match_paren.group(1)
        title = title.replace(match_paren.group(0), " ").strip()
    
    # 2. File extension hatayein
    title = os.path.splitext(title)[0].strip()
        
    # 3. Remove square brackets [tags]
    title = re.sub(r"\[.*?\]", " ", title, flags=re.IGNORECASE | re.UNICODE)
    
    # 4. Parentheses (tags) ko rehne dein (e.g., (Sakti))
    # Lekin common keywords ko hata dein
    title = re.sub(r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio|hevc)\b", " ", title, flags=re.IGNORECASE | re.UNICODE)
    
    # 5. Basic cleaning (dots/underscores)
    title = re.sub(r'[._]', ' ', title).strip()
    
    # 6. Normalize whitespace
    title = re.sub(r"\s+", " ", title, flags=re.UNICODE).strip()
    
    # 7. Fallback agar title poora clean ho gaya
    if not title:
        title = os.path.splitext(filename)[0].strip()
        title = re.sub(r'[._]', ' ', title).strip()
        title = re.sub(r"[\(\[]\s*(\d{4})\s*[\)\]]", " ", title, flags=re.UNICODE) # Year hatayein
        title = re.sub(r"\s+", " ", title, flags=re.UNICODE).strip()

    return {"title": title or "Untitled", "year": year}

def overflow_message(active_users: int) -> str: return f"⚠️ Server Capacity Reached ({active_users}/{CURRENT_CONC_LIMIT}). Nayi requests hold par hain.\nAlternate bots use karein:"

# ============ EVENT LOOP MONITOR (No changes) ============
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
    # Setup Executor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop(); loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized.")

    # Initialize Database (Must succeed or app shouldn't start)
    try:
        await db.init_db() # This now includes connection test and index creation
        logger.info("Database (MongoDB) initialization successful.")
    except Exception as e:
        logger.critical(f"FATAL: Database (MongoDB) initialization failed during startup: {e}", exc_info=True)
        # Raising exception here will prevent FastAPI from starting fully
        raise RuntimeError("Database connection failed on startup.") from e

    # Initialize Algolia (Best effort, app can run without it but degraded)
    try:
        # Yahaan algolia_client.py ka naya function call hoga
        algolia_ok = await initialize_algolia() 
        if algolia_ok: logger.info("Algolia initialization successful via lifespan. (StopWords=False)")
        else: logger.critical("ALGOLIA INITIALIZATION FAILED. Search will be unavailable.")
    except Exception as e: logger.critical(f"Error during Algolia initialization via lifespan: {e}", exc_info=True)

    # Start Monitor
    monitor_task = asyncio.create_task(monitor_event_loop()); logger.info("Event loop monitor started.")

    # Set Webhook
    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            if not current_webhook or current_webhook.url != WEBHOOK_URL:
                 logger.info(f"Attempting to set webhook to {WEBHOOK_URL}...")
                 set_result = await bot.set_webhook(url=WEBHOOK_URL, allowed_updates=dp.resolve_used_update_types(), secret_token=(WEBHOOK_SECRET or None), drop_pending_updates=True)
                 logger.info(f"Webhook set result: {set_result}")
            else: logger.info("Webhook already set correctly.")
        except Exception as e: logger.error(f"Webhook setup error: {e}", exc_info=True)
    else: logger.warning("WEBHOOK_URL not set. Running without webhook (likely polling).")

    logger.info("Application startup sequence complete.")
    yield # App runs
    logger.info("Application shutdown sequence starting...")
    # Cleanup is handled by shutdown_procedure called via signals


app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK / HEALTH / CAPACITY ============
async def _process_update_safe(update_obj: Update):
    """Safely process a single update, catching exceptions."""
    try: await dp.feed_update(bot=bot, update=update_obj)
    except Exception as e: logger.exception(f"Error processing update {update_obj.update_id}: {e}") # Log full traceback

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    """Main webhook endpoint."""
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token.")
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        telegram_update = Update(**update)
        # Schedule the update processing in the background
        background_tasks.add_task(_process_update_safe, telegram_update)
        return {"ok": True}
    except Exception as e: # Catch potential pydantic validation errors
        logger.error(f"Webhook could not parse update: {e}", exc_info=False) # Log less verbosely
        logger.debug(f"Failed update data: {update}") # Log raw data on debug level
        return {"ok": False, "error": f"Invalid update format: {e}"}

@app.get("/")
@app.get("/ping") # Add explicit ping endpoint
async def ping(): return {"status": "ok", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    # Perform checks concurrently
    db_check_task = safe_db_call(db.get_movie_count(), default=-1)
    algolia_check = is_algolia_ready()
    db_ok_raw = await db_check_task

    db_ok = isinstance(db_ok_raw, int) and db_ok_raw >= 0 # MongoDB count should be 0 or more, -1 indicates error

    status_code = 200
    status_msg = "ok"
    if not db_ok: status_msg = "error_db"; status_code = 503
    elif not algolia_check: status_msg = "degraded_algolia"; status_code = 200 # Degraded but still ok

    return {
        "status": status_msg,
        "database_connected": db_ok,
        "algolia_connected": algolia_check,
        "uptime": get_uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat() # FIX: Use timezone-aware datetime
    }


async def ensure_capacity_or_inform(message_or_callback: types.Message | types.CallbackQuery) -> bool:
    """Check capacity, update user activity, inform if full."""
    user = message_or_callback.from_user
    if not user: return True # Should not happen with user messages/callbacks

    target_chat_id = None
    if isinstance(message_or_callback, types.Message): target_chat_id = message_or_callback.chat.id
    elif isinstance(message_or_callback, types.CallbackQuery) and message_or_callback.message: target_chat_id = message_or_callback.message.chat.id

    # Update user activity first
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))

    # Admins bypass capacity limits
    if user.id == ADMIN_USER_ID: return True

    # Check concurrent users
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=CURRENT_CONC_LIMIT + 1)

    if active >= CURRENT_CONC_LIMIT:
        logger.warning(f"Capacity reached: {active}/{CURRENT_CONC_LIMIT}. User {user.id} request held.")
        # Only send message if we have a target chat
        if target_chat_id:
            await safe_tg_call(bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()))
        # Answer callback query appropriately
        if isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server busy, please use alternate bots or try again later.", show_alert=False))
        return False # Indicate capacity limit reached

    return True # Capacity available

# ============ BOT HANDLERS ============

@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user = message.from_user
    if not user: return # Ignore if no user somehow
    user_id = user.id

    bot_info = await safe_tg_call(bot.get_me(), timeout=5)
    bot_username = bot_info.username if bot_info else "Bot"

    # Update user in DB (safe_db_call handles errors)
    await safe_db_call(db.add_user(user_id, user.username, user.first_name, user.last_name))

    # --- Admin Panel ---
    if user_id == ADMIN_USER_ID:
        # Perform checks concurrently
        user_count_task = safe_db_call(db.get_user_count(), default=0)
        movie_count_task = safe_db_call(db.get_movie_count(), default=-1)
        concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        algolia_ready = is_algolia_ready()

        user_count, movie_count_raw, concurrent_users = await asyncio.gather(
            user_count_task, movie_count_task, concurrent_users_task
        )

        movie_count_str = f"{movie_count_raw:,}" if movie_count_raw >= 0 else "DB Error"
        algolia_status = "🟢 Connected" if algolia_ready else "❌ NOT CONNECTED"

        admin_message = (
            f"👑 <b>Admin: @{bot_username}</b> (MongoDB)\n\n"
            f"<b>Status</b>\n"
            f"🟢 Active ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}\n"
            f"👥 Users: {user_count:,}\n"
            f"🎬 Movies: {movie_count_str}\n"
            f"⚡️ Algolia: <b>{algolia_status}</b>\n"
            f"⏰ Uptime: {get_uptime()}\n\n"
            f"<b>Commands</b>\n"
            f"/stats | /health\n"
            f"/broadcast (Reply)\n"
            f"/import_json (Reply)\n"
            f"/add_movie (Reply: `imdb|title|year`)\n"
            f"/remove_dead_movie `IMDB_ID`\n"
            f"<b>/sync_algolia</b> ⚠️ (Full DB->Algolia)\n"
            f"<b>/rebuild_index</b> (DB Only - Mongo)\n"
            f"/cleanup_users (Inactive >30d)\n"
            f"/export_csv `users|movies` `[limit]`\n"
            f"/set_limit `N` (5-200)"
        )
        await safe_tg_call(message.answer(admin_message))
        return

    # --- Regular User ---
    if not await ensure_capacity_or_inform(message): return # Check capacity after admin check

    # Check membership status
    is_member = await check_user_membership(user_id)
    join_markup = get_join_keyboard()

    if is_member:
        welcome_text = (
            f"🎬 Namaste <b>{user.first_name}</b>!\n"
            f"Movie search bot. Naam bhejein (galat spelling bhi chalegi).\n"
            f"Example: <code>Kantara 2022</code> ya <code>Kantra</code>\n\n"
            f"⚠️ Free server start hone mein 10-15 sec lag sakte hain agar inactive tha."
        )
        # Don't show join keyboard if already member
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
@handler_timeout(1S5)
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
        "4.  <b>Dual Script:</b> Agar movie Hindi naam (e.g., `शक्ति`) se hai, toh aap `Sakti` (Latin) se bhi search kar sakte hain, agar admin ne title sahi se post kiya hai (e.g., `शक्ति (Sakti)`).\n\n"
        "⚠️ <b>Start Hone Mein Deri?</b>\n"
        "Yeh bot free server par hai. Agar 15 min use na ho, toh server 'so' jaata hai. Dobara /start karne par use 'jagne' mein 10-15 second lag sakte hain."
    )
    await safe_tg_call(message.answer(help_text))


@dp.callback_query(F.data == "check_join")
@handler_timeout(20) # Slightly longer for potential API calls
async def check_join_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))

    await safe_tg_call(callback.answer("Verifying..."))

    # Ensure capacity before giving access
    if not await ensure_capacity_or_inform(callback): return

    # BYPASS: Always give access when user clicks "I Joined" button
    # No actual membership verification performed
    active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    success_text = (
        f"✅ Verification successful, <b>{user.first_name}</b>!\n\n"
        f"Ab aap movie search kar sakte hain - bas movie ka naam bhejein.\n\n"
        f"(Server Load: {active_users}/{CURRENT_CONC_LIMIT})"
    )
    # Try editing the original message first
    edited = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
    # If editing fails (e.g., message too old), send a new message
    if not edited: await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=None))


# =======================================================
# +++++ ALGOLIA SEARCH HANDLER +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(10) # Algolia should be fast
async def search_movie_handler(message: types.Message):
    user = message.from_user
    if not user: return
    user_id = user.id

    # 1. BYPASS: Skip membership check - users can search directly
    # 2. Check Capacity & Update Activity
    if not await ensure_capacity_or_inform(message): return

    # 3. Validate Query
    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Query kam se kam 2 characters ki honi chahiye."))
        return

    # 4. Check Algolia Status
    if not is_algolia_ready():
        logger.error(f"User {user_id} search failed: Algolia service is not ready.")
        await safe_tg_call(message.answer("❌ Search Engine abhi kaam nahi kar raha hai. Kripya baad mein try karein."))
        return

    # 5. Inform User & Perform Search
    searching_msg = await safe_tg_call(message.answer(f"⚡️ <b>{original_query}</b> search ho raha hai... (Algolia Fast Search)"))
    if not searching_msg: return # Failed to send message

    search_results = await algolia_search(original_query, limit=20) # Use the wrapper

    # 6. Handle Results
    if not search_results:
        await safe_tg_call(searching_msg.edit_text(f"🥲 Maaf kijiye, '<b>{original_query}</b>' ke liye kuch nahi mila.\n(Spelling mistakes check kar li gayi hain)."))
        return

    # 7. Display Results
    buttons = []
    # Limit results shown in buttons to avoid Telegram limits (e.g., max 10-15 buttons)
    max_buttons = 15
    for movie in search_results[:max_buttons]:
        # Ensure title is not excessively long
        display_title = movie["title"][:50] + '...' if len(movie["title"]) > 50 else movie["title"]
        buttons.append([InlineKeyboardButton(text=display_title, callback_data=f"get_{movie['imdb_id']}")])

    result_count_text = f"{len(search_results)}" if len(search_results) <= max_buttons else f"{max_buttons}+"
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> - {result_count_text} results mile:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(15) # Timeout for getting file details and initiating send
async def get_movie_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User not found."))

    await safe_tg_call(callback.answer("File bheji ja rahi hai...")) # Quick feedback

    # Check capacity again before fetching from DB/sending file
    if not await ensure_capacity_or_inform(callback): return

    imdb_id = callback.data.split("_", 1)[1]

    # Fetch movie details from DB (MongoDB)
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie database mein nahi mili. Ho sakta hai remove ho gayi ho."))
        # Optional: Ask admin to sync if they triggered this
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN NOTE: Movie <code>{imdb_id}</code> Algolia mein hai par DB mein nahi. Please run /sync_algolia to fix."))
        return

    # Inform user that sending is starting (edit previous message)
    await safe_tg_call(callback.message.edit_text(f"✅ Preparing to send <b>{movie['title']}</b>..."))

    success = False
    error_detail = "Unknown error"

    # --- Attempt to send/forward ---
    try:
        # Try forwarding first if channel_id and message_id are valid
        is_valid_forward = movie.get("channel_id") and movie.get("channel_id") != 0 and \
                           movie.get("message_id") and movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER

        if is_valid_forward:
            logger.debug(f"Attempting forward for {imdb_id} from {movie['channel_id']}:{movie['message_id']}")
            fwd_result = await safe_tg_call(bot.forward_message(
                chat_id=user.id,
                from_chat_id=int(movie["channel_id"]), # Ensure int
                message_id=movie["message_id"],
            ), timeout=TG_OP_TIMEOUT * 2) # Slightly longer timeout for forward

            if fwd_result: success = True
            elif fwd_result is False: error_detail = "Bot blocked or chat not found."
            else: error_detail = "Forwarding failed (timeout or API error)."
        else:
            error_detail = "Cannot forward (invalid channel/message ID)." # Reason for not attempting forward

        # Fallback to send_document if forwarding was not attempted or failed
        if not success:
            logger.info(f"Forward failed or skipped ({error_detail}), falling back to send_document for {imdb_id} using file_id.")
            if not movie.get("file_id"):
                 error_detail = "File ID missing, cannot send document."
            else:
                send_result = await safe_tg_call(bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'N/A'})"
                ), timeout=TG_OP_TIMEOUT * 4) # Even longer timeout for sending file by ID

                if send_result: success = True
                elif send_result is False: error_detail += " (Bot blocked/Chat not found on send_doc)"
                else: error_detail += " (Sending document by file_id failed)"

    except Exception as e:
        error_detail = f"Unexpected error during send/forward: {e}"
        logger.error(f"Exception during send/forward for {imdb_id}: {e}", exc_info=True)
    # --- End Attempt ---

    # --- Final User Notification ---
    if not success:
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: <b>{movie['title']}</b> ko nahi bhej paya.\nReason: {error_detail}{admin_hint}"
        # Send error as a new message, don't edit the "Preparing..." message
        await safe_tg_call(bot.send_message(user.id, error_text))
        # Optionally edit the original result message to show failure
        try: await safe_tg_call(callback.message.edit_text(f"❌ Failed to send <b>{movie['title']}</b>."))
        except: pass # Ignore if original message edit fails


# --- ADMIN COMMANDS (Stable if/else syntax) ---

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    user_count_task = safe_db_call(db.get_user_count(), default=0)
    movie_count_task = safe_db_call(db.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    algolia_ready = is_algolia_ready()
    user_count, movie_count_raw, concurrent_users = await asyncio.gather(user_count_task, movie_count_task, concurrent_users_task)
    movie_count_str = f"{movie_count_raw:,}" if movie_count_raw >= 0 else "DB Error"
    algolia_status = "🟢 Connected" if algolia_ready else "❌ NOT CONNECTED"
    stats_msg = f"📊 Stats (MongoDB)\n🟢 Active({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}\n👥 Users: {user_count:,}\n🎬 Movies: {movie_count_str}\n⚡️ Algolia: <b>{algolia_status}</b>\n⏰ Uptime: {get_uptime()}"
    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(1800) # 30 min
async def broadcast_command(message: types.Message):
    if not message.reply_to_message: await safe_tg_call(message.answer("❌ Reply to msg.")); return
    users = await safe_db_call(db.get_all_users(), timeout=60, default=[])
    if not users: await safe_tg_call(message.answer("❌ No users.")); return
    total = len(users); s, f = 0, 0; msg = await safe_tg_call(message.answer(f"📤 Broadcasting to {total:,}..."))
    st = datetime.now(timezone.utc) # FIX: Use timezone-aware datetime
    tasks = []
    processed_count = 0

    async def send_to_user(uid):
        nonlocal s, f
        res = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=5) # Increased timeout
        if res: s += 1
        elif res is False: f += 1; await safe_db_call(db.deactivate_user(uid)) # Deactivate on block/not found
        else: f += 1 # Other fails

    for i, uid in enumerate(users):
        tasks.append(send_to_user(uid))
        processed_count += 1
        # Update status periodically or when batch is full
        now = datetime.now(timezone.utc) # FIX: Use timezone-aware datetime
        if processed_count % 100 == 0 or (now - st).total_seconds() > 10 or processed_count == total:
            if tasks: # Process the current batch
                await asyncio.gather(*tasks)
                tasks = [] # Reset batch
            if msg:
                try: await safe_tg_call(msg.edit_text(f"📤 Progress: {processed_count}/{total}\n✅ Sent: {s:,} | ❌ Failed: {f:,}"))
                except TelegramBadRequest: pass # Ignore "message is not modified"
            st = now
            if processed_count < total: await asyncio.sleep(1) # Sleep between batches

    txt = f"✅ Broadcast Done!\nSent: {s:,}\nFailed/Blocked: {f:,}\nTotal: {total:,}"
    if msg:
        await safe_tg_call(msg.edit_text(txt))
    else:
        await safe_tg_call(message.answer(txt))


@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120)
async def cleanup_users_command(message: types.Message):
    msg = await safe_tg_call(message.answer("🧹 Cleaning inactive users (>30 days)..."))
    removed = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=90, default=0)
    new_count = await safe_db_call(db.get_user_count(), default=0)
    txt = f"✅ Cleanup done!\nDeactivated: {removed:,}\nNow Active: {new_count:,}"
    if msg:
        await safe_tg_call(msg.edit_text(txt))
    else:
        await safe_tg_call(message.answer(txt))

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(20)
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document): await safe_tg_call(message.answer("❌ Reply to file: `/add_movie imdb|title|year`")); return
    try:
        parts = message.text.split("|", 2); imdb_id = parts[0].split(" ", 1)[1].strip(); title = parts[1].strip(); year = parts[2].strip() if len(parts) > 2 else None
        rpl = message.reply_to_message; file_id = rpl.video.file_id if rpl.video else rpl.document.file_id; msg_id = rpl.message_id; chan_id = rpl.chat.id
        if not imdb_id or not title: raise ValueError("Missing IMDB ID or Title")
    except Exception as e: await safe_tg_call(message.answer(f"❌ Format Error: {e}\nUse: `/add_movie imdb|title|year`")); return
    
    msg = await safe_tg_call(message.answer(f"⏳ Processing '<b>{title}</b>'..."))
    
    # --- FIX: Use the *new* (v3) clean_text_for_search ---
    clean_title_val = clean_text_for_search(title)
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, msg_id, chan_id, clean_title_val))
    
    db_map = {True: "✅ Added DB.", "updated": "✅ Updated DB.", "duplicate": "⚠️ Duplicate DB.", False: "❌ DB Error."}; db_stat = db_map.get(db_res, "❌ DB Error.")
    ag_stat = ""
    if db_res in [True, "updated"]: # Only sync if added or updated in DB
        ag_data = {'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val} # FIX: clean_title add karein
        ag_res = await algolia_add_movie(ag_data)
        ag_stat = "✅ Algolia Synced." if ag_res else "❌ Algolia Sync FAIL!"
    elif db_res == "duplicate":
        ag_stat = "ℹ️ Algolia Skipped (DB duplicate)."
    
    txt = f"{db_stat}\n{ag_stat}".strip()
    if msg:
        await safe_tg_call(msg.edit_text(txt))
    else:
        await safe_tg_call(message.answer(txt))


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800) # 30 min
async def import_json_command(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.document: await safe_tg_call(message.answer("❌ Reply to .json file.")); return
    doc = message.reply_to_message.document;
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"): await safe_tg_call(message.answer("❌ Must be .json file.")); return
    msg = await safe_tg_call(message.answer(f"⏳ Downloading `{doc.file_name}`..."));
    if not msg: return
    try:
        file = await bot.get_file(doc.file_id)
        fio = io.BytesIO()
        # Download file directly using await (bot.download_file is async)
        await bot.download_file(file.file_path, fio)
        fio.seek(0)
        mlist = json.loads(fio.read().decode('utf-8'))
        assert isinstance(mlist, list)
        logger.info(f"JSON Downloaded: {doc.file_name}, Items: {len(mlist)}")
    except Exception as e: await safe_tg_call(msg.edit_text(f"❌ Download/Parse Error: {e}")); logger.exception("JSON download/parse error"); return
    
    total = len(mlist); a, u, s, fdb = 0, 0, 0, 0; ag_batch = []; st = datetime.now(timezone.utc) # FIX: Use timezone-aware datetime
    await safe_tg_call(msg.edit_text(f"⏳ Processing {total:,} items (DB)..."))
    
    for i, item in enumerate(mlist):
        try:
            # === MODIFICATION FOR NEW JSON STRUCTURE ===
            
            # 1. Get file_id and title (filename)
            fid = item.get("file_id")
            fname = item.get("title") # Use 'title' field as per image
            
            # --- FIX: file_id integer ho sakta hai ---
            if not fid or not fname: 
                s += 1; continue # Skip if no file_id or title
            
            fid_str = str(fid) # file_id ko hamesha string banayein
            # --- END FIX ---

            # 2. Generate our own IMDB ID based on file_id, ignore JSON's imdb_id
            imdb = f"json_{hashlib.md5(fid_str.encode()).hexdigest()[:10]}" # Shorten hash
            
            # 3. Get message_id and channel_id from JSON
            message_id = item.get("message_id") or AUTO_MESSAGE_ID_PLACEHOLDER
            channel_id = item.get("channel_id") or 0
            
            # 4. Parse the title string (fname) to get a clean title and year
            # --- FIX: Use the *new* (v3) parse_filename ---
            info = parse_filename(fname); 
            title = info["title"] or "Untitled"; 
            year = info["year"] # This will extract "2023" etc.
            
            # 5. Create clean_title for search
            # --- FIX: Use the *new* (v3) clean_text_for_search ---
            clean_title_val = clean_text_for_search(title) 
            if not clean_title_val: # Fallback agar sab kuch clean ho gaya
                clean_title_val = title.lower()
            
            # === END OF MODIFICATION ===
            
            # Call DB add/update
            db_res = await safe_db_call(db.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val))
            
            # Prepare data for Algolia batch
            ag_data = {'objectID': imdb, 'imdb_id': imdb, 'title': title, 'year': year, 'clean_title': clean_title_val} # FIX: clean_title add karein
            if db_res is True: a += 1; ag_batch.append(ag_data)
            elif db_res == "updated": u += 1; ag_batch.append(ag_data)
            elif db_res == "duplicate": s += 1
            else: fdb += 1
        except Exception as e: fdb += 1; logger.error(f"Error processing JSON item {i+1}: {e}", exc_info=False); logger.debug(f"Failed item data: {item}")
        
        # Update status periodically
        now = datetime.now(timezone.utc); # FIX: Use timezone-aware datetime
        if (i + 1) % 200 == 0 or (now - st).total_seconds() > 15 or (i+1) == total:
            try: await safe_tg_call(msg.edit_text(f"⏳ DB: {i+1}/{total:,} | ✅A:{a:,} 🔄U:{u:,} ↷S:{s:,} ❌F:{fdb:,}"))
            except TelegramBadRequest: pass # Ignore "not modified"
            st = now; await asyncio.sleep(0.05) # Small sleep
    
    db_sum = f"DB Done: ✅Added:{a:,} 🔄Updated:{u:,} ↷Skipped:{s:,} ❌Failed:{fdb:,}";
    await safe_tg_call(msg.edit_text(f"{db_sum}\n⏳ Algolia Syncing {len(ag_batch):,} items..."))
    
    ag_stat = ""
    if ag_batch:
        ag_res = await algolia_add_batch_movies(ag_batch) # Use the wrapper
        ag_stat = f"✅ Algolia Synced: {len(ag_batch):,}" if ag_res else "❌ Algolia Sync FAILED!"
    else: ag_stat = "ℹ️ Algolia: Nothing to sync."
    
    await safe_tg_call(msg.edit_text(f"✅ Import Complete!\n{db_sum}\n{ag_stat}"))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split(maxsplit=1);
    if len(args) < 2: await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID")); return
    imdb_id = args[1].strip(); msg = await safe_tg_call(message.answer(f"⏳ Removing <code>{imdb_id}</code>..."))
    
    # Get title before potential DB delete
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id));
    db_del = await safe_db_call(db.remove_movie_by_imdb(imdb_id)) # Returns True/False
    
    db_stat = f"✅ DB Removed '{movie['title'] if movie else imdb_id}'." if db_del else ("ℹ️ DB Not found." if not movie else "❌ DB Error removing.")
    
    # Always attempt Algolia delete
    ag_del = await algolia_remove_movie(imdb_id) # Returns True/False/None
    ag_stat = "✅ Algolia Removed." if ag_del else "ℹ️ Algolia Not Found or Error."
    
    txt = f"{db_stat}\n{ag_stat}";
    if msg:
        await safe_tg_call(msg.edit_text(txt))
    else:
        await safe_tg_call(message.answer(txt))


@dp.message(Command("sync_algolia"), AdminFilter())
@handler_timeout(1800) # 30 min
async def sync_algolia_command(message: types.Message):
    if not is_algolia_ready(): await safe_tg_call(message.answer("❌ Algolia is not connected. Check logs.")); return
    msg = await safe_tg_call(message.answer("⚠️ Full Algolia Sync Started...\n⏳ Fetching all movies from DB (this may take a minute)..."))
    if not msg: return
    try:
        # Fetch all required movie data from DB
        # --- FIX: Yeh ab database.py ke *sahi* (v3) clean_text function ko use karega (agar zaroorat padi) ---
        all_movies_db = await safe_db_call(db.get_all_movies_for_sync(), timeout=300) # 5 min timeout
        if all_movies_db is None: # Explicit check for None which indicates DB error
            await safe_tg_call(msg.edit_text("❌ Error fetching movies from DB. Sync cancelled."))
            return
        db_count = len(all_movies_db)
        await safe_tg_call(msg.edit_text(f"✅ Fetched {db_count:,} movies from DB.\n⏳ Syncing to Algolia (replace all)... This may take several minutes."))
        # Perform the sync using replace_all_objects
        success, total_uploaded = await algolia_sync_data(all_movies_db) # Use the wrapper
        final_text = f"✅ Sync Complete! {total_uploaded:,} records replaced in Algolia index." if success else "❌ Sync Failed! Check logs for details."
        await safe_tg_call(msg.edit_text(final_text))
    except Exception as e:
        logger.error(f"Unexpected error during /sync_algolia: {e}", exc_info=True)
        await safe_tg_call(msg.edit_text(f"❌ Sync Command Error: {e}"))


@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300) # 5 min
async def rebuild_index_command(message: types.Message):
    msg = await safe_tg_call(message.answer("🔧 Rebuilding `clean_title` index in MongoDB... (DB Only)"))
    if not msg: return
    # --- FIX: Yeh *bot.py* ke naye, sahi (v3) function ko pass karega ---
    updated, total = await safe_db_call(db.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0)) # 4 min timeout for DB operation
    result_text = f"✅ DB Reindex done: Found and updated {updated:,} missing clean_titles. Total movies: {total:,}."
    if msg:
        await safe_tg_call(msg.edit_text(result_text))
    else:
        await safe_tg_call(message.answer(result_text))


@dp.message(Command("export_csv"), AdminFilter())
@handler_timeout(120) # 2 min
async def export_csv_command(message: types.Message):
    args = message.text.split(); kind = args[1].lower() if len(args)>1 else None; limit = int(args[2]) if len(args)>2 and args[2].isdigit() else 2000
    if kind not in ("users", "movies"): await safe_tg_call(message.answer("❌ Use: /export_csv users|movies [limit]")); return
    msg = await safe_tg_call(message.answer(f"⏳ Exporting max {limit:,} {kind}..."));
    if not msg: return
    try:
        if kind == "users":
            rows = await safe_db_call(db.export_users(limit=limit), timeout=90, default=[]) # 90 sec timeout
            if not rows: await safe_tg_call(msg.edit_text(f"❌ No {kind} found.")); return
            header = "user_id,username,first_name,last_name,joined_date,last_active,is_active\n"
            def format_user_row(r):
                fn = r['first_name'].replace('"', '""') if r['first_name'] else ''
                ln = r['last_name'].replace('"', '""') if r['last_name'] else ''
                return f"{r['user_id']},\"{r['username'] or ''}\",\"{fn}\",\"{ln}\",{r['joined_date']},{r['last_active']},{r['is_active']}"
            csv_data = header + "\n".join([format_user_row(r) for r in rows])
            filename = "users_export.csv"
        else: # movies
            rows = await safe_db_call(db.export_movies(limit=limit), timeout=90, default=[]) # 90 sec timeout
            if not rows: await safe_tg_call(msg.edit_text(f"❌ No {kind} found.")); return
            header = "imdb_id,title,year,channel_id,message_id,added_date\n"
            def format_movie_row(r):
                title = r['title'].replace('"', '""') if r['title'] else ''
                return f"{r['imdb_id']},\"{title}\",{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}"
            csv_data = header + "\n".join([format_movie_row(r) for r in rows])
            filename = "movies_export_db.csv"

        file_bytes = csv_data.encode("utf-8")
        if len(file_bytes) > 50 * 1024 * 1024: # Check if file > 50MB (Telegram limit)
             await safe_tg_call(msg.edit_text(f"❌ Export failed: File size exceeds 50MB Telegram limit ({len(file_bytes)/1024/1024:.1f}MB). Please use a smaller limit."))
             return

        input_file = BufferedInputFile(file_bytes, filename=filename)
        await safe_tg_call(message.answer_document(input_file, caption=f"{kind.capitalize()} export ({len(rows):,} rows)"))
        await safe_tg_call(msg.delete()) # Delete "Exporting..." message
    except Exception as e:
        logger.error(f"Export CSV Error: {e}", exc_info=True)
        await safe_tg_call(msg.edit_text(f"❌ Export Error: {e}"))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT; args = message.text.split();
    if len(args)<2 or not args[1].isdigit(): await safe_tg_call(message.answer(f"Use: /set_limit N (Current: {CURRENT_CONC_LIMIT})")); return
    try:
        val = int(args[1]); assert 5 <= val <= 200 # Set reasonable bounds
        CURRENT_CONC_LIMIT = val; await safe_tg_call(message.answer(f"✅ Concurrency limit set to {CURRENT_CONC_LIMIT}")); logger.info(f"Concurrency limit changed to {CURRENT_CONC_LIMIT} by admin.")
    except (ValueError, AssertionError): await safe_tg_call(message.answer("❌ Limit must be a number between 5 and 200."))

# --- AUTO INDEXING ---
@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    # Check if auto-indexing is enabled via channel ID
    if message.chat.id != LIBRARY_CHANNEL_ID or LIBRARY_CHANNEL_ID == 0: return
    # Check for video or document
    if not (message.video or message.document): return

    # --- FIX: Use the *new* (v3) extract_movie_info ---
    info = extract_movie_info(message.caption or "");
    # Require at least a title for indexing
    if not info or not info.get("title"):
        # Log skipped message only if caption was present but unparseable
        if message.caption: logger.warning(f"Auto-Index Skip (MsgID {message.message_id}): Could not parse title/info from caption: '{message.caption[:50]}...'")
        return

    file_id = message.video.file_id if message.video else message.document.file_id
    # Use extracted IMDB ID or generate one if missing
    imdb_id = info.get("imdb_id") or f"auto_{message.message_id}"
    title=info["title"]; year=info.get("year")
    
    # --- FIX: Use the *new* (v3) clean_text_for_search ---
    clean_title_val = clean_text_for_search(title)
    
    # Agar clean title khali hai, toh original title ka istemaal karein (fallback)
    if not clean_title_val:
        clean_title_val = title.lower() # Kam se kam kuch toh save karein

    # Perform DB operation (add or update)
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val))
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: '{title}'):"

    # Log DB result and sync to Algolia if successful
    if db_res in [True, "updated"]:
        status = 'Added' if db_res is True else 'Updated'
        logger.info(f"{log_prefix} DB {status}.")
        # Prepare data and sync to Algolia
        algolia_data = {'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year, 'clean_title': clean_title_val} # FIX: clean_title add karein
        ag_ok = await algolia_add_movie(algolia_data) # Use the wrapper
        logger.info(f"{log_prefix} Algolia Sync {'OK' if ag_ok else 'FAILED'}.")
    elif db_res == "duplicate":
        logger.warning(f"{log_prefix} DB Skipped (duplicate).")
    else: # False or None indicates error
        logger.error(f"{log_prefix} DB Operation FAILED.")


# --- ERROR HANDLER ---
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    """General error handler for dispatcher."""
    # Log the full exception traceback
    logger.exception(f"Unhandled error during update processing: {exception}", exc_info=True)

    # Attempt to notify the user if possible
    target_chat_id = None
    callback_query = None
    if update.message: target_chat_id = update.message.chat.id
    elif update.callback_query:
        callback_query = update.callback_query
        if callback_query.message: target_chat_id = callback_query.message.chat.id

    error_message = "❗️ An unexpected error occurred. Please try again later."

    # Send message if we have a chat ID
    if target_chat_id:
        try: await bot.send_message(target_chat_id, error_message)
        except Exception as notify_err: logger.error(f"Failed to notify user about error: {notify_err}")

    # Answer callback query if the error originated from one
    if callback_query:
        try: await callback_query.answer("Error processing request.", show_alert=False)
        except Exception as cb_err: logger.error(f"Failed to answer callback query during error handling: {cb_err}")

# --- Main Execution (if running directly, for local testing) ---
# This part won't run on Render with Uvicorn typically
async def main():
    logger.info("Bot starting in polling mode (for local testing)...")
    # Initialize DB and Algolia explicitly if not using FastAPI lifespan
    try:
        await db.init_db()
        await initialize_algolia()
    except Exception as init_err:
        logger.critical(f"Initialization failed in main(): {init_err}", exc_info=True)
        return # Don't start polling if init fails

    # Start monitor task
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())

    # Start polling
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    # Cleanup after polling stops (won't be reached if shutdown signal is caught)
    await shutdown_procedure(asyncio.get_running_loop())


if __name__ == "__main__":
    # Note: This block is primarily for local testing.
    # Render uses `uvicorn bot:app` which uses the FastAPI lifespan manager.
    logger.warning("Running bot directly using __main__. Uvicorn/FastAPI is recommended for deployment.")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
    except Exception as e:
        logger.critical(f"Bot failed to run: {e}", exc_info=True)
