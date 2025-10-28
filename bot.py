# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import signal
import json
import hashlib
from datetime import datetime
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
from database import Database, clean_text_for_search, AUTO_MESSAGE_ID_PLACEHOLDER
# Import the initialization function specifically
from algolia_client import initialize_algolia, is_algolia_ready, algolia_search, algolia_add_movie, algolia_add_batch_movies, algolia_remove_movie, algolia_sync_data, algolia_clear_index


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)-12s %(message)s")
logger = logging.getLogger("bot")
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
logging.getLogger("algoliasearch").setLevel(logging.WARNING)

# ============ CONFIGURATION ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "123456789"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "-1003138949015"))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
USER_GROUP_USERNAME = os.getenv("USER_GROUP_USERNAME", "THEGREATMOVIESL9")

DATABASE_URL = os.getenv("DATABASE_URL")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PUBLIC_URL = os.getenv("PUBLIC_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

DEFAULT_CONCURRENT_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

ALTERNATE_BOTS = ["Moviemaza91bot", "Moviemaza92bot", "Mazamovie9bot"]

# ============ TIMEOUTS ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 8
TG_OP_TIMEOUT = 4

# ============ SEMAPHORE ============
DB_SEMAPHORE = asyncio.Semaphore(10)

if not BOT_TOKEN or not DATABASE_URL:
    logger.critical("Missing BOT_TOKEN or DATABASE_URL! Exiting.")
    raise SystemExit(1)

def build_webhook_url() -> str:
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        return f"{base}/bot/{BOT_TOKEN}"
    logger.warning("No external URL found (RENDER_EXTERNAL_URL / PUBLIC_URL); webhook not set.")
    return ""

WEBHOOK_URL = build_webhook_url()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db = Database(DATABASE_URL) # PostgreSQL Database
start_time = datetime.utcnow()

# ============ GRACEFUL SHUTDOWN ============
def handle_shutdown_signal(signum, frame):
    logger.info(f"Received shutdown signal {signum}, cleaning up...")
    # Don't raise KeyboardInterrupt here directly, let lifespan handle cleanup
    asyncio.create_task(shutdown_procedure()) # Schedule cleanup

async def shutdown_procedure():
    """Graceful shutdown logic called by signal handler."""
    logger.info("Initiating shutdown procedure...")
    # Cancel monitor task if running
    if 'monitor_task' in globals() and monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await asyncio.wait_for(monitor_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            logger.warning("Event loop monitor did not stop gracefully during shutdown.")

    # Delete Webhook
    try:
        if WEBHOOK_URL: # Only delete if it was potentially set
            delete_result = await bot.delete_webhook(drop_pending_updates=True)
            if delete_result:
                logger.info("Webhook deleted successfully during shutdown.")
            else:
                logger.warning("Webhook delete failed or was not set during shutdown.")
    except Exception as e:
        logger.error(f"Webhook delete error during shutdown: {e}", exc_info=True)

    # Shutdown ThreadPoolExecutor if it exists
    if 'executor' in globals() and executor:
        executor.shutdown(wait=False)
        logger.info("ThreadPoolExecutor shut down during shutdown.")

    # Close bot session
    await bot.session.close()
    logger.info("Bot session closed.")

    # Stop the event loop (important for clean exit)
    loop = asyncio.get_running_loop()
    loop.stop()
    logger.info("Event loop stopped.")


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)

# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                await asyncio.sleep(0) # Yield control briefly
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} timed out after {timeout}s")
                target_chat_id = None
                callback_query = None
                if args:
                    if isinstance(args[0], types.Message):
                        target_chat_id = args[0].chat.id
                    elif isinstance(args[0], types.CallbackQuery):
                        callback_query = args[0]
                        if callback_query.message:
                            target_chat_id = callback_query.message.chat.id
                if target_chat_id:
                    try:
                        await bot.send_message(target_chat_id, "⚠️ Request timeout - kripya dobara try karein. Server busy ho sakta hai.")
                        # Answer callback if it timed out
                        if callback_query: await callback_query.answer("Request timed out.", show_alert=False)
                    except Exception as e_inner:
                        logger.warning(f"Could not send timeout message: {e_inner}")
            except Exception as e:
                # Log full error with traceback
                logger.exception(f"Handler {func.__name__} error: {e}")
        return wrapper
    return decorator

# ============ SAFE WRAPPERS ============
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """Safely execute database coroutine with semaphore and timeout."""
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB operation timed out after {timeout}s: {coro.__name__}")
        return default
    except Exception as e:
        # Log DB errors more visibly
        logger.error(f"DB operation error in {coro.__name__}: {e}", exc_info=True)
        # Attempt to handle specific connection errors if needed by disposing pool
        await db._handle_db_error(e) # Use the DB class's error handler
        return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    """Safely execute Telegram API coroutine with timeout and specific error handling."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Telegram API call timed out after {timeout}s")
        return None
    except TelegramAPIError as e:
        if "bot was blocked by the user" in str(e).lower():
            logger.info(f"Telegram API Error: Bot blocked by user.")
            return False # Special return code for blocked
        elif "chat not found" in str(e).lower():
             logger.info(f"Telegram API Error: Chat not found.")
             return False # Treat chat not found similar to blocked for broadcast etc.
        else:
             logger.warning(f"Telegram API Error: {e}")
             return None
    except TelegramBadRequest as e:
        # e.g., message is not modified, message to delete not found
        logger.warning(f"Telegram Bad Request: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error in Telegram call: {e}") # Log full traceback
        return None

# ============ FILTERS & HELPERS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    delta = datetime.utcnow() - start_time
    total_seconds = int(delta.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    # TODO: Implement actual membership check logic if needed
    # Example:
    # try:
    #     member_channel = await bot.get_chat_member(chat_id=f"@{JOIN_CHANNEL_USERNAME}", user_id=user_id)
    #     member_group = await bot.get_chat_member(chat_id=f"@{USER_GROUP_USERNAME}", user_id=user_id)
    #     # Check if status is member, administrator, or creator
    #     return member_channel.status in ["member", "administrator", "creator"] and \
    #            member_group.status in ["member", "administrator", "creator"]
    # except TelegramAPIError as e:
    #     logger.warning(f"Membership check failed for user {user_id}: {e}")
    #     return False # Assume not member if check fails
    # except Exception as e:
    #      logger.error(f"Unexpected error during membership check for {user_id}: {e}")
    #      return False
    return True # Placeholder: Assume everyone is a member


def get_join_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
        [InlineKeyboardButton(text="👥 Group Join Karein", url=f"https://t.me/{USER_GROUP_USERNAME}")],
        [InlineKeyboardButton(text="✅ Maine Join Kar Liya", callback_data="check_join")] # Changed text slightly
    ])

def get_full_limit_keyboard():
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b} (Alternate Bot)", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str):
    # Same as before
    if not caption: return None
    info = {}; lines = caption.splitlines()
    if lines:
        title = lines[0].strip()
        if len(lines) > 1 and re.search(r"[Ss]\d{1,2}", lines[1]): title += " " + lines[1].strip()
        info["title"] = title
    imdb_match = re.search(r"(tt\d{7,})", caption);
    if imdb_match: info["imdb_id"] = imdb_match.group(1)
    year_match = re.findall(r"\b(19|20)\d{2}\b", caption); # Use findall to get all years
    if year_match: info["year"] = year_match[-1][0] # Get the last found year
    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str]:
    # Same as before
    year = None
    match = re.search(r"\(((19|20)\d{2})\)", filename)
    if match: year = match.group(1)
    else:
        matches = re.findall(r"\b((19|20)\d{2})\b", filename)
        if matches: year = matches[-1][0]
    title = os.path.splitext(filename)[0]
    if year: title = title.replace(f"({year})", "").replace(year, "").strip()
    return {"title": title, "year": year}

def overflow_message(active_users: int) -> str:
    # Same as before
    return f"""⚠️ <b>Server Capacity Reached</b>

Hamari free-tier service is waqt <b>{CURRENT_CONC_LIMIT}</b> concurrent users par chal rahi hai
aur abhi <b>{active_users}</b> active hain. Nayi requests temporarily hold par hain.

Alternate bots use karein (neeche se choose karein):"""

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    # Same as before
    loop = asyncio.get_running_loop()
    while True:
        try:
            start_time = loop.time()
            await asyncio.sleep(0.1)
            lag = loop.time() - start_time
            if lag > 0.5: logger.warning(f"⚠️ Event loop lag detected: {lag:.3f}s")
            await asyncio.sleep(60)
        except asyncio.CancelledError: logger.info("Event loop monitor stopping."); break
        except Exception as e: logger.error(f"Event loop monitor error: {e}", exc_info=True); await asyncio.sleep(120)

# Declare global vars for tasks/executor to be accessible in shutdown
monitor_task = None
executor = None

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor_task, executor
    # Setup ThreadPoolExecutor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized.")

    # Initialize Database
    db_ready = False
    try:
        await db.init_db()
        db_ready = True
    except Exception as e:
        # Log critical error but DO NOT exit, allow app to start
        logger.critical(f"DATABASE INITIALIZATION FAILED: {e}", exc_info=True)
        # Bot might function partially or recover if connection resumes

    # Initialize Algolia (Now called from here)
    algolia_initialized = False
    try:
        algolia_initialized = await initialize_algolia()
        if not algolia_initialized:
            logger.critical("ALGOLIA INITIALIZATION FAILED. Search functionality will be unavailable.")
        else:
            logger.info("Algolia initialization completed successfully via lifespan.")
    except Exception as e:
        logger.critical(f"Error during Algolia initialization in lifespan: {e}", exc_info=True)


    # Start event loop monitor
    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor started.")

    # Set Webhook
    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            # Only set webhook if URL is different
            if not current_webhook or current_webhook.url != WEBHOOK_URL:
                 logger.info(f"Attempting to set webhook to {WEBHOOK_URL}...")
                 set_result = await bot.set_webhook(
                     url=WEBHOOK_URL,
                     allowed_updates=dp.resolve_used_update_types(),
                     secret_token=(WEBHOOK_SECRET or None),
                     drop_pending_updates=True,
                 )
                 if set_result:
                     logger.info(f"Webhook set successfully.")
                 else:
                     logger.error("Webhook set failed!")
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.error(f"Webhook setup error: {e}", exc_info=True)
    else:
        logger.warning("WEBHOOK_URL is not set. Bot must be run in polling mode.")

    logger.info("Application startup complete.")
    yield # Application runs here

    # Cleanup phase is now handled by shutdown_procedure triggered by signals
    logger.info("Application shutdown sequence initiated by lifespan context exit...")
    # Call the same shutdown logic used by signals for consistency
    await shutdown_procedure()
    logger.info("Lifespan cleanup finished.")


app = FastAPI(lifespan=lifespan)

# Rest of the bot handlers remain the same as the previous correct version...
# ... (start_command, help_command, check_join_callback, search_movie_handler, get_movie_callback) ...
# ... (admin commands: stats, broadcast, cleanup_users, add_movie, import_json, remove_dead_movie) ...
# ... (sync_algolia, rebuild_index, export_csv, set_limit) ...
# ... (auto_index_handler, errors_handler) ...

# ============ WEBHOOK ENDPOINT ============
async def _process_update_safe(update_obj: Update):
    """Safely process a single update, catching exceptions."""
    try:
        await dp.feed_update(bot=bot, update=update_obj)
    except Exception as e:
        logger.exception(f"Error processing update {update_obj.update_id}: {e}")

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    # Validate secret token if set
    if WEBHOOK_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
            logger.warning("Invalid webhook secret token received.")
            raise HTTPException(status_code=403, detail="Forbidden")

    try:
        telegram_update = Update(**update)
        # Process update in the background
        background_tasks.add_task(_process_update_safe, telegram_update)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook processing error (parsing update): {e}", exc_info=True)
        return {"ok": False, "error": f"Failed to parse update: {e}"}

# ============ HEALTH CHECK ENDPOINT ============
@app.get("/")
async def ping():
    logger.debug("Ping/Root endpoint hit.")
    return {"status": "ok", "service": "Movie Bot", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint hit.")
    # Check DB status by trying a lightweight query
    db_ok_raw = await safe_db_call(db.get_movie_count(), default=-1)
    db_ok = isinstance(db_ok_raw, int) and db_ok_raw >= 0

    # Check Algolia status
    algolia_ok = is_algolia_ready()
    status = "ok" if db_ok and algolia_ok else "degraded" if db_ok or algolia_ok else "error"

    return {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": get_uptime(),
        "database_connected": db_ok,
        "algolia_connected": algolia_ok,
    }

# ============ CAPACITY MANAGEMENT ============
async def ensure_capacity_or_inform(message_or_callback: types.Message | types.CallbackQuery) -> bool:
    user = message_or_callback.from_user
    target_chat_id = message_or_callback.message.chat.id if isinstance(message_or_callback, types.CallbackQuery) else message_or_callback.chat.id

    await safe_db_call(
        db.add_user(user.id, user.username, user.first_name, user.last_name),
        timeout=DB_OP_TIMEOUT
    )

    if user.id == ADMIN_USER_ID: return True

    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), timeout=DB_OP_TIMEOUT, default=CURRENT_CONC_LIMIT + 1) # Default to > limit on error

    if active >= CURRENT_CONC_LIMIT:
        logger.warning(f"Capacity reached: {active}/{CURRENT_CONC_LIMIT}. User {user.id} request held.")
        await safe_tg_call(
            bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard())
        )
        if isinstance(message_or_callback, types.CallbackQuery):
           await safe_tg_call(message_or_callback.answer("Server busy, please use alternate bots.", show_alert=False))
        return False

    return True

# ============ BOT HANDLERS ============
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user_id = message.from_user.id
    bot_info = await safe_tg_call(bot.get_me(), timeout=5)
    if not bot_info:
        await safe_tg_call(message.answer("⚠️ Bot start error. Try again later."))
        return

    await safe_db_call(db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))

    if user_id == ADMIN_USER_ID:
        # Fetch stats safely
        user_count = await safe_db_call(db.get_user_count(), default=0)
        movie_count_raw = await safe_db_call(db.get_movie_count(), default=-1)
        movie_count_str = f"{movie_count_raw:,}" if movie_count_raw >= 0 else "DB Error"
        concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        algolia_status = "🟢 Connected" if is_algolia_ready() else "❌ NOT CONNECTED (Check Logs!)"

        admin_message = f"""👑 <b>Admin Console: @{bot_info.username}</b>

<b>System Status</b>
• Bot Status: 🟢 Online
• Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}
• Postgres DB: {movie_count_str} movies
• Uptime: {get_uptime()}
• Algolia Search: <b>{algolia_status}</b>

<b>Management Commands</b>
• /stats | /health
• /broadcast (Reply)
• /import_json (Reply)
• /add_movie (Reply: imdb|title|year)
• /remove_dead_movie IMDB_ID
• <b>/sync_algolia</b> ⚠️
• /rebuild_index (DB Only)
• /cleanup_users
• /export_csv users|movies [limit]
• /set_limit N"""

        await safe_tg_call(message.answer(admin_message))
        return

    if not await ensure_capacity_or_inform(message): return

    welcome_text = f"""🎬 Namaskar <b>{message.from_user.first_name}</b>!
Swagat hai instant movie search bot mein. Naam bhejein (galat spelling bhi chalegi).
Example: <code>Kantara 2022</code> ya <code>Kantra</code>

⚠️ <b>Free Service Note:</b> Render free server 15 min inactivity ke baad 'so' jaata hai. Start hone mein 10-15 sec lag sakte hain. Search hamesha fast rahegi.

Join karke "✅ Maine Join Kar Liya" dabayen."""
    await safe_tg_call(message.answer(welcome_text, reply_markup=get_join_keyboard()))


@dp.message(Command("help"))
@handler_timeout(15)
async def help_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    help_text = """❓ <b>Bot Ka Upyog</b>
1.  <b>Instant Search:</b> Movie/Show ka naam bhejein. (<code>Jawan</code>)
2.  <b>Typo Friendly:</b> Galat spelling bhi chalegi. (<code>Mirjapur</code>)
3.  <b>Behtar Results:</b> Naam ke saath saal (year) jodein. (<code>Pushpa 2021</code>)

⚠️ <b>Start Hone Mein Deri?</b> Free server 15 min inactivity ke baad 'so' jaata hai. Start hone mein 10-15 sec lag sakte hain. Search hamesha fast rahegi."""
    await safe_tg_call(message.answer(help_text))


@dp.callback_query(F.data == "check_join")
@handler_timeout(20) # Slightly longer timeout if membership check involves API calls
async def check_join_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("Checking membership..."))

    if not await ensure_capacity_or_inform(callback): return

    is_member = await check_user_membership(callback.from_user.id) # Replace with actual check

    if is_member:
        active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = f"""✅ Verification successful, <b>{callback.from_user.first_name}</b>!
Ab aap movie search kar sakte hain.
(Capacity: {active_users}/{CURRENT_CONC_LIMIT})"""
        result = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        if not result: await safe_tg_call(bot.send_message(callback.from_user.id, success_text, reply_markup=None))
    else:
        await safe_tg_call(callback.answer("❌ Channel/Group join nahi kiya hai. Join karke dobara try karein.", show_alert=True))


@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(10)
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id
    if not await check_user_membership(user_id):
        await safe_tg_call(message.answer("⚠️ Pehle Channel/Group join karein...", reply_markup=get_join_keyboard()))
        return

    if not await ensure_capacity_or_inform(message): return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Query 2 characters se lambi honi chahiye."))
        return

    if not is_algolia_ready():
        logger.error(f"User {user_id} search failed: Algolia not ready.")
        await safe_tg_call(message.answer("❌ Search Engine abhi kaam nahi kar raha hai."))
        return

    searching_msg = await safe_tg_call(message.answer(f"⚡️ <b>{original_query}</b> search ho raha hai..."))
    if not searching_msg: return

    search_results = await algolia_search(original_query, limit=20)

    if not search_results:
        await safe_tg_call(searching_msg.edit_text(f"🥲 '<b>{original_query}</b>' ke liye kuch nahi mila."))
        return

    buttons = [[InlineKeyboardButton(text=movie["title"], callback_data=f"get_{movie['imdb_id']}")] for movie in search_results]
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> - {len(search_results)} results:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(15)
async def get_movie_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("File bheji ja rahi hai..."))
    if not await ensure_capacity_or_inform(callback): return

    imdb_id = callback.data.split("_", 1)[1]
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Movie DB mein nahi mili."))
        if callback.from_user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN: Movie '{imdb_id}' Algolia mein hai par DB mein nahi. /sync_algolia chalayein."))
        return

    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> - file bhej raha hoon..."))

    success = False
    forward_error_message = None
    try:
        # Try forwarding first (preferable)
        forward_result = await safe_tg_call(bot.forward_message(
            chat_id=callback.from_user.id,
            from_chat_id=int(movie["channel_id"]),
            message_id=movie["message_id"],
        ), timeout=TG_OP_TIMEOUT * 2) # Slightly longer timeout for forward

        if forward_result: success = True
        elif forward_result is False: forward_error_message = "Bot blocked or chat not found." # Specific false case
        else: forward_error_message = "Forwarding failed (timeout or other error)."

    except Exception as e: # Catch potential exceptions from safe_tg_call itself if needed
         forward_error_message = f"Forward exception: {e}"
         logger.error(f"Exception during forward attempt for {imdb_id}: {e}", exc_info=True)


    # Fallback to send_document if forwarding failed OR if message_id is placeholder
    if not success and (movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER or forward_error_message):
        logger.info(f"Forward failed for {imdb_id} (Reason: {forward_error_message}). Falling back to send_document.")
        try:
            send_result = await safe_tg_call(bot.send_document(
                chat_id=callback.from_user.id,
                document=movie["file_id"],
                caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'N/A'})"
            ), timeout=TG_OP_TIMEOUT * 3) # Longer timeout for sending file

            if send_result: success = True
            elif send_result is False: forward_error_message = "Bot blocked or chat not found (send_doc)."
            else: forward_error_message = "Sending document by file_id failed."

        except Exception as e2:
            forward_error_message = f"Send document exception: {e2}"
            logger.error(f"❌ DEAD FILE? Fallback send_document failed for {imdb_id}. Error: {e2}", exc_info=True)

    if not success:
        admin_hint = f"\n(Hint: /remove_dead_movie {imdb_id})" if callback.from_user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: <b>{movie['title']}</b> ki file nahi bhej paya. ({forward_error_message or 'Unknown error'})" + admin_hint
        await safe_tg_call(bot.send_message(callback.from_user.id, error_text))
        await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ki file nahi bhej paya."))


# --- ADMIN COMMANDS ---
# (broadcast, cleanup_users, add_movie, import_json, remove_dead_movie, sync_algolia, rebuild_index, export_csv, set_limit - remain same)
# ... Ensure error handling and logging in these commands are robust ...
# ... Make sure add/remove/import commands update BOTH DB and Algolia using safe_db_call and algolia functions ...

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    user_count = await safe_db_call(db.get_user_count(), default=0)
    movie_count_raw = await safe_db_call(db.get_movie_count(), default=-1)
    movie_count_str = f"{movie_count_raw:,}" if movie_count_raw >= 0 else "DB Error"
    concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    algolia_status = "🟢 Connected" if is_algolia_ready() else "❌ NOT CONNECTED"
    stats_msg = f"""📊 <b>Stats</b>
🟢 Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}
👥 Total Users: {user_count:,}
🎬 Movies (DB): {movie_count_str}
⚡️ Algolia: <b>{algolia_status}</b>
⏰ Uptime: {get_uptime()}"""
    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(1800)
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Reply to a message to broadcast."))
        return
    users = await safe_db_call(db.get_all_users(), timeout=30, default=[])
    if not users:
        await safe_tg_call(message.answer("❌ No active users found."))
        return
    total_users = len(users)
    success, failed = 0, 0
    progress_msg = await safe_tg_call(message.answer(f"📤 Broadcasting to {total_users:,} users..."))
    start_broadcast_time = datetime.utcnow()
    for i, uid in enumerate(users):
        result = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=3)
        if result: success += 1
        elif result is False: failed += 1; await safe_db_call(db.deactivate_user(uid), timeout=3)
        else: failed += 1
        now = datetime.utcnow()
        if (i + 1) % 100 == 0 or (now - start_broadcast_time).total_seconds() > 15:
             if progress_msg:
                try: await safe_tg_call(progress_msg.edit_text(f"📤 Progress: {i+1}/{total_users}\n✅ Sent: {success:,} | ❌ Failed: {failed:,}"))
                except: pass
             start_broadcast_time = now
             await asyncio.sleep(0.1)
    final_text = f"✅ Broadcast Complete!\nSent: {success:,}\nFailed/Blocked: {failed:,}\nTotal: {total_users:,}"
    if progress_msg: await safe_tg_call(progress_msg.edit_text(final_text))
    else: await safe_tg_call(message.answer(final_text))

@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120)
async def cleanup_users_command(message: types.Message):
    status_msg = await safe_tg_call(message.answer("🧹 Cleaning up users inactive for 30+ days..."))
    removed_count = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=90, default=0)
    new_count = await safe_db_call(db.get_user_count(), default=0)
    result_text = f"✅ Cleanup complete!\nDeactivated: {removed_count:,}\nActive Users: {new_count:,}"
    if status_msg: await safe_tg_call(status_msg.edit_text(result_text))
    else: await safe_tg_call(message.answer(result_text))

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(20)
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await safe_tg_call(message.answer("❌ Reply to video/doc: `/add_movie imdb|title|year`"))
        return
    try:
        parts = message.text.split("|", 2)
        imdb_id = parts[0].split(" ", 1)[1].strip()
        title = parts[1].strip()
        year = parts[2].strip() if len(parts) > 2 else None
        replied_msg = message.reply_to_message
        file_id = replied_msg.video.file_id if replied_msg.video else replied_msg.document.file_id
        message_id = replied_msg.message_id
        channel_id = replied_msg.chat.id
    except Exception:
        await safe_tg_call(message.answer("❌ Format: `/add_movie imdb|title|year`"))
        return

    status_msg = await safe_tg_call(message.answer(f"⏳ Processing '<b>{title}</b>'..."))
    db_result = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message_id, channel_id), default=False)

    db_status_map = {True: "✅ Added to DB.", "updated": "✅ Updated in DB.", "duplicate": "⚠️ Already in DB (duplicate).", False: "❌ DB Error."}
    db_status = db_status_map.get(db_result, "❌ DB Error.")

    algolia_status = ""
    if db_result in [True, "updated"]: # Only update algolia if DB was successful
        algolia_data = {'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year}
        success_algolia = await algolia_add_movie(algolia_data)
        algolia_status = "✅ Synced to Algolia." if success_algolia else "❌ Algolia Sync Error!"
    elif db_result == "duplicate":
        algolia_status = "ℹ️ Algolia not updated (DB duplicate)." # Don't sync if it was a duplicate error

    final_status = f"{db_status}\n{algolia_status}".strip()
    if status_msg: await safe_tg_call(status_msg.edit_text(final_status))
    else: await safe_tg_call(message.answer(final_status))


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800)
async def import_json_command(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        await safe_tg_call(message.answer("❌ Reply to a .json file."))
        return
    doc = message.reply_to_message.document
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"):
        await safe_tg_call(message.answer("❌ Must be a .json file."))
        return

    status_msg = await safe_tg_call(message.answer("⏳ Downloading JSON..."))
    if not status_msg: return
    try:
        file_io = io.BytesIO()
        file = await bot.get_file(doc.file_id)
        # Use executor for download
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, lambda: bot.download_file(file.file_path, file_io, timeout=120)) # Longer timeout
        file_io.seek(0)
        movies_json_list = json.loads(file_io.read().decode('utf-8'))
        if not isinstance(movies_json_list, list): raise ValueError("JSON is not a list")
    except Exception as e:
        await safe_tg_call(status_msg.edit_text(f"❌ Download/Parse Error: {e}"))
        return

    total = len(movies_json_list)
    added, updated, skipped, failed_db = 0, 0, 0, 0
    algolia_batch = []
    await safe_tg_call(status_msg.edit_text(f"⏳ Processing {total:,} items (DB)..."))
    start_import_time = datetime.utcnow()

    for i, item in enumerate(movies_json_list):
        try:
            file_id = item.get("file_id")
            filename = item.get("filename") or item.get("title")
            if not file_id or not filename: skipped += 1; continue
            imdb_id = item.get("imdb_id", f"json_{hashlib.md5(file_id.encode()).hexdigest()}")
            info = parse_filename(filename)
            title = info["title"] or "Untitled"
            year = info["year"]

            db_result = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, AUTO_MESSAGE_ID_PLACEHOLDER, 0), default=False)

            if db_result is True: added += 1; algolia_batch.append({'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year})
            elif db_result == "updated": updated += 1; algolia_batch.append({'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year})
            elif db_result == "duplicate": skipped += 1
            else: failed_db += 1
        except Exception as e: failed_db += 1; logger.error(f"Error processing item {i+1}: {e}", exc_info=False)

        now = datetime.utcnow()
        if (i + 1) % 200 == 0 or (now - start_import_time).total_seconds() > 20 or (i+1) == total:
             try: await safe_tg_call(status_msg.edit_text(f"⏳ DB: {i+1}/{total:,} | ✅A:{added:,} 🔄U:{updated:,} ↷S:{skipped:,} ❌F:{failed_db:,}"))
             except: pass
             start_import_time = now
             await asyncio.sleep(0.1)

    db_summary = f"DB Done: ✅A:{added:,} 🔄U:{updated:,} ↷S:{skipped:,} ❌F:{failed_db:,}"
    await safe_tg_call(status_msg.edit_text(f"{db_summary}\n⏳ Syncing {len(algolia_batch):,} to Algolia..."))

    algolia_status = ""
    if algolia_batch:
        success_algolia = await algolia_add_batch_movies(algolia_batch)
        algolia_status = f"✅ Algolia Synced: {len(algolia_batch):,}" if success_algolia else f"❌ Algolia Sync FAILED!"
    else: algolia_status = "ℹ️ Algolia: Nothing to sync."

    await safe_tg_call(status_msg.edit_text(f"✅ Import Complete!\n{db_summary}\n{algolia_status}"))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2: await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID")); return
    imdb_id = args[1].strip()
    status_msg = await safe_tg_call(message.answer(f"⏳ Removing <code>{imdb_id}</code>..."))

    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id)) # Get title before deleting
    db_deleted = await safe_db_call(db.remove_movie_by_imdb(imdb_id), default=False)
    db_status = f"✅ DB: Removed '{movie['title'] if movie else imdb_id}'." if db_deleted else ("ℹ️ DB: Not found." if not movie else "❌ DB: Error.")

    algolia_deleted = await algolia_remove_movie(imdb_id) # Always try removing from Algolia
    algolia_status = "✅ Algolia: Removed." if algolia_deleted else "❌ Algolia: Error or Not Found."

    final_status = f"{db_status}\n{algolia_status}"
    if status_msg: await safe_tg_call(status_msg.edit_text(final_status))
    else: await safe_tg_call(message.answer(final_status))


@dp.message(Command("sync_algolia"), AdminFilter())
@handler_timeout(1800)
async def sync_algolia_command(message: types.Message):
    if not is_algolia_ready():
        await safe_tg_call(message.answer("❌ Algolia not connected. Cannot sync."))
        return
    status_msg = await safe_tg_call(message.answer("⚠️ Full Algolia Sync Started...\n⏳ Fetching data from DB..."))
    if not status_msg: return
    try:
        all_movies_db = await safe_db_call(db.get_all_movies_for_sync(), timeout=300)
        if all_movies_db is None: await safe_tg_call(status_msg.edit_text("❌ DB Error fetching movies.")); return
        db_count = len(all_movies_db)
        await safe_tg_call(status_msg.edit_text(f"✅ Fetched {db_count:,} movies from DB.\n⏳ Syncing to Algolia (replace all)..."))
        success, total_uploaded = await algolia_sync_data(all_movies_db)
        final_text = f"✅ Sync Complete! {total_uploaded:,} records synced." if success else "❌ Sync Failed! Check logs."
        await safe_tg_call(status_msg.edit_text(final_text))
    except Exception as e:
        logger.error(f"/sync_algolia error: {e}", exc_info=True)
        await safe_tg_call(status_msg.edit_text(f"❌ Sync Error: {e}"))

@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300)
async def rebuild_index_command(message: types.Message):
    status_msg = await safe_tg_call(message.answer("🔧 Rebuilding `clean_title` index in DB..."))
    updated, total = await safe_db_call(db.rebuild_clean_titles(), timeout=240, default=(0, 0))
    result_text = f"✅ DB Reindex done: Updated {updated:,} of ~{total:,} titles."
    if status_msg: await safe_tg_call(status_msg.edit_text(result_text))
    else: await safe_tg_call(message.answer(result_text))

@dp.message(Command("export_csv"), AdminFilter())
@handler_timeout(120)
async def export_csv_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2 or args[1].lower() not in ("users", "movies"):
        await safe_tg_call(message.answer("❌ Use: /export_csv users|movies [limit]")); return
    kind, limit = args[1].lower(), int(args[2]) if len(args) > 2 and args[2].isdigit() else 2000
    status_msg = await safe_tg_call(message.answer(f"⏳ Exporting {limit:,} {kind}..."))
    if not status_msg: return
    try:
        if kind == "users":
            rows = await safe_db_call(db.export_users(limit=limit), timeout=90, default=[])
            if not rows: await safe_tg_call(status_msg.edit_text("❌ No users found.")); return
            header = "user_id,username,first_name,last_name,joined_date,last_active,is_active\n"
            csv_data = header + "\n".join([f"{r['user_id']},\"{r['username'] or ''}\",\"{r['first_name'] or ''}\",\"{r['last_name'] or ''}\",{r['joined_date']},{r['last_active']},{r['is_active']}" for r in rows])
            filename = "users_export.csv"
        else: # movies
            rows = await safe_db_call(db.export_movies(limit=limit), timeout=90, default=[])
            if not rows: await safe_tg_call(status_msg.edit_text("❌ No movies found.")); return
            header = "imdb_id,title,year,channel_id,message_id,added_date\n"
            csv_data = header + "\n".join([f"{r['imdb_id']},\"{r['title'].replace('\"', '\"\"') if r['title'] else ''}\",{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}" for r in rows])
            filename = "movies_export_db.csv"
        file_bytes = csv_data.encode("utf-8")
        input_file = BufferedInputFile(file_bytes, filename=filename)
        await safe_tg_call(message.answer_document(input_file, caption=f"{kind.capitalize()} export ({len(rows):,} rows)"))
        await safe_tg_call(status_msg.delete())
    except Exception as e: await safe_tg_call(status_msg.edit_text(f"❌ Export Error: {e}"))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit(): await safe_tg_call(message.answer(f"Use: /set_limit N (Current: {CURRENT_CONC_LIMIT})")); return
    try:
        val = int(args[1]); assert 5 <= val <= 200
        CURRENT_CONC_LIMIT = val
        await safe_tg_call(message.answer(f"✅ Concurrency limit set to {CURRENT_CONC_LIMIT}"))
        logger.info(f"Concurrency limit changed to {CURRENT_CONC_LIMIT}")
    except (ValueError, AssertionError): await safe_tg_call(message.answer("❌ Limit must be between 5 and 200."))

# --- AUTO INDEXING ---
@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document): return
    caption = message.caption or ""
    info = extract_movie_info(caption)
    if not info or not info.get("title"):
        logger.warning(f"Auto-index skipped (Msg {message.message_id}): No title found in caption.")
        return
    file_id = message.video.file_id if message.video else message.document.file_id
    imdb_id = info.get("imdb_id", f"auto_{message.message_id}")
    title = info["title"]; year = info.get("year")

    db_result = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id), default=False)
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: {title}):"
    if db_result in [True, "updated"]:
        logger.info(f"{log_prefix} {'Added' if db_result is True else 'Updated'} in DB.")
        algolia_data = {'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year}
        success_algolia = await algolia_add_movie(algolia_data)
        if success_algolia: logger.info(f"{log_prefix} Synced to Algolia.")
        else: logger.error(f"{log_prefix} FAILED Algolia sync.")
    elif db_result == "duplicate": logger.warning(f"{log_prefix} Skipped DB (duplicate).")
    else: logger.error(f"{log_prefix} FAILED DB operation.")

# --- ERROR HANDLER ---
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.exception(f"Unhandled error processing update: {exception}", exc_info=True)
    target_chat_id = None
    callback_query = None
    if update.message: target_chat_id = update.message.chat.id
    elif update.callback_query and update.callback_query.message:
        target_chat_id = update.callback_query.message.chat.id
        callback_query = update.callback_query
    if target_chat_id:
        try: await bot.send_message(target_chat_id, "❗️ Unexpected error. Please try again.")
        except: pass
    if callback_query:
         try: await callback_query.answer("Error processing.", show_alert=False)
         except: pass
