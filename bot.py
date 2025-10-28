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
import algolia_client


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

# Algolia check is now done via is_algolia_ready() function where needed

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
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)

# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                # Execution ko control yield karein taaki loop non-blocking rahe
                await asyncio.sleep(0)
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} timed out after {timeout}s")
                try:
                    # User ko timeout message bhejein
                    target = None
                    if args and isinstance(args[0], types.Message):
                        target = args[0]
                    elif args and isinstance(args[0], types.CallbackQuery):
                        target = args[0].message
                    if target:
                        await bot.send_message(target.chat.id, "⚠️ Request timeout - kripya dobara try karein. Server busy ho sakta hai.", parse_mode=ParseMode.HTML)
                except Exception as e_inner:
                    logger.warning(f"Could not send timeout message: {e_inner}")
            except Exception as e:
                logger.error(f"Handler {func.__name__} error: {e}", exc_info=True)
        return wrapper
    return decorator

# ============ SAFE WRAPPERS ============
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """Database call ko semaphore aur timeout ke saath safely execute karein."""
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB operation timed out after {timeout}s")
        return default
    except Exception as e:
        logger.error(f"DB operation error: {e}", exc_info=False) # Reduced verbosity
        return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    """Telegram API call ko timeout ke saath safely execute karein."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Telegram API call timed out after {timeout}s")
        return None
    except TelegramAPIError as e:
        logger.warning(f"Telegram API Error: {e}")
        if "bot was blocked by the user" in str(e).lower():
            return False # Signal that the bot was blocked
        return None
    except TelegramBadRequest as e:
        logger.warning(f"Telegram Bad Request: {e}") # e.g., message not modified
        return None
    except Exception as e:
        logger.error(f"Unexpected error in Telegram call: {e}", exc_info=True)
        return None

# ============ FILTERS & HELPERS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    days, hours = divmod(hours, 24)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    # TODO: Implement actual membership check logic here if needed
    # For now, assume everyone is a member
    return True

def get_join_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
        [InlineKeyboardButton(text="👥 Group Join Karein", url=f"https://t.me/{USER_GROUP_USERNAME}")],
        [InlineKeyboardButton(text="✅ I Have Joined Both", callback_data="check_join")]
    ])

def get_full_limit_keyboard():
    buttons = [[InlineKeyboardButton(text=f"🚀 @{b} (Alternate Bot)", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str):
    if not caption:
        return None
    info = {}
    lines = caption.splitlines()
    if lines:
        title = lines[0].strip()
        # Simple season check (S followed by 1 or 2 digits)
        if len(lines) > 1 and re.search(r"[Ss]\d{1,2}", lines[1]):
            title += " " + lines[1].strip()
        info["title"] = title

    imdb_match = re.search(r"(tt\d{7,})", caption)
    if imdb_match:
        info["imdb_id"] = imdb_match.group(1)

    year_match = re.search(r"\b(19|20)\d{2}\b", caption)
    if year_match:
        info["year"] = year_match.group(0) # Get the last found year

    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str]:
    """JSON se title aur year nikalne ke liye helper function."""
    year = None
    # Check for year in parentheses first like (2023)
    match = re.search(r"\(((19|20)\d{2})\)", filename)
    if match:
        year = match.group(1)
    else:
        # Otherwise, find the last 4-digit number starting with 19 or 20
        matches = re.findall(r"\b((19|20)\d{2})\b", filename)
        if matches:
            year = matches[-1][0] # Get the last match

    # Remove extension for title
    title = os.path.splitext(filename)[0]
    # Optionally, remove the year part from the title if found
    if year:
        title = title.replace(f"({year})", "").replace(year, "").strip()

    return {"title": title, "year": year}

def overflow_message(active_users: int) -> str:
    return f"""⚠️ <b>Server Capacity Reached</b>

Hamari free-tier service is waqt <b>{CURRENT_CONC_LIMIT}</b> concurrent users par chal rahi hai
aur abhi <b>{active_users}</b> active hain. Nayi requests temporarily hold par hain.

Alternate bots use karein (neeche se choose karein):"""

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    """Event loop ko blocking operations ke liye monitor karein."""
    loop = asyncio.get_running_loop()
    while True:
        try:
            start_time = loop.time()
            await asyncio.sleep(0.1) # Yield control briefly
            lag = loop.time() - start_time
            if lag > 0.5: # Log if lag is more than 0.5 seconds
                logger.warning(f"⚠️ Event loop lag detected: {lag:.3f}s")
            await asyncio.sleep(60) # Check every minute
        except asyncio.CancelledError:
            logger.info("Event loop monitor stopping.")
            break
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}", exc_info=True)
            await asyncio.sleep(120) # Wait longer after an error

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Set up ThreadPoolExecutor for blocking I/O (like file downloads)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized (max_workers=10).")

    # Initialize Database
    try:
        await db.init_db()
    except Exception as e:
        logger.critical(f"FATAL: Database initialization failed: {e}", exc_info=True)
        # Optionally, raise SystemExit or handle recovery
        raise SystemExit("Database connection failed on startup.")

    # Start event loop monitor
    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor started.")

    # Set Webhook
    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            if current_webhook.url != WEBHOOK_URL:
                set_result = await bot.set_webhook(
                    url=WEBHOOK_URL,
                    allowed_updates=dp.resolve_used_update_types(),
                    secret_token=(WEBHOOK_SECRET or None),
                    drop_pending_updates=True,
                )
                if set_result:
                    logger.info(f"Webhook set successfully to {WEBHOOK_URL}")
                else:
                    logger.error("Webhook set failed!")
            else:
                logger.info("Webhook already set correctly.")
        except Exception as e:
            logger.error(f"Webhook setup error: {e}", exc_info=True)
    else:
        logger.warning("WEBHOOK_URL is not set. Bot will run in polling mode if started directly.")

    yield # Application runs here

    # Cleanup phase
    logger.info("Shutting down...")
    monitor_task.cancel()
    try:
        # Wait briefly for monitor task to finish cancellation
        await asyncio.wait_for(monitor_task, timeout=2.0)
    except asyncio.TimeoutError:
        logger.warning("Event loop monitor did not stop gracefully.")
    except asyncio.CancelledError:
        pass # Expected

    # Delete Webhook
    try:
        delete_result = await bot.delete_webhook(drop_pending_updates=True)
        if delete_result:
            logger.info("Webhook deleted successfully.")
        else:
            logger.warning("Webhook delete failed or was not set.")
    except Exception as e:
        logger.error(f"Webhook delete error: {e}", exc_info=True)

    # Shutdown ThreadPoolExecutor
    executor.shutdown(wait=False) # Don't wait for pending tasks
    logger.info("ThreadPoolExecutor shut down.")

app = FastAPI(lifespan=lifespan)

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
        # Return error but don't raise HTTPException to avoid Telegram retries for parsing errors
        return {"ok": False, "error": f"Failed to parse update: {e}"}

# ============ HEALTH CHECK ENDPOINT ============
@app.get("/")
async def ping():
    logger.debug("Ping/Root endpoint hit (keep-alive).")
    return {"status": "ok", "service": "Movie Bot (Algolia)", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint hit.")
    db_ok = await safe_db_call(db.get_movie_count(), default=-1) >= 0
    algolia_ok = algolia_client.is_algolia_ready()
    status = "ok" if db_ok and algolia_ok else "error"
    return {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": get_uptime(),
        "database_connected": db_ok,
        "algolia_connected": algolia_ok,
    }

# ============ CAPACITY MANAGEMENT ============
async def ensure_capacity_or_inform(message_or_callback: types.Message | types.CallbackQuery) -> bool:
    """Capacity check karein aur user ko update karein."""
    user = message_or_callback.from_user
    target_chat_id = message_or_callback.message.chat.id if isinstance(message_or_callback, types.CallbackQuery) else message_or_callback.chat.id

    # Add/update user in DB (safe call handles errors)
    await safe_db_call(
        db.add_user(user.id, user.username, user.first_name, user.last_name),
        timeout=DB_OP_TIMEOUT
    )

    # Admin always has capacity
    if user.id == ADMIN_USER_ID:
        return True

    # Get active user count
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), timeout=DB_OP_TIMEOUT, default=0)

    # Check against limit
    if active >= CURRENT_CONC_LIMIT: # Use >= to be safe
        logger.warning(f"Capacity reached: {active}/{CURRENT_CONC_LIMIT}. User {user.id} request held.")
        # Try sending overflow message
        await safe_tg_call(
            bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard())
        )
        # Optionally answer callback query if it's a callback
        if isinstance(message_or_callback, types.CallbackQuery):
           await safe_tg_call(message_or_callback.answer("Server busy, please use alternate bots.", show_alert=False))
        return False # Capacity exceeded

    return True # Capacity available

# ============ BOT HANDLERS ============
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user_id = message.from_user.id
    try:
        # Use safe_tg_call for robustness
        bot_info = await safe_tg_call(bot.get_me(), timeout=5)
        if not bot_info:
             await safe_tg_call(message.answer("⚠️ Bot mein technical error hai. Kripya thodi der baad /start karein."))
             return
    except Exception as e: # Catch any other unexpected error during get_me
        logger.error(f"Error getting bot info: {e}", exc_info=True)
        await safe_tg_call(message.answer("⚠️ Bot shuru karne mein error aaya. Kripya thodi der baad /start karein."))
        return

    # Add/Update user (handles potential DB errors)
    await safe_db_call(db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))

    if user_id == ADMIN_USER_ID:
        user_count = await safe_db_call(db.get_user_count(), default=0)
        movie_count = await safe_db_call(db.get_movie_count(), default=0)
        concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)

        algolia_status = "🟢 Connected" if algolia_client.is_algolia_ready() else "❌ NOT CONNECTED (Check Logs!)"

        admin_message = f"""👑 <b>Admin Console: @{bot_info.username}</b>
Access Level: Full Management

<b>System Status</b>
• Bot Status: 🟢 Online
• Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/<b>{CURRENT_CONC_LIMIT}</b>
• Postgres DB: {movie_count:,} movies
• Uptime: {get_uptime()}
• Algolia Search: <b>{algolia_status}</b>
• Service Note: Render free tier spins down after 15 min inactivity.

<b>Management Commands</b>
• /stats — Real-time stats
• /health — Detailed health check
• /broadcast — Reply to message to send
• /import_json — Reply to a .json file (DB + Algolia Upsert)
• /add_movie — Reply: /add_movie imdb | title | year (DB + Algolia Upsert)
• /remove_dead_movie IMDB_ID — (DB + Algolia Delete)
• <b>/sync_algolia</b> — ⚠️ Force syncs DB to Algolia (Use to fix Algolia issues!)
• /rebuild_index — (DB Only) Re-index clean_titles
• /cleanup_users — Deactivate inactive users
• /export_csv users|movies [limit]
• /set_limit N — Change concurrency cap"""

        await safe_tg_call(message.answer(admin_message))
        return

    # Check capacity for regular users
    if not await ensure_capacity_or_inform(message):
        return

    welcome_text = f"""🎬 Namaskar <b>{message.from_user.first_name}</b>!

Swagat hai instant movie search bot mein.

Bas movie ka naam bhejein (spelling galat hogi toh bhi yeh dhoondh lega).
Example: <code>Kantara 2022</code> ya <code>Kantra</code>

⚠️ <b>Free Service Note:</b>
Yeh bot Render ke free server par chalta hai. Agar 15 minute tak use nahi hota, toh yeh 'so' jaata hai.
Agar bot /start par 10-15 second lagaye, toh chinta na karein, yeh bas server ko jaga raha hai. (Search hamesha fast rahegi).

Join karke "I Have Joined Both" dabayen aur access paayen."""

    await safe_tg_call(message.answer(welcome_text, reply_markup=get_join_keyboard()))

@dp.message(Command("help"))
@handler_timeout(15)
async def help_command(message: types.Message):
    # Add/Update user silently
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))

    help_text = """❓ <b>Bot Ka Upyog Kaise Karein</b>

1.  <b>Instant Search:</b> Movie/Show ka naam seedha message mein bhejein. (Example: <code>Jawan</code>)
2.  <b>Typo Friendly:</b> Agar aap <code>Mirzapur</code> ke bajaye <code>Mirjapur</code> bhi likhenge, toh bhi bot search kar lega. (Thanks to Algolia)
3.  <b>Behtar Results:</b> Naam ke saath saal (year) jodein. (Example: <code>Pushpa 2021</code>)

⚠️ <b>Bot Slow Kyon Hai? (Sirf Start Hone Mein)</b>
Yeh bot free server par hai.
• <b>Start Hone Mein Deri:</b> Agar bot 15 minute use na ho, toh server "so" jaata hai. Dobara /start karne par use "jagne" mein 10-15 second lagte hain.
• <b>Search Hamesha Fast Rahegi:</b> Search (khoj) hamesha instant (0.1 sec) rahegi kyonki hum Algolia ka istemaal karte hain."""

    await safe_tg_call(message.answer(help_text))

@dp.callback_query(F.data == "check_join")
@handler_timeout(15)
async def check_join_callback(callback: types.CallbackQuery):
    # Answer immediately to stop the loading spinner
    await safe_tg_call(callback.answer("Verifying membership..."))

    # Check capacity first
    if not await ensure_capacity_or_inform(callback):
        # ensure_capacity_or_inform already sent the message and answered the callback
        return

    # Check actual membership (currently always True)
    is_member = await check_user_membership(callback.from_user.id)

    if is_member:
        # Get current active user count again for the message
        active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = f"""✅ Verification successful, <b>{callback.from_user.first_name}</b>!

Ab aap instant search access kar sakte hain — apni pasand ki title ka naam bhejein.

(Free tier capacity: {CURRENT_CONC_LIMIT}, abhi active: {active_users}.)"""

        # Try editing the original message
        result = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        # If editing fails (e.g., message too old), send a new message
        if not result:
            await safe_tg_call(bot.send_message(callback.from_user.id, success_text, reply_markup=None))
    else:
        # If membership check fails, answer the callback with an alert
        await safe_tg_call(callback.answer("❌ Aapne abhi tak Channel/Group join nahi kiya hai. Kripya join karke dobara try karein.", show_alert=True))

# =======================================================
# +++++ ALGOLIA SEARCH HANDLER +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(10) # Algolia is fast, 10s should be enough
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id

    # Check membership silently first (replace with real check if needed)
    if not await check_user_membership(user_id):
        await safe_tg_call(message.answer("⚠️ Kripya pehle Channel aur Group join karein...", reply_markup=get_join_keyboard()))
        return

    # Check capacity (also updates user activity)
    if not await ensure_capacity_or_inform(message):
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Kripya kam se kam 2 characters ka query bhejein."))
        return

    # Check if Algolia is ready before proceeding
    if not algolia_client.is_algolia_ready():
        logger.error(f"User {user_id} search failed: Algolia client not ready.")
        await safe_tg_call(message.answer("❌ Search Engine abhi kaam nahi kar raha hai. Admin se sampark karein."))
        return

    # Send searching message
    searching_msg = await safe_tg_call(message.answer(f"⚡️ <b>{original_query}</b> ki khoj jaari hai... (Fast Algolia Search)"))
    if not searching_msg:
        logger.warning(f"Could not send 'searching' message to user {user_id}")
        return # Cannot proceed if we can't edit the message

    # --- Perform Algolia Search ---
    # Use await as algolia_search is now async
    search_results = await algolia_client.algolia_search(original_query, limit=20)

    # Handle no results
    if not search_results:
        await safe_tg_call(searching_msg.edit_text(
            f"🥲 Maaf kijiye, '<b>{original_query}</b>' ke liye match nahi mila.\n(Spelling mistakes check kar li gayi hain)."
        ))
        return

    # Build buttons for results
    buttons = [[InlineKeyboardButton(
        text=movie["title"],
        callback_data=f"get_{movie['imdb_id']}" # Use imdb_id (objectID) for callback
    )] for movie in search_results]

    # Edit the message with results
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> ke liye {len(search_results)} results mile (Instant Search):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))

@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(15)
async def get_movie_callback(callback: types.CallbackQuery):
    # Answer immediately
    await safe_tg_call(callback.answer("File forward ki ja rahi hai…"))

    # Check capacity
    if not await ensure_capacity_or_inform(callback):
        return

    imdb_id = callback.data.split("_", 1)[1]

    # --- Fetch movie details from DB using imdb_id ---
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai."))
        # Inform admin if the user is admin
        if callback.from_user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN: Movie '{imdb_id}' Algolia mein hai par DB mein nahi. /sync_algolia chalayein."))
        return

    # Inform user that forwarding is starting
    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file forward ki ja rahi hai, kripya chat check karein."))

    success = False
    forward_error_message = None

    try:
        # Try forwarding first
        forward_result = await bot.forward_message(
            chat_id=callback.from_user.id,
            from_chat_id=int(movie["channel_id"]),
            message_id=movie["message_id"],
        )
        if forward_result:
            success = True
        else:
             # safe_tg_call might return None or False
             forward_error_message = "Forwarding failed (check bot permissions)."

    except (TelegramAPIError, TelegramBadRequest) as e:
        forward_error_message = str(e).lower()
        logger.error(f"Forward failed for {imdb_id} (MessageID: {movie['message_id']}): {e}")

        # If forward failed because message not found OR it's a JSON import placeholder
        if movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER or 'message to forward not found' in forward_error_message:
            logger.info(f"Forward failed, falling back to send_document for {imdb_id} (FileID: {movie['file_id']})")
            try:
                # Try sending by file_id as fallback
                send_result = await bot.send_document(
                    chat_id=callback.from_user.id,
                    document=movie["file_id"],
                    caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'N/A'})"
                )
                if send_result:
                    success = True
                else:
                    forward_error_message = "Sending document by file_id failed."
            except Exception as e2:
                forward_error_message = f"Send document failed: {e2}"
                logger.error(f"❌ DEAD FILE: Fallback send_document failed for '{movie['title']}' (IMDB: {imdb_id}). Error: {e2}")
    except Exception as e: # Catch any other unexpected error
         forward_error_message = f"Unexpected error during forwarding: {e}"
         logger.error(f"Unexpected error forwarding {imdb_id}: {e}", exc_info=True)


    if not success:
        admin_hint = f"\n\n(Admin Hint: /remove_dead_movie {imdb_id})" if callback.from_user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ Takneeki samasya: <b>{movie['title']}</b> ki file uplabdh nahi hai. ({forward_error_message or 'Unknown error'})" + admin_hint
        # Send error to user
        await safe_tg_call(bot.send_message(callback.from_user.id, error_text))
        # Edit original message to show failure
        await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ki file send nahi ho payi."))

# =======================================================
# +++++ ADMIN COMMANDS (DUAL WRITE TO DB & ALGOLIA) +++++
# =======================================================

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    # Update admin's last active time
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))

    user_count = await safe_db_call(db.get_user_count(), default=0)
    movie_count = await safe_db_call(db.get_movie_count(), default=0)
    concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)

    algolia_status = "🟢 Connected" if algolia_client.is_algolia_ready() else "❌ NOT CONNECTED"

    stats_msg = f"""📊 <b>Live System Statistics</b>

🟢 Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}
👥 Total Users: {user_count:,}
🎬 Indexed Movies (DB): {movie_count:,}
⚡️ Algolia Status: <b>{algolia_status}</b>
⚙️ Status: Operational
⏰ Uptime: {get_uptime()}"""

    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(1800) # 30 min timeout for potentially long operation
async def broadcast_command(message: types.Message):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Broadcast ke liye kisi message ko reply karein."))
        return

    users = await safe_db_call(db.get_all_users(), timeout=30, default=[])
    if not users:
        await safe_tg_call(message.answer("❌ Database mein koi active user nahi mila."))
        return

    total_users = len(users)
    success, failed = 0, 0
    progress_msg = await safe_tg_call(message.answer(f"📤 Broadcasting to <b>{total_users:,}</b> users…"))
    if not progress_msg:
        logger.error("Failed to send initial broadcast progress message.")
        return

    start_broadcast_time = datetime.utcnow()

    for i, uid in enumerate(users):
        # Use copy_to for flexibility
        result = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=3)

        if result:
            success += 1
        elif result is False: # Explicit False means bot was blocked
            failed += 1
            await safe_db_call(db.deactivate_user(uid), timeout=3) # Deactivate user
        else: # None means timeout or other Telegram error
            failed += 1
            # Optionally deactivate on other errors too, or just log
            logger.warning(f"Broadcast failed for user {uid} (not blocked, maybe timeout or deleted chat)")

        # Update progress message every 100 users or every 10 seconds
        now = datetime.utcnow()
        if (i + 1) % 100 == 0 or (now - start_broadcast_time).total_seconds() > 10:
             if progress_msg:
                try:
                    await safe_tg_call(progress_msg.edit_text(
                        f"📤 Broadcasting… {i+1}/{total_users}\n"
                        f"✅ Sent: {success:,} | ❌ Failed/Blocked: {failed:,}"
                    ))
                    start_broadcast_time = now # Reset timer after update
                except TelegramBadRequest: # Ignore "message is not modified"
                    pass
             await asyncio.sleep(0.05) # Small sleep to yield control

    # Final update
    final_text = f"✅ <b>Broadcast Complete!</b>\nSent: {success:,}\nFailed/Blocked: {failed:,}\nTotal Users: {total_users:,}"
    if progress_msg:
        await safe_tg_call(progress_msg.edit_text(final_text))
    else:
        await safe_tg_call(message.answer(final_text)) # Fallback if initial message failed


@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120) # Increase timeout slightly
async def cleanup_users_command(message: types.Message):
    status_msg = await safe_tg_call(message.answer("🧹 30 din se inactive users ko clean kiya ja raha hai…"))
    removed_count = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=90, default=0) # Longer timeout for DB op
    new_count = await safe_db_call(db.get_user_count(), default=0)

    result_text = f"✅ <b>Cleanup complete!</b>\nDeactivated: {removed_count:,}\nRemaining Active Users: {new_count:,}"
    if status_msg:
        await safe_tg_call(status_msg.edit_text(result_text))
    else:
        await safe_tg_call(message.answer(result_text))

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(20)
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await safe_tg_call(message.answer("❌ Kripya video/document par reply karke command bhejein:\n<code>/add_movie imdb_id | title | year</code>"))
        return

    try:
        # Extract details from command text
        full_command = message.text.split(" ", 1)[1] # Remove /add_movie part
        parts = [p.strip() for p in full_command.split("|")]
        if len(parts) < 2: raise ValueError("Not enough parts")
        imdb_id = parts[0]
        title = parts[1]
        year = parts[2] if len(parts) > 2 else None

        # Get file details from replied message
        replied_msg = message.reply_to_message
        file_id = replied_msg.video.file_id if replied_msg.video else replied_msg.document.file_id
        message_id = replied_msg.message_id
        channel_id = replied_msg.chat.id

    except Exception as e:
        logger.warning(f"Failed to parse /add_movie command: {e}")
        await safe_tg_call(message.answer("❌ Format galat hai. Use:\n<code>/add_movie imdb_id | title | year</code>"))
        return

    # 1. Add/Update in Database
    status_msg = await safe_tg_call(message.answer(f"⏳ Processing '<b>{title}</b>' in DB..."))
    db_result = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, message_id=message_id, channel_id=channel_id
    ), default=False)

    if db_result is True:
        db_status = f"✅ Movie '<b>{title}</b>' database mein add ho gayi."
    elif db_result == "updated":
        db_status = f"✅ Movie '<b>{title}</b>' database mein update ho gayi."
    elif db_result == "duplicate":
        db_status = f"⚠️ Movie '<b>{title}</b>' pehle se database mein hai (Integrity Error)."
        if status_msg: await safe_tg_call(status_msg.edit_text(db_status))
        return # No need to update Algolia if it was a DB constraint error
    else: # False or None
        db_status = "❌ Movie DB mein add/update karne me error aaya."
        if status_msg: await safe_tg_call(status_msg.edit_text(db_status))
        return # Stop if DB failed

    if status_msg: await safe_tg_call(status_msg.edit_text(db_status + "\n⏳ Processing in Algolia..."))
    else: await safe_tg_call(message.answer(db_status + "\n⏳ Processing in Algolia..."))


    # 2. Add/Update in Algolia
    algolia_data = {'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year}
    success_algolia = await algolia_client.algolia_add_movie(algolia_data) # This is now upsert

    if success_algolia:
        algolia_status = "✅ Movie Algolia index mein bhi add/update ho gayi."
    else:
        algolia_status = "❌ WARNING: Movie DB mein add/update hui, par Algolia mein fail ho gayi! /sync_algolia chalayein."

    final_status = db_status + "\n" + algolia_status
    if status_msg:
        await safe_tg_call(status_msg.edit_text(final_status))
    else:
        # If the status message failed, send the final status as a new message
        await safe_tg_call(message.answer(final_status))


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800) # 30 min
async def import_json_command(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        await safe_tg_call(message.answer("❌ Kripya ek .json file par reply karke command bhejein."))
        return
    doc = message.reply_to_message.document
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"):
        await safe_tg_call(message.answer("❌ File .json format mein honi chahiye."))
        return

    status_msg = await safe_tg_call(message.answer("⏳ JSON file download ki ja rahi hai..."))
    if not status_msg: return

    try:
        file_io = io.BytesIO()
        file = await bot.get_file(doc.file_id)
        # Use ThreadPoolExecutor for blocking download
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: bot.download_file(file.file_path, file_io, timeout=60))
        # await bot.download_file(file.file_path, file_io, timeout=60) # This might block

        file_io.seek(0)
        movies_json_list = json.loads(file_io.read().decode('utf-8'))
        if not isinstance(movies_json_list, list):
             await safe_tg_call(status_msg.edit_text("❌ Error: JSON file ek list (array) nahi hai."))
             return
    except Exception as e:
        logger.error(f"JSON download/parse error: {e}", exc_info=True)
        await safe_tg_call(status_msg.edit_text(f"❌ JSON file padhne ya download karne me error: {e}"))
        return

    total = len(movies_json_list)
    added_db = 0
    updated_db = 0
    skipped_db = 0 # For items missing file_id/title or DB errors
    failed_db = 0
    algolia_batch_to_upsert = [] # Batch for Algolia upsert

    await safe_tg_call(status_msg.edit_text(f"⏳ (Step 1/2) JSON import DB mein shuru... Total {total:,} items."))

    start_import_time = datetime.utcnow()

    for i, item in enumerate(movies_json_list):
        try:
            file_id = item.get("file_id")
            # Use filename from JSON if available, fallback to title (less reliable)
            filename = item.get("filename") or item.get("title")

            if not file_id or not filename:
                logger.warning(f"Skipping item {i+1}: Missing file_id or filename/title.")
                skipped_db += 1
                continue

            # Generate a consistent imdb_id for JSON imports if not provided
            # Use file_id hash which should be unique
            imdb_id = item.get("imdb_id", f"json_{hashlib.md5(file_id.encode()).hexdigest()}")
            parsed_info = parse_filename(filename)

            # --- DB Upsert Call ---
            db_result = await safe_db_call(db.add_movie(
                imdb_id=imdb_id, title=parsed_info["title"] or "Untitled", year=parsed_info["year"],
                file_id=file_id, message_id=AUTO_MESSAGE_ID_PLACEHOLDER, channel_id=0 # Placeholder channel/message ID
            ), default=False)

            if db_result is True:
                added_db += 1
                # Add to Algolia batch only if new or updated successfully in DB
                algolia_batch_to_upsert.append({
                    'objectID': imdb_id, 'imdb_id': imdb_id,
                    'title': parsed_info["title"] or "Untitled", 'year': parsed_info["year"]
                })
            elif db_result == "updated":
                updated_db += 1
                # Also add updated items to Algolia batch
                algolia_batch_to_upsert.append({
                    'objectID': imdb_id, 'imdb_id': imdb_id,
                    'title': parsed_info["title"] or "Untitled", 'year': parsed_info["year"]
                })
            elif db_result == "duplicate":
                # This case should ideally not happen with the new upsert logic,
                # but log it just in case. It likely means an integrity error other than PK.
                 logger.warning(f"Skipped item {i+1} due to DB duplicate/integrity error: {imdb_id}")
                 skipped_db += 1
            else: # False or None
                logger.error(f"DB add/update failed for item {i+1}: {imdb_id}")
                failed_db += 1

        except Exception as e:
            logger.error(f"Error processing JSON item {i+1}: {e}", exc_info=True)
            failed_db += 1

        # Update status periodically
        now = datetime.utcnow()
        if (i + 1) % 100 == 0 or (now - start_import_time).total_seconds() > 10 or (i+1) == total:
            try:
                await safe_tg_call(status_msg.edit_text(
                    f"⏳ (Step 1/2) Processing DB... {i+1}/{total:,}\n"
                    f"✅ Added: {added_db:,} | 🔄 Updated: {updated_db:,}\n"
                    f"↷ Skipped: {skipped_db:,} | ❌ Failed: {failed_db:,}"
                ))
                start_import_time = now # Reset timer
            except TelegramBadRequest: pass # Ignore "message not modified"
            await asyncio.sleep(0.1) # Yield control

    db_summary = (f"✅ <b>DB Import Complete!</b>\n"
                  f"• Added: {added_db:,} | Updated: {updated_db:,}\n"
                  f"• Skipped: {skipped_db:,} | Failed: {failed_db:,}")

    await safe_tg_call(status_msg.edit_text(
        f"{db_summary}\n\n"
        f"⏳ (Step 2/2) Ab {len(algolia_batch_to_upsert):,} records ko Algolia mein upload/update kiya ja raha hai..."
    ))

    # 2. Batch Upsert to Algolia
    if algolia_batch_to_upsert:
        success_algolia = await algolia_client.algolia_add_batch_movies(algolia_batch_to_upsert) # This is now upsert
        if success_algolia:
            algolia_status = f"• Algolia Uploaded/Updated: {len(algolia_batch_to_upsert):,}"
            final_status = f"✅ <b>JSON Import Complete! (DB + Algolia)</b>"
        else:
             algolia_status = "• Algolia Upload FAILED! /sync_algolia chalayein."
             final_status = f"❌ <b>JSON Import Failed (Algolia Step)!</b>"

        await safe_tg_call(status_msg.edit_text(
            f"{final_status}\n"
            f"• DB Added: {added_db:,} | DB Updated: {updated_db:,}\n"
            f"• DB Skipped: {skipped_db:,}\n"
            f"{algolia_status}"
        ))
    else:
        # No items to upload to Algolia (all skipped or failed in DB)
        await safe_tg_call(status_msg.edit_text(
            f"{db_summary}\n"
            f"• Algolia Upload: 0 (kuch naya ya update karne layak nahi tha)"
        ))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID"))
        return
    imdb_id = args[1].strip()

    status_msg = await safe_tg_call(message.answer(f"⏳ Removing movie <code>{imdb_id}</code>..."))

    # Check if movie exists in DB first (optional but good)
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    if not movie:
        db_status = f"ℹ️ Movie <code>{imdb_id}</code> DB mein nahi mili (shayad pehle hi delete ho gayi)."
    else:
        # 1. Remove from DB
        success_db = await safe_db_call(db.remove_movie_by_imdb(imdb_id), default=False)
        db_status = f"✅ DB se remove ho gayi: <b>{movie['title']}</b>" if success_db else f"❌ DB se remove karne mein error."

    if status_msg: await safe_tg_call(status_msg.edit_text(db_status + "\n⏳ Removing from Algolia..."))
    else: await safe_tg_call(message.answer(db_status + "\n⏳ Removing from Algolia..."))


    # 2. Remove from Algolia (always try even if DB failed or movie wasn't found in DB)
    success_algolia = await algolia_client.algolia_remove_movie(imdb_id)
    algolia_status = f"✅ Algolia se bhi remove ho gayi." if success_algolia else f"❌ Algolia se remove karne mein error (ya pehle se nahi thi)."

    final_status = db_status + "\n" + algolia_status
    if status_msg:
        await safe_tg_call(status_msg.edit_text(final_status))
    else:
        await safe_tg_call(message.answer(final_status))


# =======================================================
# +++++ ALGOLIA SYNC COMMAND +++++
# =======================================================
@dp.message(Command("sync_algolia"), AdminFilter())
@handler_timeout(1800) # 30 min
async def sync_algolia_command(message: types.Message):
    """
    Yeh "Fix-it" button hai. Poore PostgreSQL DB ko padhega aur Algolia index ko overwrite kar dega.
    """
    if not algolia_client.is_algolia_ready():
        await safe_tg_call(message.answer("❌ Algolia client configure nahi hai. Environment variables check karein."))
        return

    status_msg = await safe_tg_call(message.answer("⚠️ Full Sync Shuru...\n"
                                     "Yeh process poore PostgreSQL database ko padhega aur Algolia index ko overwrite karega.\n"
                                     "Ismein database size ke hisaab se time lag sakta hai..."))
    if not status_msg: return

    try:
        # 1. Fetch all necessary data from DB
        logger.info("Sync: Fetching all movies from DB for Algolia sync...")
        await safe_tg_call(status_msg.edit_text(status_msg.text + "\n⏳ DB se data fetch kiya ja raha hai..."))

        all_movies_db = await safe_db_call(db.get_all_movies_for_sync(), timeout=300) # 5 min timeout for fetch

        if all_movies_db is None: # Indicates DB error during fetch
            await safe_tg_call(status_msg.edit_text("❌ DB se data fetch karne mein error aaya. Sync cancelled."))
            return

        db_count = len(all_movies_db)
        logger.info(f"Sync: Fetched {db_count:,} movies from DB.")
        await safe_tg_call(status_msg.edit_text(status_msg.text + f"\n✅ DB se {db_count:,} movies fetch ho gayi."))

        # 2. Sync data to Algolia (replace all objects)
        await safe_tg_call(status_msg.edit_text(status_msg.text + "\n⏳ Ab Algolia par data upload/overwrite kiya ja raha hai..."))
        logger.info("Sync: Starting Algolia index replacement...")

        success, total_uploaded = await algolia_client.algolia_sync_data(all_movies_db)

        if success:
            final_text = (f"✅ <b>Full Sync Complete!</b>\n"
                          f"{total_uploaded:,} movies Algolia index mein safalta se upload/overwrite ho gayi.")
            logger.info("Sync: Algolia sync completed successfully.")
        else:
            final_text = ("❌ <b>Sync Failed!</b>\n"
                          "Algolia par upload karne mein error aaya. Server logs check karein.")
            logger.error("Sync: Algolia sync failed.")

        await safe_tg_call(status_msg.edit_text(final_text))

    except Exception as e:
        logger.error(f"Error during /sync_algolia command: {e}", exc_info=True)
        await safe_tg_call(message.answer(f"❌ Sync command fail ho gaya: {e}"))
        if status_msg:
             try: await status_msg.delete() # Clean up status message on error
             except: pass


@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300) # 5 min
async def rebuild_index_command(message: types.Message):
    status_msg = await safe_tg_call(message.answer("🔧 (PostgreSQL DB Only) Clean titles reindex ho rahe hain…"))
    result = await safe_db_call(db.rebuild_clean_titles(), timeout=240, default=(0, 0)) # Longer timeout
    updated, total = result
    result_text = f"✅ DB Reindex complete: Updated <b>{updated:,}</b> of ~{total:,} titles."
    if status_msg:
        await safe_tg_call(status_msg.edit_text(result_text))
    else:
        await safe_tg_call(message.answer(result_text))

@dp.message(Command("export_csv"), AdminFilter())
@handler_timeout(120) # Increase timeout for potentially large exports
async def export_csv_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2 or args[1].lower() not in ("users", "movies"):
        await safe_tg_call(message.answer("❌ Use: /export_csv users|movies [limit]\nExample: <code>/export_csv movies 5000</code>"))
        return

    kind = args[1].lower()
    limit = int(args[2]) if len(args) > 2 and args[2].isdigit() else 2000 # Default limit
    if limit <= 0: limit = 2000

    status_msg = await safe_tg_call(message.answer(f"⏳ Exporting {kind} data (limit: {limit:,})..."))
    if not status_msg: return

    try:
        if kind == "users":
            rows = await safe_db_call(db.export_users(limit=limit), timeout=90, default=[])
            if not rows: await safe_tg_call(status_msg.edit_text("❌ Export ke liye koi user data nahi mila.")); return
            header = "user_id,username,first_name,last_name,joined_date,last_active,is_active\n"
            # Handle potential commas or quotes in names
            csv_data = header + "\n".join([
                f"{r['user_id']},"
                f"\"{r['username'] or ''}\","
                f"\"{r['first_name'] or ''}\","
                f"\"{r['last_name'] or ''}\","
                f"{r['joined_date']},{r['last_active']},{r['is_active']}"
                for r in rows
            ])
            filename = "users_export.csv"
        else: # movies
            rows = await safe_db_call(db.export_movies(limit=limit), timeout=90, default=[])
            if not rows: await safe_tg_call(status_msg.edit_text("❌ Export ke liye koi movie data nahi mila.")); return
            header = "imdb_id,title,year,channel_id,message_id,added_date\n"
            # Handle potential commas or quotes in titles
            csv_data = header + "\n".join([
                f"{r['imdb_id']},"
                f"\"{r['title'].replace('\"', '\"\"') if r['title'] else ''}\","
                f"{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}"
                for r in rows
            ])
            filename = "movies_export_db.csv"

        # Send as document
        file_bytes = csv_data.encode("utf-8")
        input_file = BufferedInputFile(file_bytes, filename=filename)
        await safe_tg_call(message.answer_document(input_file, caption=f"{kind.capitalize()} export ({len(rows):,} rows)"))
        await safe_tg_call(status_msg.delete()) # Delete "Exporting..." message

    except Exception as e:
        logger.error(f"CSV Export Error: {e}", exc_info=True)
        await safe_tg_call(status_msg.edit_text(f"❌ Export fail ho gaya: {e}"))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer(f"Use: /set_limit N (Current: {CURRENT_CONC_LIMIT})\nExample: <code>/set_limit 50</code>"))
        return
    try:
        val = int(args[1])
        if not (5 <= val <= 200): # Allow higher limit if needed
            await safe_tg_call(message.answer("❌ Limit 5 se 200 ke beech honi chahiye."))
            return
        CURRENT_CONC_LIMIT = val
        await safe_tg_call(message.answer(f"✅ Concurrency limit set to <b>{CURRENT_CONC_LIMIT}</b>"))
        logger.info(f"Concurrency limit changed to {CURRENT_CONC_LIMIT} by admin.")
    except ValueError:
         await safe_tg_call(message.answer("❌ Invalid number."))

# =======================================================
# +++++ AUTO INDEXING FROM CHANNEL POSTS +++++
# =======================================================
@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    # Check if it's the correct channel and a video/document
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document):
        return

    caption = message.caption or ""
    movie_info = extract_movie_info(caption)

    if not movie_info or not movie_info.get("title"):
        logger.warning(f"Auto-index skipped (Channel: {message.chat.id}, Msg: {message.message_id}): Could not parse title from caption: {caption[:80]}...")
        return

    file_id = message.video.file_id if message.video else message.document.file_id
    # Use IMDB ID if found, otherwise generate one based on message ID
    imdb_id = movie_info.get("imdb_id", f"auto_{message.message_id}")

    # 1. Add/Update in Database
    db_result = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=movie_info["title"], year=movie_info.get("year"),
        file_id=file_id, message_id=message.message_id, channel_id=message.chat.id,
    ), default=False)

    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: {movie_info['title']}):"

    if db_result is True:
        logger.info(f"{log_prefix} Added to DB.")
        db_success = True
    elif db_result == "updated":
        logger.info(f"{log_prefix} Updated in DB.")
        db_success = True
    elif db_result == "duplicate":
        logger.warning(f"{log_prefix} Skipped DB (duplicate/integrity error).")
        db_success = False # Don't try Algolia if DB had issues
    else: # False or None
        logger.error(f"{log_prefix} FAILED DB operation.")
        db_success = False

    # 2. Add/Update in Algolia only if DB operation was successful
    if db_success:
        algolia_data = {
            'objectID': imdb_id,
            'imdb_id': imdb_id,
            'title': movie_info["title"],
            'year': movie_info.get("year")
        }
        success_algolia = await algolia_client.algolia_add_movie(algolia_data)
        if success_algolia:
            logger.info(f"{log_prefix} Added/Updated in Algolia.")
        else:
            logger.error(f"{log_prefix} FAILED Algolia operation.")


# =======================================================
# +++++ GLOBAL ERROR HANDLER +++++
# =======================================================
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    # Log the detailed error
    logger.exception(f"Unhandled error during update processing: {exception}", exc_info=True)

    # Try to inform the user (if possible)
    message_to_send = "❗️ Ek unexpected error hua. Kripya dobara try karein ya admin se sampark karein."
    target_chat_id = None

    if update.message:
        target_chat_id = update.message.chat.id
    elif update.callback_query and update.callback_query.message:
        target_chat_id = update.callback_query.message.chat.id
        # Also try to answer the callback query to remove loading state
        try:
            await update.callback_query.answer("Error processing request.", show_alert=False)
        except Exception:
            pass # Ignore if answering fails

    if target_chat_id:
        try:
            await bot.send_message(target_chat_id, message_to_send)
        except Exception as e_inner:
            logger.error(f"Error handler failed to send message to {target_chat_id}: {e_inner}")

# =======================================================
# +++++ POLLING (for local testing, if webhook is not set) +++++
# =======================================================
# This part is usually commented out when deploying with webhook
# async def main():
#     await db.init_db()
#     if not WEBHOOK_URL:
#         logger.warning("No WEBHOOK_URL detected, running in polling mode.")
#         await bot.delete_webhook(drop_pending_updates=True)
#         await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
#     else:
#         logger.info("Webhook URL is set. Start the FastAPI app (e.g., uvicorn bot:app) to use webhook.")

# if __name__ == '__main__':
#      try:
#          asyncio.run(main())
#      except (KeyboardInterrupt, SystemExit):
#          logger.info("Bot stopped.")
