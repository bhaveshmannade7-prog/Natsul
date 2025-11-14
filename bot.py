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
from typing import List, Dict, Callable
from functools import wraps
import concurrent.futures

# --- Load dotenv FIRST ---
from dotenv import load_dotenv
load_dotenv()

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
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Database Imports ---
from database import Database, clean_text_for_search
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
    
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
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

if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID set nahi hai. Admin commands kaam nahi karenge.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID set nahi hai. Auto-indexing aur Migration kaam nahi karenge.")
if not JOIN_CHANNEL_USERNAME and not USER_GROUP_USERNAME:
    logger.warning("--- KOI JOIN CHECK SET NAHI HAI. Membership check skip ho jayega. ---")


# ============ TIMEOUTS & SEMAPHORES ============
HANDLER_TIMEOUT = 15
DB_OP_TIMEOUT = 10
TG_OP_TIMEOUT = 8

DB_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(15)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(25)

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
try:
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    storage = MemoryStorage()
    
    # --- 3 Database Objects ---
    db_primary = Database(DATABASE_URL_PRIMARY)
    db_fallback = Database(DATABASE_URL_FALLBACK)
    db_neon = NeonDB(NEON_DATABASE_URL)
    
    # --- Dependency Injection ---
    dp = Dispatcher(
        storage=storage, 
        db_primary=db_primary, 
        db_fallback=db_fallback, 
        db_neon=db_neon
    )
    
    logger.info("Bot, Dispatcher, aur 3 Database objects (M+M+N) initialize ho gaye.")
except Exception as e:
    logger.critical(f"Bot/Dispatcher initialize nahi ho paya: {e}", exc_info=True)
    raise SystemExit("Bot initialization fail.")

start_time = datetime.now(timezone.utc)
monitor_task: asyncio.Task | None = None
executor: concurrent.futures.ThreadPoolExecutor | None = None
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure():
    logger.info("Graceful shutdown shuru ho raha hai...")
    
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try: await asyncio.wait_for(monitor_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError): pass
            
    if WEBHOOK_URL:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook delete kar diya gaya hai.")
        except Exception as e: logger.error(f"Webhook delete karte waqt error: {e}")
            
    try: await dp.storage.close()
    except Exception as e: logger.error(f"Dispatcher storage close karte waqt error: {e}")
        
    try:
        if bot.session: await bot.session.close()
        logger.info("Bot session close ho gaya.")
    except Exception as e: logger.error(f"Bot session close karte waqt error: {e}")
        
    if executor:
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("ThreadPoolExecutor shutdown ho gaya.")
        
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
    logger.info("Signal handlers (SIGTERM, SIGINT) set ho gaye.")

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
                    try: await bot.send_message(target_chat_id, "⚠️ Server request time out ho gayi. Kripya dobara koshish karein.")
                    except Exception: pass
                if callback_query:
                    try: await callback_query.answer("Timeout", show_alert=False)
                    except Exception: pass
            except Exception as e:
                logger.exception(f"Handler {func.__name__} mein error: {e}")
        return wrapper
    return decorator

# ============ SAFE API CALL WRAPPERS ============
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB call timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         if hasattr(coro, '__self__') and hasattr(coro.__self__, '_handle_db_error'):
             await coro.__self__._handle_db_error(e)
         return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None):
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    try:
        async with semaphore_to_use:
            if semaphore: await asyncio.sleep(0.1) 
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError: 
        logger.warning(f"TG call timeout: {getattr(coro, '__name__', 'unknown_coro')}"); return None
    except (TelegramAPIError, TelegramBadRequest) as e:
        error_msg = str(e).lower()
        if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
            logger.info(f"TG: Bot block ya user deactivated."); return False
        elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
            logger.info(f"TG: Chat nahi mila."); return False
        elif "message is not modified" in error_msg:
            logger.debug(f"TG: Message modify nahi hua."); return None
        elif "message to delete not found" in error_msg or "message to copy not found" in error_msg:
            logger.debug(f"TG: Message (delete/copy) nahi mila."); return None
        elif "too many requests" in error_msg:
            logger.warning(f"TG: FLOOD WAIT (Too Many Requests). {e}"); await asyncio.sleep(5); return None
        else:
            logger.warning(f"TG Error: {e}"); return None
    except Exception as e:
        logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}"); return None

# ============ FILTERS & HELPER FUNCTIONS ============
class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    delta = datetime.now(timezone.utc) - start_time; total_seconds = int(delta.total_seconds())
    days, r = divmod(total_seconds, 86400); hours, r = divmod(r, 3600); minutes, seconds = divmod(r, 60)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    check_channel = bool(JOIN_CHANNEL_USERNAME)
    check_group = bool(USER_GROUP_USERNAME)
    if not check_channel and not check_group: return True
    try:
        tasks_to_run = []
        if check_channel:
            tasks_to_run.append(safe_tg_call(bot.get_chat_member(chat_id=f"@{JOIN_CHANNEL_USERNAME}", user_id=user_id), timeout=5))
        if check_group:
            tasks_to_run.append(safe_tg_call(bot.get_chat_member(chat_id=f"@{USER_GROUP_USERNAME}", user_id=user_id), timeout=5))
        results = await asyncio.gather(*tasks_to_run)
        valid_statuses = {"member", "administrator", "creator"}
        is_in_channel = True; is_in_group = True; result_index = 0
        if check_channel:
            channel_member = results[result_index]
            is_in_channel = isinstance(channel_member, types.ChatMember) and channel_member.status in valid_statuses
            if channel_member in [False, None]: logger.warning(f"Membership check fail (Channel @{JOIN_CHANNEL_USERNAME}).")
            result_index += 1
        if check_group:
            group_member = results[result_index]
            is_in_group = isinstance(group_member, types.ChatMember) and group_member.status in valid_statuses
            if group_member in [False, None]: logger.warning(f"Membership check fail (Group @{USER_GROUP_USERNAME}).")
        return is_in_channel and is_in_group
    except Exception as e:
        if not isinstance(e, (TelegramBadRequest, TelegramAPIError)): logger.error(f"Membership check mein error {user_id}: {e}", exc_info=True)
        else: logger.info(f"Membership check API error {user_id}: {e}")
        return False

def get_join_keyboard() -> InlineKeyboardMarkup | None:
    buttons = []
    if JOIN_CHANNEL_USERNAME: buttons.append([InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https.t.me/{JOIN_CHANNEL_USERNAME}")])
    if USER_GROUP_USERNAME: buttons.append([InlineKeyboardButton(text="👥 Group Join Karein", url=f"https.t.me/{USER_GROUP_USERNAME}")])
    if buttons: buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya (Verify)", callback_data="check_join")]); return InlineKeyboardMarkup(inline_keyboard=buttons)
    return None

def get_full_limit_keyboard() -> InlineKeyboardMarkup | None:
    if not ALTERNATE_BOTS: return None
    buttons = [[InlineKeyboardButton(text=f"🚀 Dusra Bot @{b}", url=f"https.t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str | None) -> Dict[str, str] | None:
    if not caption: return None
    info = {}; lines = caption.splitlines(); title = lines[0].strip() if lines else ""
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]): title += " " + lines[1].strip()
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
            f"Aapki request abhi hold par hai. Kripya thodi der baad try karein ya neeche diye gaye alternate bots ka istemal karein:")

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    loop = asyncio.get_running_loop()
    while True:
        try:
            start_time = loop.time()
            await asyncio.sleep(1)
            lag = (loop.time() - start_time) - 1
            if lag > 0.5: logger.warning(f"⚠️ Event loop lag detect hua: {lag:.3f}s")
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.info("Event loop monitor band ho raha hai."); break
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}", exc_info=True); await asyncio.sleep(120)

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor_task, executor
    logger.info("Application startup shuru ho raha hai...")
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop(); loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialize ho gaya.")

    # --- 3 DB Init ---
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
        await db_neon.init_db()
        logger.info("Database 3 (NeonDB/Postgres) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database 3 (NeonDB/Postgres) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("NeonDB/Postgres connection fail (startup).") from e

    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor start ho gaya.")

    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            if not current_webhook or current_webhook.url != WEBHOOK_URL:
                 logger.info(f"Webhook set kiya ja raha hai: {WEBHOOK_URL}...")
                 await bot.set_webhook(
                     url=WEBHOOK_URL,
                     allowed_updates=dp.resolve_used_update_types(),
                     secret_token=(WEBHOOK_SECRET or None),
                     drop_pending_updates=True
                 )
                 logger.info("Webhook set ho gaya.")
            else:
                 logger.info("Webhook pehle se sahi set hai.")
        except Exception as e:
            logger.error(f"Webhook setup mein error: {e}", exc_info=True)
    else:
        logger.warning("WEBHOOK_URL set nahi hai.")

    setup_signal_handlers()
    logger.info("Application startup poora hua. Bot taiyar hai.")
    yield
    logger.info("Application shutdown sequence shuru ho raha hai...")
    await shutdown_procedure()
    logger.info("Application shutdown poora hua.")


app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK / HEALTHCHECK ROUTES ============

async def _process_update_safe(update_obj: Update, bot_instance: Bot):
    try:
        # 3 DBs ko pass karein
        await dp.feed_update(
            bot=bot_instance, 
            update=update_obj, 
            db_primary=db_primary, 
            db_fallback=db_fallback, 
            db_neon=db_neon
        )
    except Exception as e:
        logger.exception(f"Update process karte waqt error {update_obj.update_id}: {e}")

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token mila.")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid Secret Token")
    try:
        telegram_update = Update(**update)
        background_tasks.add_task(_process_update_safe, telegram_update, bot)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook update parse nahi kar paya: {e}", exc_info=False)
        logger.debug(f"Failed update data: {update}")
        return {"ok": False, "error": f"Invalid update format: {e}"}

@app.get("/")
@app.get("/ping")
async def ping():
    return {"status": "ok", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    # Teeno DBs ko parallel check karein
    db_primary_ok, db_fallback_ok, neon_ok = await asyncio.gather(
        safe_db_call(db_primary.is_ready(), default=False),
        safe_db_call(db_fallback.is_ready(), default=False),
        safe_db_call(db_neon.is_ready(), default=False)
    )

    status_code = 200
    status_msg = "ok"
    
    if not db_primary_ok:
        status_msg = "error_mongodb_primary_connection"
        status_code = 503
    elif not db_fallback_ok:
        status_msg = "degraded_mongodb_fallback_connection"
    elif not neon_ok:
        status_msg = "degraded_neondb_connection"
    
    return {
        "status": status_msg,
        "database_mongo_primary_connected": db_primary_ok,
        "database_mongo_fallback_connected": db_fallback_ok,
        "database_neon_connected": neon_ok,
        "search_logic": "Mongo1 -> Neon -> Mongo2",
        "uptime": get_uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, status_code

# ============ USER CAPACITY CHECK ============

async def ensure_capacity_or_inform(
    message_or_callback: types.Message | types.CallbackQuery,
    db_primary: Database # User data sirf primary DB mein
) -> bool:
    user = message_or_callback.from_user
    if not user: return True
    
    target_chat_id = None
    if isinstance(message_or_callback, types.Message):
        target_chat_id = message_or_callback.chat.id
    elif isinstance(message_or_callback, types.CallbackQuery) and message_or_callback.message:
        target_chat_id = message_or_callback.message.chat.id
    
    # User ko sirf primary DB mein add/update karein
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    if user.id == ADMIN_USER_ID: return True
        
    active = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    if active >= CURRENT_CONC_LIMIT:
        logger.warning(f"Capacity full: {active}/{CURRENT_CONC_LIMIT}. User {user.id} ki request hold par.")
        if target_chat_id:
            await safe_tg_call(
                bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()),
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
        if isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server busy, kripya thodi der baad try karein.", show_alert=True))
        return False
        
    return True

# =======================================================
# +++++ BOT HANDLERS: USER COMMANDS +++++
# =======================================================

@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    user = message.from_user
    if not user: return
    user_id = user.id

    # Admin Panel
    if user_id == ADMIN_USER_ID:
        user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
        mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
        mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
        neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
        concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users = await asyncio.gather(
            user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task
        )
        
        mongo_1_ready, mongo_2_ready, neon_ready = await asyncio.gather(
            safe_db_call(db_primary.is_ready(), default=False),
            safe_db_call(db_fallback.is_ready(), default=False),
            safe_db_call(db_neon.is_ready(), default=False)
        )
        
        def status_icon(is_ok): return "🟢" if is_ok else "❌"
        def count_str(c): return f"{c:,}" if c >= 0 else "Error"

        admin_message = (
            f"👑 <b>Admin Panel (Mongo+Mongo+Neon)</b>\n\n"
            f"<b>📊 Live Status</b>\n"
            f"  - 🟢 Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
            f"  - 👥 Total Users (Mongo 1): {count_str(user_count)}\n"
            f"  - 🎬 Movies (Mongo 1): {count_str(mongo_1_count)}\n"
            f"  - 🗂️ Movies (Mongo 2): {count_str(mongo_2_count)}\n"
            f"  - 🗄️ Index (Neon): {count_str(neon_count)}\n"
            f"  - ⏰ Uptime: {get_uptime()}\n\n"
            f"<b>🔌 Connections</b>\n"
            f"  - {status_icon(mongo_1_ready)} MongoDB 1 (Primary)\n"
            f"  - {status_icon(mongo_2_ready)} MongoDB 2 (Fallback)\n"
            f"  - {status_icon(neon_ready)} NeonDB (Fuzzy)\n\n"
            f"<b>Search Logic:</b> Mongo 1 → Neon → Mongo 2\n\n"
            f"<b>🛠️ Commands</b>\n"
            f"  - /stats | /health | /get_user `ID`\n"
            f"  - /broadcast (Reply karke)\n"
            f"  - /set_limit `N` (e.g., 5-200)\n\n"
            f"<b>🗃️ Data & Indexing</b>\n"
            f"  - <b>/remove_library_duplicates</b> ⚠️ (NeonDB + All DBs)\n"
            f"  - <b>/backup_channel</b> 🚀 (NeonDB → New Channel)\n"
            f"  - <b>/sync_mongo_1_to_2</b> 🔄 (Mongo 1 → Mongo 2)\n"
            f"  - <b>/sync_mongo_1_to_neon</b> 🔄 (Mongo 1 → Neon)\n"
            f"  - <b>/cleanup_mongo_1</b> (Sirf Mongo 1)\n"
            f"  - <b>/cleanup_mongo_2</b> (Sirf Mongo 2)\n"
            f"  - /rebuild_index_1 (Sirf Mongo 1)\n"
            f"  - /rebuild_index_2 (Sirf Mongo 2)\n"
            f"  - /cleanup_users (Inactive > 30d)\n\n"
            f"<b>⭐️ Migration:</b> Files ko `LIBRARY_CHANNEL` se *forward* karein (Sirf Admin)."
        )
        await safe_tg_call(message.answer(admin_message), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # --- Regular User ---
    if not await ensure_capacity_or_inform(message, db_primary):
        return
        
    is_member = await check_user_membership(user.id)
    join_markup = get_join_keyboard()
    
    if is_member:
        welcome_text = (
            f"🎬 Namaste <b>{user.first_name}</b>!\n\n"
            f"Yeh ek movie search bot hai. Bas movie ka naam bhejein.\n"
            f"Example: <code>Kantara 2022</code> ya <code>Mirzapur</code>\n\n"
            f"ℹ️ Agar bot 15 minute se use nahi hua hai, toh free server ko start hone mein 10-15 seconds lag sakte hain."
        )
        await safe_tg_call(message.answer(welcome_text, reply_markup=None), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        welcome_text = (
            f"🎬 Namaste <b>{user.first_name}</b>!\n"
            f"Movie search bot mein swagat hai.\n\n"
            f"Bot ko istemal karne ke liye, kripya neeche diye gaye Channel aur Group join karein, phir '✅ Maine Join Kar Liya' button dabayen."
        )
        if join_markup:
            await safe_tg_call(message.answer(welcome_text, reply_markup=join_markup), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            logger.error("User ne start kiya par koi JOIN_CHANNEL/GROUP set nahi hai.")
            await safe_tg_call(message.answer("Configuration Error: Admin ne join channels set nahi kiye hain."), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("help"))
@handler_timeout(10)
async def help_command(message: types.Message, db_primary: Database):
    user = message.from_user
    if not user: return
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    # --- YEH FIXED SECTION HAI ---
    # Pichhli baar yahaan galti se comment reh gaya tha
    help_text = (
        "❓ <b>Bot Ka Istemal Kaise Karein</b>\n\n"
        "<b>1. Seedha Search:</b>\n"
        "   Movie ya Show ka naam seedha message mein bhejein.\n"
        "   Example: <code>Jawan</code>\n\n"
        "<b>2. Galat Spelling (Typo):</b>\n"
        "   Agar aap spelling galat likhte hain (e.g., <code>Mirjapur</code>), toh bhi bot search kar lega.\n\n"
        "<b>3. Behtar Results Ke Liye:</b>\n"
        "   Naam ke saath saal (year) jodein.\n"
        "   Example: <code>Pathaan 2023</code>\n\n"
        "---\n"  # <--- FIXED LINE
        "⚠️ <b>Server Start Hone Mein Deri?</b>\n"
        "Yeh bot free server par hai. Agar 15 min use na ho, toh server 'so' (sleep) jaata hai. Dobara /start karne par use 'jagne' (wake up) mein 10-15 second lag sakte hain. Ek baar jaagne ke baad, search hamesha fast rahegi."
    )
    await safe_tg_call(message.answer(help_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery, db_primary: Database):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User nahi mila."))
        
    await safe_tg_call(callback.answer("Checking..."))
    
    if not await ensure_capacity_or_inform(callback, db_primary):
        return

    is_member = await check_user_membership(user.id)
    
    if is_member:
        active_users = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = (
            f"✅ Verification safal, <b>{user.first_name}</b>!\n\n"
            f"Ab aap movie search kar sakte hain - bas movie ka naam bhejein.\n\n"
            f"(Server Load: {active_users}/{CURRENT_CONC_LIMIT})"
        )
        try:
            await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        except Exception:
            await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=None), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        await safe_tg_call(callback.answer("❌ Aapne Channel/Group join nahi kiya hai. Kripya join karke dobara try karein.", show_alert=True))
        join_markup = get_join_keyboard()
        if callback.message and (not callback.message.reply_markup or not callback.message.reply_markup.inline_keyboard):
             if callback.message.text and join_markup:
                 await safe_tg_call(callback.message.edit_reply_markup(reply_markup=join_markup))

# =======================================================
# +++++ BOT HANDLERS: SEARCH & SEND +++++
# =======================================================

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20)
async def search_movie_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message, db_primary):
        return
        
    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Query bahut chhoti hai. Kam se kam 2 characters likhein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    searching_msg = await safe_tg_call(message.answer(f"🔎 <b>{original_query}</b> search ho raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not searching_msg: return

    search_results: List[Dict] = []
    search_engine_used = ""

    # --- Search Engine Waterfall (M1 -> N -> M2) ---
    
    # 1. Try 1: Mongo Primary Search (DB 1 - Fast Regex)
    search_results = await safe_db_call(db_primary.mongo_primary_search(original_query, limit=20), default=[])
    if search_results:
        search_engine_used = "Mongo 1 (Fast)"
    
    # 2. Try 2: NeonDB Search (Fuzzy/Typo)
    if not search_results:
        logger.debug(f"Mongo 1 fail '{original_query}', NeonDB try...")
        search_results = await safe_db_call(db_neon.neondb_search(original_query, limit=20), default=[])
        if search_results:
            search_engine_used = "Neon (Fuzzy)"
            
    # 3. Try 3: Mongo Fallback Search (DB 2 - $text)
    if not search_results:
        logger.debug(f"NeonDB fail '{original_query}', Mongo 2 fallback try...")
        search_results = await safe_db_call(db_fallback.mongo_fallback_search(original_query, limit=20), default=[])
        if search_results:
            search_engine_used = "Mongo 2 (Fallback)"

    # --- End Search Waterfall ---

    if not search_results:
        await safe_tg_call(searching_msg.edit_text(f"🥲 Maaf kijiye, '<b>{original_query}</b>' ke liye kuch nahi mila."))
        return

    buttons = []
    max_buttons = 15
    for movie in search_results[:max_buttons]:
        display_title = movie["title"][:50] + '...' if len(movie["title"]) > 50 else movie["title"]
        year_str = f" ({movie.get('year')})" if movie.get('year') else ""
        buttons.append([InlineKeyboardButton(text=f"🎬 {display_title}{year_str}", callback_data=f"get_{movie['imdb_id']}")])

    result_count = len(search_results)
    result_count_text = f"{result_count}" if result_count <= max_buttons else f"{max_buttons}+"
    
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> - {result_count_text} results mile:\n(Source: {search_engine_used})",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20)
async def get_movie_callback(callback: types.CallbackQuery, db_primary: Database, db_fallback: Database):
    user = callback.from_user
    if not user: return await safe_tg_call(callback.answer("Error: User nahi mila."))
        
    await safe_tg_call(callback.answer("✅ File nikali ja rahi hai..."))
    
    if not await ensure_capacity_or_inform(callback, db_primary):
        return

    imdb_id = callback.data.split("_", 1)[1]
    
    # --- File details hamesha PRIMARY DB se lein ---
    movie = await safe_db_call(db_primary.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    # Agar primary mein nahi mila (sync issue), toh fallback se try karein
    if not movie:
        logger.warning(f"Movie {imdb_id} not found in db_primary, checking db_fallback...")
        movie = await safe_db_call(db_fallback.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie database mein nahi mili. Ho sakta hai remove ho gayi ho."))
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN NOTE: Movie <code>{imdb_id}</code> search mein hai par DBs mein nahi. Please run sync commands."))
        return

    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> bheji ja rahi hai..."))
    
    success = False; error_detail = "Unknown error"
    
    try:
        is_valid_for_copy = all([
            movie.get("channel_id"), movie.get("channel_id") != 0,
            movie.get("message_id"), movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER
        ])
        
        if is_valid_for_copy:
            copy_result = await safe_tg_call(
                bot.copy_message(
                    chat_id=user.id,
                    from_chat_id=int(movie["channel_id"]),
                    message_id=movie["message_id"],
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
                caption_text = f"🎬 <b>{movie['title']}</b> ({movie.get('year') or 'N/A'})"
                send_result = await safe_tg_call(bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=caption_text
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

    if not success:
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: <b>{movie['title']}</b> ko nahi bhej paya.\nKaaran: {error_detail}{admin_hint}"
        await safe_tg_call(bot.send_message(user.id, error_text), semaphore=TELEGRAM_COPY_SEMAPHORE)
        try: await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ko bhejne mein fail."))
        except Exception: pass

# =======================================================
# +++++ BOT HANDLERS: ADMIN MIGRATION (3-DB WRITE) +++++
# =======================================================

@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    if not message.forward_from_chat or message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0: await safe_tg_call(message.answer("❌ Migration Error: `LIBRARY_CHANNEL_ID` set nahi hai."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else: await safe_tg_call(message.answer(f"Migration ke liye, files ko seedha apne `LIBRARY_CHANNEL` (ID: `{LIBRARY_CHANNEL_ID}`) se forward karein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    if not (message.video or message.document): return

    info = extract_movie_info(message.caption or "")
    if not info or not info.get("title"):
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption parse nahi kar paya.")
        await safe_tg_call(message.answer(f"❌ Migration Skipped: MessageID `{message.forward_from_message_id}` ka caption parse nahi kar paya."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    file_data = message.video or message.document
    file_id = file_data.file_id; file_unique_id = file_data.file_unique_id
    message_id = message.forward_from_message_id
    channel_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{message_id}"
    title = info["title"]; year = info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    # --- Triple DB Indexing (Parallel) ---
    
    db1_task = safe_db_call(db_primary.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    db2_task = safe_db_call(db_fallback.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    neon_task = safe_db_call(db_neon.add_movie(message_id, channel_id, file_id, file_unique_id, imdb_id, title))
    
    db1_res, db2_res, neon_res = await asyncio.gather(db1_task, db2_task, neon_task)
    
    def get_status(res):
        return "✅ Added" if res is True else ("🔄 Updated" if res == "updated" else ("ℹ️ Skipped" if res == "duplicate" else "❌ FAILED"))

    db1_status = get_status(db1_res)
    db2_status = get_status(db2_res)
    neon_status = "✅ Synced" if neon_res else "❌ FAILED"
    
    await safe_tg_call(message.answer(f"<b>{title}</b>\nDB1: {db1_status} | DB2: {db2_status} | Neon: {neon_status}"), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
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
    clean_title_val = clean_text_for_search(title)
    
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: '{title}'):"

    # --- Triple DB Indexing (Parallel) ---
    db1_task = db_primary.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id)
    db2_task = db_fallback.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id)
    neon_task = db_neon.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title)
    
    # Fire and forget (in background)
    asyncio.gather(safe_db_call(db1_task), safe_db_call(db2_task), safe_db_call(neon_task))
    
    logger.info(f"{log_prefix} Teeno DBs ko sync ke liye bhej diya.")

# =======================================================
# +++++ BOT HANDLERS: ADMIN COMMANDS (3-DB LOGIC) +++++
# =======================================================

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB):
    msg = await safe_tg_call(message.answer("📊 Stats fetch kiye ja rahe hain..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
    mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
    mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users = await asyncio.gather(
        user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task
    )
    
    mongo_1_ready, mongo_2_ready, neon_ready = await asyncio.gather(
        safe_db_call(db_primary.is_ready(), default=False),
        safe_db_call(db_fallback.is_ready(), default=False),
        safe_db_call(db_neon.is_ready(), default=False)
    )
    
    def status_icon(is_ok): return "🟢" if is_ok else "❌"
    def count_str(c): return f"{c:,}" if c >= 0 else "Error"
        
    stats_msg = (
        f"📊 <b>Bot Stats (M+M+N)</b>\n\n"
        f"<b>Live Status</b>\n"
        f"  - 🟢 Active ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
        f"  - 👥 Users (Mongo 1): {count_str(user_count)}\n"
        f"  - 🎬 Movies (Mongo 1): {count_str(mongo_1_count)}\n"
        f"  - 🗂️ Movies (Mongo 2): {count_str(mongo_2_count)}\n"
        f"  - 🗄️ Index (Neon): {count_str(neon_count)}\n"
        f"  - ⏰ Uptime: {get_uptime()}\n\n"
        f"<b>Connections</b>\n"
        f"  - {status_icon(mongo_1_ready)} MongoDB 1 (Primary)\n"
        f"  - {status_icon(mongo_2_ready)} MongoDB 2 (Fallback)\n"
        f"  - {status_icon(neon_ready)} NeonDB (Fuzzy)"
    )
    await safe_tg_call(msg.edit_text(stats_msg))


@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600)
async def broadcast_command(message: types.Message, db_primary: Database):
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Broadcast karne ke liye kisi message ko reply karein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    users = await safe_db_call(db_primary.get_all_users(), timeout=60, default=[])
    if not users:
        await safe_tg_call(message.answer("❌ Database (Mongo 1) mein koi active users nahi mile."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    total = len(users); msg = await safe_tg_call(message.answer(f"📤 Broadcast shuru... {total:,} users ko target kiya gaya."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    start_broadcast_time = datetime.now(timezone.utc)
    success_count, failed_count = 0, 0
    tasks = []
    
    async def send_to_user(user_id: int):
        nonlocal success_count, failed_count
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
        await safe_tg_call(message.answer("❌ Istemal: /get_user `USER_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    user_id_to_find = int(args[1])
    user_data = await safe_db_call(db_primary.get_user_info(user_id_to_find))
    if not user_data:
        await safe_tg_call(message.answer(f"❌ User <code>{user_id_to_find}</code> database (Mongo 1) mein nahi mila."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
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
        await safe_tg_call(message.answer("❌ Kripya .json file ko reply karke command dein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    doc = message.reply_to_message.document
    if not doc.file_name or (not doc.file_name.lower().endswith(".json") and doc.mime_type != "application/json"):
        await safe_tg_call(message.answer("❌ Yeh .json file nahi hai."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    msg = await safe_tg_call(message.answer(f"⏳ `{doc.file_name}` download ho raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    try:
        file = await bot.get_file(doc.file_id);
        if file.file_path is None: await safe_tg_call(msg.edit_text(f"❌ File path nahi mila.")); return
        fio = io.BytesIO(); await bot.download_file(file.file_path, fio); fio.seek(0)
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
            imdb = f"json_{hashlib.md5(fid_str.encode()).hexdigest()[:10]}"
            message_id = item.get("message_id") or AUTO_MESSAGE_ID_PLACEHOLDER
            channel_id = item.get("channel_id") or 0
            info = parse_filename(fname); title = info["title"] or "Untitled"; year = info["year"]
            clean_title_val = clean_text_for_search(title)
            
            db1_tasks.append(db_primary.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id))
            db2_tasks.append(db_fallback.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id))
            neon_tasks.append(db_neon.add_movie(message_id, channel_id, fid_str, file_unique_id, imdb, title))
            
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
            start_import_time = now
    
    await safe_tg_call(msg.edit_text(f"✅ Import Poora Hua!\nProcessed: {total-s-f:,} | Skipped: {s:,} | Failed: {f:,}"))


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
    
    db1_stat = "✅ DB1" if db1_del else "❌ DB1"
    db2_stat = "✅ DB2" if db2_del else "❌ DB2"
    neon_stat = "✅ Neon" if neon_del else "❌ Neon"
    
    await safe_tg_call(msg.edit_text(f"Deletion status for <code>{imdb_id}</code>:\n{db1_stat} | {db2_stat} | {neon_stat}"))


@dp.message(Command("cleanup_mongo_1"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 1) mein `imdb_id` duplicates dhoondhe ja rahe hain (Batch: 100)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    deleted_count, duplicates_found = await safe_db_call(db_primary.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ (Mongo 1) {deleted_count} duplicates delete kiye.\nℹ️ Baaki: {max(0, duplicates_found - deleted_count)}. Command dobara chalayein."))
    else:
        await safe_tg_call(msg.edit_text("✅ (Mongo 1) mein koi duplicates nahi mile."))

@dp.message(Command("cleanup_mongo_2"), AdminFilter())
@handler_timeout(300)
async def cleanup_mongo_2_command(message: types.Message, db_fallback: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 2) mein `imdb_id` duplicates dhoondhe ja rahe hain (Batch: 100)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    deleted_count, duplicates_found = await safe_db_call(db_fallback.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(f"✅ (Mongo 2) {deleted_count} duplicates delete kiye.\nℹ️ Baaki: {max(0, duplicates_found - deleted_count)}. Command dobara chalayein."))
    else:
        await safe_tg_call(msg.edit_text("✅ (Mongo 2) mein koi duplicates nahi mile."))


@dp.message(Command("remove_library_duplicates"), AdminFilter())
@handler_timeout(3600)
async def remove_library_duplicates_command(message: types.Message, db_neon: NeonDB):
    # Yeh command NeonDB se duplicates dhoondhta hai aur Telegram se delete karta hai
    # Iske baad sync commands chalana zaroori hai
    msg = await safe_tg_call(message.answer("⏳ NeonDB se `file_unique_id` duplicates dhoondhe ja rahe hain... (Batch: 100)"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    messages_to_delete, total_duplicates = await safe_db_call(db_neon.find_and_delete_duplicates(batch_limit=100), default=([], 0))
    
    if not messages_to_delete:
        await safe_tg_call(msg.edit_text("✅ Library mein koi duplicate files nahi mili."))
        return
        
    await safe_tg_call(msg.edit_text(f"✅ {total_duplicates} duplicates mile.\n⏳ Ab {len(messages_to_delete)} files ko channel se delete kiya ja raha hai..."))
    
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
        f"✅ Cleanup Done!\n"
        f"🗑️ Channel se Delete kiye: {deleted_count}\n"
        f"❌ Fail hue: {failed_count}\n"
        f"ℹ️ Baaki Duplicates (DB): {max(0, total_duplicates - deleted_count)}\n"
        f"⚠️ Abhi bhi {max(0, total_duplicates - deleted_count)} duplicates hain. Command dobara chalayein.\n"
        f"⚠️ Poora hone ke baad, Mongo DBs ko update karne ke liye sync commands chalayein."
    ))


@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(7200)
async def backup_channel_command(message: types.Message, db_neon: NeonDB):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Istemal: /backup_channel `BACKUP_CHANNEL_ID_OR_USERNAME`"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    target_channel = args[1].strip()
    try:
        if not (target_channel.startswith("@") or target_channel.startswith("-100")):
             raise ValueError("Invalid target channel format.")
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ Error: {e}"), semaphore=TELEGRAM_COPY_SEMAPHORE); return

    msg = await safe_tg_call(message.answer(f"⏳ NeonDB se unique files ki list fetch ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    unique_files = await safe_db_call(db_neon.get_unique_movies_for_backup(), default=[])
    if not unique_files:
        await safe_tg_call(msg.edit_text("❌ NeonDB mein backup ke liye koi files nahi mili.")); return
        
    total_files = len(unique_files)
    await safe_tg_call(msg.edit_text(f"✅ {total_files:,} unique files mili.\n🚀 Ab {target_channel} par *copy* kiya ja raha hai..."))
    
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
        await safe_tg_call(msg.edit_text("❌ (Mongo 1) mein sync ke liye koi movies nahi mili.")); return
    
    await safe_tg_call(msg.edit_text(f"✅ {len(mongo_movies):,} movies mili. Ab NeonDB mein sync kiya ja raha hai..."))
    processed_count = await safe_db_call(db_neon.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    await safe_tg_call(msg.edit_text(f"✅ Sync (Mongo 1 → Neon) poora hua! {processed_count:,} movies process ki gayin."))

@dp.message(Command("sync_mongo_1_to_2"), AdminFilter())
@handler_timeout(1800)
async def sync_mongo_1_to_2_command(message: types.Message, db_primary: Database, db_fallback: Database):
    msg = await safe_tg_call(message.answer("⏳ (Mongo 1) se sabhi movies fetch ki ja rahi hain..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    mongo_movies = await safe_db_call(db_primary.get_all_movies_for_mongo_sync(), timeout=300)
    if not mongo_movies:
        await safe_tg_call(msg.edit_text("❌ (Mongo 1) mein sync ke liye koi movies nahi mili.")); return
    
    await safe_tg_call(msg.edit_text(f"✅ {len(mongo_movies):,} movies mili. Ab (Mongo 2) mein sync (bulk upsert) kiya ja raha hai..."))
    processed_count = await safe_db_call(db_fallback.sync_from_mongo_bulk(mongo_movies), timeout=1500, default=0)
    await safe_tg_call(msg.edit_text(f"✅ Sync (Mongo 1 → Mongo 2) poora hua! {processed_count:,} movies process (added/updated) ki gayin."))


@dp.message(Command("rebuild_index_1"), AdminFilter())
@handler_timeout(300)
async def rebuild_index_1_command(message: types.Message, db_primary: Database):
    msg = await safe_tg_call(message.answer("🔧 (Mongo 1) mein `clean_title` aur text index banaya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    updated, total = await safe_db_call(db_primary.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    await safe_db_call(db_primary.init_db())
    result_text = f"✅ (Mongo 1) Reindex poora hua: {updated:,} titles update kiye. Total: {total:,}. Text index (re)created."
    await safe_tg_call(msg.edit_text(result_text))

@dp.message(Command("rebuild_index_2"), AdminFilter())
@handler_timeout(300)
async def rebuild_index_2_command(message: types.Message, db_fallback: Database):
    msg = await safe_tg_call(message.answer("🔧 (Mongo 2) mein `clean_title` aur text index banaya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    updated, total = await safe_db_call(db_fallback.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    await safe_db_call(db_fallback.init_db())
    result_text = f"✅ (Mongo 2) Reindex poora hua: {updated:,} titles update kiye. Total: {total:,}. Text index (re)created."
    await safe_tg_call(msg.edit_text(result_text))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT; args = message.text.split()
    if len(args)<2 or not args[1].isdigit(): await safe_tg_call(message.answer(f"Istemal: /set_limit N (Abhi: {CURRENT_CONC_LIMIT})"), semaphore=TELEGRAM_COPY_SEMAPHORE); return
    try:
        val = int(args[1]); assert 5 <= val <= 200
        CURRENT_CONC_LIMIT = val; await safe_tg_call(message.answer(f"✅ Concurrency limit ab {CURRENT_CONC_LIMIT} par set hai."), semaphore=TELEGRAM_COPY_SEMAPHORE); logger.info(f"Concurrency limit admin ne {CURRENT_CONC_LIMIT} kar diya hai.")
    except (ValueError, AssertionError): await safe_tg_call(message.answer("❌ Limit 5 aur 200 ke beech ek number hona chahiye."), semaphore=TELEGRAM_COPY_SEMAPHORE)

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
            
    error_message = "❗️ Ek anjaani error aa gayi hai. Team ko soochit kar diya gaya hai. Kripya thodi der baad try karein."
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
        await db_primary.init_db()
        await db_fallback.init_db()
        await db_neon.init_db()
    except Exception as init_err:
        logger.critical(f"Local main() mein DB init fail: {init_err}", exc_info=True); return

    await bot.delete_webhook(drop_pending_updates=True)
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    setup_signal_handlers()

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            db_primary=db_primary,
            db_fallback=db_fallback,
            db_neon=db_neon
        )
    finally:
        await shutdown_procedure()

if __name__ == "__main__":
    logger.warning("Bot ko seedha __main__ se run kiya ja raha hai. Deployment ke liye Uvicorn/FastAPI ka istemal karein.")
    if not WEBHOOK_URL:
        try: asyncio.run(main_polling())
        except (KeyboardInterrupt, SystemExit): logger.info("Bot polling band kar raha hai.")
    else:
        logger.error("WEBHOOK_URL set hai. Local polling nahi chalega.")
        logger.error("Run karne ke liye: uvicorn bot:app --host 0.0.0.0 --port 8000")
