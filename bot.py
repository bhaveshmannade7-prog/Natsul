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
# Console par behtar logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("bot")

# Zaroori libraries ke logs ko shaant karein
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)
logging.getLogger("fastapi").setLevel(logging.WARNING)


# ============ CONFIGURATION ============
try:
    BOT_TOKEN = os.environ["BOT_TOKEN"]
    DATABASE_URL = os.environ["DATABASE_URL"]
    NEON_DATABASE_URL = os.environ["NEON_DATABASE_URL"]
    
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
    LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))

    # Join check usernames (defaults fixed)
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
    logger.critical("Bot band ho raha hai. Kripya apni .env file check karein.")
    raise SystemExit(f"Missing env var: {e}")
except ValueError as e:
    logger.critical(f"--- INVALID ENVIRONMENT VARIABLE: {e} ---")
    logger.critical("Bot band ho raha hai. Kripya ADMIN_USER_ID / CONCURRENT_LIMIT check karein.")
    raise SystemExit(f"Invalid env var: {e}")

# Global variable (runtime par change ho sakta hai)
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

# --- Critical Config Warnings ---
if ADMIN_USER_ID == 0:
    logger.warning("ADMIN_USER_ID set nahi hai. Admin commands kaam nahi karenge.")
if LIBRARY_CHANNEL_ID == 0:
    logger.warning("LIBRARY_CHANNEL_ID set nahi hai. Auto-indexing aur Migration kaam nahi karenge.")
if not JOIN_CHANNEL_USERNAME and not USER_GROUP_USERNAME:
    logger.warning("--- KOI JOIN CHECK SET NAHI HAI. Membership check skip ho jayega. ---")


# ============ TIMEOUTS & SEMAPHORES ============
HANDLER_TIMEOUT = 15  # Seconds
DB_OP_TIMEOUT = 10    # Seconds
TG_OP_TIMEOUT = 8     # Seconds

# Semaphores (Rate Limits se bachne ke liye)
# Telegram API limits (approx): 1 msg/sec (group), 30 msg/sec (private), 20 msg/min (admin actions)
DB_SEMAPHORE = asyncio.Semaphore(15) # DB operations
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10) # Delete (admin action)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(15) # Copy/Send (private)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(25) # Broadcast (private)

# ============ WEBHOOK URL ============
def build_webhook_url() -> str:
    """Render/Public URL se webhook URL banata hai."""
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        webhook_path = f"/bot/{BOT_TOKEN}"
        # Edge cases handle karein (agar URL mein '/bot' pehle se hai)
        if base.endswith('/bot'): base = base.rsplit('/bot', 1)[0]
        elif base.endswith('/bot/'): base = base.rsplit('/bot/', 1)[0]
        
        final_url = f"{base}{webhook_path}"
        logger.info(f"Webhook URL set kiya gaya: {final_url}")
        return final_url
    logger.warning("RENDER_EXTERNAL_URL ya PUBLIC_URL nahi mila. Webhook set nahi ho sakta.")
    return ""

WEBHOOK_URL = build_webhook_url()

# ============ BOT & DB INITIALIZATION ============
# Inhe global rakha gaya hai taaki lifespan aur handlers access kar sakein
try:
    # Default parse_mode ko HTML set karein
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    
    # MemoryStorage ka istemal (Broadcast jaise tasks ke liye zaroori nahi, par achhi practice hai)
    storage = MemoryStorage()
    
    # DB objects ko Dispatcher mein pass karein (Dependency Injection)
    # Isse handlers mein type hinting aur access aasan hoga
    db = Database(DATABASE_URL)
    neondb = NeonDB(NEON_DATABASE_URL)
    
    dp = Dispatcher(storage=storage, db=db, neondb=neondb)
    
    logger.info("Bot, Dispatcher, aur Database objects initialize ho gaye.")
except Exception as e:
    logger.critical(f"Bot/Dispatcher initialize nahi ho paya: {e}", exc_info=True)
    raise SystemExit("Bot initialization fail.")

# Global state
start_time = datetime.now(timezone.utc)
monitor_task: asyncio.Task | None = None
executor: concurrent.futures.ThreadPoolExecutor | None = None

# Placeholder (JSON import ke liye)
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure():
    """Bot ko gracefully band karne ka process."""
    logger.info("Graceful shutdown shuru ho raha hai...")
    
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await asyncio.wait_for(monitor_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            logger.warning("Event loop monitor task ko cancel karne mein timeout.")
            
    if WEBHOOK_URL:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook delete kar diya gaya hai.")
        except Exception as e:
            logger.error(f"Webhook delete karte waqt error: {e}")
            
    try:
        await dp.storage.close()
        logger.info("Dispatcher storage close ho gaya.")
    except Exception as e:
        logger.error(f"Dispatcher storage close karte waqt error: {e}")
        
    try:
        if bot.session:
            await bot.session.close()
            logger.info("Bot session close ho gaya.")
    except Exception as e:
        logger.error(f"Bot session close karte waqt error: {e}")
        
    if executor:
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("ThreadPoolExecutor shutdown ho gaya.")
        
    try:
        if db and db.client:
            db.client.close()
            logger.info("MongoDB client connection close ho gaya.")
        if neondb:
            await neondb.close() # Yeh async hai
    except Exception as e:
        logger.error(f"Database connections close karte waqt error: {e}")
        
    logger.info("Graceful shutdown poora hua.")


def setup_signal_handlers():
    """OS signals (like Ctrl+C) ko handle karta hai."""
    loop = asyncio.get_running_loop()
    
    def handle_signal(signum):
        logger.info(f"Signal {signum} mila. Shutdown shuru...")
        asyncio.create_task(shutdown_procedure())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal, sig)
    logger.info("Signal handlers (SIGTERM, SIGINT) set ho gaye.")

# ============ TIMEOUT DECORATOR ============
def handler_timeout(timeout: int = HANDLER_TIMEOUT):
    """
    Ek decorator jo handler ke time-out hone par use automatically cancel kar deta hai
    aur user ko soochit karta hai.
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                # Handler ko timeout ke saath run karein
                await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(f"Handler {func.__name__} {timeout}s ke baad time out ho gaya.")
                
                # User ko soochit karne ki koshish karein
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
                        await bot.send_message(target_chat_id, "⚠️ Server request time out ho gayi. Kripya dobara koshish karein.")
                    except Exception:
                        pass # Agar user ne bot block kar diya hai toh fail hoga
                if callback_query:
                    try:
                        await callback_query.answer("Timeout", show_alert=False)
                    except Exception:
                        pass
            except Exception as e:
                # Handler mein koi aur error
                logger.exception(f"Handler {func.__name__} mein error: {e}")
        return wrapper
    return decorator

# ============ SAFE API CALL WRAPPERS ============
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """
    Database calls ke liye ek safe wrapper, timeout aur error handling ke saath.
    """
    try:
        async with DB_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"DB call timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return default
    except Exception as e:
         logger.error(f"DB error in {getattr(coro, '__name__', 'unknown_coro')}: {e}", exc_info=True)
         # DB error ko handle karne ki koshish (e.g., reconnect)
         if hasattr(db, '_handle_db_error'):
             await db._handle_db_error(e) # Mongo error
         # NeonDB errors (PoolError) 'is_ready' mein handle hote hain
         return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None):
    """
    Telegram API calls ke liye safe wrapper, rate limits aur common errors handle karta hai.
    Returns:
        - API result (e.g., Message object) agar safal.
        - False agar user ne bot block kiya hai.
        - None agar koi aur temporary error (e.g., timeout, flood wait, not found).
    """
    semaphore_to_use = semaphore or asyncio.Semaphore(1) # Default 1 agar koi na diya ho
    
    try:
        async with semaphore_to_use:
            # Chhota sa delay rate limit se bachne ke liye
            if semaphore: 
                await asyncio.sleep(0.1) 
            
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError: 
        logger.warning(f"TG call timeout: {getattr(coro, '__name__', 'unknown_coro')}")
        return None
    
    except (TelegramAPIError, TelegramBadRequest) as e:
        error_msg = str(e).lower()
        
        if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
            logger.info(f"TG: Bot block ya user deactivated.")
            return False # Permanent fail (user ke liye)
        elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
            logger.info(f"TG: Chat nahi mila.")
            return False # Permanent fail (chat ke liye)
        elif "message is not modified" in error_msg:
            logger.debug(f"TG: Message modify nahi hua.")
            return None
        elif "message to delete not found" in error_msg or "message to copy not found" in error_msg:
            logger.debug(f"TG: Message (delete/copy) nahi mila.")
            return None
        elif "too many requests" in error_msg:
            logger.warning(f"TG: FLOOD WAIT (Too Many Requests). {e}")
            # Flood wait ke liye thoda pause
            await asyncio.sleep(5) 
            return None
        else:
            # Koi aur Telegram error
            logger.warning(f"TG Error: {e}")
            return None
            
    except Exception as e:
        # Koi anjaani error
        logger.exception(f"TG Unexpected error in {getattr(coro, '__name__', 'unknown_coro')}: {e}")
        return None

# ============ FILTERS & HELPER FUNCTIONS ============
class AdminFilter(BaseFilter):
    """Sirf ADMIN_USER_ID ko commands execute karne deta hai."""
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user and (message.from_user.id == ADMIN_USER_ID)

def get_uptime() -> str:
    """Bot ka uptime calculate karta hai (e.g., "1d 2h 3m")."""
    delta = datetime.now(timezone.utc) - start_time
    total_seconds = int(delta.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if days > 0: return f"{days}d {hours}h {minutes}m"
    if hours > 0: return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"

async def check_user_membership(user_id: int) -> bool:
    """
    User ki membership configured Channel aur Group mein check karta hai.
    User ko *dono* (agar dono set hain) mein hona zaroori hai.
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

        results = await asyncio.gather(*tasks_to_run)
        
        valid_statuses = {"member", "administrator", "creator"}
        
        # Default True (paas) hai agar check nahi kiya gaya
        is_in_channel = True 
        is_in_group = True

        result_index = 0
        if check_channel:
            channel_member = results[result_index]
            is_in_channel = isinstance(channel_member, types.ChatMember) and channel_member.status in valid_statuses
            if channel_member in [False, None]:
                    logger.warning(f"Membership check fail (Channel @{JOIN_CHANNEL_USERNAME}). Bot admin hai ya username sahi hai?")
            result_index += 1
        
        if check_group:
            group_member = results[result_index]
            is_in_group = isinstance(group_member, types.ChatMember) and group_member.status in valid_statuses
            if group_member in [False, None]:
                    logger.warning(f"Membership check fail (Group @{USER_GROUP_USERNAME}). Bot admin hai ya username sahi hai?")

        # User ko sabhi *configured* channels/groups mein hona zaroori hai
        return is_in_channel and is_in_group

    except Exception as e:
            if not isinstance(e, (TelegramBadRequest, TelegramAPIError)):
                logger.error(f"Membership check mein error {user_id}: {e}", exc_info=True)
            else:
                logger.info(f"Membership check API error {user_id}: {e}")
            return False # Error par fail karein (access na dein)

def get_join_keyboard() -> InlineKeyboardMarkup | None:
    """Join buttons ka keyboard banata hai."""
    buttons = []
    if JOIN_CHANNEL_USERNAME: 
        buttons.append([InlineKeyboardButton(text="📢 Channel Join Karein", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")])
    if USER_GROUP_USERNAME: 
        buttons.append([InlineKeyboardButton(text="👥 Group Join Karein", url=f"https://t.me/{USER_GROUP_USERNAME}")])
    
    if buttons: 
        buttons.append([InlineKeyboardButton(text="✅ Maine Join Kar Liya (Verify)", callback_data="check_join")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    return None

def get_full_limit_keyboard() -> InlineKeyboardMarkup | None:
    """Alternate bots ka keyboard banata hai."""
    if not ALTERNATE_BOTS: 
        return None
    buttons = [[InlineKeyboardButton(text=f"🚀 Dusra Bot @{b}", url=f"https://t.me/{b}")] for b in ALTERNATE_BOTS]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_movie_info(caption: str | None) -> Dict[str, str] | None:
    """Caption se Title, IMDB ID, aur Year nikalta hai."""
    if not caption: 
        return None
        
    info = {}
    lines = caption.splitlines()
    title = lines[0].strip() if lines else ""
    
    # Agar doosri line mein 'Season' hai, toh use title mein jodein (TV Show ke liye)
    if len(lines) > 1 and re.search(r"^\s*[Ss](eason)?\s*\d{1,2}\b", lines[1]):
        title += " " + lines[1].strip()
        
    if title: 
        info["title"] = title
        
    # IMDB ID (tt1234567)
    imdb_match = re.search(r"(tt\d{7,})", caption)
    if imdb_match:
        info["imdb_id"] = imdb_match.group(1)
        
    # Year (1980-2029)
    year_match = re.findall(r"\b(19[89]\d|20[0-2]\d)\b", caption)
    if year_match:
        info["year"] = year_match[-1] # Hamesha aakhri waala lein
        
    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str | None]:
    """Filename se Title aur Year nikalne ki koshish karta hai."""
    if not filename: 
        return {"title": "Untitled", "year": None}
        
    year = None
    
    # 1. (2023) jaise pattern dhoondein
    match_paren = re.search(r"\(((19[89]\d|20[0-3]\d))\)", filename)
    if match_paren:
        year = match_paren.group(1)
    else:
        # 2. 2023 jaise pattern dhoondein
        matches_bare = re.findall(r"\b((19[89]\d|20[0-3]\d))\b", filename)
        if matches_bare:
            year = matches_bare[-1][0]
            
    # File extension hatayein
    title = os.path.splitext(filename)[0].strip()
    
    # Title se year aur faltu tags hatayein
    if year:
        title = re.sub(rf"(\s*\(?{year}\)?\s*)$", "", title, flags=re.IGNORECASE).strip()
    
    title = re.sub(r"\[.*?\]", "", title, flags=re.IGNORECASE) # [Tags]
    title = re.sub(r"\(.*?\)", "", title, flags=re.IGNORECASE) # (Tags)
    
    # Common faltu keywords
    common_tags = r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio|esub|full hd)\b"
    title = re.sub(common_tags, "", title, flags=re.IGNORECASE)
    
    title = re.sub(r'[._]', ' ', title).strip() # Dots/Underscores ko space
    title = re.sub(r"\s+", " ", title).strip() # Extra space
    
    # Agar title poora clean ho gaya, toh original (kam cleaning waala) use karein
    if not title:
        title = os.path.splitext(filename)[0].strip()
        title = re.sub(r"\[.*?\]", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r"\(.*?\)", "", title, flags=re.IGNORECASE).strip()
        title = re.sub(r'[._]', ' ', title).strip()
        title = re.sub(r"\s+", " ", title).strip()
        
    return {"title": title or "Untitled", "year": year}

def overflow_message(active_users: int) -> str:
    """Server load full hone par message."""
    return (
        f"⚠️ Server par load zyada hai ({active_users}/{CURRENT_CONC_LIMIT}).\n"
        f"Aapki request abhi hold par hai. Kripya thodi der baad try karein ya neeche diye gaye alternate bots ka istemal karein:"
    )

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    """Event loop lag ko monitor karta hai."""
    loop = asyncio.get_running_loop()
    while True:
        try:
            start_time = loop.time()
            await asyncio.sleep(1) # 1 sec soyein
            lag = (loop.time() - start_time) - 1
            
            if lag > 0.5: # 500ms se zyada lag
                logger.warning(f"⚠️ Event loop lag detect hua: {lag:.3f}s")
                
            await asyncio.sleep(30) # Har 30 sec check karein
        except asyncio.CancelledError:
            logger.info("Event loop monitor band ho raha hai.")
            break
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}", exc_info=True)
            await asyncio.sleep(120) # Error par lamba pause

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI server ke start aur stop par chalta hai."""
    global monitor_task, executor, db, neondb
    
    logger.info("Application startup shuru ho raha hai...")
    
    # ThreadPoolExecutor (JSON parsing jaise blocking tasks ke liye)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialize ho gaya.")

    # --- MongoDB Init ---
    try:
        await db.init_db()
        logger.info("Database (MongoDB) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database (MongoDB) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("MongoDB connection fail (startup).") from e

    # --- NeonDB (Postgres) Init ---
    try:
        await neondb.init_db()
        logger.info("Database (NeonDB/Postgres) initialization safal.")
    except Exception as e:
        logger.critical(f"FATAL: Database (NeonDB/Postgres) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("NeonDB/Postgres connection fail (startup).") from e

    # Event loop monitor start karein
    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor start ho gaya.")

    # Webhook set karein
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
        logger.warning("WEBHOOK_URL set nahi hai. Bot polling mode (local) ke liye taiyar hai, par deploy nahi hoga.")

    # OS Signals setup karein
    setup_signal_handlers()

    logger.info("Application startup poora hua. Bot taiyar hai.")
    
    yield # Application yahan chalta hai
    
    logger.info("Application shutdown sequence shuru ho raha hai...")
    # Cleanup 'shutdown_procedure' (jo signal se trigger hota hai) mein handle hota hai
    # Yahan hum use manually call kar sakte hain agar signal nahi mila
    await shutdown_procedure()
    logger.info("Application shutdown poora hua.")


app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK / HEALTHCHECK ROUTES ============

async def _process_update_safe(update_obj: Update, bot_instance: Bot):
    """Update ko safely process karta hai."""
    try:
        # DB objects ko DI ke through pass karein
        await dp.feed_update(bot=bot_instance, update=update_obj, db=db, neondb=neondb)
    except Exception as e:
        logger.exception(f"Update process karte waqt error {update_obj.update_id}: {e}")

@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    """Telegram se updates receive karta hai."""
    
    # Secret token check karein (agar set hai)
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token mila.")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid Secret Token")
        
    try:
        telegram_update = Update(**update)
        # Update ko background task mein process karein taaki Telegram ko jaldi response mile
        background_tasks.add_task(_process_update_safe, telegram_update, bot)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook update parse nahi kar paya: {e}", exc_info=False)
        logger.debug(f"Failed update data: {update}")
        return {"ok": False, "error": f"Invalid update format: {e}"}

@app.get("/")
@app.get("/ping")
async def ping():
    """Simple check ki server zinda hai."""
    return {"status": "ok", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    """Bot aur databases ka detailed health check."""
    
    # Dono DBs ko parallel check karein
    db_check_task = safe_db_call(db.is_ready(), default=False)
    neondb_check_task = safe_db_call(neondb.is_ready(), default=False)
    
    db_ok, neondb_ok = await asyncio.gather(
        db_check_task, neondb_check_task
    )

    status_code = 200
    status_msg = "ok"
    
    if not db_ok:
        status_msg = "error_mongodb_connection"
        status_code = 503
    elif not neondb_ok:
        status_msg = "degraded_neondb_connection"
        # Isse service down nahi maana jayega, kyonki Mongo fallback hai
    
    return {
        "status": status_msg,
        "database_mongo_connected": db_ok,
        "database_neon_connected": neondb_ok,
        "uptime": get_uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }, status_code

# ============ USER CAPACITY CHECK ============

async def ensure_capacity_or_inform(
    message_or_callback: types.Message | types.CallbackQuery,
    db: Database
) -> bool:
    """
    Check karta hai ki server par capacity hai ya nahi.
    User ko DB mein add/update bhi karta hai.
    """
    user = message_or_callback.from_user
    if not user: 
        return True # User nahi mila (e.g., channel post), ignore
    
    target_chat_id = None
    if isinstance(message_or_callback, types.Message):
        target_chat_id = message_or_callback.chat.id
    elif isinstance(message_or_callback, types.CallbackQuery) and message_or_callback.message:
        target_chat_id = message_or_callback.message.chat.id
    
    # User ko DB mein add/update karein (background mein nahi, kyonki agla check ispar depend karta hai)
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))
    
    # Admin ke liye koi limit nahi
    if user.id == ADMIN_USER_ID:
        return True
        
    # Active users ka count check karein
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    if active >= CURRENT_CONC_LIMIT:
        logger.warning(f"Capacity full: {active}/{CURRENT_CONC_LIMIT}. User {user.id} ki request hold par.")
        if target_chat_id:
            await safe_tg_call(
                bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()),
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
        if isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server busy, kripya thodi der baad try karein.", show_alert=True))
        return False # Capacity full hai
        
    return True # Capacity hai

# =======================================================
# +++++ BOT HANDLERS: USER COMMANDS +++++
# =======================================================

@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message, db: Database, neondb: NeonDB):
    """/start command handle karta hai (Admin aur Regular User)."""
    user = message.from_user
    if not user: return
    user_id = user.id

    # Admin Panel
    if user_id == ADMIN_USER_ID:
        # Sabhi stats parallel fetch karein
        user_count_task = safe_db_call(db.get_user_count(), default=-1)
        mongo_count_task = safe_db_call(db.get_movie_count(), default=-1)
        neon_count_task = safe_db_call(neondb.get_movie_count(), default=-1)
        concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        user_count, mongo_count, neon_count, concurrent_users = await asyncio.gather(
            user_count_task, mongo_count_task, neon_count_task, concurrent_users_task
        )
        
        # Connection status
        mongo_ready, neon_ready = await asyncio.gather(
            safe_db_call(db.is_ready(), default=False),
            safe_db_call(neondb.is_ready(), default=False)
        )
        
        def status_icon(is_ok): return "🟢" if is_ok else "❌"
        def count_str(c): return f"{c:,}" if c >= 0 else "Error"

        admin_message = (
            f"👑 <b>Admin Panel (Dual-DB)</b>\n\n"
            f"<b>📊 Live Status</b>\n"
            f"  - 🟢 Active Users ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
            f"  - 👥 Total Users (Mongo): {count_str(user_count)}\n"
            f"  - 🎬 Movies (Mongo): {count_str(mongo_count)}\n"
            f"  - 🗂️ Index (Neon): {count_str(neon_count)}\n"
            f"  - ⏰ Uptime: {get_uptime()}\n\n"
            f"<b>🔌 Connections</b>\n"
            f"  - {status_icon(mongo_ready)} MongoDB\n"
            f"  - {status_icon(neon_ready)} NeonDB (Postgres)\n\n"
            f"<b>Search Logic:</b> Mongo (Regex) → Neon (FTS) → Mongo (Fallback)\n\n"
            f"<b>🛠️ Commands</b>\n"
            f"  - /stats | /health | /get_user `ID`\n"
            f"  - /broadcast (Reply karke)\n"
            f"  - /set_limit `N` (e.g., 5-200)\n\n"
            f"<b>🗃️ Data & Indexing</b>\n"
            f"  - <b>/remove_library_duplicates</b> ⚠️ (NeonDB + Channel)\n"
            f"  - <b>/backup_channel</b> 🚀 (NeonDB → New Channel)\n"
            f"  - <b>/sync_mongo_to_neon</b> 🔄 (Mongo → Neon)\n"
            f"  - <b>/cleanup_mongo_duplicates</b> (Sirf Mongo DB)\n"
            f"  - /rebuild_index (Sirf Mongo DB)\n"
            f"  - /cleanup_users (Inactive > 30d)\n\n"
            f"<b>⭐️ Migration:</b> Files ko `LIBRARY_CHANNEL` se *forward* karein (Sirf Admin)."
        )
        await safe_tg_call(message.answer(admin_message), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # --- Regular User ---
    # Capacity check (yeh user ko DB mein add bhi kar dega)
    if not await ensure_capacity_or_inform(message, db):
        return
        
    is_member = await check_user_membership(user_id)
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
            # Agar admin ne koi channel set nahi kiya
            logger.error("User ne start kiya par koi JOIN_CHANNEL/GROUP set nahi hai.")
            await safe_tg_call(message.answer("Configuration Error: Admin ne join channels set nahi kiye hain."), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("help"))
@handler_timeout(10)
async def help_command(message: types.Message, db: Database):
    user = message.from_user
    if not user: return
    # User ko active mark karein
    await safe_db_call(db.add_user(user.id, user.username, user.first_name, user.last_name))
    
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
        "---
\n"
        "⚠️ <b>Server Start Hone Mein Deri?</b>\n"
        "Yeh bot free server par hai. Agar 15 min use na ho, toh server 'so' (sleep) jaata hai. Dobara /start karne par use 'jagne' (wake up) mein 10-15 second lag sakte hain. Ek baar jaagne ke baad, search hamesha fast rahegi."
    )
    await safe_tg_call(message.answer(help_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery, db: Database):
    """'Maine Join Kar Liya' button ko handle karta hai."""
    user = callback.from_user
    if not user: 
        return await safe_tg_call(callback.answer("Error: User nahi mila."))
        
    await safe_tg_call(callback.answer("Checking..."))
    
    # Capacity check (DB mein user update bhi karega)
    if not await ensure_capacity_or_inform(callback, db):
        return

    is_member = await check_user_membership(user.id)
    
    if is_member:
        active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        success_text = (
            f"✅ Verification safal, <b>{user.first_name}</b>!\n\n"
            f"Ab aap movie search kar sakte hain - bas movie ka naam bhejein.\n\n"
            f"(Server Load: {active_users}/{CURRENT_CONC_LIMIT})"
        )
        # Button hatane ke liye message ko edit karein
        try:
            await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        except Exception:
            # Agar message edit nahi ho paya (e.g., purana message), toh naya bhej dein
            await safe_tg_call(bot.send_message(user.id, success_text, reply_markup=None), semaphore=TELEGRAM_COPY_SEMAPHORE)
    else:
        # User ne join nahi kiya
        await safe_tg_call(callback.answer("❌ Aapne Channel/Group join nahi kiya hai. Kripya join karke dobara try karein.", show_alert=True))
        
        # Agar buttons pehle se nahi hain, toh unhe dobara add karein
        join_markup = get_join_keyboard()
        if callback.message and (not callback.message.reply_markup or not callback.message.reply_markup.inline_keyboard):
             if callback.message.text and join_markup:
                 await safe_tg_call(callback.message.edit_reply_markup(reply_markup=join_markup))

# =======================================================
# +++++ BOT HANDLERS: SEARCH & SEND +++++
# =======================================================

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20) # Search mein thoda extra time
async def search_movie_handler(message: types.Message, db: Database, neondb: NeonDB):
    """
    Main search handler.
    Search Waterfall: Mongo (Regex) -> Neon (FTS) -> Mongo (Fallback $text)
    """
    user = message.from_user
    if not user: return
    user_id = user.id

    # 1. Capacity Check
    if not await ensure_capacity_or_inform(message, db):
        return
        
    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Query bahut chhoti hai. Kam se kam 2 characters likhein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # 2. User ko "Searching..." message dein
    searching_msg = await safe_tg_call(message.answer(f"🔎 <b>{original_query}</b> search ho raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not searching_msg: 
        logger.warning(f"User {user_id} ko 'searching' message nahi bhej paye.")
        return # Agar message nahi bhej paye toh aage na badhein

    search_results: List[Dict] = []
    search_engine_used = ""

    # --- Search Engine Waterfall ---
    
    # 3. Try 1: Mongo Primary Search (Regex - Tez)
    search_results = await safe_db_call(db.mongo_primary_search(original_query, limit=20), default=[])
    if search_results:
        search_engine_used = "Mongo (Fast)"
    
    # 4. Try 2: NeonDB Search (FTS/Trigram - Acchi typo handling)
    if not search_results:
        logger.debug(f"Mongo primary fail '{original_query}', NeonDB try...")
        search_results = await safe_db_call(neondb.neondb_search(original_query, limit=20), default=[])
        if search_results:
            search_engine_used = "Neon (Index)"
            
    # 5. Try 3: Mongo Fallback Search ($text - Alag logic)
    if not search_results:
        logger.debug(f"NeonDB fail '{original_query}', Mongo fallback try...")
        search_results = await safe_db_call(db.mongo_fallback_search(original_query, limit=20), default=[])
        if search_results:
            search_engine_used = "Mongo (Fallback)"

    # --- End Search Waterfall ---

    # 6. Koi result nahi mila
    if not search_results:
        await safe_tg_call(searching_msg.edit_text(f"🥲 Maaf kijiye, '<b>{original_query}</b>' ke liye kuch nahi mila."))
        return

    # 7. Results ko buttons mein format karein
    buttons = []
    max_buttons = 15 # Telegram limit
    for movie in search_results[:max_buttons]:
        # Title ko 50 char tak kaat dein
        display_title = movie["title"][:50] + '...' if len(movie["title"]) > 50 else movie["title"]
        # NeonDB se year nahi aata, isliye check karein
        year_str = f" ({movie.get('year')})" if movie.get('year') else ""
        
        # Callback data mein 'get_' prefix + imdb_id
        buttons.append([
            InlineKeyboardButton(
                text=f"🎬 {display_title}{year_str}", 
                callback_data=f"get_{movie['imdb_id']}"
            )
        ])

    result_count = len(search_results)
    result_count_text = f"{result_count}" if result_count <= max_buttons else f"{max_buttons}+"
    
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> - {result_count_text} results mile:\n(Source: {search_engine_used})",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))


@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20) # File bhejne mein time lag sakta hai
async def get_movie_callback(callback: types.CallbackQuery, db: Database):
    """Movie button (get_imdbid) ko handle karta hai aur file bhejta hai."""
    user = callback.from_user
    if not user: 
        return await safe_tg_call(callback.answer("Error: User nahi mila."))
        
    await safe_tg_call(callback.answer("✅ File nikali ja rahi hai..."))
    
    # 1. Capacity Check
    if not await ensure_capacity_or_inform(callback, db):
        return

    # 2. IMDB ID parse karein
    imdb_id = callback.data.split("_", 1)[1]
    
    # 3. DB se movie details nikaalein (sabse nayi waali)
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)

    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie database mein nahi mili. Ho sakta hai remove ho gayi ho."))
        if user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN NOTE: Movie <code>{imdb_id}</code> search mein hai par DB mein nahi. Please run /sync_mongo_to_neon."))
        return

    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> bheji ja rahi hai..."))
    
    success = False
    error_detail = "Unknown error"
    
    # --- NAYI LOGIC: COPY_MESSAGE ---
    try:
        # Check karein ki message_id aur channel_id valid hain
        is_valid_for_copy = all([
            movie.get("channel_id"),
            movie.get("channel_id") != 0,
            movie.get("message_id"),
            movie.get("message_id") != AUTO_MESSAGE_ID_PLACEHOLDER
        ])
        
        if is_valid_for_copy:
            logger.debug(f"Attempting copy for {imdb_id} from {movie['channel_id']}:{movie['message_id']}")
            
            # 4. Try 1: Message ko copy karein (Caption ke saath)
            copy_result = await safe_tg_call(
                bot.copy_message(
                    chat_id=user.id,
                    from_chat_id=int(movie["channel_id"]),
                    message_id=movie["message_id"],
                ), 
                timeout=TG_OP_TIMEOUT * 2, # Copy mein extra time
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
            
            if copy_result: 
                success = True
            elif copy_result is False: 
                error_detail = "Bot blocked ya chat not found."
            else: 
                error_detail = "Copying fail (timeout, ya message channel se delete ho gaya)."
        else:
            error_detail = "Cannot copy (invalid channel/message ID, shayad JSON import)."
        
        # 5. Try 2: Fallback (agar copy fail hua ya valid ID nahi thi)
        if not success:
            logger.info(f"Copy fail ({error_detail}), ab send_document (file_id) try...")
            
            if not movie.get("file_id"):
                 error_detail = "File ID missing, document nahi bhej sakte."
            else:
                # File_id se bhejte waqt caption manually daalna padega
                caption_text = f"🎬 <b>{movie['title']}</b> ({movie.get('year') or 'N/A'})"
                
                send_result = await safe_tg_call(bot.send_document(
                    chat_id=user.id,
                    document=movie["file_id"],
                    caption=caption_text
                ), 
                timeout=TG_OP_TIMEOUT * 4, # Upload mein extra time
                semaphore=TELEGRAM_COPY_SEMAPHORE
                )
                
                if send_result: 
                    success = True
                elif send_result is False: 
                    error_detail += " (Bot blocked/Chat not found on send_doc)"
                else: 
                    error_detail += " (Sending document by file_id failed)"
                    
    except Exception as e:
        error_detail = f"File bhejte waqt anjaani error: {e}"
        logger.error(f"Exception during send/copy {imdb_id}: {e}", exc_info=True)

    # 6. Agar sab fail ho gaya
    if not success:
        admin_hint = f"\n(Admin: /remove_dead_movie {imdb_id})" if user.id == ADMIN_USER_ID else ""
        error_text = f"❗️ File Error: <b>{movie['title']}</b> ko nahi bhej paya.\nKaaran: {error_detail}{admin_hint}"
        await safe_tg_call(bot.send_message(user.id, error_text), semaphore=TELEGRAM_COPY_SEMAPHORE)
        try: 
            await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ko bhejne mein fail."))
        except Exception: 
            pass # Edit fail hone par ignore karein

# =======================================================
# +++++ BOT HANDLERS: ADMIN MIGRATION +++++
# =======================================================

@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message, db: Database, neondb: NeonDB):
    """
    Admin jab LIBRARY_CHANNEL se file forward karta hai, toh use index karta hai.
    """
    if not message.forward_from_chat or message.forward_from_chat.id != LIBRARY_CHANNEL_ID:
        if LIBRARY_CHANNEL_ID == 0:
            await safe_tg_call(message.answer("❌ Migration Error: `LIBRARY_CHANNEL_ID` set nahi hai."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        else:
            await safe_tg_call(message.answer(f"Migration ke liye, files ko seedha apne `LIBRARY_CHANNEL` (ID: `{LIBRARY_CHANNEL_ID}`) se forward karein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    if not (message.video or message.document):
        logger.warning("Admin ne non-file message forward kiya, migration skip.")
        return

    # Caption se info nikaalein
    info = extract_movie_info(message.caption or "")
    if not info or not info.get("title"):
        logger.warning(f"Migration Skip (Fwd MsgID {message.forward_from_message_id}): Caption se info parse nahi kar paya.")
        await safe_tg_call(message.answer(f"❌ Migration Skipped: MessageID `{message.forward_from_message_id}` ka caption parse nahi kar paya."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    # Zaroori IDs collect karein
    file_data = message.video or message.document
    file_id = file_data.file_id
    file_unique_id = file_data.file_unique_id
    
    # Original Channel/Message ID
    message_id = message.forward_from_message_id
    channel_id = message.forward_from_chat.id
    
    imdb_id = info.get("imdb_id") or f"auto_{message_id}" # Agar IMDB nahi hai toh auto-ID
    title = info["title"]
    year = info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    # --- Dual DB Indexing ---
    
    # 1. MongoDB mein Add/Update karein
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message_id, channel_id, clean_title_val, file_unique_id))
    db_map = {True: "✅ Migrated (Added DB)", "updated": "✅ Migrated (Updated DB)", "duplicate": "ℹ️ Migrated (Skipped DB)", False: "❌ DB Error"}
    db_status = db_map.get(db_res, "❌ DB Error")

    # 2. NeonDB mein Add/Update karein
    neon_res = await safe_db_call(neondb.add_movie(message_id, channel_id, file_id, file_unique_id, imdb_id, title))
    neon_status = "✅ Neon Synced" if neon_res else "❌ Neon Sync Fail"
    
    # Admin ko feedback dein
    await safe_tg_call(message.answer(f"{db_status} | {neon_status}\n<b>{title}</b>"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    
    # Typesense hata diya gaya hai


@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message, db: Database, neondb: NeonDB):
    """
    LIBRARY_CHANNEL mein naye posts ko automatically index karta hai.
    """
    if message.chat.id != LIBRARY_CHANNEL_ID or LIBRARY_CHANNEL_ID == 0:
        return
    if not (message.video or message.document):
        return
        
    info = extract_movie_info(message.caption or "")
    if not info or not info.get("title"):
        if message.caption: 
            logger.warning(f"Auto-Index Skip (MsgID {message.message_id}): Caption parse nahi kar paya: '{message.caption[:50]}...'")
        else:
            logger.warning(f"Auto-Index Skip (MsgID {message.message_id}): Koi caption nahi.")
        return

    file_data = message.video or message.document
    file_id = file_data.file_id
    file_unique_id = file_data.file_unique_id
    
    imdb_id = info.get("imdb_id") or f"auto_{message.message_id}"
    title = info["title"]
    year = info.get("year")
    clean_title_val = clean_text_for_search(title)
    
    log_prefix = f"Auto-Index (Msg: {message.message_id}, Title: '{title}'):"

    # 1. MongoDB
    db_res = await safe_db_call(db.add_movie(imdb_id, title, year, file_id, message.message_id, message.chat.id, clean_title_val, file_unique_id))
    if db_res in [True, "updated"]: 
        logger.info(f"{log_prefix} DB {'Added' if db_res is True else 'Updated'}.")
    elif db_res == "duplicate":
        logger.warning(f"{log_prefix} DB Skipped (duplicate).")
    else: 
        logger.error(f"{log_prefix} DB Operation FAILED.")
    
    # 2. NeonDB
    neon_res = await safe_db_call(neondb.add_movie(message.message_id, message.chat.id, file_id, file_unique_id, imdb_id, title))
    if neon_res: 
        logger.info(f"{log_prefix} NeonDB Synced.")
    else: 
        logger.error(f"{log_prefix} NeonDB Sync FAILED.")

    # 3. Typesense (Hata diya gaya)


# =======================================================
# +++++ BOT HANDLERS: ADMIN COMMANDS +++++
# =======================================================

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message, db: Database, neondb: NeonDB):
    """Admin ko detailed stats bhejta hai."""
    
    msg = await safe_tg_call(message.answer("📊 Stats fetch kiye ja rahe hain..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    user_count_task = safe_db_call(db.get_user_count(), default=-1)
    mongo_count_task = safe_db_call(db.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(neondb.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    user_count, mongo_count, neon_count, concurrent_users = await asyncio.gather(
        user_count_task, mongo_count_task, neon_count_task, concurrent_users_task
    )
    
    mongo_ready, neon_ready = await asyncio.gather(
        safe_db_call(db.is_ready(), default=False),
        safe_db_call(neondb.is_ready(), default=False)
    )
    
    def status_icon(is_ok): return "🟢" if is_ok else "❌"
    def count_str(c): return f"{c:,}" if c >= 0 else "Error"
        
    stats_msg = (
        f"📊 <b>Bot Stats (Dual-DB)</b>\n\n"
        f"<b>Live Status</b>\n"
        f"  - 🟢 Active ({ACTIVE_WINDOW_MINUTES}m): {concurrent_users:,} / {CURRENT_CONC_LIMIT}\n"
        f"  - 👥 Users (Mongo): {count_str(user_count)}\n"
        f"  - 🎬 Movies (Mongo): {count_str(mongo_count)}\n"
        f"  - 🗂️ Index (Neon): {count_str(neon_count)}\n"
        f"  - ⏰ Uptime: {get_uptime()}\n\n"
        f"<b>Connections</b>\n"
        f"  - {status_icon(mongo_ready)} MongoDB\n"
        f"  - {status_icon(neon_ready)} NeonDB (Postgres)"
    )
    await safe_tg_call(msg.edit_text(stats_msg))


@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600) # 1 ghanta timeout
async def broadcast_command(message: types.Message, db: Database):
    """Admin ko sabhi active users ko message broadcast karne deta hai."""
    if not message.reply_to_message:
        await safe_tg_call(message.answer("❌ Broadcast karne ke liye kisi message ko reply karein."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    users = await safe_db_call(db.get_all_users(), timeout=60, default=[])
    if not users:
        await safe_tg_call(message.answer("❌ Database mein koi active users nahi mile."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    total = len(users)
    msg = await safe_tg_call(message.answer(f"📤 Broadcast shuru... {total:,} users ko target kiya gaya."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    start_broadcast_time = datetime.now(timezone.utc)
    success_count, failed_count = 0, 0
    tasks = []
    
    async def send_to_user(user_id: int):
        nonlocal success_count, failed_count
        # copy_to() 'reply_to_message' ko copy karta hai
        res = await safe_tg_call(
            message.reply_to_message.copy_to(user_id),
            timeout=10, # Broadcast ke liye extra time
            semaphore=TELEGRAM_BROADCAST_SEMAPHORE
        )
        if res:
            success_count += 1
        elif res is False:
            # User ne bot block kar diya hai, DB se deactivate karein
            failed_count += 1
            await safe_db_call(db.deactivate_user(user_id))
        else:
            # Temporary error (e.g., flood wait, timeout)
            failed_count += 1

    last_update_time = start_broadcast_time
    
    for i, user_id in enumerate(users):
        tasks.append(send_to_user(user_id))
        processed_count = i + 1
        
        now = datetime.now(timezone.utc)
        # Har 100 users ya 15 seconds mein status update karein
        if processed_count % 100 == 0 or (now - last_update_time).total_seconds() > 15 or processed_count == total:
            await asyncio.gather(*tasks) # Current batch ko poora karein
            tasks = []
            
            elapsed = (now - start_broadcast_time).total_seconds()
            speed = processed_count / elapsed if elapsed > 0 else 0
            
            try:
                await safe_tg_call(msg.edit_text(
                    f"📤 Progress: {processed_count} / {total}\n\n"
                    f"✅ Safal: {success_count:,}\n"
                    f"❌ Fail/Block: {failed_count:,}\n"
                    f"⏱️ Speed: {speed:.1f} users/sec"
                ))
            except TelegramBadRequest:
                pass # Message not modified
            
            last_update_time = now
            
    final_text = (
        f"✅ Broadcast Poora Hua!\n\n"
        f"Sent: {success_count:,}\n"
        f"Failed/Blocked: {failed_count:,}\n"
        f"Total Users: {total:,}"
    )
    await safe_tg_call(msg.edit_text(final_text))


@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(120)
async def cleanup_users_command(message: types.Message, db: Database):
    """30 din se inactive users ko DB se deactivate karta hai."""
    msg = await safe_tg_call(message.answer("🧹 30 din se purane inactive users ko clean kiya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    removed = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=90, default=0)
    new_count = await safe_db_call(db.get_user_count(), default=0)
    
    txt = f"✅ Cleanup poora hua!\nDeactivated: {removed:,}\nAb Active: {new_count:,}"
    await safe_tg_call(msg.edit_text(txt))


@dp.message(Command("get_user"), AdminFilter())
@handler_timeout(10)
async def get_user_command(message: types.Message, db: Database):
    """User ID se user ki jaankari nikalta hai."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer("❌ Istemal: /get_user `USER_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
    
    user_id_to_find = int(args[1])
    user_data = await safe_db_call(db.get_user_info(user_id_to_find))
    
    if not user_data:
        await safe_tg_call(message.answer(f"❌ User <code>{user_id_to_find}</code> database mein nahi mila."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    def format_dt(dt):
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC') if dt else 'N/A'

    user_text = (
        f"<b>User Info:</b> <code>{user_data.get('user_id')}</code>\n"
        f"<b>Username:</b> @{user_data.get('username') or 'N/A'}\n"
        f"<b>First Name:</b> {user_data.get('first_name') or 'N/A'}\n"
        f"<b>Last Name:</b> {user_data.get('last_name') or 'N/A'}\n"
        f"<b>Joined:</b> {format_dt(user_data.get('joined_date'))}\n"
        f"<b>Last Active:</b> {format_dt(user_data.get('last_active'))}\n"
        f"<b>Is Active:</b> {user_data.get('is_active', True)}"
    )
    await safe_tg_call(message.answer(user_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800) # 30 min
async def import_json_command(message: types.Message, db: Database, neondb: NeonDB):
    """Reply kiye gaye JSON file se data import karta hai."""
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
        file = await bot.get_file(doc.file_id)
        if file.file_path is None:
             await safe_tg_call(msg.edit_text(f"❌ File path nahi mila."))
             return
             
        fio = io.BytesIO()
        await bot.download_file(file.file_path, fio)
        fio.seek(0)
        
        # Blocking call ko ThreadPoolExecutor mein run karein
        loop = asyncio.get_running_loop()
        mlist = await loop.run_in_executor(executor, lambda: json.loads(fio.read().decode('utf-8')))
        assert isinstance(mlist, list)
        logger.info(f"JSON Downloaded: {doc.file_name}, Items: {len(mlist)}")
    except Exception as e:
        await safe_tg_call(msg.edit_text(f"❌ Download/Parse Error: {e}")); 
        logger.exception("JSON download/parse error")
        return
    
    total = len(mlist); added_db, updated_db, skipped_db, failed_db = 0, 0, 0, 0
    neon_batch_data = [] # NeonDB ke liye data
    
    await safe_tg_call(msg.edit_text(f"⏳ {total:,} items ko process kiya ja raha hai (DB)..."))
    start_import_time = datetime.now(timezone.utc)
    
    for i, item in enumerate(mlist):
        try:
            fid = item.get("file_id")
            fname = item.get("title") # Filename ko title ki tarah lein
            
            if not fid or not fname: 
                skipped_db += 1
                continue
                
            fid_str = str(fid)
            # file_unique_id JSON mein nahi hoga, isliye file_id ko fallback ki tarah use karein
            file_unique_id = item.get("file_unique_id") or fid_str 
            
            # IMDB ID (JSON se aayi file ke liye auto-generate karein)
            imdb = f"json_{hashlib.md5(fid_str.encode()).hexdigest()[:10]}"
            message_id = item.get("message_id") or AUTO_MESSAGE_ID_PLACEHOLDER
            channel_id = item.get("channel_id") or 0
            
            info = parse_filename(fname)
            title = info["title"] or "Untitled"
            year = info["year"]
            clean_title_val = clean_text_for_search(title)
            
            # 1. MongoDB mein add karein
            db_res = await safe_db_call(db.add_movie(imdb, title, year, fid_str, message_id, channel_id, clean_title_val, file_unique_id))
            
            # 2. NeonDB batch ke liye data taiyar karein
            neon_data = {
                "message_id": message_id, "channel_id": channel_id, "file_id": fid_str, 
                "file_unique_id": file_unique_id, "imdb_id": imdb, "title": title
            }

            if db_res is True: 
                added_db += 1
                neon_batch_data.append(neon_data)
            elif db_res == "updated": 
                updated_db += 1
                neon_batch_data.append(neon_data)
            elif db_res == "duplicate": 
                skipped_db += 1
            else: 
                failed_db += 1
                
        except Exception as e: 
            failed_db += 1
            logger.error(f"Error processing JSON item {i+1}: {e}", exc_info=False)
            logger.debug(f"Failed item data: {item}")
        
        # Status update
        now = datetime.now(timezone.utc)
        if (i + 1) % 200 == 0 or (now - start_import_time).total_seconds() > 15 or (i+1) == total:
            try: 
                await safe_tg_call(msg.edit_text(
                    f"⏳ DB: {i+1}/{total:,}\n"
                    f"  ✅ Added: {added_db:,}\n"
                    f"  🔄 Updated: {updated_db:,}\n"
                    f"  ↷ Skipped: {skipped_db:,}\n"
                    f"  ❌ Failed: {failed_db:,}"
                ))
            except TelegramBadRequest: pass
            start_import_time = now
    
    db_summary = f"DB Done: ✅A:{added_db:,} 🔄U:{updated_db:,} ↷S:{skipped_db:,} ❌F:{failed_db:,}"
    await safe_tg_call(msg.edit_text(f"{db_summary}\n\n⏳ Ab {len(neon_batch_data):,} items ko NeonDB mein sync kiya ja raha hai..."))
    
    # --- NeonDB Sync ---
    neon_stat = ""
    if neon_batch_data:
        neon_res_count = await safe_db_call(neondb.sync_from_mongo(neon_batch_data), default=0)
        neon_stat = f"✅ NeonDB: {neon_res_count:,} items process kiye."
    else: 
        neon_stat = "ℹ️ NeonDB: Sync ke liye kuch nahi."

    await safe_tg_call(msg.edit_text(f"✅ Import Poora Hua!\n{db_summary}\n{neon_stat}"))


@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message, db: Database):
    """IMDB ID se movie ko DB se hatata hai (Neon se nahi)."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Istemal: /remove_dead_movie `IMDB_ID`"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    imdb_id = args[1].strip()
    msg = await safe_tg_call(message.answer(f"⏳ <code>{imdb_id}</code> ko Mongo DB se hataya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    db_del = await safe_db_call(db.remove_movie_by_imdb(imdb_id))
    
    title_str = f"'{movie['title']}'" if movie else f"'{imdb_id}'"
    db_stat = f"✅ Mongo DB se {title_str} remove kiya." if db_del else ("ℹ️ Mongo DB mein nahi mila." if not movie else "❌ Mongo DB Error.")
    
    # NeonDB se remove karna mushkil hai kyonki IMDB_ID unique nahi hai
    # /remove_library_duplicates isko behtar handle karega
    
    await safe_tg_call(msg.edit_text(f"{db_stat}\nℹ️ NeonDB cleanup ke liye /remove_library_duplicates chalayein."))


@dp.message(Command("cleanup_mongo_duplicates"), AdminFilter())
@handler_timeout(300) # 5 min
async def cleanup_mongo_duplicates_command(message: types.Message, db: Database):
    """MongoDB se duplicate IMDB_ID entries hatata hai (sirf data cleanup)."""
    msg = await safe_tg_call(message.answer("⏳ MongoDB mein `imdb_id` duplicates dhoondhe ja rahe hain (Batch: 100)..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    deleted_count, duplicates_found = await safe_db_call(db.cleanup_mongo_duplicates(batch_limit=100), default=(0,0))
    
    if deleted_count > 0:
        await safe_tg_call(msg.edit_text(
            f"✅ {deleted_count} duplicate entries Mongo se delete kiye.\n"
            f"ℹ️ Abhi bhi {max(0, duplicates_found - deleted_count)} duplicates baaki hain. Command dobara chalayein."
        ))
    else:
        await safe_tg_call(msg.edit_text("✅ MongoDB mein `imdb_id` duplicates nahi mile."))


@dp.message(Command("remove_library_duplicates"), AdminFilter())
@handler_timeout(3600) # 1 ghanta
async def remove_library_duplicates_command(message: types.Message, neondb: NeonDB):
    """
    NeonDB ka istemal karke actual duplicate files ko LIBRARY_CHANNEL se delete karta hai.
    """
    msg = await safe_tg_call(message.answer("⏳ NeonDB se `file_unique_id` duplicates dhoondhe ja rahe hain... (Batch: 100)"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # 1. NeonDB se delete karne waali list lein (yeh DB se delete bhi kar deta hai)
    messages_to_delete, total_duplicates = await safe_db_call(neondb.find_and_delete_duplicates(batch_limit=100), default=([], 0))
    
    if not messages_to_delete:
        await safe_tg_call(msg.edit_text("✅ Library mein koi duplicate files nahi mili."))
        return
        
    await safe_tg_call(msg.edit_text(f"✅ {total_duplicates} duplicates mile.\n⏳ Ab {len(messages_to_delete)} files ko channel se delete kiya ja raha hai... (Rate limit ke saath)"))
    
    # 2. Telegram se delete karein (rate limit ke saath)
    deleted_count, failed_count = 0, 0
    tasks = []
    
    async def delete_message(msg_id: int, chat_id: int):
        nonlocal deleted_count, failed_count
        res = await safe_tg_call(
            bot.delete_message(chat_id=chat_id, message_id=msg_id),
            semaphore=TELEGRAM_DELETE_SEMAPHORE
        )
        if res or res is None: # None matlab 'not found', jo thik hai
            deleted_count += 1
        else:
            failed_count += 1

    for msg_id, chat_id in messages_to_delete:
        tasks.append(delete_message(msg_id, chat_id))
        
    await asyncio.gather(*tasks) # Sabhi deletion tasks complete karein
    
    await safe_tg_call(msg.edit_text(
        f"✅ Cleanup Done!\n"
        f"🗑️ Channel se Delete kiye: {deleted_count}\n"
        f"❌ Fail hue: {failed_count}\n"
        f"ℹ️ Baaki Duplicates (DB): {max(0, total_duplicates - deleted_count)}\n"
        f"ℹ️ Command dobara chalayein."
    ))


@dp.message(Command("backup_channel"), AdminFilter())
@handler_timeout(7200) # 2 ghante
async def backup_channel_command(message: types.Message, neondb: NeonDB):
    """NeonDB se unique files ko naye backup channel mein *copy* karta hai."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await safe_tg_call(message.answer(
            "❌ Istemal: /backup_channel `BACKUP_CHANNEL_ID_OR_USERNAME`\n"
            "Example: /backup_channel -1002417767287\n"
            "Example: /backup_channel @MeraBackupChannel"
        ), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    target_channel = args[1].strip()
    try:
        # Check karein ki target ID valid hai (int ya @username)
        if not (target_channel.startswith("@") or target_channel.startswith("-100")):
             raise ValueError("Invalid target channel format.")
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ Error: {e}"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return

    msg = await safe_tg_call(message.answer(f"⏳ NeonDB se unique files ki list fetch ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    unique_files = await safe_db_call(neondb.get_unique_movies_for_backup(), default=[])
    
    if not unique_files:
        await safe_tg_call(msg.edit_text("❌ NeonDB mein backup ke liye koi files nahi mili."))
        return
        
    total_files = len(unique_files)
    await safe_tg_call(msg.edit_text(
        f"✅ {total_files:,} unique files mili.\n"
        f"🚀 Ab {target_channel} par *copy* kiya ja raha hai... (Rate limit ke saath)"
    ))
    
    copied_count, failed_count = 0, 0
    tasks = []
    
    async def copy_file(msg_id: int, chat_id: int):
        nonlocal copied_count, failed_count
        # Hum `copy_message` ka istemal karenge taaki caption bhi saath aaye
        res = await safe_tg_call(
            bot.copy_message(chat_id=target_channel, from_chat_id=chat_id, message_id=msg_id),
            timeout=TG_OP_TIMEOUT * 2,
            semaphore=TELEGRAM_COPY_SEMAPHORE # Copy semaphore use karein
        )
        if res:
            copied_count += 1
        else:
            failed_count += 1

    start_backup_time = datetime.now(timezone.utc)

    for i, (msg_id, chat_id) in enumerate(unique_files):
        tasks.append(copy_file(msg_id, chat_id))
        
        # Har 50 files par status update karein
        if (i + 1) % 50 == 0 or (i + 1) == total_files:
            await asyncio.gather(*tasks) # Batch ko complete karein
            tasks = []
            try:
                await safe_tg_call(msg.edit_text(
                    f"🚀 Progress: {(i+1)} / {total_files}\n"
                    f"✅ Copied: {copied_count}\n"
                    f"❌ Failed: {failed_count}"
                ))
            except TelegramBadRequest: pass # Message not modified
            
            # Thoda pause
            await asyncio.sleep(1.0)
            
    await safe_tg_call(msg.edit_text(
        f"✅ Backup Poora Hua!\n"
        f"Total Unique Files: {total_files}\n"
        f"✅ Copied: {copied_count}\n"
        f"❌ Failed: {failed_count}"
    ))


@dp.message(Command("sync_mongo_to_neon"), AdminFilter())
@handler_timeout(1800) # 30 min
async def sync_mongo_to_neon_command(message: types.Message, db: Database, neondb: NeonDB):
    """MongoDB ke sabhi data ko NeonDB mein sync (insert/skip) karta hai."""
    msg = await safe_tg_call(message.answer("⏳ MongoDB se sabhi movies fetch ki ja rahi hain... (Time lag sakta hai)"), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    mongo_movies = await safe_db_call(db.get_all_movies_for_neon_sync(), timeout=300)
    
    if not mongo_movies:
        await safe_tg_call(msg.edit_text("❌ MongoDB mein sync ke liye koi movies nahi mili."))
        return
    
    await safe_tg_call(msg.edit_text(f"✅ {len(mongo_movies):,} movies mili. Ab NeonDB mein sync kiya ja raha hai... (Duplicates skip ho jayenge)"))
    
    processed_count = await safe_db_call(neondb.sync_from_mongo(mongo_movies), timeout=1500, default=0)
    
    await safe_tg_call(msg.edit_text(f"✅ Sync poora hua! {processed_count:,} movies NeonDB ko sync ke liye bheji gayin."))


@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300) # 5 min
async def rebuild_index_command(message: types.Message, db: Database):
    """MongoDB mein `clean_title` aur $text index ko rebuild karta hai."""
    msg = await safe_tg_call(message.answer("🔧 MongoDB mein `clean_title` aur text index banaya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # 1. clean_title field ko update karein
    updated, total = await safe_db_call(db.rebuild_clean_titles(clean_text_for_search), timeout=240, default=(0,0))
    
    # 2. $text index ko ensure karein
    await safe_db_call(db.init_db()) # init_db index create karta hai
    
    result_text = f"✅ DB Reindex poora hua: {updated:,} missing 'clean_titles' update kiye. Total movies: {total:,}. MongoDB $text index (re)created."
    await safe_tg_call(msg.edit_text(result_text))


@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    """Concurrent user limit ko set karta hai."""
    global CURRENT_CONC_LIMIT
    args = message.text.split()
    
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer(f"Istemal: /set_limit `N` (Abhi: {CURRENT_CONC_LIMIT})"), semaphore=TELEGRAM_COPY_SEMAPHORE)
        return
        
    try:
        val = int(args[1])
        if not (5 <= val <= 200):
            raise ValueError("Limit range ke bahar hai")
            
        CURRENT_CONC_LIMIT = val
        await safe_tg_call(message.answer(f"✅ Concurrency limit ab {CURRENT_CONC_LIMIT} par set hai."), semaphore=TELEGRAM_COPY_SEMAPHORE)
        logger.info(f"Concurrency limit admin ne {CURRENT_CONC_LIMIT} kar diya hai.")
    except (ValueError, AssertionError):
        await safe_tg_call(message.answer("❌ Limit 5 aur 200 ke beech ek number hona chahiye."), semaphore=TELEGRAM_COPY_SEMAPHORE)

# =======================================================
# +++++ ERROR HANDLER +++++
# =======================================================

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    """Bot mein hone waali sabhi anjaani errors ko handle karta hai."""
    
    # Timeouts ko hum handler_timeout decorator mein pehle hi handle kar rahe hain
    if isinstance(exception, asyncio.TimeoutError):
        logger.warning(f"Error handler ne ek unhandled TimeoutError pakda: {exception}")
        return # Ise dobara handle na karein
        
    logger.exception(f"--- UNHANDLED ERROR ---: {exception}", exc_info=True)
    logger.error(f"Update jo fail hua: {update.model_dump_json(indent=2, exclude_none=True)}")

    # User ko soochit karein
    target_chat_id = None
    callback_query = None
    if update.message: 
        target_chat_id = update.message.chat.id
    elif update.callback_query:
        callback_query = update.callback_query
        if callback_query.message: 
            target_chat_id = callback_query.message.chat.id
            
    error_message = "❗️ Ek anjaani error aa gayi hai. Team ko soochit kar diya gaya hai. Kripya thodi der baad try karein."
    
    if target_chat_id:
        try: 
            await bot.send_message(target_chat_id, error_message)
        except Exception as notify_err: 
            logger.error(f"User ko error notify karne mein bhi error: {notify_err}")
            
    if callback_query:
        try: 
            await callback_query.answer("Error", show_alert=True)
        except Exception as cb_err: 
            logger.error(f"Error callback answer karne mein error: {cb_err}")

# =======================================================
# +++++ LOCAL POLLING (Testing ke liye) +++++
# =======================================================
async def main_polling():
    """Bot ko local machine par (webhook ke bina) chalata hai."""
    logger.info("Bot polling mode mein start ho raha hai (local testing)...")
    
    # DBs initialize karein
    try:
        await db.init_db()
        await neondb.init_db()
    except Exception as init_err:
        logger.critical(f"Local main() mein DB init fail: {init_err}", exc_info=True)
        return

    # Webhook delete karein (agar set ho)
    await bot.delete_webhook(drop_pending_updates=True)
    
    global monitor_task
    monitor_task = asyncio.create_task(monitor_event_loop())
    setup_signal_handlers()

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            db=db,
            neondb=neondb
        )
    finally:
        await shutdown_procedure()


if __name__ == "__main__":
    # Yeh tab chalta hai jab file ko seedha `python bot.py` se run kiya jaaye
    # Deployment (Uvicorn) ke liye yeh code nahi chalega
    logger.warning("Bot ko seedha __main__ se run kiya ja raha hai. Deployment ke liye Uvicorn/FastAPI ka istemal karein.")
    
    if not WEBHOOK_URL:
        try:
            asyncio.run(main_polling())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot polling band kar raha hai.")
    else:
        logger.error("WEBHOOK_URL set hai. Local polling nahi chalega.")
        logger.error("Run karne ke liye: uvicorn bot:app --host 0.0.0.0 --port 8000")
