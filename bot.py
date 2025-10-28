# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import uvloop  # <-- FIX 3: UVLoop import karein
uvloop.install()  # <-- FIX 3: UVLoop ko activate karein

from dotenv import load_dotenv  # <-- FIX 1: load_dotenv ko sabse pehle call karein
load_dotenv()

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

# --- FastAPI Import (YAHAN FIX HUA) ---
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest 
from aiogram.client.default import DefaultBotProperties

# --- Database aur Algolia Imports ---
# Ab environment variables load ho chuke hain
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
    
# FIX 1: Ab algolia_client.is_algolia_ready() sahi se check karega
if not algolia_client.is_algolia_ready():
    logger.critical("Algolia environment variables (APP_ID, ADMIN_KEY, INDEX_NAME) missing or incorrect!")

def build_webhook_url() -> str:
    base = RENDER_EXTERNAL_URL or PUBLIC_URL
    if base:
        base = base.rstrip("/")
        return f"{base}/bot/{BOT_TOKEN}"
    logger.warning("No external URL found (RENDER_EXTERNAL_URL); webhook not set.")
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
                    if args and isinstance(args[0], (types.Message, types.CallbackQuery)):
                        user_id = args[0].from_user.id
                        await bot.send_message(user_id, "⚠️ Request timeout - kripya dobara try karein. Server busy ho sakta hai.", parse_mode=ParseMode.HTML)
                except Exception:
                    pass
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
        logger.debug(f"DB operation error (handled internally): {e}") 
        return default

async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT):
    """Telegram API call ko timeout ke saath safely execute karein."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Telegram API call timed out after {timeout}s")
        return None
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.warning(f"Telegram API Error: {e}")
            if "bot was blocked by the user" in str(e).lower():
                return False
        else:
            logger.error(f"Unexpected error in Telegram call: {e}")
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
    # TODO: Yahaan actual membership check logic daalein
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
        if len(lines) > 1 and re.search(r"S\d{1,2}", lines[1], re.IGNORECASE):
            title += " " + lines[1].strip()
        info["title"] = title
    
    imdb_match = re.search(r"(tt\d{7,})", caption)
    if imdb_match:
        info["imdb_id"] = imdb_match.group(1)
        
    year_match = re.search(r"\b(19|20)\d{2}\b", caption)
    if year_match:
        info["year"] = year_match.group(0)
    return info if "title" in info else None

def parse_filename(filename: str) -> Dict[str, str]:
    """JSON se title aur year nikalne ke liye helper function."""
    year = None
    match = re.search(r"\(((19|20)\d{2})\)", filename)
    if match:
        year = match.group(1)
    else:
        matches = re.findall(r"\b((19|20)\d{2})\b", filename)
        if matches:
            year = matches[-1][0] 
    
    title = os.path.splitext(filename)[0]
    return {"title": title, "year": year}

def overflow_message(active_users: int) -> str:
    return f"""⚠️ <b>Server Capacity Reached</b>

Hamari free-tier service is waqt <b>{CURRENT_CONC_LIMIT}</b> concurrent users par chal rahi hai 
aur abhi <b>{active_users}</b> active hain. Nayi requests temporarily hold par hain.

Alternate bots use karein (neeche se choose karein):"""

# ============ EVENT LOOP MONITOR ============
async def monitor_event_loop():
    """Event loop ko blocking operations ke liye monitor karein."""
    while True:
        try:
            start = asyncio.get_event_loop().time()
            await asyncio.sleep(0.1)
            lag = asyncio.get_event_loop().time() - start
            if lag > 0.5:
                logger.warning(f"⚠️ Event loop lag detected: {lag:.3f}s")
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("Event loop monitor stopping.")
            break
        except Exception as e:
            logger.error(f"Event loop monitor error: {e}")
            await asyncio.sleep(60)

# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialized (max_workers=10).")
    
    await db.init_db() 
    
    monitor_task = asyncio.create_task(monitor_event_loop())
    logger.info("Event loop monitor started.")

    if WEBHOOK_URL:
        try:
            current_webhook = await bot.get_webhook_info()
            if current_webhook.url != WEBHOOK_URL:
                await bot.set_webhook(
                    url=WEBHOOK_URL,
                    allowed_updates=dp.resolve_used_update_types(),
                    secret_token=(WEBHOOK_SECRET or None),
                    drop_pending_updates=True,
                )
                logger.info(f"Webhook set to {WEBHOOK_URL}")
            else:
                logger.info("Webhook already set.")
        except Exception as e:
            logger.error(f"Webhook setup error: {e}", exc_info=True)

    yield

    logger.info("Shutting down...")
    monitor_task.cancel()
    try:
        await asyncio.sleep(2)
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted.")
    except Exception as e:
        logger.error(f"Webhook delete error: {e}", exc_info=True)
        
    executor.shutdown(wait=False)
    logger.info("ThreadPoolExecutor shut down.")

app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK ENDPOINT ============
async def _process_update(u: Update):
    try:
        await dp.feed_update(bot=bot, update=u)
    except Exception as e:
        logger.exception(f"feed_update failed: {e}")
        
@app.post(f"/bot/{BOT_TOKEN}")
async def bot_webhook(update: dict, background_tasks: BackgroundTasks, request: Request):
    try:
        if WEBHOOK_SECRET:
            if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
                logger.warning("Invalid webhook secret token")
                raise HTTPException(status_code=403, detail="Forbidden")
        
        telegram_update = Update(**update)
        
        # --- Lamba chalne waale commands ke liye dynamic timeout ---
        timeout_duration = HANDLER_TIMEOUT
        msg_text = None
        if telegram_update.message and telegram_update.message.text:
            msg_text = telegram_update.message.text
        
        if msg_text:
            if msg_text.startswith("/import_json") or msg_text.startswith("/broadcast") or msg_text.startswith("/sync_algolia"):
                timeout_duration = 1810  # 30 minute + 10s buffer
                logger.info(f"Long-running command '{msg_text.split()[0]}' detected, setting timeout to {timeout_duration}s")
            elif msg_text.startswith("/rebuild_index"):
                timeout_duration = 310  # 5 minute + 10s buffer
                logger.info(f"Medium-running command '/rebuild_index' detected, setting timeout to {timeout_duration}s")
        
        async def _process_with_timeout():
            try:
                await asyncio.wait_for(_process_update(telegram_update), timeout=timeout_duration)
            except asyncio.TimeoutError:
                logger.error(f"Update processing timed out after {timeout_duration}s: {telegram_update.update_id}")
        
        background_tasks.add_task(_process_with_timeout)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}

# ============ HEALTH CHECK ENDPOINT ============
@app.get("/")
async def ping():
    logger.info("Ping/Root endpoint hit (keep-alive).")
    return {"status": "ok", "service": "Movie Bot (Algolia)", "uptime": get_uptime()}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint hit (keep-alive).")
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "uptime": get_uptime()}

# ============ CAPACITY MANAGEMENT ============
async def ensure_capacity_or_inform(message: types.Message) -> bool:
    """Capacity check karein aur user ko update karein."""
    user_id = message.from_user.id
    
    await safe_db_call(
        db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name),
        timeout=DB_OP_TIMEOUT
    )

    if user_id == ADMIN_USER_ID:
        return True
    
    active = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), timeout=DB_OP_TIMEOUT, default=0)
    if active > CURRENT_CONC_LIMIT: 
        logger.warning(f"Capacity reached: {active}/{CURRENT_CONC_LIMIT}. User {user_id} request held.")
        try:
            await asyncio.wait_for(message.answer(overflow_message(active), reply_markup=get_full_limit_keyboard()), timeout=TG_OP_TIMEOUT)
        except:
            pass
        return False
        
    return True

# ============ BOT HANDLERS ============
@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message):
    user_id = message.from_user.id
    try:
        bot_info = await asyncio.wait_for(bot.get_me(), timeout=5)
    except (asyncio.TimeoutError, TelegramAPIError):
        await safe_tg_call(message.answer("⚠️ Bot mein technical error hai. Kripya thodi der baad /start karein."))
        return

    if user_id == ADMIN_USER_ID:
        await safe_db_call(db.add_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
        user_count = await safe_db_call(db.get_user_count(), default=0)
        movie_count = await safe_db_call(db.get_movie_count(), default=0)
        concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
        
        # FIX 1: Yeh ab sahi status dikhayega
        algolia_status = "🟢 Connected" if algolia_client.is_algolia_ready() else "❌ NOT CONNECTED (Search Slow/Broken)"
        
        admin_message = f"""👑 <b>Admin Console: @{bot_info.username}</b>
Access Level: Full Management

<b>System Status (Render Free Tier + Algolia)</b>
• Bot Status: 🟢 Online
• Active Users (5m): {concurrent_users:,}/<b>{CURRENT_CONC_LIMIT}</b>
• Postgres DB: {movie_count:,} movies
• Uptime: {get_uptime()}
• Algolia Search: <b>{algolia_status}</b>
• Service Note: Render free tier 15 min inactivity ke baad spin down hota hai.

<b>Management Commands</b>
• /stats — Real-time stats
• /broadcast — Reply to message to send
• /import_json — Reply to a .json file (Updates DB + Algolia)
• /add_movie — Reply: /add_movie imdb | title | year (Updates DB + Algolia)
• /remove_dead_movie IMDB_ID — (Updates DB + Algolia)
• <b>/sync_algolia</b> — ⚠️ Force syncs DB to Algolia (Use this to fix Algolia issues!)
• /rebuild_index — (DB Only) Re-index clean_titles
• /cleanup_users — Deactivate inactive users
• /export_csv users|movies [limit]
• /set_limit N — Change concurrency cap"""
        
        await safe_tg_call(message.answer(admin_message))
        return

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
    await safe_tg_call(callback.answer("Verifying..."))
    
    active_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    if active_users > CURRENT_CONC_LIMIT and callback.from_user.id != ADMIN_USER_ID:
        await safe_tg_call(callback.message.edit_text(overflow_message(active_users)))
        await safe_tg_call(bot.send_message(callback.from_user.id, "Alternate bots ka upyog karein:", reply_markup=get_full_limit_keyboard()))
        return
            
    is_member = await check_user_membership(callback.from_user.id)
    
    if is_member:
        success_text = f"""✅ Verification successful, <b>{callback.from_user.first_name}</b>!

Ab aap instant search access kar sakte hain — apni pasand ki title ka naam bhejein.

(Free tier capacity: {CURRENT_CONC_LIMIT}, abhi active: {active_users}.)"""
            
        result = await safe_tg_call(callback.message.edit_text(success_text, reply_markup=None))
        if not result:
            await safe_tg_call(bot.send_message(callback.from_user.id, success_text, reply_markup=None))
    else:
        await safe_tg_call(callback.answer("❌ Aapne abhi tak Channel/Group join nahi kiya hai. Kripya join karke dobara try karein.", show_alert=True))

# =======================================================
# +++++ ALGOLIA SEARCH HANDLER +++++
# =======================================================
@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(10) # Algolia fast hai, 10 sec kaafi hai
async def search_movie_handler(message: types.Message):
    user_id = message.from_user.id

    if not await check_user_membership(user_id):
        await safe_tg_call(message.answer("⚠️ Kripya pehle Channel aur Group join karein...", reply_markup=get_join_keyboard()))
        return

    if not await ensure_capacity_or_inform(message):
        return

    original_query = message.text.strip()
    if len(original_query) < 2:
        await safe_tg_call(message.answer("🤔 Kripya kam se kam 2 characters ka query bhejein."))
        return

    # Check karein ki Algolia chal raha hai ya nahi. Agar nahi, toh fail fast.
    # FIX 1: Yeh check ab sahi se kaam karega.
    if not algolia_client.is_algolia_ready():
        logger.warning(f"User {user_id} search failed: Algolia client not ready (Slow Search).")
        await safe_tg_call(message.answer("❌ Search Engine configure nahi hai ya abhi kaam nahi kar raha hai. Admin se sampark karein."))
        return

    searching_msg = await safe_tg_call(message.answer(f"⚡️ <b>{original_query}</b> ki khoj jaari hai... (Fast Algolia Search)"))
    if not searching_msg:
        return
    
    # --- Yahaan DB ke bajaye ALGOLIA ko call karein ---
    top = await algolia_client.algolia_search(original_query, limit=20)
    
    if not top:
        await safe_tg_call(searching_msg.edit_text(
            f"🥲 Maaf kijiye, <b>{original_query}</b> ke liye match nahi mila.\n(Algolia ne spelling mistakes check kar li hain)."
        ))
        return

    # Result milne par, hum DB se file ID nikalne ke bajaye seedha callback banayenge.
    # get_movie_callback function DB se data nikaal lega.
    buttons = [[InlineKeyboardButton(text=movie["title"], callback_data=f"get_{movie['imdb_id']}")] for movie in top]
    await safe_tg_call(searching_msg.edit_text(
        f"⚡️ <b>{original_query}</b> ke liye {len(top)} results mile (Instant Search):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    ))

@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(15)
async def get_movie_callback(callback: types.CallbackQuery):
    await safe_tg_call(callback.answer("File forward ki ja rahi hai…"))
    imdb_id = callback.data.split("_", 1)[1]
    
    # ✅ FIX: capacity check mein message object use karein
    if not await ensure_capacity_or_inform(callback.message):
        return
        
    # --- Search Algolia se, lekin File details DB se ---
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id), timeout=DB_OP_TIMEOUT)
    if not movie:
        await safe_tg_call(callback.message.edit_text("❌ Yeh movie ab database me uplabdh nahi hai."))
        # Admin ko sync karne ke liye bolein
        if callback.from_user.id == ADMIN_USER_ID:
            await safe_tg_call(callback.message.answer(f"ADMIN: Movie Algolia mein hai par DB mein nahi. /sync_algolia chalayein."))
        return
        
    success = False
    await safe_tg_call(callback.message.edit_text(f"✅ <b>{movie['title']}</b> — file forward ki ja rahi hai, kripya chat check karein."))
    
    try:
        await asyncio.wait_for(
            bot.forward_message(
                chat_id=callback.from_user.id,
                from_chat_id=int(movie["channel_id"]),
                message_id=movie["message_id"],
            ),
            timeout=TG_OP_TIMEOUT 
        )
        success = True
        
    except (asyncio.TimeoutError, TelegramAPIError) as e:
        forward_failed_msg = str(e).lower()
        logger.error(f"Forward failed for {imdb_id}: {e}")
        
        if movie["message_id"] == AUTO_MESSAGE_ID_PLACEHOLDER or 'message to forward not found' in forward_failed_msg:
            logger.info(f"Forward failed, falling back to send_document for {imdb_id} (FileID: {movie['file_id']})")
            try:
                await asyncio.wait_for(
                    bot.send_document(
                        chat_id=callback.from_user.id,
                        document=movie["file_id"], 
                        caption=f"🎬 <b>{movie['title']}</b> ({movie['year'] or 'N/A'})" 
                    ),
                    timeout=TG_OP_TIMEOUT * 2
                )
                success = True
            except Exception as e2:
                logger.error(f"❌ DEAD FILE: Movie '{movie['title']}' (IMDB: {imdb_id}) failed. Error: {e2}")
        
    if not success:
        admin_hint = f"\n\n(Admin Hint: /remove_dead_movie {imdb_id})" if callback.from_user.id == ADMIN_USER_ID else ""
        await safe_tg_call(bot.send_message(
            callback.from_user.id, 
            f"❗️ Takneeki samasya: <b>{movie['title']}</b> ki file uplabdh nahi hai. File ID invalid hai." + admin_hint
        ))
        await safe_tg_call(callback.message.edit_text(f"❌ <b>{movie['title']}</b> ki file send nahi ho payi."))

# =======================================================
# +++++ ADMIN COMMANDS (DUAL WRITE) +++++
# =======================================================

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message):
    await safe_db_call(db.add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
    user_count = await safe_db_call(db.get_user_count(), default=0)
    movie_count = await safe_db_call(db.get_movie_count(), default=0)
    concurrent_users = await safe_db_call(db.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    # FIX 1: Yeh ab sahi status dikhayega
    algolia_status = "🟢 Connected" if algolia_client.is_algolia_ready() else "❌ NOT CONNECTED"
    
    stats_msg = f"""📊 <b>Live System Statistics</b>

🟢 Active Users (5m): {concurrent_users:,}/{CURRENT_CONC_LIMIT}
👥 Total Users: {user_count:,}
🎬 Indexed Movies (DB): {movie_count:,}
⚡️ Algolia Status: <b>{algolia_status}</b>
⚙️ Status: Operational
⏰ Uptime: {get_uptime()}"""
    
    await safe_tg_call(message.answer(stats_msg))

@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(1800) # 30 min
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
    progress_msg = await safe_tg_call(message.answer(f"📤 Broadcasting to <b>{total_users}</b> users…"))
    
    for uid in users:
        result = await safe_tg_call(message.reply_to_message.copy_to(uid), timeout=3) 
        if result:
            success += 1
        else:
            failed += 1
            await safe_db_call(db.deactivate_user(uid), timeout=3) # User ko inactive karein
            
        if (success + failed) % 100 == 0 and (success + failed) > 0 and progress_msg:
            try:
                await safe_tg_call(progress_msg.edit_text(f"""📤 Broadcasting…
✅ Sent: {success} | ❌ Failed (or Blocked): {failed} | ⏳ Total: {total_users}"""))
            except TelegramBadRequest: pass
            
        # ✅ FIX: Small sleep to prevent event loop from blocking during the broadcast loop
        await asyncio.sleep(0.05) 
        
    if progress_msg:
        await safe_tg_call(progress_msg.edit_text(f"✅ <b>Broadcast Complete!</b> (Success: {success}, Failed: {failed})"))

@dp.message(Command("cleanup_users"), AdminFilter())
@handler_timeout(60)
async def cleanup_users_command(message: types.Message):
    await safe_tg_call(message.answer("🧹 30 din se inactive users ko clean kiya ja raha hai…"))
    removed_count = await safe_db_call(db.cleanup_inactive_users(days=30), timeout=45, default=0)
    new_count = await safe_db_call(db.get_user_count(), default=0)
    await safe_tg_call(message.answer(f"✅ <b>Cleanup complete!</b> (Deactivated: {removed_count}, Active Users: {new_count})"))

@dp.message(Command("add_movie"), AdminFilter())
@handler_timeout(20)
async def add_movie_command(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.video or message.reply_to_message.document):
        await safe_tg_call(message.answer("❌ Kripya video/document par reply karke command bhejein: /add_movie imdb_id | title | year"))
        return
    try:
        full_command = message.text.replace("/add_movie", "", 1).strip()
        parts = [p.strip() for p in full_command.split("|")]
        imdb_id = parts[0]
        title = parts[1]
        year = parts[2] if len(parts) > 2 else None
        file_id = message.reply_to_message.video.file_id if message.reply_to_message.video else message.reply_to_message.document.file_id
    except Exception:
        await safe_tg_call(message.answer("❌ Format galat hai; use: /add_movie imdb_id | title | year"))
        return
        
    # 1. Pehle Database mein save/update karein
    success_db = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=title, year=year,
        file_id=file_id, message_id=message.reply_to_message.message_id, channel_id=message.reply_to_message.chat.id
    ), default=False)
    
    algolia_data = {'objectID': imdb_id, 'imdb_id': imdb_id, 'title': title, 'year': year}

    if success_db is True:
        await safe_tg_call(message.answer(f"✅ Movie '<b>{title}</b>' database mein add ho gayi."))
        # 2. Ab Algolia mein bhi save karein
        success_algolia = await algolia_client.algolia_add_movie(algolia_data)
        if success_algolia:
            await safe_tg_call(message.answer("✅ Movie Algolia index mein bhi add ho gayi."))
        else:
            await safe_tg_call(message.answer("❌ WARNING: Movie DB mein add hui, par Algolia mein fail ho gayi! /sync_algolia chalayein."))

    # FIX 2: "updated" status ko handle karein
    elif success_db == "updated":
        await safe_tg_call(message.answer(f"✅ Movie '<b>{title}</b>' database mein update ho gayi."))
        # 2. Algolia mein bhi update karein
        success_algolia = await algolia_client.algolia_add_movie(algolia_data)
        if success_algolia:
            await safe_tg_call(message.answer("✅ Movie Algolia index mein bhi update ho gayi."))
        else:
            await safe_tg_call(message.answer("❌ WARNING: Movie DB mein update hui, par Algolia mein fail ho gayi! /sync_algolia chalayein."))

    elif success_db == "duplicate":
        await safe_tg_call(message.answer(f"⚠️ Movie '<b>{title}</b>' pehle se database mein hai (Integrity Error)."))
    else:
        await safe_tg_call(message.answer("❌ Movie DB mein add/update karne me error aaya."))

@dp.message(Command("import_json"), AdminFilter())
@handler_timeout(1800) # 30 min
async def import_json_command(message: types.Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        await safe_tg_call(message.answer("❌ Kripya ek .json file par reply karke command bhejein."))
        return
    doc = message.reply_to_message.document
    if doc.mime_type != "application/json" and not doc.file_name.endswith(".json"):
        await safe_tg_call(message.answer("❌ File .json format mein honi chahiye."))
        return

    await safe_tg_call(message.answer("⏳ JSON file download ki ja rahi hai..."))
    try:
        file_io = io.BytesIO()
        file = await bot.get_file(doc.file_id)
        await bot.download_file(file.file_path, file_io, timeout=60)
        file_io.seek(0)
        movies_json_list = json.loads(file_io.read().decode('utf-8'))
        if not isinstance(movies_json_list, list):
             await safe_tg_call(message.answer("❌ Error: JSON file ek list (array) nahi hai."))
             return
    except Exception as e:
        await safe_tg_call(message.answer(f"❌ JSON file padhne ya download karne me error: {e}"))
        return

    total = len(movies_json_list)
    added_db = 0
    updated_db = 0  # <-- FIX 2: Updated counter
    skipped_db = 0
    failed_db = 0
    algolia_batch_to_add_or_update = [] # Algolia mein add/update karne ke liye batch
    
    progress_msg = await safe_tg_call(message.answer(f"⏳ (Step 1/2) JSON import DB mein shuru... Total {total} movies."))
    if not progress_msg: return

    for i, item in enumerate(movies_json_list):
        try:
            file_id = item.get("file_id")
            filename = item.get("title")
            if not file_id or not filename:
                skipped_db += 1; continue
            
            imdb_id = f"json_{hashlib.md5(file_id.encode()).hexdigest()}"
            parsed_info = parse_filename(filename)
            
            success_db = await safe_db_call(db.add_movie(
                imdb_id=imdb_id, title=parsed_info["title"], year=parsed_info["year"],
                file_id=file_id, message_id=AUTO_MESSAGE_ID_PLACEHOLDER, channel_id=0
            ), default=False)
            
            algolia_data = {
                'objectID': imdb_id, 'imdb_id': imdb_id,
                'title': parsed_info["title"], 'year': parsed_info["year"]
            }

            # FIX 2: "updated" status ko handle karein
            if success_db is True:
                added_db += 1
                algolia_batch_to_add_or_update.append(algolia_data)
            elif success_db == "updated":
                updated_db += 1
                algolia_batch_to_add_or_update.append(algolia_data)
            elif success_db == "duplicate":
                skipped_db += 1
            else:
                failed_db += 1
        except Exception as e:
            logger.error(f"JSON import error for item {i}: {e}"); failed_db += 1
        
        if (i + 1) % 100 == 0 or (i + 1) == total:
            try:
                await safe_tg_call(progress_msg.edit_text(
                    f"⏳ (Step 1/2) Processing DB... {i+1}/{total}\n"
                    f"✅ Added: {added_db} | 🔄 Updated: {updated_db} | ↷ Skipped: {skipped_db} | ❌ Failed: {failed_db}"
                ))
            except TelegramBadRequest: pass
            await asyncio.sleep(0.5) 

    await safe_tg_call(progress_msg.edit_text(
        f"✅ <b>DB Import Complete!</b>\n"
        f"• Added: {added_db} | Updated: {updated_db} | Skipped: {skipped_db} | Failed: {failed_db}\n\n"
        f"⏳ (Step 2/2) Ab {len(algolia_batch_to_add_or_update)} movies ko Algolia mein upload/update kiya ja raha hai..."
    ))

    # 2. Sabhi nayi/updated movies ko ek saath batch mein Algolia par upload karein
    if algolia_batch_to_add_or_update:
        success_algolia = await algolia_client.algolia_add_batch_movies(algolia_batch_to_add_or_update)
        if success_algolia:
            await safe_tg_call(progress_msg.edit_text(
                f"✅ <b>JSON Import Complete! (DB + Algolia)</b>\n"
                f"• DB Added: {added_db} | DB Updated: {updated_db} | DB Skipped: {skipped_db}\n"
                f"• Algolia Uploaded/Updated: {len(algolia_batch_to_add_or_update)}"
            ))
        else:
             await safe_tg_call(progress_msg.edit_text(
                f"❌ <b>JSON Import Failed!</b>\n"
                f"• DB Added: {added_db} | DB Updated: {updated_db}\n"
                f"• Algolia Upload FAILED! /sync_algolia chalayein."
            ))
    else:
        await safe_tg_call(progress_msg.edit_text(
            f"✅ <b>JSON Import Complete!</b>\n"
            f"• DB Added: {added_db} | DB Updated: {updated_db} | DB Skipped: {skipped_db}\n"
            f"• Algolia Upload: 0 (kuch naya nahi tha)"
        ))

@dp.message(Command("remove_dead_movie"), AdminFilter())
@handler_timeout(15)
async def remove_dead_movie_command(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await safe_tg_call(message.answer("❌ Use: /remove_dead_movie IMDB_ID"))
        return
    imdb_id = args[1].strip()
    movie = await safe_db_call(db.get_movie_by_imdb(imdb_id))
    if not movie:
        await safe_tg_call(message.answer(f"❌ Movie <code>{imdb_id}</code> DB mein nahi mili."))
        return
    
    # 1. Pehle DB se remove karein
    success_db = await safe_db_call(db.remove_movie_by_imdb(imdb_id), default=False)
    
    if success_db:
        await safe_tg_call(message.answer(f"✅ DB se remove ho gayi: <b>{movie['title']}</b>"))
        
        # 2. Ab Algolia se bhi remove karein
        success_algolia = await algolia_client.algolia_remove_movie(imdb_id)
        if success_algolia:
            await safe_tg_call(message.answer(f"✅ Algolia se bhi remove ho gayi."))
        else:
            await safe_tg_call(message.answer(f"❌ WARNING: DB se remove hui, par Algolia se nahi! /sync_algolia chalayein."))
    else:
        await safe_tg_call(message.answer(f"❌ DB se remove karne mein error."))

# =======================================================
# +++++ NEW ADMIN COMMAND: /sync_algolia (Must be present in bot.py) +++++
# =======================================================
@dp.message(Command("sync_algolia"), AdminFilter())
@handler_timeout(1800) # 30 min
async def sync_algolia_command(message: types.Message):
    """
    Yeh "Fix-it" button hai.
    Yeh poore PostgreSQL DB ko padhega aur Algolia index ko overwrite kar dega.
    """
    if not algolia_client.is_algolia_ready():
        await safe_tg_call(message.answer("❌ Algolia client configure nahi hai. .env check karein."))
        return
        
    await safe_tg_call(message.answer("⚠️ Full Sync Shuru...\n"
                                     "Yeh process poore PostgreSQL database ko padhega aur Algolia index ko overwrite karega.\n"
                                     "Ismein 10-20 minute lag sakte hain..."))
    
    try:
        # 1. DB se saara data fetch karein
        logger.info("Sync: Fetching all movies from DB...")
        all_movies_db = await safe_db_call(db.get_all_movies_for_sync(), timeout=300) # 5 min timeout
        
        if all_movies_db is None: # DB error
            await safe_tg_call(message.answer("❌ DB se data fetch karne mein error aaya. Sync cancelled."))
            return
            
        if not all_movies_db:
            await safe_tg_call(message.answer("ℹ️ DB khaali hai. Algolia index ko clear kiya ja raha hai..."))
            success = await algolia_client.algolia_clear_index()
            if success:
                await safe_tg_call(message.answer("✅ Algolia index clear ho gaya."))
            else:
                await safe_tg_call(message.answer("❌ Algolia index clear karne mein fail hua."))
            return

        await safe_tg_call(message.answer(f"✅ DB se {len(all_movies_db)} movies fetch ho gayi. Ab Algolia par upload kiya ja raha hai..."))
        
        # 2. Algolia par data bhej
        # FIX 4: Yeh ab behtar 'replace_all_objects_async' function (algolia_client.py mein) ka istemaal karega
        success, total_uploaded = await algolia_client.algolia_sync_data(all_movies_db)
        
        if success:
            await safe_tg_call(message.answer(f"✅ <b>Full Sync Complete!</b>\n"
                                             f"{total_uploaded} movies Algolia index mein safalta se upload ho gayi."))
        else:
            await safe_tg_call(message.answer("❌ <b>Sync Failed!</b>\n"
                                             "Algolia par upload karne mein error aaya. Logs check karein."))
                                             
    except Exception as e:
        logger.error(f"Error during /sync_algolia: {e}", exc_info=True)
        await safe_tg_call(message.answer(f"❌ Sync command fail ho gaya: {e}"))

@dp.message(Command("rebuild_index"), AdminFilter())
@handler_timeout(300) # 5 min
async def rebuild_index_command(message: types.Message):
    await safe_tg_call(message.answer("🔧 (PostgreSQL DB Only) Clean titles reindex ho rahe hain…"))
    result = await safe_db_call(db.rebuild_clean_titles(), timeout=180, default=(0, 0))
    updated, total = result
    await safe_tg_call(message.answer(f"✅ DB Reindex complete: Updated <b>{updated}</b> of ~{total} titles."))

@dp.message(Command("export_csv"), AdminFilter())
@handler_timeout(60)
async def export_csv_command(message: types.Message):
    # ... (Yeh function waisa hi rahega, DB se export karega) ...
    args = message.text.split()
    if len(args) < 2 or args[1] not in ("users", "movies"):
        await safe_tg_call(message.answer("Use: /export_csv users|movies [limit]")); return
    kind, limit = args[1], int(args[2]) if len(args) > 2 and args[2].isdigit() else 2000
    
    if kind == "users":
        rows = await safe_db_call(db.export_users(limit=limit), timeout=30, default=[])
        if not rows: await safe_tg_call(message.answer("❌ No user data.")); return
        header = "user_id,username,first_name,last_name,joined_date,last_active,is_active\n"
        csv = header + "\n".join([f"{r['user_id']},{r['username'] or ''},{r['first_name'] or ''},{r['last_name'] or ''},{r['joined_date']},{r['last_active']},{r['is_active']}" for r in rows])
        await safe_tg_call(message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="users.csv"), caption="Users export"))
    else:
        rows = await safe_db_call(db.export_movies(limit=limit), timeout=30, default=[])
        if not rows: await safe_tg_call(message.answer("❌ No movie data.")); return
        header = "imdb_id,title,year,channel_id,message_id,added_date\n"
        csv = header + "\n".join([f"{r['imdb_id']},\"{r['title'].replace('\"', '\"\"')}\",{r['year'] or ''},{r['channel_id']},{r['message_id']},{r['added_date']}" for r in rows])
        await safe_tg_call(message.answer_document(BufferedInputFile(csv.encode("utf-8"), filename="movies.csv"), caption="Movies export (DB)"))

@dp.message(Command("set_limit"), AdminFilter())
@handler_timeout(10)
async def set_limit_command(message: types.Message):
    global CURRENT_CONC_LIMIT
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await safe_tg_call(message.answer(f"Use: /set_limit N (current: {CURRENT_CONC_LIMIT})")); return
    val = int(args[1])
    if not (5 <= val <= 100):
        await safe_tg_call(message.answer("Allowed range: 5–100.")); return
    CURRENT_CONC_LIMIT = val
    await safe_tg_call(message.answer(f"✅ Concurrency limit set to <b>{CURRENT_CONC_LIMIT}</b>"))

@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message):
    if message.chat.id != LIBRARY_CHANNEL_ID or not (message.video or message.document):
        return
    caption = message.caption or ""
    movie_info = extract_movie_info(caption)
    if not movie_info:
        logger.warning(f"Auto-index skipped: could not parse caption: {caption[:80]}")
        return
    
    file_id = message.video.file_id if message.video else message.document.file_id
    imdb_id = movie_info.get("imdb_id", f"auto_{message.message_id}") 
    
    # 1. DB mein add/update karein
    success_db = await safe_db_call(db.add_movie(
        imdb_id=imdb_id, title=movie_info.get("title"), year=movie_info.get("year"),
        file_id=file_id, message_id=message.message_id, channel_id=message.chat.id,
    ), default=False)
    
    algolia_data = {
        'objectID': imdb_id, 'imdb_id': imdb_id,
        'title': movie_info.get("title"), 'year': movie_info.get("year")
    }

    if success_db is True:
        logger.info(f"Auto-indexed to DB: {movie_info.get('title')}")
        # 2. Ab Algolia mein bhi add karein
        success_algolia = await algolia_client.algolia_add_movie(algolia_data)
        if success_algolia:
            logger.info(f"Auto-indexed to Algolia: {movie_info.get('title')}")
        else:
            logger.error(f"Auto-index FAILED for Algolia: {movie_info.get('title')}")

    # FIX 2: "updated" status ko handle karein
    elif success_db == "updated":
        logger.info(f"Auto-indexed (updated) in DB: {movie_info.get('title')}")
        # 2. Algolia mein bhi update karein
        success_algolia = await algolia_client.algolia_add_movie(algolia_data)
        if success_algolia:
            logger.info(f"Auto-indexed (updated) in Algolia: {movie_info.get('title')}")
        else:
            logger.error(f"Auto-index FAILED for Algolia (update): {movie_info.get('title')}")

    elif success_db == "duplicate":
        logger.info(f"Auto-index skipped (duplicate): {movie_info.get('title')}")
    else:
        logger.error(f"Auto-index failed (DB): {movie_info.get('title')}")

# Error handler
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    logger.error(f"Unhandled error in dispatcher: {exception}", exc_info=True)
    try:
        if update.message:
            await update.message.answer("❗️ Ek unexpected error hua. Kripya dobara try karein.")
        elif update.callback_query:
            await update.callback_query.message.answer("❗️ Ek unexpected error hua. Kripya dobara try karein.")
    except Exception as e:
        logger.error(f"Error handler failed to send message: {e}")
