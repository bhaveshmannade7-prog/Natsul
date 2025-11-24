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
from redis_cache import redis_cache, RedisCacheLayer
from queue_wrapper import priority_queue, PriorityQueueWrapper, QUEUE_CONCURRENCY, PRIORITY_ADMIN
# --- END NEW IMPORTS ---

# --- NAYA FUZZY SEARCH IMPORT ---
# ... existing code ...

# ============ CONFIGURATION ============
try:
    BOT_TOKEN = os.environ["BOT_TOKEN"]
    
    # --- 3 DB Connections ---
    DATABASE_URL_PRIMARY = os.environ["DATABASE_URL_PRIMARY"]
    DATABASE_URL_FALLBACK = os.environ["DATABASE_URL_FALLBACK"]
    NEON_DATABASE_URL = os.environ["NEON_DATABASE_URL"]
    
    # --- NEW: Redis URL
    REDIS_URL = os.getenv("REDIS_URL")
    
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
    # ... existing code ...
    
    ALTERNATE_BOTS_RAW = os.getenv("ALTERNATE_BOTS", "")
    ALTERNATE_BOTS = [b.strip() for b in ALTERNATE_BOTS_RAW.split(',') if b.strip()] if ALTERNATE_BOTS_RAW else []

except KeyError as e:
# ... existing code ...

CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

# ... existing code ...

# ============ BOT & DB INITIALIZATION ============

# --- NEW: Multi-Bot Manager Class ---
class BotManager:
    """Multi-Bot (Token) instances ko manage karta hai।"""
    def __init__(self, main_token: str, alternate_tokens: List[str]):
        # Main bot instance (already created)
        self.main_bot = bot 
        
        # All tokens in a hashable list
        self.all_tokens = [main_token] + alternate_tokens
        self.bots: Dict[str, Bot] = {main_token: self.main_bot}
        
        # Alternate bots ko initialize karein
        for token in alternate_tokens:
            if token not in self.bots:
                 self.bots[token] = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
                 logger.info(f"Alternate Bot instance for {token[:4]}... initialize ho gaya।")

    def get_bot_by_token(self, token: str) -> Bot:
        """Webhook se aaye token ke hisaab se bot instance return karein।"""
        return self.bots.get(token, self.main_bot) # Agar token nahi mila toh main bot return karein
        
    def get_all_bots(self) -> List[Bot]:
        return list(self.bots.values())
# --- END Multi-Bot Manager ---


try:
    # Existing bot (Main bot instance)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    
    # --- NEW: Bot Manager ---
    bot_manager = BotManager(BOT_TOKEN, ALTERNATE_BOTS)
    
    storage = MemoryStorage()
    
    # --- 3 Database Objects ---
    db_primary = Database(DATABASE_URL_PRIMARY)
    db_fallback = Database(DATABASE_URL_FALLBACK)
    db_neon = NeonDB(NEON_DATABASE_URL)
    
    # --- Dependency Injection ---
    # NOTE: Redis Cache ko yahaan DP mein pass kar rahe hain
    dp = Dispatcher(
        storage=storage, 
        db_primary=db_primary, 
        db_fallback=db_fallback, 
        db_neon=db_neon,
        redis_cache=redis_cache
    )
    
    logger.info("Bot, Dispatcher, aur 3 Database objects (M+M+N) initialize ho gaye।")
    logger.info(f"Multi-Bot Manager mein {len(bot_manager.all_tokens)} tokens configured hain।")
except Exception as e:
# ... existing code ...

start_time = datetime.now(timezone.utc)
monitor_task: asyncio.Task | None = None
executor: concurrent.futures.ThreadPoolExecutor | None = None
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

# --- NAYA FUZZY CACHE (Dict[str, Dict] format) ---
fuzzy_movie_cache: Dict[str, Dict] = {}
FUZZY_CACHE_LOCK = asyncio.Lock()


# ============ GRACEFUL SHUTDOWN ============
async def shutdown_procedure():
    logger.info("Graceful shutdown shuru ho raha hai...")
    
    # --- NEW: Stop Queue Workers ---
    await priority_queue.stop_workers()
    # --- END NEW ---
    
    if monitor_task and not monitor_task.done():
# ... existing code ...
            
    # --- NEW: Delete webhooks for all bots and close sessions ---
    tasks = []
    for bot_instance in bot_manager.get_all_bots():
        if WEBHOOK_URL:
            # Har bot ke liye webhook delete karein
            tasks.append(safe_tg_call(bot_instance.delete_webhook(drop_pending_updates=True)))
        tasks.append(safe_tg_call(bot_instance.session.close()))
    
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"{len(tasks)//2} bot sessions aur webhooks close/delete ho gaye।")
    # --- END NEW ---
            
    try: await dp.storage.close()
    except Exception as e: logger.error(f"Dispatcher storage close karte waqt error: {e}")
        
    if executor:
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("ThreadPoolExecutor shutdown ho gaya।")
        
    # --- NEW: Close Redis Connection ---
    await redis_cache.close()
    # --- END NEW ---
        
    try:
# ... existing DB close logic ...
        if db_neon:
            await db_neon.close()
    except Exception as e:
        logger.error(f"Database connections close karte waqt error: {e}")
        
    logger.info("Graceful shutdown poora hua।")

# ... existing code (setup_signal_handlers) ...

# ============ TIMEOUT DECORATOR ============
# ... existing code (handler_timeout, safe_db_call, safe_tg_call) ...

# ============ FILTERS & HELPER FUNCTIONS ============
# ... existing code (AdminFilter, get_uptime, check_user_membership, get_join_keyboard, get_full_limit_keyboard) ...

# --- NAYA FUZZY CACHE FUNCTIONS ---
async def load_fuzzy_cache(db: Database):
    """
    Mongo/Redis se movie titles fetch karke in-memory fuzzy cache banata hai।
    (Ab Redis fallback ke saath)
    """
    global fuzzy_movie_cache
    async with FUZZY_CACHE_LOCK:
        logger.info("In-Memory Fuzzy Cache load ho raha hai (Redis > Mongo se)...")
        try:
            # Yeh function ab database.py mein Redis logic ko use karega
            movies_list = await safe_db_call(db.get_all_movies_for_fuzzy_cache(), timeout=300, default=[])
            temp_cache = {}
            if movies_list:
                for movie_dict in movies_list:
                    orig_clean = movie_dict.get('clean_title', '')
                    if orig_clean:
                         if orig_clean not in temp_cache:
                             temp_cache[orig_clean] = movie_dict
                fuzzy_movie_cache = temp_cache
                logger.info(f"✅ In-Memory Fuzzy Cache {len(fuzzy_movie_cache):,} unique titles ke saath loaded।")
                
                # Agar data Mongo se aaya hai, toh use Redis mein save karne ki koshish karein (database.py mein hook hai)
            else:
                logger.error("Fuzzy cache load nahi ho paya (Redis/Mongo se koi data nahi mila)।")
                fuzzy_movie_cache = {}
        except Exception as e:
            logger.error(f"Fuzzy cache load karte waqt error: {e}", exc_info=True)
            fuzzy_movie_cache = {}

# ... existing code (get_smart_match_score, python_fuzzy_search) ...


# ============ LIFESPAN MANAGEMENT (FastAPI) ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitor_task, executor
    logger.info("Application startup shuru ho raha hai...")
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop(); loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor initialize ho gaya।")

    # --- NEW: Redis Init ---
    await redis_cache.init_cache()
    # --- END NEW ---

    # --- 3 DB Init ---
# ... existing DB init logic ...
    try:
        await db_primary.init_db()
        logger.info("Database 1 (MongoDB Primary) initialization safal।")
    except Exception as e:
        logger.critical(f"FATAL: Database 1 (MongoDB Primary) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("MongoDB 1 connection fail (startup)।") from e

    try:
        await db_fallback.init_db()
        logger.info("Database 2 (MongoDB Fallback) initialization safal।")
    except Exception as e:
        logger.critical(f"FATAL: Database 2 (MongoDB Fallback) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("MongoDB 2 connection fail (startup)।") from e

    try:
        await db_neon.init_db()
        logger.info("Database 3 (NeonDB/Postgres Backup) initialization safal।")
    except Exception as e:
        logger.critical(f"FATAL: Database 3 (NeonDB/Postgres Backup) initialize nahi ho paya: {e}", exc_info=True)
        raise RuntimeError("NeonDB/Postgres connection fail (startup)।") from e

    # --- NAYA: Fuzzy Cache Load Karein (ab Redis/Mongo se) ---
    await load_fuzzy_cache(db_primary)

    # --- NEW: Start Queue Workers ---
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
    logger.info("Event loop monitor start ho gaya।")

    # --- NEW: Webhook set up for ALL BOTS ---
    for bot_instance in bot_manager.get_all_bots():
        token = bot_instance.token
        webhook_url_for_token = build_webhook_url().replace(BOT_TOKEN, token)
        if webhook_url_for_token:
            try:
                current_webhook = await bot_instance.get_webhook_info()
                if not current_webhook or current_webhook.url != webhook_url_for_token:
                     logger.info(f"Webhook set kiya ja raha hai for {token[:4]}...: {webhook_url_for_token}")
                     await bot_instance.set_webhook(
                         url=webhook_url_for_token,
                         allowed_updates=dp.resolve_used_update_types(),
                         secret_token=(WEBHOOK_SECRET or None),
                         drop_pending_updates=True
                     )
                     logger.info(f"Webhook set ho gaya for {token[:4]}...।")
                else:
                     logger.info(f"Webhook pehle se sahi set hai for {token[:4]}...।")
            except Exception as e:
                logger.error(f"Webhook setup mein error for {token[:4]}...: {e}", exc_info=True)
        else:
            logger.warning("WEBHOOK_URL set nahi hai।")
    # --- END NEW ---

    setup_signal_handlers()
    logger.info("Application startup poora hua. Bot taiyar hai।")
    yield
    logger.info("Application shutdown sequence shuru ho raha hai...")
    await shutdown_procedure()
    logger.info("Application shutdown poora hua।")


app = FastAPI(lifespan=lifespan)

# ============ WEBHOOK / HEALTHCHECK ROUTES ============

@app.post(f"/bot/{{token}}") # Token ko URL se parse karein
async def bot_webhook(token: str, update: dict, background_tasks: BackgroundTasks, request: Request):
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token mila।")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid Secret Token")
        
    # --- NEW: Bot Manager se Bot Instance select karein ---
    bot_instance = bot_manager.get_bot_by_token(token)
    if bot_instance.token != token:
        logger.warning(f"Invalid token {token[:4]}... received।")
        raise HTTPException(status_code=404, detail="Not Found: Invalid Bot Token")
    # --- END NEW ---
    
    try:
        telegram_update = Update(**update)
        
        # --- NEW: BackgroundTasks hata kar PriorityQueue mein submit karein ---
        # Queue non-blocking tarike se update ko process karega
        db_objects_for_queue = {
            'db_primary': db_primary,
            'db_fallback': db_fallback,
            'db_neon': db_neon,
            'redis_cache': redis_cache, # Redis object
            'admin_id': ADMIN_USER_ID
        }
        priority_queue.submit(telegram_update, bot_instance, db_objects_for_queue)
        # --- END NEW ---
        
        # NOTE: Hum ne `_process_update_safe` ko queue worker mein move kar diya hai
        # background_tasks.add_task(_process_update_safe, telegram_update, bot_instance) # OLD CODE REMOVED
        
        return {"ok": True, "token_check": True}
    except Exception as e:
# ... existing code ...
        logger.debug(f"Failed update data: {update}")
        return {"ok": False, "error": f"Invalid update format: {e}"}

@app.get("/")
@app.get("/ping")
async def ping():
    return {"status": "ok", "uptime": get_uptime(), "queue_size": priority_queue._queue.qsize()}

@app.get("/health")
async def health_check():
    # Teeno DBs ko parallel check karein
    db_primary_ok, db_fallback_ok, neon_ok, redis_ok = await asyncio.gather(
        safe_db_call(db_primary.is_ready(), default=False),
        safe_db_call(db_fallback.is_ready(), default=False),
        safe_db_call(db_neon.is_ready(), default=False),
        redis_cache.is_ready() # Redis check
    )

    status_code = 200
    status_msg = "ok"
    
    # ... existing DB status logic ...
    
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
    redis_cache: RedisCacheLayer # NAYA PARAMETER
) -> bool:
    user = message_or_callback.from_user
    if not user: return True
    
    target_chat_id = None
    if isinstance(message_or_callback, types.Message):
        target_chat_id = message_or_callback.chat.id
    elif isinstance(message_or_callback, types.CallbackQuery) and message_or_callback.message:
        target_chat_id = message_or_callback.message.chat.id
    
    # Redis hook ke saath add_user call (database.py mein updated)
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    if user.id == ADMIN_USER_ID: 
        return True
        
    # Ab get_concurrent_user_count pehle Redis check karega, phir Mongo
    active = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    if active >= CURRENT_CONC_LIMIT:
# ... existing overflow logic ...
        logger.warning(f"Capacity full: {active}/{CURRENT_CONC_LIMIT}. User {user.id} ki request hold par।")
        
        # Yahaan par, agar yeh high priority queue task nahi hai, toh ek message bhej de
        is_high_priority = (
             isinstance(message_or_callback, types.Message) and 
             message_or_callback.text and 
             message_or_callback.text.startswith('/')
        )

        if not is_high_priority and target_chat_id: # Sirf non-command requests par overflow message dein
            await safe_tg_call(
                bot.send_message(target_chat_id, overflow_message(active), reply_markup=get_full_limit_keyboard()),
                semaphore=TELEGRAM_COPY_SEMAPHORE
            )
        if isinstance(message_or_callback, types.CallbackQuery):
            await safe_tg_call(message_or_callback.answer("Server busy, kripya thodi der baad try karein।", show_alert=True))
        return False
        
    return True

# =======================================================
# +++++ BOT HANDLERS: USER COMMANDS +++++
# =======================================================

# NOTE: Sabhi handlers mein 'redis_cache: RedisCacheLayer' ko add karna hoga

@dp.message(CommandStart())
@handler_timeout(15)
async def start_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
# ... existing code ...
    if user_id == ADMIN_USER_ID:
        # ... existing admin stats fetch tasks ...
        
        # --- NEW: Redis status task ---
        redis_ok_task = redis_cache.is_ready()
        # --- END NEW ---
        
        user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users, redis_ready = await asyncio.gather(
            user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task, redis_ok_task
        )
        
        # ... existing DB status logic ...
        
        def status_icon(is_ok): return "🟢" if is_ok else "❌"
        # ... existing code ...

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
    if not await ensure_capacity_or_inform(message, db_primary, redis_cache): # redis_cache added
        return
        
# ... existing membership check logic ...


@dp.message(Command("help"))
@handler_timeout(10)
async def help_command(message: types.Message, db_primary: Database, redis_cache: RedisCacheLayer): # redis_cache added
    user = message.from_user
    if not user: return
    # Redis hook ke saath add_user call
    await safe_db_call(db_primary.add_user(user.id, user.username, user.first_name, user.last_name))
    
    # ... existing help text ...


@dp.callback_query(F.data == "check_join")
@handler_timeout(20)
async def check_join_callback(callback: types.CallbackQuery, db_primary: Database, redis_cache: RedisCacheLayer): # redis_cache added
    user = callback.from_user
# ... existing code ...
        
    await safe_tg_call(callback.answer("Checking..."))
    
    if not await ensure_capacity_or_inform(callback, db_primary, redis_cache): # redis_cache added
        return

    is_member = await check_user_membership(user.id)
    
    if is_member:
        # Ab get_concurrent_user_count pehle Redis check karega, phir Mongo
        active_users = await safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
# ... existing code ...

# =======================================================
# +++++ BOT HANDLERS: NAYA HYBRID SEARCH LOGIC +++++
# =======================================================

@dp.message(F.text & ~F.text.startswith("/") & (F.chat.type == "private"))
@handler_timeout(20)
async def search_movie_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer): # redis_cache added
    user = message.from_user
    if not user: return
    user_id = user.id

    if not await ensure_capacity_or_inform(message, db_primary, redis_cache): # redis_cache added
        return
        
# ... existing search logic ...

@dp.callback_query(F.data.startswith("get_"))
@handler_timeout(20)
async def get_movie_callback(callback: types.CallbackQuery, db_primary: Database, db_fallback: Database, redis_cache: RedisCacheLayer): # redis_cache added
    user = callback.from_user
# ... existing code ...
        
    await safe_tg_call(callback.answer("✅ File nikali ja rahi hai..."))
    
    if not await ensure_capacity_or_inform(callback, db_primary, redis_cache): # redis_cache added
        return

# ... existing file retrieval logic ...


# =======================================================
# +++++ BOT HANDLERS: ADMIN MIGRATION (FIXED for clean_title) +++++
# =======================================================

@dp.message(AdminFilter(), F.forward_from_chat)
@handler_timeout(20)
async def migration_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer): # redis_cache added
# ... existing code ...
    db1_res, db2_res, neon_res = await asyncio.gather(db1_task, db2_task, neon_task)
    
    # ... existing code ...
    
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
                    # Seedha Redis par ek chota update set karne ki koshish karein
                    asyncio.create_task(redis_cache.set(f"movie_title_{clean_title_val}", json.dumps(movie_data), ttl=86400))
                # --- END NEW ---
    
    await safe_tg_call(message.answer(f"<b>{title}</b>\nDB1: {db1_status} | DB2: {db2_status} | Neon: {neon_status}"), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.channel_post()
@handler_timeout(20)
async def auto_index_handler(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer): # redis_cache added
# ... existing code ...
    
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
    
    asyncio.create_task(run_tasks())
    
# ... existing code (admin commands) ...

@dp.message(Command("stats"), AdminFilter())
@handler_timeout(15)
async def stats_command(message: types.Message, db_primary: Database, db_fallback: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer): # redis_cache added
    msg = await safe_tg_call(message.answer("📊 System Telemetry fetch ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return

    # ... existing DB tasks ...
    
    # --- NEW: Redis status task ---
    redis_ok_task = redis_cache.is_ready()
    # --- END NEW ---

    user_count_task = safe_db_call(db_primary.get_user_count(), default=-1)
    mongo_1_count_task = safe_db_call(db_primary.get_movie_count(), default=-1)
    mongo_2_count_task = safe_db_call(db_fallback.get_movie_count(), default=-1)
    neon_count_task = safe_db_call(db_neon.get_movie_count(), default=-1)
    concurrent_users_task = safe_db_call(db_primary.get_concurrent_user_count(ACTIVE_WINDOW_MINUTES), default=0)
    
    user_count, mongo_1_count, mongo_2_count, neon_count, concurrent_users, redis_ready = await asyncio.gather(
        user_count_task, mongo_1_count_task, mongo_2_count_task, neon_count_task, concurrent_users_task, redis_ok_task
    )
    
    # ... existing DB ready tasks ...
    
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
        f"  - 🎬 Movies (M1): {count_str(mongo_1_count)}\n"
        f"  - 🗂️ Movies (M2): {count_str(mongo_2_count)}\n"
        f"  - 🗄️ Backup (Neon): {count_str(neon_count)}\n"
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


# ... other admin commands updated with 'redis_cache' in parameters ...

@dp.message(Command("reload_fuzzy_cache"), AdminFilter())
@handler_timeout(300)
async def reload_fuzzy_cache_command(message: types.Message, db_primary: Database, redis_cache: RedisCacheLayer): # redis_cache added
    msg = await safe_tg_call(message.answer("🧠 In-Memory Fuzzy Cache ko Mongo/Redis se reload kiya ja raha hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
    
    # Yeh function ab Redis check karega
    await load_fuzzy_cache(db_primary) 
    await safe_tg_call(msg.edit_text(f"✅ In-Memory Fuzzy Cache {len(fuzzy_movie_cache):,} titles ke saath reload ho gaya।"))

@dp.message(Command("check_db"), AdminFilter())
@handler_timeout(15)
async def check_db_command(message: types.Message, db_primary: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer): # redis_cache added
# ... existing code ...
    
    # --- NEW: Redis Cache ko bhi check karein ---
    redis_status = "❌ NOT READY"
    if redis_cache.is_ready():
        redis_status = "✅ READY"
        
    # ... existing code ...

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
# +++++ ERROR HANDLER (Dependency Injection fix) +++++
# =======================================================

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
# ... existing error handler ...
# NOTE: Is function mein koi DB/Redis dependency injection ki zaroorat nahi hai.

# =======================================================
# +++++ LOCAL POLLING (Testing ke liye) +++++
# =======================================================
async def main_polling():
    logger.info("Bot polling mode mein start ho raha hai (local testing)...")
    try:
        await redis_cache.init_cache() # Redis init
        await db_primary.init_db()
        await db_fallback.init_db()
        await db_neon.init_db()
        await load_fuzzy_cache(db_primary) # Fuzzy cache load karein
    except Exception as init_err:
# ... existing code ...

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
# ... existing code ...
