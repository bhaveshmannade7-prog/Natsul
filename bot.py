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
# FIX: Core utilities yahan se import honge
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
# ... (rest of imports)
from aiogram.fsm.storage.memory import MemoryStorage

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# --- Database Imports ---
from database import Database
from neondb import NeonDB

# ADDED IMPORTS FOR FEATURES
from admin_features import admin_router # ADDED
# END ADDED

# ============ LOGGING SETUP ============
# ... (rest of logging setup)
# ... (rest of config)
CURRENT_CONC_LIMIT = DEFAULT_CONCURRENT_LIMIT

# --- NAYA SEARCH LOGIC ---
# ... (rest of search logic)
# ... (rest of config checks)
# ... (rest of functions)
# ... (rest of bot and DB init)
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
    
    # ADDED: Router registration for Admin Features
    dp.include_router(admin_router) # ADDED
    # END ADDED
    
    logger.info("Bot, Dispatcher, aur 3 Database objects (M+M+N) initialize ho gaye.")
    logger.info(f"Multi-Bot Manager mein {len(bot_manager.all_tokens)} tokens configured hain।")
except Exception as e:
# ... (rest of try/except block)
# ... (rest of graceful shutdown)
# ... (rest of lifespan function)
    setup_signal_handlers()
    logger.info("Application startup poora hua. Bot taiyar hai.")
    
    # --- ADDED: Register Global Middleware (Ban Check) ---
    from ban_middleware import BanCheckMiddleware # Naya import
    dp.update.outer_middleware.append(BanCheckMiddleware()) # ADDED
    logger.info("Global BanCheckMiddleware registered.")
    # --- END ADDED ---
    
    yield
    logger.info("Application shutdown sequence shuru ho raha hai...")
    await shutdown_procedure()
# ... (rest of code)
# ... (rest of handler implementations)
# ... (rest of admin commands)
# ... (rest of final admin commands)

@dp.message(Command("check_db"), AdminFilter())
@handler_timeout(15)
async def check_db_command(message: types.Message, db_primary: Database, db_neon: NeonDB, redis_cache: RedisCacheLayer):
    msg = await safe_tg_call(message.answer("🕵️‍♂️ Diagnostics run ki ja rahi hai..."), semaphore=TELEGRAM_COPY_SEMAPHORE)
    if not msg: return
# ... (rest of check_db_command)

@dp.message(Command("search_switch"), AdminFilter())
@handler_timeout(10)
async def search_switch_command(message: types.Message):
    await safe_tg_call(message.answer("ℹ️ Search switch ki ab zaroorat nahi hai.\nBot ab hamesha <b>Hybrid</b> ka istemal karta hai।"), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("broadcast"), AdminFilter())
@handler_timeout(3600)
async def broadcast_command(message: types.Message, db_primary: Database):
# ... (rest of broadcast_command)
# ... (rest of cleanup_users_command)

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
    # --- ADDED: Ban status check ---
    is_banned = await safe_db_call(db_primary.is_user_banned(user_id_to_find), default=False) # ADDED
    user_text += f"\n<b>Is Banned:</b> {'🔴 YES' if is_banned else '🟢 NO'}" # ADDED
    # --- END ADDED ---
    await safe_tg_call(message.answer(user_text), semaphore=TELEGRAM_COPY_SEMAPHORE)


@dp.message(Command("import_json"), AdminFilter())
# ... (rest of import_json_command)
# ... (rest of remove_dead_movie_command)
# ... (rest of cleanup_mongo_1_command)
# ... (rest of cleanup_mongo_2_command)
# ... (rest of remove_library_duplicates_command)
# ... (rest of backup_channel_command)
# ... (rest of sync_mongo_1_to_neon_command)
# ... (rest of sync_mongo_1_to_2_command)
# ... (rest of rebuild_clean_titles_m1_command)
# ... (rest of force_rebuild_m1_command)
# ... (rest of rebuild_clean_titles_m2_command)
# ... (rest of set_limit_command)
# ... (rest of rebuild_neon_vectors_command)
# ... (rest of reload_fuzzy_cache_command)
# ... (rest of check_db_command, error handler, main polling)
