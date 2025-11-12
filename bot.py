# -*- coding: utf-8 -*-
import os
import asyncio
import logging
import re
import io
import json
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

try:
    import uvloop
    uvloop.install()
except: pass

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from fastapi import FastAPI, BackgroundTasks

from database import Database
from neondb import NeonDB

# --- CONFIG ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", "0"))
JOIN_CHANNEL = os.getenv("JOIN_CHANNEL_USERNAME", "").replace("@", "")
JOIN_GROUP = os.getenv("USER_GROUP_USERNAME", "").replace("@", "")

# Multiple Databases
DB_URL_1 = os.getenv("DATABASE_URL")
DB_URL_2 = os.getenv("DATABASE_URL_2", DB_URL_1) # Fallback to 1 if 2 missing
NEON_URL = os.getenv("NEON_DATABASE_URL")

CONC_LIMIT = int(os.getenv("CONCURRENT_LIMIT", "35"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("bot")

# Init Databases
db1 = Database(DB_URL_1, "MongoDB_1")
db2 = Database(DB_URL_2, "MongoDB_2")
neondb = NeonDB(NEON_URL)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing 3-DB Cluster...")
    await asyncio.gather(db1.init_db(), db2.init_db(), neondb.init_db())
    logger.info("Bot Ready.")
    yield
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.post(f"/bot/{BOT_TOKEN}")
async def webhook(update: dict, background_tasks: BackgroundTasks):
    background_tasks.add_task(dp.feed_update, bot=bot, update=types.Update(**update))
    return {"ok": True}

# --- HELPERS ---
async def check_membership(user_id):
    if not JOIN_CHANNEL and not JOIN_GROUP: return True
    try:
        if JOIN_CHANNEL:
            m = await bot.get_chat_member(f"@{JOIN_CHANNEL}", user_id)
            if m.status not in ['member', 'administrator', 'creator']: return False
        if JOIN_GROUP:
            m = await bot.get_chat_member(f"@{JOIN_GROUP}", user_id)
            if m.status not in ['member', 'administrator', 'creator']: return False
        return True
    except: return False

def join_kb():
    btns = []
    if JOIN_CHANNEL: btns.append([InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{JOIN_CHANNEL}")])
    if JOIN_GROUP: btns.append([InlineKeyboardButton(text="👥 Join Group", url=f"https://t.me/{JOIN_GROUP}")])
    if btns: btns.append([InlineKeyboardButton(text="✅ Verified", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

# --- AUTOMATIC SHIFTING SEARCH (Waterfall) ---
async def smart_search(query):
    """Tries DB1 -> DB2 -> NeonDB automatically."""
    # Priority Order
    dbs = [(db1, "Primary"), (db2, "Secondary"), (neondb, "NeonDB")]
    
    for db, name in dbs:
        try:
            # Check if DB is responsive
            if isinstance(db, NeonDB):
                res = await db.neondb_search(query)
            else:
                res = await db.search(query)
            
            if res: return res, name
        except Exception as e:
            logger.warning(f"{name} failed: {e}")
            continue # Shift to next DB
            
    return [], "None"

# --- HANDLERS ---

@dp.message(CommandStart())
async def start(message: types.Message):
    uid = message.from_user.id
    # Save user to both MongoDBs for backup
    asyncio.create_task(db1.add_user(uid, message.from_user.username, message.from_user.first_name, ""))
    asyncio.create_task(db2.add_user(uid, message.from_user.username, message.from_user.first_name, ""))

    if await check_membership(uid):
        await message.answer(
            f"👋 <b>Hello {message.from_user.first_name}!</b>\n\n"
            f"🔍 <b>Smart Search System Online</b>\n"
            f"• <code>MongoDB 1</code> + <code>MongoDB 2</code> + <code>NeonDB</code>\n"
            f"• <b>Zero Downtime:</b> Auto-switching databases.\n\n"
            f"👇 <i>Type a movie name to start:</i>"
        )
    else:
        await message.answer("🔒 <b>Access Denied</b>\nPlease join our channels to use the bot.", reply_markup=join_kb())

@dp.callback_query(F.data == "check_join")
async def verify_join(cb: types.CallbackQuery):
    if await check_membership(cb.from_user.id):
        await cb.message.edit_text("✅ <b>Access Granted!</b>\nYou can now search.")
    else:
        await cb.answer("❌ You haven't joined yet!", show_alert=True)

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_search(message: types.Message):
    # Concurrency Check
    active = await db1.get_concurrent_user_count(5)
    if active > CONC_LIMIT and message.from_user.id != ADMIN_USER_ID:
        return await message.answer("⚠️ Server busy. Please wait 2 mins.")
        
    msg = await message.answer(f"⚡️ Searching for <b>{message.text}</b>...")
    
    results, source = await smart_search(message.text)
    
    if not results:
        return await msg.edit_text(f"❌ No results found via any database.")
        
    kb = []
    for r in results[:15]:
        title = r['title'][:45]
        year = f" ({r['year']})" if r.get('year') else ""
        kb.append([InlineKeyboardButton(text=f"📂 {title}{year}", callback_data=f"get_{r['imdb_id']}")])
        
    await msg.edit_text(f"✅ <b>Found {len(results)} results</b>\n🔎 Source: {source}", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# --- COPY MODE (No Forward Tag) ---
@dp.callback_query(F.data.startswith("get_"))
async def send_movie(cb: types.CallbackQuery):
    imdb = cb.data.split("_")[1]
    
    # Find movie in any DB
    movie = await db1.get_movie_by_imdb(imdb)
    if not movie: movie = await db2.get_movie_by_imdb(imdb)
    
    if not movie:
        return await cb.message.edit_text("❌ Movie not found in DB.")
        
    await cb.answer("📂 Sending File...")
    try:
        # COPY MESSAGE (Main Request: Forwarding mode hata ke Copy mode)
        await bot.copy_message(
            chat_id=cb.from_user.id,
            from_chat_id=movie['channel_id'],
            message_id=movie['message_id'],
            caption=f"🎬 <b>{movie['title']}</b>\n💾 <i>Saved via Bot</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await cb.message.answer(f"❌ Error sending file: {e}")

# --- ALL ADMIN COMMANDS (Retained) ---

@dp.message(Command("stats"), F.from_user.id == ADMIN_USER_ID)
async def stats(m: types.Message):
    c1 = await db1.get_movie_count()
    c2 = await db2.get_movie_count()
    cn = await neondb.get_movie_count()
    u = await db1.get_user_count()
    await m.answer(
        f"📊 <b>System Status</b>\n"
        f"Users: {u}\n"
        f"DB1 (Mongo): {c1} files\n"
        f"DB2 (Mongo): {c2} files\n"
        f"Neon (PG): {cn} files\n"
        f"Mode: Auto-Failover"
    )

@dp.message(Command("broadcast"), F.from_user.id == ADMIN_USER_ID)
async def broadcast(m: types.Message):
    if not m.reply_to_message: return await m.answer("Reply to a message.")
    users = await db1.get_all_users()
    sent = 0
    msg = await m.answer(f"📢 Sending to {len(users)} users...")
    for uid in users:
        try:
            await m.reply_to_message.copy_to(uid)
            sent += 1
            await asyncio.sleep(0.05)
        except: pass
    await msg.edit_text(f"✅ Broadcast sent to {sent} users.")

@dp.message(Command("cleanup_users"), F.from_user.id == ADMIN_USER_ID)
async def clean_users(m: types.Message):
    count = await db1.cleanup_inactive_users(30)
    await m.answer(f"🗑️ Removed {count} inactive users.")

@dp.message(Command("sync_mongo_to_neon"), F.from_user.id == ADMIN_USER_ID)
async def sync_neon(m: types.Message):
    msg = await m.answer("⏳ Syncing Mongo -> Neon...")
    movies = await db1.get_all_movies_for_neon_sync()
    count = await neondb.sync_from_mongo(movies)
    await msg.edit_text(f"✅ Synced {count} movies to NeonDB.")

@dp.message(Command("remove_dead_movie"), F.from_user.id == ADMIN_USER_ID)
async def remove_movie(m: types.Message):
    try:
        imdb = m.text.split()[1]
        await db1.remove_movie_by_imdb(imdb)
        await db2.remove_movie_by_imdb(imdb)
        await m.answer(f"🗑️ Removed {imdb} from both MongoDBs.")
    except: await m.answer("Usage: /remove_dead_movie tt12345")

@dp.message(Command("import_json"), F.from_user.id == ADMIN_USER_ID)
async def import_json(m: types.Message):
    if not m.reply_to_message or not m.reply_to_message.document: return
    f = await bot.get_file(m.reply_to_message.document.file_id)
    data = json.loads((await bot.download_file(f.file_path)).read().decode())
    count = 0
    msg = await m.answer("⏳ Importing...")
    for i, item in enumerate(data):
        title = item.get('title')
        if title:
            fid = item.get('file_id')
            # Add to all 3 DBs
            await db1.add_movie(f"json_{i}", title, None, fid, 0, 0, title.lower(), fid)
            await db2.add_movie(f"json_{i}", title, None, fid, 0, 0, title.lower(), fid)
            count += 1
    await msg.edit_text(f"✅ Imported {count} items.")

# --- AUTO INDEXING ---
@dp.channel_post()
async def auto_index(m: types.Message):
    if m.chat.id != LIBRARY_CHANNEL_ID: return
    if not (m.video or m.document): return
    
    # Extract Info
    cap = m.caption or ""
    title_match = cap.splitlines()[0] if cap else "Untitled"
    imdb_match = re.search(r"tt\d+", cap)
    imdb = imdb_match.group(0) if imdb_match else f"auto_{m.message_id}"
    
    file = m.video or m.document
    
    # Add to ALL DBs (Redundancy)
    await asyncio.gather(
        db1.add_movie(imdb, title_match, None, file.file_id, m.message_id, m.chat.id, title_match.lower(), file.file_unique_id),
        db2.add_movie(imdb, title_match, None, file.file_id, m.message_id, m.chat.id, title_match.lower(), file.file_unique_id),
        neondb.add_movie(m.message_id, m.chat.id, file.file_id, file.file_unique_id, imdb, title_match)
    )
