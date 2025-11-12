# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any, Literal, Optional
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import certifi # SSL Fix

logging.basicConfig(level=logging.INFO)

def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

class Database:
    def __init__(self, database_url: str, instance_name: str = "MongoDB"):
        self.database_url = database_url
        self.instance_name = instance_name
        self.client = None
        self.db = None
        self.users = None
        self.movies = None
        self.logger = logging.getLogger(f"bot.db.{instance_name}")

    async def _connect(self):
        if self.client is not None: return True
        try:
            # Timeout kam rakha hai taaki failover jaldi ho
            self.client = AsyncIOMotorClient(self.database_url, serverSelectionTimeoutMS=3000, tls=True, tlsCAFile=certifi.where())
            await self.client.admin.command('ping')
            self.db = self.client["MovieBotDB"]
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            self.logger.info(f"Connected successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.client = None
            return False

    async def is_ready(self) -> bool:
        if not self.client: return False
        try:
            await self.client.admin.command('ping')
            return True
        except: return False

    async def init_db(self):
        if await self._connect():
            # Indexes create karein
            try:
                await self.users.create_index("user_id", unique=True)
                await self.users.create_index("last_active")
                await self.movies.create_index("imdb_id", unique=True)
                await self.movies.create_index("clean_title")
                await self.movies.create_index("added_date")
                # Text Index for search
                await self.movies.create_index([("clean_title", "text"), ("title", "text")], name="title_text_index")
            except Exception as e:
                self.logger.warning(f"Index creation warning: {e}")

    # --- Search Logic ---
    async def search(self, query: str, limit: int = 20) -> List[Dict]:
        if not await self.is_ready(): return []
        try:
            clean_query = clean_text_for_search(query)
            results = []
            
            # 1. Text Search (Fastest)
            cursor = self.movies.find(
                { "$text": { "$search": clean_query } },
                { "score": { "$meta": "textScore" } }
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)
            
            async for m in cursor:
                results.append(self._format_movie_doc(m))
            
            # 2. Regex Fallback (Agar text search se kam result mile)
            if len(results) < 5:
                regex = re.compile(re.escape(clean_query), re.IGNORECASE)
                cursor_regex = self.movies.find({"clean_title": {"$regex": regex}}).limit(limit - len(results))
                async for m in cursor_regex:
                    if not any(r['imdb_id'] == m['imdb_id'] for r in results):
                        results.append(self._format_movie_doc(m))
                        
            return results
        except Exception as e:
            self.logger.error(f"Search error: {e}")
            return []

    # --- Core Methods (Retaining all old functionality) ---
    def _format_movie_doc(self, m):
        return {
            'imdb_id': m.get("imdb_id"),
            'title': m.get("title"),
            'year': m.get("year"),
            'file_id': m.get("file_id"),
            'channel_id': m.get("channel_id"),
            'message_id': m.get("message_id"),
            'file_unique_id': m.get("file_unique_id")
        }

    async def get_movie_by_imdb(self, imdb_id):
        if not await self.is_ready(): await self._connect()
        return self._format_movie_doc(await self.movies.find_one({"imdb_id": imdb_id}))

    async def add_movie(self, imdb_id, title, year, file_id, message_id, channel_id, clean_title, file_unique_id):
        if not await self.is_ready(): await self._connect()
        data = {
            "imdb_id": imdb_id, "title": title, "year": year, "clean_title": clean_title,
            "file_id": file_id, "message_id": message_id, "channel_id": channel_id,
            "file_unique_id": file_unique_id, "added_date": datetime.now(timezone.utc)
        }
        try:
            await self.movies.update_one({"imdb_id": imdb_id}, {"$set": data}, upsert=True)
            return True
        except: return False

    # --- Admin / Stats Methods ---
    async def get_movie_count(self):
        return await self.movies.count_documents({}) if await self.is_ready() else -1
    
    async def get_user_count(self):
        return await self.users.count_documents({}) if await self.is_ready() else 0

    async def add_user(self, user_id, username, first_name, last_name):
        if not await self.is_ready(): await self._connect()
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"username": username, "first_name": first_name, "last_active": datetime.now(timezone.utc), "is_active": True},
             "$setOnInsert": {"joined_date": datetime.now(timezone.utc)}},
            upsert=True
        )

    async def get_all_users(self):
        if not await self.is_ready(): return []
        cursor = self.users.find({"is_active": True}, {"user_id": 1})
        return [u["user_id"] async for u in cursor]

    async def get_concurrent_user_count(self, minutes):
        if not await self.is_ready(): return 0
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        return await self.users.count_documents({"last_active": {"$gte": cutoff}})

    async def cleanup_inactive_users(self, days):
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        res = await self.users.update_many({"last_active": {"$lt": cutoff}}, {"$set": {"is_active": False}})
        return res.modified_count

    async def remove_movie_by_imdb(self, imdb_id):
        res = await self.movies.delete_many({"imdb_id": imdb_id})
        return res.deleted_count > 0

    async def get_all_movies_for_neon_sync(self):
        # Sync feature ke liye
        cursor = self.movies.find({})
        return await cursor.to_list(length=None)

    async def cleanup_mongo_duplicates(self, batch_limit=100):
        # Optimized Duplicate Remover
        pipeline = [
            {"$group": {"_id": "$imdb_id", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        deleted = 0
        async for g in self.movies.aggregate(pipeline):
            to_delete = g['ids'][1:] # Keep one, delete rest
            if to_delete:
                await self.movies.delete_many({"_id": {"$in": to_delete}})
                deleted += len(to_delete)
            if deleted >= batch_limit: break
        return deleted, 0
