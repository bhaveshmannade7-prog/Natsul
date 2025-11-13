# -*- coding: utf-8 -*-
import logging
import re
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi
from bson import ObjectId

logger = logging.getLogger("bot.database")

class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.users = None
        self.movies = None

    async def _connect(self):
        if self.client is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except: self.client = None

        try:
            self.client = AsyncIOMotorClient(self.database_url, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
            await self.client.admin.command('ping')
            self.db = self.client["MovieBotDB"]
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            logger.info("✅ MongoDB Atlas Connected.")
            return True
        except Exception as e:
            logger.critical(f"❌ MongoDB Connection Failed: {e}")
            return False
            
    async def is_ready(self) -> bool:
        return self.client is not None

    async def init_db(self):
        if not await self._connect():
            # Retry logic could go here, but keeping it simple for init
            pass
        try:
            await self.users.create_index("user_id", unique=True)
            await self.movies.create_index("imdb_id", unique=True)
            await self.create_mongo_text_index()
        except: pass

    async def create_mongo_text_index(self):
        try: await self.movies.create_index([("clean_title", "text"), ("title", "text")])
        except: pass

    # --- UPDATED SEARCH LOGIC FOR TYPOS ---
    async def mongo_search_internal(self, query: str, limit: int = 20) -> List[Dict]:
        if not await self.is_ready(): await self._connect()
        results = []
        try:
            # 1. Regex Search (Used for Web Series Mode & Exact patterns)
            if "|" in query or "(" in query:
                # "Flash.*(season\s*0?1|s0?1)" -> handled by regex
                regex = re.compile(query, re.IGNORECASE)
                cursor = self.movies.find({"title": {"$regex": regex}}).limit(limit)
                async for doc in cursor:
                    results.append(self._fmt(doc))
            
            # 2. Text Search (Standard - Fast)
            if not results:
                cursor = self.movies.find(
                    {"$text": {"$search": query}},
                    {"score": {"$meta": "textScore"}}
                ).sort([("score", {"$meta": "textScore"})]).limit(limit)
                async for doc in cursor:
                    results.append(self._fmt(doc))
            
            # 3. SUPER FUZZY REGEX (For Typos like 'Aveger' -> 'Avengers')
            # If text search failed and it's a simple query (no regex symbols)
            if not results and "|" not in query and len(query) > 2:
                # Creates a regex: A.*v.*e.*g.*e.*r
                # This matches any title containing these characters in this order
                chars = list(query.lower().replace(" ", ""))
                fuzzy_pattern = ".*".join(map(re.escape, chars))
                
                cursor = self.movies.find(
                    {"title": {"$regex": fuzzy_pattern, "$options": "i"}}
                ).limit(5) # Limit to 5 because fuzzy is broad
                async for doc in cursor:
                    results.append(self._fmt(doc))
                    
            return results
        except Exception as e:
            logger.error(f"Mongo Search Error: {e}")
            return []

    def _fmt(self, m):
        return {"imdb_id": m["imdb_id"], "title": m["title"], "year": m.get("year"), "file_id": m.get("file_id"), "channel_id": m.get("channel_id"), "message_id": m.get("message_id")}

    # --- ALL ORIGINAL METHODS RESTORED (Full Code) ---
    async def add_user(self, uid, u, f, l):
        if not await self.is_ready(): await self._connect()
        try:
            await self.users.update_one(
                {"user_id": uid},
                {"$set": {"username": u, "first_name": f, "last_name": l, "last_active": datetime.now(timezone.utc), "is_active": True},
                 "$setOnInsert": {"joined_date": datetime.now(timezone.utc)}},
                upsert=True
            )
        except: pass

    async def get_concurrent_user_count(self, minutes: int) -> int:
        if not await self.is_ready(): return 999
        cut = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        return await self.users.count_documents({"last_active": {"$gte": cut}, "is_active": True})

    async def get_user_count(self):
        if not await self.is_ready(): return 0
        return await self.users.count_documents({"is_active": True})

    async def get_movie_count(self):
        if not await self.is_ready(): return 0
        return await self.movies.count_documents({})

    async def get_movie_by_imdb(self, imdb_id: str):
        if not await self.is_ready(): await self._connect()
        m = await self.movies.find_one({"imdb_id": imdb_id})
        return self._fmt(m) if m else None

    async def add_movie(self, imdb, title, year, fid, mid, cid, clean, fuid):
        if not await self.is_ready(): await self._connect()
        doc = {"imdb_id": imdb, "title": title, "clean_title": clean, "year": year, "file_id": fid, "file_unique_id": fuid, "channel_id": cid, "message_id": mid, "added_date": datetime.now(timezone.utc)}
        try:
            res = await self.movies.update_one({"imdb_id": imdb}, {"$set": doc}, upsert=True)
            return True if res.upserted_id else "updated"
        except DuplicateKeyError: return "duplicate"
        except Exception as e: logger.error(f"Add Movie Error: {e}"); return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): return False
        r = await self.movies.delete_many({"imdb_id": imdb_id})
        return r.deleted_count > 0

    async def get_all_users(self) -> List[int]:
        if not await self.is_ready(): return []
        c = self.users.find({"is_active": True}, {"user_id": 1})
        return [u["user_id"] async for u in c]

    async def deactivate_user(self, uid):
        if not await self.is_ready(): await self._connect()
        await self.users.update_one({"user_id": uid}, {"$set": {"is_active": False}})

    async def cleanup_inactive_users(self, days=30):
        cut = datetime.now(timezone.utc) - timedelta(days=days)
        r = await self.users.update_many({"last_active": {"$lt": cut}}, {"$set": {"is_active": False}})
        return r.modified_count

    async def get_user_info(self, uid):
        if not await self.is_ready(): return None
        return await self.users.find_one({"user_id": uid})

    async def get_all_movies_for_neon_sync(self):
        if not await self.is_ready(): return []
        return await self.movies.find({}, {"_id": 0}).to_list(length=None)
        
    async def get_all_movies_for_sync(self):
        # Alias for sync logic
        return await self.get_all_movies_for_neon_sync()

    async def cleanup_mongo_duplicates(self, batch_limit=100):
        pipeline = [{"$group": {"_id": "$imdb_id", "ids": {"$push": "$_id"}, "count": {"$sum": 1}}}, {"$match": {"count": {"$gt": 1}}}]
        d = 0
        async for g in self.movies.aggregate(pipeline):
            ids = g['ids'][1:]
            if ids:
                await self.movies.delete_many({"_id": {"$in": ids}})
                d += len(ids)
        return d, 0

    async def rebuild_clean_titles(self, func):
        c = self.movies.find({})
        u = 0
        async for m in c:
            if "title" in m:
                await self.movies.update_one({"_id": m["_id"]}, {"$set": {"clean_title": func(m["title"])}})
                u += 1
        return u, 0
