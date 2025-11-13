# -*- coding: utf-8 -*-
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi 
from bson import ObjectId

logger = logging.getLogger("bot.database")

def clean_text_for_search(text: str) -> str:
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.users = None
        self.movies = None

    async def _connect(self):
        if self.client is not None and self.db is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except ConnectionFailure:
                self.client = None 
            except Exception:
                 self.client = None 

        try:
            logger.info("Connecting to Primary MongoDB...")
            ca = certifi.where()
            self.client = AsyncIOMotorClient(
                self.database_url, 
                serverSelectionTimeoutMS=10000,
                tls=True,
                tlsCAFile=ca
            )
            await self.client.admin.command('ping')
            self.db = self.client["MovieBotDB"]
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            logger.info(f"Connected to Primary MongoDB: {self.db.name}")
            return True
        except Exception as e:
            logger.critical(f"Failed to connect to Primary MongoDB: {e}")
            self.client = None
            return False
            
    async def is_ready(self) -> bool:
        if self.client is None or self.db is None: return False
        try:
            await self.client.admin.command('ping')
            return True
        except: return False

    async def create_mongo_text_index(self):
        if not await self.is_ready(): await self._connect()
        try:
            await self.movies.create_index(
                [("clean_title", "text"), ("title", "text")],
                name="title_text_index",
                default_language="none"
            )
        except Exception: pass

    async def init_db(self):
        if not await self._connect():
            raise RuntimeError("Primary DB Connection Failed.")
        try:
            await self.users.create_index("user_id", unique=True)
            await self.users.create_index("is_active")
            await self.users.create_index("last_active")
            await self.movies.create_index("imdb_id", unique=True)
            await self.movies.create_index("file_unique_id")
            await self.movies.create_index("clean_title")
            await self.movies.create_index("added_date")
            await self.create_mongo_text_index()
            logger.info("Primary DB Indexes Verified.")
        except Exception as e:
            logger.error(f"Index Error: {e}")

    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (ConnectionFailure, asyncio.TimeoutError)):
             self.client = None
             return True
        return False

    async def mongo_search_internal(self, query: str, limit: int = 20) -> List[Dict]:
        if not await self.is_ready(): return []
        try:
            clean_query = re.sub(r"[^a-z0-9\s]+", " ", query.lower()).strip()
            if not clean_query: return []
                
            cursor = self.movies.find(
                { "$text": { "$search": clean_query } },
                { "score": { "$meta": "textScore" } } 
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)

            results = []
            async for movie in cursor:
                results.append({
                    'imdb_id': movie['imdb_id'],
                    'title': movie['title'],
                    'year': movie.get('year')
                })
            
            if not results and len(clean_query) > 2:
                regex_query = re.compile(clean_query, re.IGNORECASE)
                cursor_regex = self.movies.find(
                    {"clean_title": {"$regex": regex_query}}
                ).limit(limit)
                async for movie in cursor_regex:
                    results.append({
                        'imdb_id': movie['imdb_id'],
                        'title': movie['title'],
                        'year': movie.get('year')
                    })

            return results
        except Exception as e:
            await self._handle_db_error(e)
            return []
    
    # --- User Methods ---
    async def add_user(self, user_id, username, first_name, last_name):
        if not await self.is_ready(): await self._connect()
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "last_active": datetime.now(timezone.utc),
                    "is_active": True
                },
                "$setOnInsert": {
                    "joined_date": datetime.now(timezone.utc)
                }},
                upsert=True
            )
        except Exception as e: await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        if not await self.is_ready(): await self._connect()
        try: await self.users.update_one({"user_id": user_id}, {"$set": {"is_active": False}})
        except Exception as e: await self._handle_db_error(e)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            return await self.users.count_documents({"last_active": {"$gte": cutoff}, "is_active": True})
        except Exception as e:
            await self._handle_db_error(e)
            return 9999 

    async def get_user_count(self) -> int:
        if not await self.is_ready(): await self._connect()
        try: return await self.users.count_documents({"is_active": True})
        except Exception: return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            res = await self.users.update_many({"last_active": {"$lt": cutoff}, "is_active": True}, {"$set": {"is_active": False}})
            return res.modified_count
        except Exception: return 0

    async def get_all_users(self) -> List[int]:
        if not await self.is_ready(): await self._connect()
        try:
            return [user["user_id"] async for user in self.users.find({"is_active": True}, {"user_id": 1})]
        except Exception: return []

    async def get_user_info(self, user_id: int) -> Dict | None:
        if not await self.is_ready(): await self._connect()
        try: return await self.users.find_one({"user_id": user_id})
        except Exception: return None

    # --- Movie Methods ---
    async def get_movie_count(self) -> int:
        if not await self.is_ready(): await self._connect()
        try: return await self.movies.count_documents({})
        except Exception: return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        if not await self.is_ready(): await self._connect()
        try:
            movie = await self.movies.find_one({"imdb_id": imdb_id}, sort=[("added_date", pymongo.DESCENDING)])
            return self._format_movie_doc(movie) if movie else None
        except Exception as e:
            await self._handle_db_error(e)
            return None

    def _format_movie_doc(self, movie_doc: Dict) -> Dict:
        if not movie_doc: return None
        return {
            'imdb_id': movie_doc.get("imdb_id"),
            'title': movie_doc.get("title"),
            'year': movie_doc.get("year"),
            'file_id': movie_doc.get("file_id"),
            'channel_id': movie_doc.get("channel_id"),
            'message_id': movie_doc.get("message_id"),
        }

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int, clean_title: str, file_unique_id: str) -> Literal[True, "updated", "duplicate", False]:
        if not await self.is_ready(): await self._connect()
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            "clean_title": clean_title,
            "year": year,
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "added_date": datetime.now(timezone.utc)
        }
        try:
            result = await self.movies.update_one({"imdb_id": imdb_id}, {"$set": movie_doc}, upsert=True)
            if result.upserted_id: return True
            elif result.modified_count > 0: return "updated"
            else: return "duplicate"
        except DuplicateKeyError: return "duplicate"
        except Exception as e:
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception: return False

    async def remove_json_imports(self) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": {"$regex": "^json_"}})
            return result.deleted_count
        except Exception: return 0

    async def cleanup_mongo_duplicates(self, batch_limit: int = 100) -> Tuple[int, int]:
        if not await self.is_ready(): await self._connect()
        pipeline = [
            {"$group": {"_id": "$imdb_id", "count": {"$sum": 1}, "docs": {"$push": {"_id": "$_id", "added_date": "$added_date"}}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        try:
            ids_to_delete = []
            duplicates_found_pass = 0
            async for group in self.movies.aggregate(pipeline):
                duplicates_found_pass += (group['count'] - 1)
                sorted_docs = sorted(group['docs'], key=lambda x: x.get('added_date', datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
                ids_to_delete.extend([doc['_id'] for doc in sorted_docs[1:]])
                if len(ids_to_delete) >= batch_limit: break
            
            if not ids_to_delete: return (0, 0)
            ids_to_delete = ids_to_delete[:batch_limit]
            result = await self.movies.delete_many({"_id": {"$in": ids_to_delete}})
            return (result.deleted_count, duplicates_found_pass)
        except Exception: return (0, 0)

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        if not await self.is_ready(): await self._connect()
        updated_count, total_count = 0, 0
        try:
            total_count = await self.movies.count_documents({})
            cursor = self.movies.find({"$or": [{"clean_title": {"$exists": False}}, {"clean_title": ""}, {"clean_title": None}]}, {"title": 1})
            bulk_ops = []
            async for movie in cursor:
                if "title" in movie and movie["title"]:
                    bulk_ops.append(pymongo.UpdateOne({"_id": movie["_id"]}, {"$set": {"clean_title": clean_title_func(movie["title"])}}))
            if bulk_ops:
                result = await self.movies.bulk_write(bulk_ops, ordered=False)
                updated_count = result.modified_count
            return (updated_count, total_count)
        except Exception: return (updated_count, total_count)

    async def get_all_movies_for_sync(self) -> List[Dict] | None:
        if not await self.is_ready(): await self._connect()
        try:
            pipeline = [
                {"$sort": {"imdb_id": 1, "added_date": -1}},
                {"$group": {"_id": "$imdb_id", "doc": {"$first": "$$ROOT"}}},
                {"$replaceRoot": {"newRoot": "$doc"}},
                {"$project": {"imdb_id": 1, "title": 1, "year": 1, "clean_title": 1, "_id": 0}}
            ]
            return [m async for m in self.movies.aggregate(pipeline)]
        except Exception: return None

    async def get_all_movies_for_neon_sync(self) -> List[Dict] | None:
        if not await self.is_ready(): await self._connect()
        try:
            cursor = self.movies.find({}, {"message_id": 1, "channel_id": 1, "file_id": 1, "file_unique_id": 1, "imdb_id": 1, "title": 1, "year": 1, "_id": 0})
            return await cursor.to_list(length=None)
        except Exception: return None
