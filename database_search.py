# database_search.py
# YEH AAPKA DUSRA MONGO DB HAI (DATABASE_URL_SEARCH)
import logging
import re
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Any
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi # SSL Fix

logger = logging.getLogger("bot.database_search")

class DatabaseSearch:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.movies = None

    async def _connect(self):
        """Internal method to establish connection and select collections."""
        if self.client is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except Exception:
                self.client = None

        try:
            logger.info("Attempting to connect to Search MongoDB...")
            
            # --- SSL FIX FOR RENDER ---
            self.client = AsyncIOMotorClient(
                self.database_url, 
                serverSelectionTimeoutMS=5000,
                tlsCAFile=certifi.where()
            )
            
            await self.client.admin.command('ping')
            logger.info("Search MongoDB connected successfully.")
            
            DATABASE_NAME = "MovieBotSearchDB"
            self.db = self.client[DATABASE_NAME]
            self.movies = self.db["movies"]
            return True
        except Exception as e:
            logger.critical(f"Failed to connect to Search DB. CHECK IP WHITELIST (0.0.0.0/0) IN ATLAS! Error: {e}")
            self.client = None
            return False
            
    async def is_ready(self) -> bool:
        if self.client is None: return False
        try:
            await self.client.admin.command('ping')
            return True
        except:
            return False

    async def create_mongo_text_index(self):
        if not await self.is_ready(): await self._connect()
        try:
            await self.movies.create_index(
                [("clean_title", "text"), ("title", "text")],
                name="title_text_index",
                default_language="none"
            )
        except Exception as e:
            logger.warning(f"Search DB Index Warning: {e}")

    async def init_db(self):
        if not await self._connect():
            raise RuntimeError("Search DB Connection Failed. Check MongoDB Atlas Network Access (IP Whitelist).")
        
        try:
            await self.movies.create_index("imdb_id", unique=True)
            await self.movies.create_index("clean_title")
            await self.create_mongo_text_index()
            logger.info("Search DB Indexes Verified.")
        except Exception as e:
            logger.error(f"Search DB Index Error: {e}")

    async def _handle_db_error(self, e: Exception) -> bool:
        logger.error(f"Search DB Error: {e}")
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
            logger.error(f"Search DB Search Error: {e}")
            return []

    async def add_movie(self, imdb_id: str, title: str, year: str | None, clean_title: str) -> bool:
        if not await self.is_ready(): await self._connect()
        movie_doc = {
            "imdb_id": imdb_id, "title": title, "clean_title": clean_title,
            "year": year, "added_date": datetime.now(timezone.utc)
        }
        try:
            await self.movies.update_one({"imdb_id": imdb_id}, {"$set": movie_doc}, upsert=True)
            return True
        except Exception as e:
            logger.error(f"Search DB add_movie failed: {e}")
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception: return False

    async def sync_data(self, all_movies_data: List[Dict]) -> Tuple[bool, int]:
        if not await self.is_ready(): return False, 0
        try:
            await self.movies.drop()
            await self.create_mongo_text_index()
            
            if not all_movies_data: return True, 0
            
            bulk_ops = []
            for item in all_movies_data:
                movie_doc = {
                    "imdb_id": item['imdb_id'], "title": item['title'],
                    "clean_title": item.get('clean_title'), "year": item.get('year'),
                    "added_date": datetime.now(timezone.utc)
                }
                bulk_ops.append(pymongo.UpdateOne({"imdb_id": item['imdb_id']}, {"$set": movie_doc}, upsert=True))

            if bulk_ops:
                await self.movies.bulk_write(bulk_ops, ordered=False)
            return True, len(bulk_ops)
        except Exception as e:
            logger.error(f"Search DB sync failed: {e}")
            return False, 0
