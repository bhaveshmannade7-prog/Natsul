# -*- coding: utf-8 -*-
import logging
import re
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi

# Logger setup
logger = logging.getLogger("bot.secondary_db")

class SecondaryDB:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.movies = None

    async def _connect(self):
        """Establishes connection to the Secondary MongoDB."""
        if self.client is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except Exception:
                self.client = None # Force reconnect

        try:
            # certifi.where() is used to fix SSL certificate errors on some hosting environments
            self.client = AsyncIOMotorClient(
                self.database_url,
                tlsCAFile=certifi.where(),
                serverSelectionTimeoutMS=5000
            )
            await self.client.admin.command('ping')
            
            # Database Name: SecondaryMovieDB
            self.db = self.client["SecondaryMovieDB"]
            self.movies = self.db["movies"]
            
            logger.info("✅ Connected to Secondary MongoDB.")
            return True
        except Exception as e:
            logger.error(f"❌ Secondary DB Connection Failed: {e}")
            self.client = None
            return False

    async def init_db(self):
        """
        Initializes the database and creates necessary indexes.
        This method is called by bot.py on startup.
        """
        if await self._connect():
            try:
                # Create Unique Index on IMDB ID
                await self.movies.create_index("imdb_id", unique=True)
                
                # Create Index on clean_title for faster regex search
                await self.movies.create_index("clean_title")
                
                # Create Text Index for keyword search
                await self.movies.create_index(
                    [("clean_title", "text"), ("title", "text")],
                    name="sec_text_index"
                )
                logger.info("✅ Secondary DB Indexes created/verified.")
            except Exception as e:
                logger.warning(f"⚠️ Secondary DB Index warning: {e}")

    async def is_ready(self) -> bool:
        """Checks if the database connection is active."""
        if not self.client: return False
        try:
            await self.client.admin.command('ping')
            return True
        except: return False

    async def add_movie(self, imdb_id, title, year, message_id, channel_id, file_id, file_unique_id, clean_title):
        """Adds or updates a movie in the secondary database."""
        if not await self.is_ready(): await self._connect()
        
        doc = {
            "imdb_id": imdb_id,
            "title": title,
            "year": year,
            "message_id": message_id,
            "channel_id": channel_id,
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "clean_title": clean_title,
            "updated_at": datetime.now(timezone.utc)
        }
        
        try:
            await self.movies.update_one(
                {"imdb_id": imdb_id},
                {"$set": doc},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Secondary Add Error ({title}): {e}")
            return False

    async def add_batch_movies(self, movies_list: List[Dict]):
        """Bulk adds movies (used in import_json)."""
        if not await self.is_ready(): await self._connect()
        if not movies_list: return False
        
        from pymongo import UpdateOne
        ops = []
        
        for m in movies_list:
            ops.append(UpdateOne(
                {"imdb_id": m["imdb_id"]},
                {"$set": {
                    "title": m["title"],
                    "year": m["year"],
                    "message_id": m["message_id"],
                    "channel_id": m["channel_id"],
                    "file_id": m["file_id"],
                    "file_unique_id": m["file_unique_id"],
                    "clean_title": m["clean_title"],
                    "updated_at": datetime.now(timezone.utc)
                }},
                upsert=True
            ))
            
        try:
            if ops:
                await self.movies.bulk_write(ops, ordered=False)
            return True
        except Exception as e:
            logger.error(f"Secondary Batch Error: {e}")
            return False

    async def get_movie(self, imdb_id: str) -> Dict | None:
        """Fetches a single movie by IMDB ID."""
        if not await self.is_ready(): await self._connect()
        try:
            return await self.movies.find_one({"imdb_id": imdb_id})
        except Exception as e:
            logger.error(f"Secondary Get Error: {e}")
            return None

    async def search_movies(self, query: str, limit: int = 20) -> List[Dict]:
        """
        Searches for movies using Text Search first, then Regex fallback.
        """
        if not await self.is_ready(): await self._connect()
        results = []
        try:
            # 1. Try Text Search (Best for keywords)
            cursor = self.movies.find(
                {"$text": {"$search": query}},
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)
            
            async for doc in cursor:
                results.append(doc)

            # 2. Fallback to Regex (if few results)
            if len(results) < 5:
                seen_ids = {r['imdb_id'] for r in results}
                regex_query = re.compile(re.escape(query), re.IGNORECASE)
                
                cursor_regex = self.movies.find({
                    "clean_title": {"$regex": regex_query}
                }).limit(limit - len(results))
                
                async for doc in cursor_regex:
                    if doc['imdb_id'] not in seen_ids:
                        results.append(doc)
                        
            return results
        except Exception as e:
            logger.error(f"Secondary Search Error: {e}")
            return []

    def close(self):
        """Closes the connection."""
        if self.client:
            self.client.close()
