# -*- coding: utf-8 -*-
import logging
import re
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
import certifi

logger = logging.getLogger("bot.secondary_db")

def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

class SecondaryDB:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.movies = None

    async def _connect(self):
        """Establishes connection to the Secondary MongoDB."""
        if not self.database_url:
            logger.warning("SECONDARY_DATABASE_URL not set. Secondary DB will not work.")
            return False

        if self.client is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except Exception:
                self.client = None # Reconnect if ping fails

        try:
            logger.info("Connecting to Secondary MongoDB...")
            ca = certifi.where()
            self.client = AsyncIOMotorClient(
                self.database_url,
                serverSelectionTimeoutMS=5000,
                tls=True,
                tlsCAFile=ca
            )
            await self.client.admin.command('ping')
            
            # Database name can be same as main or different. Using 'MovieBotSecondary' to be safe.
            self.db = self.client["MovieBotSecondary"] 
            self.movies = self.db["movies"]
            
            # Create Indexes immediately
            # Text index for search
            await self.movies.create_index([("clean_title", "text"), ("title", "text")])
            # Unique ID index
            await self.movies.create_index("imdb_id", unique=True)
            
            logger.info("Connected to Secondary MongoDB.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Secondary MongoDB: {e}")
            self.client = None
            return False

    async def is_ready(self) -> bool:
        if self.client is None:
            return False
        try:
            await self.client.admin.command('ping')
            return True
        except:
            return False

    async def search_movies(self, query: str, limit: int = 20) -> List[Dict]:
        """Fast text search on Secondary DB (Replacement for Typesense search)."""
        if not await self.is_ready():
            await self._connect()
            if not await self.is_ready():
                return []

        try:
            clean_query = clean_text_for_search(query)
            if not clean_query: return []

            # Priority 1: Text Search (Faster)
            cursor = self.movies.find(
                { "$text": { "$search": clean_query } },
                { "score": { "$meta": "textScore" } }
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)

            results = []
            async for movie in cursor:
                results.append({
                    'imdb_id': movie['imdb_id'],
                    'title': movie['title'],
                    'year': movie.get('year', 'N/A')
                })

            # Priority 2: Regex Fallback (Slower but finds partial matches)
            if not results and len(clean_query) > 2:
                regex_query = re.compile(clean_query, re.IGNORECASE)
                cursor_regex = self.movies.find(
                    {"clean_title": {"$regex": regex_query}}
                ).limit(limit)
                async for movie in cursor_regex:
                    results.append({
                        'imdb_id': movie['imdb_id'],
                        'title': movie['title'],
                        'year': movie.get('year', 'N/A')
                    })
            
            return results
        except Exception as e:
            logger.error(f"Secondary DB Search Error: {e}")
            return []

    async def add_movie(self, movie_data: Dict) -> bool:
        """Adds/Updates a single movie."""
        if not await self.is_ready(): await self._connect()
        try:
            if not movie_data.get('imdb_id'): return False
            
            clean_t = movie_data.get('clean_title')
            if not clean_t and movie_data.get('title'):
                clean_t = clean_text_for_search(movie_data['title'])

            update_data = {
                "imdb_id": movie_data['imdb_id'],
                "title": movie_data.get('title'),
                "year": movie_data.get('year'),
                "clean_title": clean_t,
                "updated_at": datetime.now(timezone.utc)
            }

            await self.movies.update_one(
                {"imdb_id": movie_data['imdb_id']},
                {"$set": update_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Secondary DB Add Error: {e}")
            return False

    async def add_batch_movies(self, movies_list: List[Dict]) -> bool:
        """Adds multiple movies at once (For Import JSON / Sync)."""
        if not await self.is_ready(): await self._connect()
        if not movies_list: return True
        
        try:
            ops = []
            for movie in movies_list:
                if not movie.get('imdb_id'): continue
                clean_t = movie.get('clean_title')
                if not clean_t and movie.get('title'):
                    clean_t = clean_text_for_search(movie['title'])
                
                doc = {
                    "imdb_id": movie['imdb_id'],
                    "title": movie.get('title'),
                    "year": movie.get('year'),
                    "clean_title": clean_t,
                    "updated_at": datetime.now(timezone.utc)
                }
                ops.append(pymongo.UpdateOne(
                    {"imdb_id": movie['imdb_id']},
                    {"$set": doc},
                    upsert=True
                ))
            
            if ops:
                await self.movies.bulk_write(ops, ordered=False)
            return True
        except Exception as e:
            logger.error(f"Secondary DB Batch Error: {e}")
            return False

    async def remove_movie(self, imdb_id: str) -> bool:
        """Removes a movie by IMDB ID."""
        if not await self.is_ready(): await self._connect()
        try:
            res = await self.movies.delete_one({"imdb_id": imdb_id})
            return res.deleted_count > 0
        except Exception as e:
            logger.error(f"Secondary DB Remove Error: {e}")
            return False

    async def get_movie_count(self) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            return await self.movies.count_documents({})
        except:
            return -1

    async def clear_all_data(self):
        """Wipes Secondary DB for fresh sync."""
        if not await self.is_ready(): await self._connect()
        try:
            await self.movies.delete_many({})
            return True
        except:
            return False
