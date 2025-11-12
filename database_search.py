# database_search.py
# YEH AAPKA DUSRA MONGO DB HAI (DATABASE_URL_SEARCH)
# Yeh sirf search ke liye use hoga (title, year, imdb_id).
# Iske liye .env mein DATABASE_URL_SEARCH set karein.

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

# Helper function
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

class DatabaseSearch:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.movies = None

    async def _connect(self):
        """Internal method to establish connection and select collections."""
        if self.client is not None and self.db is not None:
            try:
                await self.client.admin.command('ping')
                logger.debug("Search DB connection re-verified.")
                return True
            except ConnectionFailure:
                logger.warning("Search DB connection lost. Reconnecting...")
                self.client = None # Force reconnect
            except Exception as e:
                 logger.error(f"Error pinging Search DB: {e}", exc_info=True)
                 self.client = None # Force reconnect

        try:
            logger.info("Attempting to connect to Search MongoDB...")
            ca = certifi.where()
            self.client = AsyncIOMotorClient(
                self.database_url, 
                serverSelectionTimeoutMS=10000,
                tls=True,
                tlsCAFile=ca
            )
            await self.client.admin.command('ping')
            logger.info("Search MongoDB cluster connection successful (ping ok).")
            DATABASE_NAME = "MovieBotSearchDB" # Search DB ka naam alag rakhein
            self.db = self.client[DATABASE_NAME]
            if self.db is None:
                raise Exception(f"Could not select search database: {DATABASE_NAME}")
            self.movies = self.db["movies"]
            logger.info(f"Connected to Search MongoDB, selected database: {self.db.name}")
            return True
        except ConnectionFailure as e:
            logger.critical(f"Failed to connect to Search MongoDB: {e}", exc_info=True)
            self.client = None
            return False
        except Exception as e:
            logger.critical(f"An unexpected error occurred during Search MongoDB connection: {e}", exc_info=True)
            self.client = None
            return False
            
    async def is_ready(self) -> bool:
        """Checks if the connection is active."""
        if self.client is None or self.db is None:
            return False
        try:
            await self.client.admin.command('ping')
            return True
        except:
            return False

    async def create_mongo_text_index(self):
        """MongoDB text search ke liye index banata hai."""
        if not await self.is_ready(): await self._connect()
        try:
            await self.movies.create_index(
                [("clean_title", "text"), ("title", "text")],
                name="title_text_index",
                default_language="none"
            )
            logger.info("Search MongoDB text index created/verified.")
        except OperationFailure as e:
            if "IndexOptionsConflict" in str(e) or "already exists" in str(e):
                 logger.warning(f"Search MongoDB text index warning (likely harmless): {e}")
            else:
                 logger.error(f"Failed to create text index for Search DB: {e}", exc_info=True)
                 raise
        except Exception as e:
            logger.error(f"Failed to create text index for Search DB: {e}", exc_info=True)
            raise

    async def init_db(self):
        """Initialize DB connection and create indexes."""
        if not await self._connect():
            raise RuntimeError("Search DB connection failed on startup. Bot cannot continue.")
        
        try:
            logger.info("Creating search database indexes...")
            # Movie indexes
            await self.movies.create_index("imdb_id", unique=True)
            await self.movies.create_index("clean_title")
            
            await self.create_mongo_text_index()
            
            logger.info("Search Database indexes created/verified.")
        except Exception as e:
            logger.error(f"Error during search index creation: {e}", exc_info=True)
            pass 

    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (ConnectionFailure, asyncio.TimeoutError)):
             logger.error(f"Search DB connection error detected: {type(e).__name__}. Will try to reconnect.", exc_info=False)
             self.client = None
             return True
        elif isinstance(e, DuplicateKeyError):
             logger.warning(f"Search DB DuplicateKeyError: {e.details}")
             return False
        else:
             logger.error(f"Unhandled Search DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False

    async def mongo_search_internal(self, query: str, limit: int = 20) -> List[Dict]:
        """Search MongoDB text search."""
        if not await self.is_ready():
            logger.error("mongo_search_internal (Search DB): DB not ready.")
            return []
        try:
            clean_query = re.sub(r"[^a-z0-9\s]+", " ", query.lower()).strip()
            if not clean_query:
                return []
                
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
                logger.debug(f"Search Mongo text search failed for '{clean_query}', trying regex...")
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
            logger.error(f"mongo_search_internal (Search DB) failed for '{query}': {e}", exc_info=True)
            await self._handle_db_error(e)
            return []

    async def add_movie(self, imdb_id: str, title: str, year: str | None, clean_title: str) -> bool:
        """Search DB mein movie add/update karta hai (sirf search data)."""
        if not await self.is_ready(): await self._connect()
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            "clean_title": clean_title,
            "year": year,
            "added_date": datetime.now(timezone.utc)
        }
        try:
            await self.movies.update_one(
                {"imdb_id": imdb_id},
                {"$set": movie_doc},
                upsert=True
            )
            return True
        except DuplicateKeyError:
            logger.warning(f"Search DB add_movie DuplicateKeyError: {title} ({imdb_id}).")
            return False
        except Exception as e:
            logger.error(f"Search DB add_movie failed for {title} ({imdb_id}): {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Search DB remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def sync_data(self, all_movies_data: List[Dict]) -> Tuple[bool, int]:
        """
        Poore Main DB ko Search DB se sync karta hai.
        Yeh purana data delete karke naya data daalta hai.
        """
        if not await self.is_ready():
            logger.error("Search DB not ready for sync.")
            return False, 0
        
        count = len(all_movies_data)
        
        try:
            # 1. Purana collection delete karein
            logger.info(f"Search DB Sync: Deleting old 'movies' collection...")
            await self.movies.drop()
            logger.info("Search DB Sync: Old collection dropped.")
            
            # 2. Collection aur Index dobara banayein
            await self.create_mongo_text_index()
            
            # 3. Naya data batch mein import karein
            if not all_movies_data:
                logger.info("Search DB Sync: No data from Main DB, empty collection created.")
                return True, 0
                
            logger.info(f"Search DB Sync: Importing {count:,} documents...")
            
            # Data ko bulk operations ke liye prepare karein
            bulk_ops = []
            for item in all_movies_data:
                # 'id' field ki zaroorat nahi, seedha upsert use karenge
                movie_doc = {
                    "imdb_id": item['imdb_id'],
                    "title": item['title'],
                    "clean_title": item.get('clean_title') or clean_text_for_search(item['title']),
                    "year": item.get('year'),
                    "added_date": datetime.now(timezone.utc)
                }
                bulk_ops.append(
                    pymongo.UpdateOne(
                        {"imdb_id": item['imdb_id']},
                        {"$set": movie_doc},
                        upsert=True
                    )
                )

            if bulk_ops:
                await self.movies.bulk_write(bulk_ops, ordered=False)
                logger.info(f"Search DB Sync: Bulk import of {len(bulk_ops)} items complete.")
            
            return True, len(bulk_ops)

        except Exception as e:
            logger.error(f"Search DB sync failed: {e}", exc_info=True)
            return False, 0
