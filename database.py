# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi # SSL Fix

logger = logging.getLogger("bot.database")

# Helper function
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

class Database:
    def __init__(self, database_url: str, db_name_suffix: str = ""):
        self.database_url = database_url
        self.db_name_suffix = db_name_suffix
        self.client = None
        self.db = None
        self.users = None
        self.movies = None

    async def _connect(self):
        """Internal method to establish connection and select collections."""
        if self.client is not None and self.db is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except ConnectionFailure:
                logger.warning(f"Database {self.db_name_suffix} connection lost. Reconnecting...")
                self.client = None # Force reconnect
            except Exception as e:
                 logger.error(f"Error pinging database {self.db_name_suffix}: {e}", exc_info=False)
                 self.client = None # Force reconnect

        try:
            logger.info(f"Attempting to connect to MongoDB Atlas ({self.db_name_suffix})...")
            ca = certifi.where()
            self.client = AsyncIOMotorClient(
                self.database_url, 
                serverSelectionTimeoutMS=5000, # Faster failover
                tls=True,
                tlsCAFile=ca
            )
            await self.client.admin.command('ping')
            
            DATABASE_NAME = "MovieBotDB"
            self.db = self.client[DATABASE_NAME]
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            logger.info(f"Connected to MongoDB ({self.db_name_suffix}).")
            return True
        except Exception as e:
            logger.critical(f"Failed to connect to MongoDB ({self.db_name_suffix}): {e}", exc_info=True)
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
            logger.info(f"MongoDB ({self.db_name_suffix}) text index created/verified.")
        except Exception as e:
            logger.warning(f"MongoDB ({self.db_name_suffix}) index warning: {e}")

    async def init_db(self):
        """Initialize DB connection and create indexes."""
        if not await self._connect():
            logger.error(f"Database {self.db_name_suffix} failed to connect on init.")
            return False # Don't raise error, allow bot to start with other DBs
        
        try:
            # User indexes
            await self.users.create_index("user_id", unique=True)
            await self.users.create_index("is_active")
            await self.users.create_index("last_active")
            
            # Movie indexes
            await self.movies.create_index("imdb_id", unique=True)
            await self.movies.create_index("file_unique_id")
            await self.movies.create_index("clean_title")
            await self.movies.create_index("added_date")
            
            await self.create_mongo_text_index()
            return True
        except Exception as e:
            logger.error(f"Error during index creation ({self.db_name_suffix}): {e}", exc_info=True)
            return False

    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (ConnectionFailure, asyncio.TimeoutError)):
             logger.error(f"DB {self.db_name_suffix} connection error. Will try to reconnect.", exc_info=False)
             self.client = None
             return True
        elif isinstance(e, DuplicateKeyError):
             return False
        else:
             logger.error(f"DB {self.db_name_suffix} Exception: {e}", exc_info=True)
             return False

    async def mongo_search_internal(self, query: str, limit: int = 20) -> List[Dict]:
        """MongoDB text search."""
        if not await self.is_ready(): return []
        try:
            clean_query = re.sub(r"[^a-z0-9\s]+", " ", query.lower()).strip()
            if not clean_query: return []
                
            # Text Search
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
            
            # Fallback Regex Search
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
            logger.error(f"mongo_search ({self.db_name_suffix}) failed: {e}")
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
        except Exception as e:
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        if not await self.is_ready(): await self._connect()
        try:
            await self.users.update_one({"user_id": user_id}, {"$set": {"is_active": False}})
        except Exception as e:
            await self._handle_db_error(e)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        if not await self.is_ready(): return 9999
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            count = await self.users.count_documents({
                "last_active": {"$gte": cutoff},
                "is_active": True
            })
            return count
        except Exception as e:
            await self._handle_db_error(e)
            return 9999 

    async def get_user_count(self) -> int:
        if not await self.is_ready(): return 0
        try:
            count = await self.users.count_documents({"is_active": True})
            return count
        except Exception as e:
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        if not await self.is_ready(): return 0
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            result = await self.users.update_many(
                {"last_active": {"$lt": cutoff}, "is_active": True},
                {"$set": {"is_active": False}}
            )
            return result.modified_count
        except Exception as e:
            await self._handle_db_error(e)
            return 0

    async def get_all_users(self) -> List[int]:
        if not await self.is_ready(): return []
        try:
            users_cursor = self.users.find({"is_active": True}, {"user_id": 1})
            return [user["user_id"] async for user in users_cursor]
        except Exception as e:
            await self._handle_db_error(e)
            return []

    async def get_user_info(self, user_id: int) -> Dict | None:
        if not await self.is_ready(): return None
        try:
            return await self.users.find_one({"user_id": user_id})
        except Exception as e:
            await self._handle_db_error(e)
            return None

    # --- Movie Methods ---
    async def get_movie_count(self) -> int:
        if not await self.is_ready(): return -1
        try:
            return await self.movies.count_documents({})
        except Exception as e:
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        if not await self.is_ready(): await self._connect()
        try:
            movie = await self.movies.find_one({"imdb_id": imdb_id})
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
            result = await self.movies.update_one(
                {"imdb_id": imdb_id},
                {"$set": movie_doc},
                upsert=True
            )
            
            if result.upserted_id: return True
            elif result.modified_count > 0: return "updated"
            else: return "duplicate"
            
        except DuplicateKeyError:
            return "duplicate"
        except Exception as e:
            logger.error(f"add_movie failed ({self.db_name_suffix}): {e}")
            await self._handle_db_error(e)
            return False

    async def add_movie_batch(self, movies_list: List[Dict]) -> int:
        """Bulk insert for syncing secondary DB."""
        if not await self.is_ready() or not movies_list: return 0
        try:
            ops = []
            for m in movies_list:
                ops.append(pymongo.UpdateOne(
                    {"imdb_id": m['imdb_id']},
                    {"$set": m},
                    upsert=True
                ))
            if ops:
                res = await self.movies.bulk_write(ops, ordered=False)
                return res.upserted_count + res.modified_count
            return 0
        except Exception as e:
            logger.error(f"add_movie_batch failed: {e}")
            return 0

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            await self._handle_db_error(e)
            return False

    async def cleanup_mongo_duplicates(self, batch_limit: int = 100) -> Tuple[int, int]:
        if not await self.is_ready(): return (0, 0)
        pipeline = [
            {"$group": {
                "_id": "$imdb_id", 
                "count": {"$sum": 1},
                "docs": {"$push": {"_id": "$_id", "added_date": "$added_date"}}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]
        duplicates_found_pass = 0
        ids_to_delete = []
        try:
            async for group in self.movies.aggregate(pipeline):
                duplicates_found_pass += (group['count'] - 1)
                sorted_docs = sorted(group['docs'], key=lambda x: x.get('added_date', datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
                ids_to_delete.extend([doc['_id'] for doc in sorted_docs[1:]])
                if len(ids_to_delete) >= batch_limit: break
            
            if not ids_to_delete: return (0, 0)
            result = await self.movies.delete_many({"_id": {"$in": ids_to_delete[:batch_limit]}})
            return (result.deleted_count, duplicates_found_pass)
        except Exception as e:
            await self._handle_db_error(e)
            return (0, 0)

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        if not await self.is_ready(): return (0,0)
        updated_count, total_count = 0, 0
        try:
            total_count = await self.movies.count_documents({})
            cursor = self.movies.find({"$or": [{"clean_title": {"$exists": False}}, {"clean_title": ""}]}, {"title": 1})
            bulk_ops = []
            async for movie in cursor:
                if "title" in movie:
                    bulk_ops.append(pymongo.UpdateOne({"_id": movie["_id"]}, {"$set": {"clean_title": clean_title_func(movie["title"])}}))
            if bulk_ops:
                result = await self.movies.bulk_write(bulk_ops, ordered=False)
                updated_count = result.modified_count
            return (updated_count, total_count)
        except Exception:
            return (0, 0)

    async def get_all_movies_raw(self) -> List[Dict]:
        """Used for syncing between DBs."""
        if not await self.is_ready(): return []
        try:
            cursor = self.movies.find({})
            return await cursor.to_list(length=None)
        except Exception:
            return []
