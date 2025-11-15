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
from bson import ObjectId

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
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.users = None
        self.movies = None

    async def _connect(self):
        """Internal method to establish connection and select collections."""
        if self.client is not None and self.db is not None:
            try:
                await self.client.admin.command('ping')
                logger.debug("Database connection re-verified.")
                return True
            except ConnectionFailure:
                logger.warning("Database connection lost. Reconnecting...")
                self.client = None # Force reconnect
            except Exception as e:
                 logger.error(f"Error pinging database: {e}", exc_info=True)
                 self.client = None # Force reconnect

        try:
            logger.info("Attempting to connect to MongoDB Atlas...")
            ca = certifi.where()
            self.client = AsyncIOMotorClient(
                self.database_url, 
                serverSelectionTimeoutMS=10000,
                tls=True,
                tlsCAFile=ca
            )
            await self.client.admin.command('ping')
            logger.info("MongoDB cluster connection successful (ping ok).")
            DATABASE_NAME = "MovieBotDB" 
            self.db = self.client[DATABASE_NAME]
            if self.db is None:
                raise Exception(f"Could not select database: {DATABASE_NAME}")
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            logger.info(f"Connected to MongoDB Atlas, selected database: {self.db.name}")
            return True
        except ConnectionFailure as e:
            logger.critical(f"Failed to connect to MongoDB Atlas: {e}", exc_info=True)
            self.client = None
            return False
        except Exception as e:
            logger.critical(f"An unexpected error occurred during MongoDB connection: {e}", exc_info=True)
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
            # --- SIRF 'clean_title' PAR TEXT INDEX BANAYEIN ---
            # Isse search fast aur focused rahega
            await self.movies.create_index(
                [("clean_title", "text")],
                name="title_text_index",
                default_language="none"
            )
            logger.info("MongoDB text index ('clean_title') created/verified.")
        except OperationFailure as e:
            if "IndexOptionsConflict" in str(e) or "already exists" in str(e):
                 logger.warning(f"MongoDB text index warning (likely harmless): {e}")
            else:
                 logger.error(f"Failed to create text index: {e}", exc_info=True)
                 raise
        except Exception as e:
            logger.error(f"Failed to create text index: {e}", exc_info=True)
            raise

    async def init_db(self):
        """Initialize DB connection and create indexes."""
        if not await self._connect():
            raise RuntimeError("Database connection failed on startup. Bot cannot continue.")
        
        try:
            logger.info("Creating database indexes...")
            # User indexes
            await self.users.create_index("user_id", unique=True)
            await self.users.create_index("is_active")
            await self.users.create_index("last_active")
            
            # Movie indexes
            await self.movies.create_index("imdb_id", unique=True)
            await self.movies.create_index("file_unique_id")
            await self.movies.create_index("clean_title") # Simple index
            await self.movies.create_index("added_date")
            
            # Text search ke liye special index
            await self.create_mongo_text_index()
            
            logger.info("Database indexes created/verified.")
        except Exception as e:
            logger.error(f"Error during index creation: {e}", exc_info=True)
            pass 

    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (ConnectionFailure, asyncio.TimeoutError)):
             logger.error(f"DB connection error detected: {type(e).__name__}. Will try to reconnect.", exc_info=False)
             self.client = None
             return True
        elif isinstance(e, DuplicateKeyError):
             logger.warning(f"DB DuplicateKeyError: {e.details}")
             return False
        else:
             logger.error(f"Unhandled DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False

    # --- YEH HAI AAPKA NAYA EXACT SEARCH LOGIC ---
    async def mongo_primary_search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        MongoDB text search (primary).
        Yeh function 'clean_title' par $text search karega.
        Yeh "Kantara" ya "Avengers" jaisi sahi spelling ke liye hai.
        """
        if not await self.is_ready():
            logger.error("mongo_primary_search: DB not ready.")
            return []
        
        # Query pehle hi 'bot.py' mein clean ho jayegi
        clean_query = query
        if not clean_query:
            return []
            
        try:
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
            
            # --- REGEX FALLBACK HATA DIYA GAYA ---
            # NeonDB ab fuzzy/regex ka kaam karega.
            # Isse Mongo query fast hogi.

            return results
        except Exception as e:
            logger.error(f"mongo_primary_search failed for '{query}': {e}", exc_info=True)
            await self._handle_db_error(e)
            return []
            
    async def mongo_fallback_search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        MongoDB text search (fallback).
        Yeh M1 (primary) ke logic ki duplicate hai.
        """
        return await self.mongo_primary_search(query, limit)
    
    # --- User Methods (Koi change nahi) ---
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
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        if not await self.is_ready(): await self._connect()
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": False}}
            )
            logger.info(f"Deactivated user {user_id}.")
        except Exception as e:
            logger.error(f"deactivate_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            count = await self.users.count_documents({
                "last_active": {"$gte": cutoff},
                "is_active": True
            })
            return count
        except Exception as e:
            logger.error(f"get_concurrent_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 9999 

    async def get_user_count(self) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            count = await self.users.count_documents({"is_active": True})
            return count
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            filter_query = {
                "last_active": {"$lt": cutoff},
                "is_active": True
            }
            count = await self.users.count_documents(filter_query)
            if count > 0:
                result = await self.users.update_many(
                    filter_query,
                    {"$set": {"is_active": False}}
                )
                logger.info(f"Deactivated {result.modified_count} inactive users.")
                return result.modified_count
            return 0
        except Exception as e:
            logger.error(f"cleanup_inactive_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def get_all_users(self) -> List[int]:
        if not await self.is_ready(): await self._connect()
        try:
            users_cursor = self.users.find(
                {"is_active": True},
                {"user_id": 1}
            )
            return [user["user_id"] async for user in users_cursor]
        except Exception as e:
            logger.error(f"get_all_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    async def get_user_info(self, user_id: int) -> Dict | None:
        if not await self.is_ready(): await self._connect()
        try:
            user = await self.users.find_one({"user_id": user_id})
            return user
        except Exception as e:
            logger.error(f"get_user_info error for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    # --- Movie Methods (Koi change nahi) ---
    async def get_movie_count(self) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            count = await self.movies.count_documents({})
            return count
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        if not await self.is_ready(): await self._connect()
        try:
            movie = await self.movies.find_one(
                {"imdb_id": imdb_id},
                sort=[("added_date", pymongo.DESCENDING)]
            )
            return self._format_movie_doc(movie) if movie else None
        except Exception as e:
            logger.error(f"get_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
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
            
            if result.upserted_id:
                return True
            elif result.modified_count > 0:
                return "updated"
            else:
                return "duplicate"
            
        except DuplicateKeyError as e:
            logger.warning(f"add_movie DuplicateKeyError: {title} ({imdb_id}). Error: {e.details}")
            return "duplicate"
        except Exception as e:
            logger.error(f"add_movie failed for {title} ({imdb_id}): {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_json_imports(self) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            filter_query = {"imdb_id": {"$regex": "^json_"}}
            result = await self.movies.delete_many(filter_query)
            logger.info(f"Removed {result.deleted_count} entries from JSON imports.")
            return result.deleted_count
        except Exception as e:
            logger.error(f"remove_json_imports error: {e}", exc_info=True)
            await self._handle_db_error(e)
            return 0

    async def cleanup_mongo_duplicates(self, batch_limit: int = 100) -> Tuple[int, int]:
        if not await self.is_ready(): await self._connect()
        
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
                
                sorted_docs = sorted(
                    group['docs'],
                    key=lambda x: x.get('added_date', datetime.min.replace(tzinfo=timezone.utc)),
                    reverse=True
                )
                
                ids_to_delete.extend([doc['_id'] for doc in sorted_docs[1:]])
                
                if len(ids_to_delete) >= batch_limit:
                    break
            
            if not ids_to_delete:
                return (0, 0)
            
            ids_to_delete = ids_to_delete[:batch_limit]
            
            result = await self.movies.delete_many({"_id": {"$in": ids_to_delete}})
            
            deleted_count = result.deleted_count
            logger.info(f"Successfully deleted {deleted_count} Mongo duplicates (by imdb_id).")
            
            return (deleted_count, duplicates_found_pass)
        
        except Exception as e:
            logger.error(f"cleanup_mongo_duplicates error: {e}", exc_info=True)
            await self._handle_db_error(e)
            return (0, 0)

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        if not await self.is_ready(): await self._connect()
        updated_count, total_count = 0, 0
        try:
            total_count = await self.movies.count_documents({})
            if total_count == 0:
                return (0, 0)
            cursor = self.movies.find(
                {"$or": [{"clean_title": {"$exists": False}}, {"clean_title": ""}, {"clean_title": None}]},
                {"title": 1}
            )
            bulk_ops = []
            async for movie in cursor:
                if "title" in movie and movie["title"]:
                    new_clean_title = clean_title_func(movie["title"])
                    bulk_ops.append(
                        pymongo.UpdateOne(
                            {"_id": movie["_id"]},
                            {"$set": {"clean_title": new_clean_title}}
                        )
                    )
            if bulk_ops:
                result = await self.movies.bulk_write(bulk_ops, ordered=False)
                updated_count = result.modified_count
                logger.info(f"rebuild_clean_titles: Bulk updated {updated_count} titles.")
            return (updated_count, total_count)
        except Exception as e:
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return (updated_count, total_count)

    async def get_all_movies_for_sync(self) -> List[Dict] | None:
        """Typesense sync ke liye data nikalta hai."""
        if not await self.is_ready(): await self._connect()
        try:
            pipeline = [
                {"$sort": {"imdb_id": 1, "added_date": -1}},
                {"$group": {
                    "_id": "$imdb_id",
                    "doc": {"$first": "$$ROOT"}
                }},
                {"$replaceRoot": {"newRoot": "$doc"}},
                {"$project": {
                    "imdb_id": 1, "title": 1, "year": 1, "clean_title": 1, "_id": 0
                }}
            ]
            
            movies = []
            async for m in self.movies.aggregate(pipeline):
                title = m.get("title", "N/A")
                movies.append({
                    'imdb_id': m["imdb_id"],
                    'title': title,
                    'year': m.get("year"),
                    'clean_title': m.get("clean_title") or clean_text_for_search(title)
                })
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def get_all_movies_for_neon_sync(self) -> List[Dict] | None:
        """NeonDB sync ke liye MongoDB se data nikalta hai."""
        if not await self.is_ready(): await self._connect()
        try:
            cursor = self.movies.find(
                {}, 
                {
                    "message_id": 1, 
                    "channel_id": 1, 
                    "file_id": 1, 
                    "file_unique_id": 1, 
                    "imdb_id": 1, 
                    "title": 1, 
                    "_id": 0
                    # clean_title ki zaroorat nahi, NeonDB khud banayega
                }
            )
            movies = await cursor.to_list(length=None)
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_neon_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        if not await self.is_ready(): await self._connect()
        try:
            cursor = self.movies.find().limit(limit)
            movies = []
            async for m in cursor:
                movies.append({
                    'imdb_id': m.get("imdb_id"),
                    'title': m.get("title"),
                    'year': m.get("year"),
                    'channel_id': m.get("channel_id"),
                    'message_id': m.get("message_id"),
                    'added_date': m.get("added_date", datetime.min.replace(tzinfo=timezone.utc)).isoformat()
                })
            return movies
        except Exception as e:
            logger.error(f"export_movies error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    # --- NAYA DIAGNOSTIC FUNCTION ---
    async def check_mongo_clean_title(self) -> Dict | None:
        """Checks if clean_title exists in Mongo."""
        if not await self.is_ready(): return None
        try:
            # Find one document that *has* a clean_title
            movie = await self.movies.find_one(
                {"clean_title": {"$exists": True, "$ne": ""}},
                {"title": 1, "clean_title": 1}
            )
            if movie:
                return {"title": movie.get("title"), "clean_title": movie.get("clean_title")}
            
            # If none found, find one that *doesn't*
            movie_bad = await self.movies.find_one(
                {"$or": [{"clean_title": {"$exists": False}}, {"clean_title": ""}]},
                {"title": 1}
            )
            if movie_bad:
                return {"title": movie_bad.get("title"), "clean_title": "--- KHAALI HAI (Run /rebuild_clean_titles_m1) ---"}
            return {"title": "N/A", "clean_title": "DB Khaali Hai"}
        except Exception as e:
            return {"title": "Error", "clean_title": str(e)}
