# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone 
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi 

logger = logging.getLogger("bot.database")

# Helper function (fallback ke liye)
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
                self.client = None 
            except Exception as e:
                 logger.error(f"Error pinging database: {e}", exc_info=True)
                 self.client = None 

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
            await self.movies.create_index("file_id", unique=True, partialFilterExpression={"file_id": {"$exists": True}})
            await self.movies.create_index("clean_title") 
            
            # --- NEW: MongoDB Fallback Search ke liye Text Index ---
            try:
                await self.movies.create_index(
                    [("clean_title", pymongo.TEXT), ("title", pymongo.TEXT)], 
                    name="title_search_index",
                    default_language="none" # Stopwords/stemming ko disable karein
                )
                logger.info("MongoDB text search index created/verified.")
            except OperationFailure as text_index_e:
                if "IndexOptionsConflict" in str(text_index_e) or "IndexKeySpecsConflict" in str(text_index_e) or "already exists" in str(text_index_e):
                     logger.warning(f"Text index creation warning (likely harmless): {text_index_e}")
                else:
                    logger.error(f"Failed to create text index: {text_index_e}", exc_info=True)
            # --- END NEW ---

            logger.info("Database indexes created/verified.")
        except OperationFailure as e:
            if "IndexOptionsConflict" in str(e) or "IndexKeySpecsConflict" in str(e) or "already exists" in str(e):
                 logger.warning(f"Index creation warning (likely harmless): {e}")
            else:
                 logger.error(f"Failed to create indexes: {e}", exc_info=True)
                 raise 
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}", exc_info=True)
            raise

    async def _handle_db_error(self, e: Exception) -> bool:
        """Handle connection errors."""
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

    # --- User Methods (No Changes) ---
    async def add_user(self, user_id, username, first_name, last_name):
        if self.users is None: await self._connect()
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
        if self.users is None: await self._connect()
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
        if self.users is None: await self._connect()
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
        if self.users is None: await self._connect()
        try:
            count = await self.users.count_documents({"is_active": True})
            return count
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        if self.users is None: await self._connect()
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
        if self.users is None: await self._connect()
        try:
            users_cursor = self.users.find(
                {"is_active": True},
                {"user_id": 1} # Projection
            )
            return [user["user_id"] async for user in users_cursor]
        except Exception as e:
            logger.error(f"get_all_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        if self.users is None: await self._connect()
        try:
            users_cursor = self.users.find().limit(limit)
            users = []
            async for u in users_cursor:
                users.append({
                    'user_id': u.get("user_id"),
                    'username': u.get("username"),
                    'first_name': u.get("first_name"),
                    'last_name': u.get("last_name"),
                    'joined_date': u.get("joined_date", datetime.min).isoformat(),
                    'last_active': u.get("last_active", datetime.min).isoformat(),
                    'is_active': u.get("is_active", False)
                })
            return users
        except Exception as e:
            logger.error(f"export_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    # --- Movie Methods ---
    async def get_movie_count(self) -> int:
        if self.movies is None: await self._connect()
        try:
            count = await self.movies.count_documents({})
            return count
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        if self.movies is None: await self._connect()
        try:
            movie = await self.movies.find_one({"imdb_id": imdb_id})
            return self._format_movie_doc(movie) if movie else None
        except Exception as e:
            logger.error(f"get_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    def _format_movie_doc(self, movie_doc: Dict) -> Dict:
        """Helper to format MongoDB document to the structure bot.py expects."""
        if not movie_doc: return None
        return {
            'imdb_id': movie_doc.get("imdb_id"),
            'title': movie_doc.get("title"),
            'year': movie_doc.get("year"),
            'file_id': movie_doc.get("file_id"),
            'channel_id': movie_doc.get("channel_id"),
            'message_id': movie_doc.get("message_id"),
        }

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int, clean_title: str) -> Literal[True, "updated", "duplicate", False]:
        if self.movies is None: await self._connect()
        
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            "clean_title": clean_title,
            "year": year,
            "file_id": file_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "added_date": datetime.now(timezone.utc) 
        }

        try:
            existing = await self.movies.find_one({
                "$or": [{"imdb_id": imdb_id}, {"file_id": file_id}]
            })
            
            if existing:
                await self.movies.update_one(
                    {"_id": existing["_id"]},
                    {"$set": movie_doc} 
                )
                return "updated"
            else:
                await self.movies.insert_one(movie_doc)
                return True
        
        except DuplicateKeyError as e:
            logger.warning(f"add_movie DuplicateKeyError: {title} ({imdb_id}/{file_id}). Error: {e.details}")
            try:
                await self.movies.update_one(
                    {"imdb_id": imdb_id},
                    {"$set": movie_doc},
                    upsert=False 
                )
                return "updated" 
            except Exception as ue:
                 logger.error(f"Failed to update after DuplicateKeyError: {ue}")
                 return "duplicate" 
        
        except Exception as e:
            logger.error(f"add_movie failed for {title} ({imdb_id}): {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if self.movies is None: await self._connect()
        try:
            result = await self.movies.delete_one({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        if self.movies is None: await self._connect()
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
        """Gets all movies formatted for Typesense sync."""
        if self.movies is None: await self._connect()
        try:
            cursor = self.movies.find(
                {},
                # Typesense ke schema ke according fields fetch karein
                {"imdb_id": 1, "title": 1, "year": 1, "clean_title": 1, "_id": 0} 
            )
            movies = []
            async for m in cursor:
                title = m.get("title", "N/A")
                movies.append({
                    # 'id': m["imdb_id"], # Typesense client library 'id' ko prefer karti hai
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

    # --- NEW: MongoDB Fallback Search Method ---
    async def mongo_search(self, query: str, limit: int = 20) -> List[Dict] | None:
        """Performs a text search directly on MongoDB."""
        if self.movies is None: await self._connect()
        try:
            # Text search query
            filter_query = {"$text": {"$search": query}}
            # Projection to get text score for sorting
            projection = {"score": {"$meta": "textScore"}}
            # Sort by relevance (text score)
            sort_order = [("score", {"$meta": "textScore"})]

            cursor = self.movies.find(filter_query, projection=projection).sort(sort_order).limit(limit)
            
            results = []
            async for m in cursor:
                results.append({
                    'imdb_id': m["imdb_id"],
                    'title': m.get("title", "Title Missing"),
                    'year': m.get("year")
                })
                
            # Agar text search se kuch na mile, simple regex try karein (slower)
            if not results and len(query) > 3:
                logger.debug(f"Mongo text search failed for '{query}', trying regex...")
                # 'clean_title' par case-insensitive regex search
                regex_query = {"clean_title": {"$regex": re.escape(query), "$options": "i"}}
                cursor_regex = self.movies.find(regex_query).limit(limit)
                async for m_regex in cursor_regex:
                     results.append({
                        'imdb_id': m_regex["imdb_id"],
                        'title': m_regex.get("title", "Title Missing"),
                        'year': m_regex.get("year")
                     })

            return results
        except Exception as e:
            logger.error(f"mongo_search error for '{query}': {e}", exc_info=False)
            await self._handle_db_error(e)
            return []
    # --- END NEW ---

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        if self.movies is None: await self._connect()
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
                    'added_date': m.get("added_date", datetime.min).isoformat()
                })
            return movies
        except Exception as e:
            logger.error(f"export_movies error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []
