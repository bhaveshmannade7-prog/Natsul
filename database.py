# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone # `timezone` import zaroori hai
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi # SSL Fix ke liye import karein

logger = logging.getLogger("bot.database")

# Helper function (fallback ke liye)
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text) # Special characters hatayein
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text) # "season 01" etc hatayein
    text = re.sub(r"\s+", " ", text).strip() # Extra space hatayein
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
        # Check using 'is None'
        if self.client is not None and self.db is not None:
            # Ping to check connection
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
                tls=True, # Explicitly enable TLS/SSL
                tlsCAFile=ca # certifi ka CA bundle use karein
            )

            # Test connection
            await self.client.admin.command('ping')
            logger.info("MongoDB cluster connection successful (ping ok).")

            DATABASE_NAME = "MovieBotDB" 
            self.db = self.client[DATABASE_NAME]
            
            # Check using 'is None'
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
            # Yeh index search ke liye zaroori hai
            await self.movies.create_index("clean_title") 
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
             self.client = None # Force reconnect on next call
             return True
        elif isinstance(e, DuplicateKeyError):
             logger.warning(f"DB DuplicateKeyError: {e.details}")
             return False # Not a connection error
        else:
             logger.error(f"Unhandled DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False

    # --- User Methods ---
    async def add_user(self, user_id, username, first_name, last_name):
        # Check using 'is None'
        if self.users is None: await self._connect()
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "last_active": datetime.now(timezone.utc), # Use timezone-aware datetime
                    "is_active": True
                },
                "$setOnInsert": {
                    "joined_date": datetime.now(timezone.utc) # Use timezone-aware datetime
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        # Check using 'is None'
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
        # Check using 'is None'
        if self.users is None: await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes) # Use timezone-aware datetime
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
        # Check using 'is None'
        if self.users is None: await self._connect()
        try:
            count = await self.users.count_documents({"is_active": True})
            return count
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        # Check using 'is None'
        if self.users is None: await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days) # Use timezone-aware datetime
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
        # Check using 'is None'
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
        # Check using 'is None'
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
        # Check using 'is None'
        if self.movies is None: await self._connect()
        try:
            count = await self.movies.count_documents({})
            return count
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        # Check using 'is None'
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
        # Check using 'is None'
        if self.movies is None: await self._connect()
        
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            "clean_title": clean_title, # `clean_title` ko save karein
            "year": year,
            "file_id": file_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "added_date": datetime.now(timezone.utc) # Use timezone-aware datetime
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
                # Agar duplicate key error aaye, to update karne ki koshish karein
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
        # Check using 'is None'
        if self.movies is None: await self._connect()
        try:
            result = await self.movies.delete_one({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        # Check using 'is None'
        if self.movies is None: await self._connect()
        updated_count, total_count = 0, 0
        try:
            total_count = await self.movies.count_documents({})
            if total_count == 0:
                return (0, 0)

            # Un movies ko dhoondhein jinka `clean_title` missing hai
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
                logger.info(f"rebuild_clean_titles: Found {len(bulk_ops)} movies to update...")
                result = await self.movies.bulk_write(bulk_ops, ordered=False)
                updated_count = result.modified_count
                logger.info(f"rebuild_clean_titles: Bulk updated {updated_count} titles.")
            
            return (updated_count, total_count)
        
        except Exception as e:
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return (updated_count, total_count)

    async def get_all_movies_for_sync(self) -> List[Dict] | None:
        # Check using 'is None'
        if self.movies is None: await self._connect()
        try:
            # --- FIX: `clean_title` ko bhi sync ke liye fetch karein ---
            cursor = self.movies.find(
                {},
                {"imdb_id": 1, "title": 1, "year": 1, "clean_title": 1, "_id": 0} # Projection updated
            )
            movies = []
            async for m in cursor:
                title = m.get("title", "N/A")
                movies.append({
                    'objectID': m["imdb_id"], # Required by Algolia
                    'imdb_id': m["imdb_id"],
                    'title': title,
                    'year': m.get("year"),
                    # `clean_title` ko add karein, agar DB mein na ho to generate karein
                    'clean_title': m.get("clean_title") or clean_text_for_search(title) # Fallback
                })
            # --- END FIX ---
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        # Check using 'is None'
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
