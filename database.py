# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure

logger = logging.getLogger("bot.database")

# clean_text_for_search function ab bot.py mein hai
# AUTO_MESSAGE_ID_PLACEHOLDER ab bot.py mein hai

class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.users = None
        self.movies = None

    async def _connect(self):
        """Internal method to establish connection and select collections."""
        if self.client and self.db:
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
            # Set serverSelectionTimeoutMS to 10 seconds
            self.client = AsyncIOMotorClient(self.database_url, serverSelectionTimeoutMS=10000)
            # Test connection
            await self.client.admin.command('ping')
            self.db = self.client.get_default_database() # Get DB from connection string
            if not self.db:
                 # Fallback if default DB is not in URI
                 self.db = self.client["telegram_bot_db"]
                 logger.warning(f"Using fallback DB name: {self.db.name}")
            
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            logger.info(f"Connected to MongoDB Atlas, database: {self.db.name}")
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
            # Text index for fallback search (if needed, though Algolia is primary)
            await self.movies.create_index("clean_title") 
            logger.info("Database indexes created/verified.")
        except OperationFailure as e:
             # Handle "IndexOptionsConflict" or "IndexKeySpecsConflict" (means index exists with different options)
            if "IndexOptionsConflict" in str(e) or "IndexKeySpecsConflict" in str(e) or "already exists" in str(e):
                 logger.warning(f"Index creation warning (likely harmless): {e}")
            else:
                 logger.error(f"Failed to create indexes: {e}", exc_info=True)
                 raise # Re-raise if it's a serious issue
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
        if not self.users: await self._connect()
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "last_active": datetime.utcnow(),
                    "is_active": True
                },
                "$setOnInsert": {
                    "joined_date": datetime.utcnow()
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        if not self.users: await self._connect()
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
        if not self.users: await self._connect()
        try:
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
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
        if not self.users: await self._connect()
        try:
            count = await self.users.count_documents({"is_active": True})
            return count
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        if not self.users: await self._connect()
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            filter_query = {
                "last_active": {"$lt": cutoff},
                "is_active": True
            }
            
            # First, count how many users will be affected
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
        if not self.users: await self._connect()
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
        if not self.users: await self._connect()
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
        if not self.movies: await self._connect()
        try:
            count = await self.movies.count_documents({})
            return count
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        if not self.movies: await self._connect()
        try:
            movie = await self.movies.find_one({"imdb_id": imdb_id})
            return self._format_movie_doc(movie) if movie else None
        except Exception as e:
            logger.error(f"get_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    def _format_movie_doc(self, movie_doc: Dict) -> Dict:
        """Helper to format MongoDB document to the structure bot.py expects."""
        return {
            'imdb_id': movie_doc.get("imdb_id"),
            'title': movie_doc.get("title"),
            'year': movie_doc.get("year"),
            'file_id': movie_doc.get("file_id"),
            'channel_id': movie_doc.get("channel_id"),
            'message_id': movie_doc.get("message_id"),
        }

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int, clean_title: str) -> Literal[True, "updated", "duplicate", False]:
        if not self.movies: await self._connect()
        
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            "clean_title": clean_title,
            "year": year,
            "file_id": file_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "added_date": datetime.utcnow()
        }

        try:
            # Check if imdb_id or file_id already exists
            existing = await self.movies.find_one({
                "$or": [{"imdb_id": imdb_id}, {"file_id": file_id}]
            })
            
            if existing:
                # Update existing movie
                await self.movies.update_one(
                    {"_id": existing["_id"]},
                    {"$set": movie_doc} # Overwrite all fields with new data
                )
                return "updated"
            else:
                # Insert new movie
                await self.movies.insert_one(movie_doc)
                return True
        
        except DuplicateKeyError as e:
            logger.warning(f"add_movie DuplicateKeyError: {title} ({imdb_id}/{file_id}). Error: {e.details}")
            # This might happen in a race condition if index check logic fails
            # Try to update just in case
            try:
                await self.movies.update_one(
                    {"imdb_id": imdb_id},
                    {"$set": movie_doc},
                    upsert=False # Don't upsert if imdb_id doesn't exist
                )
                return "updated" # Was a duplicate, but we updated it
            except Exception as ue:
                 logger.error(f"Failed to update after DuplicateKeyError: {ue}")
                 return "duplicate" # Failed to resolve, report as duplicate
        
        except Exception as e:
            logger.error(f"add_movie failed for {title} ({imdb_id}): {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not self.movies: await self._connect()
        try:
            result = await self.movies.delete_one({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        if not self.movies: await self._connect()
        updated_count, total_count = 0, 0
        try:
            total_count = await self.movies.count_documents({})
            if total_count == 0:
                return (0, 0)

            # Find movies where clean_title is missing or empty
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
        if not self.movies: await self._connect()
        try:
            cursor = self.movies.find(
                {},
                {"imdb_id": 1, "title": 1, "year": 1, "_id": 0} # Projection
            )
            movies = []
            async for m in cursor:
                movies.append({
                    'objectID': m["imdb_id"], # Required by Algolia
                    'imdb_id': m["imdb_id"],
                    'title': m.get("title", "N/A"),
                    'year': m.get("year")
                })
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        if not self.movies: await self._connect()
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
