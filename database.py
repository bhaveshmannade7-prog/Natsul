# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi  # SSL Fix
from bson import ObjectId

logger = logging.getLogger("bot.database")

# clean_text_for_search ki ab zaroorat nahi, Atlas Search yeh khud karta hai.

class Database:
    """MongoDB Atlas ke liye Async Database Interface Class (Atlas Search ke saath)."""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client: AsyncIOMotorClient | None = None
        self.db: Any = None
        self.users: Any = None
        self.movies: Any = None
        self._is_connecting = asyncio.Lock()

    async def _connect(self) -> bool:
        if self.client and await self.is_ready():
            logger.debug("MongoDB connection pehle se active hai.")
            return True

        async with self._is_connecting:
            if self.client and await self.is_ready():
                return True
                
            logger.info(f"MongoDB Atlas (URI: ...{self.database_url[-20:]}) se connect kiya ja raha hai...")
            try:
                ca = certifi.where()
                self.client = AsyncIOMotorClient(
                    self.database_url,
                    serverSelectionTimeoutMS=10000,
                    tls=True,
                    tlsCAFile=ca,
                    maxPoolSize=100,
                    waitQueueTimeoutMS=5000
                )
                await self.client.admin.command('ping')
                
                DATABASE_NAME = "MovieBotDB"
                self.db = self.client[DATABASE_NAME]
                self.users = self.db["users"]
                self.movies = self.db["movies"]
                
                logger.info(f"MongoDB Atlas se connection safal (Database: {self.db.name}).")
                return True
            except (ConnectionFailure, OperationFailure) as e:
                logger.critical(f"MongoDB Atlas se connect nahi ho paya: {e}", exc_info=True)
                self.client = None
                return False
            except Exception as e:
                logger.critical(f"MongoDB connection mein anjaani error: {e}", exc_info=True)
                self.client = None
                return False

    async def init_db(self):
        """
        Database connection banata hai aur zaroori indexes create karta hai.
        (Atlas Search index UI se banaya jaata hai)
        """
        if not await self._connect():
            raise RuntimeError("Database connection fail hua. Bot start nahi ho sakta.")
        
        try:
            logger.info(f"DB ({self.db.name}) indexes banaye ja rahe hain...")
            # User indexes
            await self.users.create_index("user_id", unique=True)
            await self.users.create_index("is_active")
            await self.users.create_index("last_active")
            
            # Movie indexes
            await self.movies.create_index("imdb_id", unique=True)
            await self.movies.create_index("file_unique_id")
            await self.movies.create_index("added_date")
            
            logger.info(f"DB ({self.db.name}) standard indexes create/verify ho gaye.")
            # Atlas Search index ($search) UI se banaya jaata hai.
        except Exception as e:
            logger.error(f"Index creation ke dauran error: {e}", exc_info=True)
            pass

    async def is_ready(self) -> bool:
        if self.client is None or self.db is None:
            return False
        try:
            await self.client.admin.command('ping')
            return True
        except (ConnectionFailure, OperationFailure):
            logger.warning("DB is_ready check failed (ping error).")
            return False
        except Exception:
            return False

    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (ConnectionFailure, asyncio.TimeoutError, OperationFailure)):
             logger.error(f"DB connection error: {type(e).__name__}. Reconnect ki koshish...", exc_info=False)
             if self.client:
                 self.client.close()
             self.client = None
             return True
        elif isinstance(e, DuplicateKeyError):
             logger.warning(f"DB DuplicateKeyError: {e.details}")
             return False
        else:
             logger.error(f"Unhandled DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False

    # --- Search Methods (NAYA ATLAS SEARCH LOGIC) ---

    async def mongo_primary_search(self, query: str, limit: int = 20) -> List[Dict]:
        """
        MongoDB Primary Search (Atlas Search - Autocomplete)
        Yeh "Katra" ko "Kantara" se match karega.
        """
        if not await self.is_ready():
            logger.error("Mongo Primary Search: DB ready nahi hai.")
            return []
            
        pipeline = [
            {
                "$search": {
                    "index": "default",  # Yeh naam UI mein set kiye gaye naam se match hona chahiye
                    "autocomplete": {
                        "query": query,
                        "path": "title",
                        "fuzzy": {
                            "maxEdits": 1,      # 1 typo allow karein
                            "prefixLength": 2   # Pehle 2 অক্ষর sahi hone chahiye
                        }
                    }
                }
            },
            {"$limit": limit},
            {"$project": {"imdb_id": 1, "title": 1, "year": 1, "_id": 0, "score": {"$meta": "searchScore"}}}
        ]
        
        try:
            cursor = self.movies.aggregate(pipeline)
            return [doc async for doc in cursor]
        except OperationFailure as e:
            logger.error(f"Atlas Search (Primary) fail hua: {e}. Kya aapne 'default' index banaya hai?")
            return []
        except Exception as e:
            logger.error(f"mongo_primary_search fail hua '{query}': {e}", exc_info=True)
            await self._handle_db_error(e)
            return []

    async def mongo_fallback_search(self, query: str, limit: int = 20) -> List[Dict]:
        """
        MongoDB Fallback Search (Atlas Search - Text)
        Yeh "avtar" ko "Avatar" se match karega.
        """
        if not await self.is_ready():
            logger.error("Mongo Fallback Search: DB ready nahi hai.")
            return []
            
        pipeline = [
            {
                "$search": {
                    "index": "default", # Yeh naam UI mein set kiye gaye naam se match hona chahiye
                    "text": {
                        "query": query,
                        "path": "title",
                        "fuzzy": {
                            "maxEdits": 2  # 2 typos allow karein (zyada flexible)
                        }
                    }
                }
            },
            {"$limit": limit},
            {"$project": {"imdb_id": 1, "title": 1, "year": 1, "score": {"$meta": "searchScore"}, "_id": 0}},
            {"$sort": {"score": -1}} # Relevance ke hisaab se sort karein
        ]

        try:
            cursor = self.movies.aggregate(pipeline)
            return [doc async for doc in cursor]
        except OperationFailure as e:
            logger.error(f"Atlas Search (Fallback) fail hua: {e}. Kya aapne 'default' index banaya hai?")
            return []
        except Exception as e:
            logger.error(f"mongo_fallback_search fail hua '{query}': {e}", exc_info=True)
            await self._handle_db_error(e)
            return []
    
    # --- User Methods (Yeh sirf db_primary par call honge) ---
    
    async def add_user(self, user_id: int, username: str | None, first_name: str, last_name: str | None):
        if not await self.is_ready(): await self._connect()
        try:
            now = datetime.now(timezone.utc)
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "last_active": now,
                    "is_active": True
                },
                "$setOnInsert": {
                    "joined_date": now
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"add_user fail hua {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        if not await self.is_ready(): await self._connect()
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": False}}
            )
            logger.info(f"User {user_id} ko deactivate kiya.")
        except Exception as e:
            logger.error(f"deactivate_user fail hua {user_id}: {e}", exc_info=False)
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
            return -1

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            filter_query = {
                "last_active": {"$lt": cutoff},
                "is_active": True
            }
            result = await self.users.update_many(
                filter_query,
                {"$set": {"is_active": False}}
            )
            deactivated_count = result.modified_count
            if deactivated_count > 0:
                logger.info(f"{deactivated_count} inactive users ko deactivate kiya.")
            return deactivated_count
        except Exception as e:
            logger.error(f"cleanup_inactive_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def get_all_users(self) -> List[int]:
        if not await self.is_ready(): await self._connect()
        try:
            users_cursor = self.users.find(
                {"is_active": True},
                {"user_id": 1, "_id": 0}
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
            logger.error(f"get_user_info error {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    # --- Movie Methods (Yeh db_primary aur db_fallback dono par call honge) ---

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
            logger.error(f"get_movie_by_imdb error {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    def _format_movie_doc(self, movie_doc: Dict | None) -> Dict | None:
        if not movie_doc: 
            return None
        return {
            'imdb_id': movie_doc.get("imdb_id"),
            'title': movie_doc.get("title"),
            'year': movie_doc.get("year"),
            'file_id': movie_doc.get("file_id"),
            'channel_id': movie_doc.get("channel_id"),
            'message_id': movie_doc.get("message_id"),
        }

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int, file_unique_id: str, **kwargs) -> Literal[True, "updated", "duplicate", False]:
        # **kwargs ko add kiya taaki agar `clean_title` pass ho toh error na aaye
        if not await self.is_ready(): await self._connect()
        
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            # clean_title ki ab zaroorat nahi
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
            logger.error(f"add_movie fail hua {title} ({imdb_id}): {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            result = await self.movies.delete_many({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def cleanup_mongo_duplicates(self, batch_limit: int = 100) -> Tuple[int, int]:
        if not await self.is_ready(): await self._connect()
        
        pipeline = [
            {"$sort": {"added_date": pymongo.DESCENDING}},
            {"$group": {
                "_id": "$imdb_id", 
                "count": {"$sum": 1},
                "doc_ids_to_delete": {"$push": "$_id"}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$project": {
                "ids_to_delete": {"$slice": ["$doc_ids_to_delete", 1, {"$size": "$doc_ids_to_delete"}]},
                "duplicate_count_found": {"$subtract": ["$count", 1]}
            }}
        ]
        
        total_duplicates_found = 0
        all_ids_to_delete = []
        
        try:
            async for group in self.movies.aggregate(pipeline):
                total_duplicates_found += group['duplicate_count_found']
                all_ids_to_delete.extend(group['ids_to_delete'])
                
                if len(all_ids_to_delete) >= batch_limit:
                    break
            
            if not all_ids_to_delete:
                return (0, 0)
            
            ids_for_this_batch = all_ids_to_delete[:batch_limit]
            
            result = await self.movies.delete_many({"_id": {"$in": ids_for_this_batch}})
            
            deleted_count = result.deleted_count
            logger.info(f"({self.db.name}) {deleted_count} Mongo duplicates (by imdb_id) delete kiye.")
            
            return (deleted_count, total_duplicates_found)
        
        except Exception as e:
            logger.error(f"cleanup_mongo_duplicates error: {e}", exc_info=True)
            await self._handle_db_error(e)
            return (0, 0)

    # rebuild_clean_titles() HATA DIYA GAYA HAI

    async def get_all_movies_for_neon_sync(self) -> List[Dict] | None:
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
                }
            )
            movies = await cursor.to_list(length=None) 
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_neon_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def get_all_movies_for_mongo_sync(self) -> List[Dict] | None:
        if not await self.is_ready(): await self._connect()
        try:
            cursor = self.movies.find({}, {"_id": 0}) 
            movies = await cursor.to_list(length=None)
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_mongo_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def sync_from_mongo_bulk(self, movies_data: List[Dict]) -> int:
        if not await self.is_ready() or not movies_data:
            return 0
        
        bulk_ops = []
        for movie in movies_data:
            if not movie.get("imdb_id"):
                continue
                
            bulk_ops.append(
                pymongo.UpdateOne(
                    {"imdb_id": movie["imdb_id"]},
                    {"$set": movie},
                    upsert=True
                )
            )
            
        if not bulk_ops:
            return 0
            
        try:
            result = await self.movies.bulk_write(bulk_ops, ordered=False)
            total_processed = result.upserted_count + result.modified_count
            logger.info(f"Mongo-to-Mongo Sync ({self.db.name}): {result.upserted_count} added, {result.modified_count} updated.")
            return total_processed
        except Exception as e:
            logger.error(f"sync_from_mongo_bulk error: {e}", exc_info=True)
            await self._handle_db_error(e)
            return 0
