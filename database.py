# database.py
import logging
import re
import asyncio
import uuid # NEW: For unique tokens/IDs
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any, Literal, Callable
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure, ExecutionTimeout
import certifi # SSL Fix
from bson import ObjectId
import os # Naya import

# --- ADD Redis Import ---
try:
    from redis_cache import redis_cache, RedisCacheLayer
except ImportError:
    class RedisCacheLayer:
        def is_ready(self): return False
    redis_cache = RedisCacheLayer()
# --- END Redis Import ---

logger = logging.getLogger("bot.database")

# Helper function (FUZZY SEARCH ke saath SYNCHRONIZED kiya gaya)
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing (Synchronized with bot.py's safer version)."""
    if not text: return ""
    text = text.lower()
    # Separators like dot, underscore ko space mein badle
    text = re.sub(r"[._\-]+", " ", text)
    # Sirf a-z aur 0-9 rakhein
    text = re.sub(r"[^a-z0-9\s]+", "", text)
    # Season info remove karein
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    # Extra space hatayein
    text = re.sub(r"\s+", " ", text).strip()
    return text

# --- NEW: Function to clean title of unwanted junk ---
def remove_junk_from_title(title: str) -> str:
    """Removes @usernames and t.me/ URLs from a movie title."""
    if not title: return ""
    # 1. Telegram URLs (t.me/example) aur general HTTP links
    cleaned = re.sub(r'https?://\S+|t\.me/\S+', '', title, flags=re.IGNORECASE).strip()
    # 2. @usernames
    cleaned = re.sub(r'@[a-zA-Z0-9_]+', '', cleaned).strip()
    # 3. Multiple spaces to single space
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.client = None
        self.db = None
        self.users = None
        self.movies = None
        self.locks = None # NAYA: Locks collection
        self.bans = None # NAYA: Ban collection
        # --- NEW: COLLECTIONS FOR UPGRADED FEATURES ---
        self.ads = None
        self.shortlink_tokens = None
        self.settings = None 
        self.analytics = None

    async def _connect(self):
        """Internal method to establish connection and select collections।"""
        if self.client is not None and self.db is not None:
            try:
                await self.client.admin.command('ping')
                logger.debug("Database connection re-verified।")
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
            # F.I.X: connection check yahan zaroori hai
            await self.client.admin.command('ping') 
            logger.info("MongoDB cluster connection successful (ping ok)।")
            DATABASE_NAME = "MovieBotDB" 
            self.db = self.client[DATABASE_NAME]
            if self.db is None:
                raise Exception(f"Could not select database: {DATABASE_NAME}")
            self.users = self.db["users"]
            self.movies = self.db["movies"]
            self.locks = self.db["locks"] # NAYA: Locks collection
            self.bans = self.db["bans"] # NAYA: Ban collection initialization
            # --- Initialize New Tables ---
            self.ads = self.db["ads"]
            self.shortlink_tokens = self.db["shortlink_tokens"]
            self.settings = self.db["settings"]
            self.analytics = self.db["analytics"]
            
            logger.info(f"Connected to MongoDB Atlas, selected database: {self.db.name}")
            return True
        except ConnectionFailure as e:
            logger.critical(f"Failed to connect to MongoDB Atlas: {e}", exc_info=True)
            self.client = None
            # F.I.X: Connection fail hone par False return karein
            return False 
        except Exception as e:
            logger.critical(f"An unexpected error occurred during MongoDB connection: {e}", exc_info=True)
            self.client = None
            return False
            
    async def is_ready(self) -> bool:
        """Checks if the connection is active।"""
        if self.client is None or self.db is None:
            return False
        try:
            # The ping command is a low-latency operation.
            await self.client.admin.command('ping')
            return True
        except:
            return False

    async def create_mongo_text_index(self):
        """MongoDB text search ke liye index banata hai।"""
        if not await self.is_ready(): await self._connect()
        try:
            # --- SIRF 'clean_title' PAR TEXT INDEX BANAYEIN ---
            await self.movies.create_index(
                [("clean_title", "text")],
                name="title_text_index",
                default_language="none"
            )
            logger.info("MongoDB text index ('clean_title') created/verified।")
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
        """Initialize DB connection and create indexes।"""
        if not await self._connect():
            # F.I.X: Yahan koi exception raise nahi karna chahiye, _connect() already False return karega
            return False 
        
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

            # NAYA: Lock index
            # Ensure unique locks and TTL for auto-cleanup if a worker dies
            await self.locks.create_index("lock_name", unique=True)
            await self.locks.create_index("expires_at", expireAfterSeconds=0) # TTL index

            # NAYA: Ban index
            await self.bans.create_index("user_id", unique=True)
            
            # --- NEW INDEXES FOR UPGRADES ---
            await self.ads.create_index("ad_id", unique=True)
            await self.shortlink_tokens.create_index("token", unique=True)
            await self.shortlink_tokens.create_index("expiry", expireAfterSeconds=0) # Auto expire tokens
            await self.analytics.create_index("type")

            # Text search ke liye special index
            await self.create_mongo_text_index()
            
            logger.info("Database indexes created/verified।")
            return True # F.I.X: Success hone par True return karein
        except Exception as e:
            logger.critical(f"❌ CRITICAL: Database Index Creation Failed: {e}", exc_info=True)
            # Return True to allow bot start (degraded mode), but logged as CRITICAL
            return True 


    # ==========================================
    # NEW FEATURE A: ADS MANAGEMENT
    # ==========================================
    async def add_ad(self, text, btn_text=None, btn_url=None):
        """Saves a new sponsor ad in the database."""
        ad_id = str(uuid.uuid4())[:8]
        await self.ads.insert_one({
            "ad_id": ad_id,
            "text": text,
            "btn_text": btn_text,
            "btn_url": btn_url,
            "status": True,
            "views": 0,
            "clicks": 0
        })
        return ad_id

    async def get_random_ad(self):
        """Fetches a random active ad for display."""
        cursor = self.ads.aggregate([{"$match": {"status": True}}, {"$sample": {"size": 1}}])
        ads = await cursor.to_list(length=1)
        return ads[0] if ads else None

    async def toggle_ad(self, ad_id):
        """Enables or disables an ad."""
        ad = await self.ads.find_one({"ad_id": ad_id})
        if ad:
            await self.ads.update_one({"ad_id": ad_id}, {"$set": {"status": not ad["status"]}})
            return True
        return False

    async def delete_ad(self, ad_id):
        """Removes an ad from the database."""
        res = await self.ads.delete_one({"ad_id": ad_id})
        return res.deleted_count > 0
        
    # --- TASK FEATURE: CLEAR ALL ADS ---
    async def clear_all_ads(self):
        """Deletes all ads from the collection at once."""
        if not await self.is_ready(): await self._connect()
        result = await self.ads.delete_many({})
        return result.deleted_count

    # ==========================================
    # NEW FEATURE B: SHORTLINK MONETIZATION DB METHODS
    # ==========================================
    async def create_unlock_token(self, user_id, imdb_id):
        """Creates a unique token for shortlink bypass prevention."""
        token = uuid.uuid4().hex
        expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
        await self.shortlink_tokens.insert_one({
            "token": token,
            "user_id": user_id,
            "imdb_id": imdb_id,
            "used": False,
            "expiry": expiry
        })
        return token

    async def verify_unlock_token(self, token, user_id):
        """Verifies and marks token as used."""
        res = await self.shortlink_tokens.find_one_and_update(
            {"token": token, "user_id": user_id, "used": False},
            {"$set": {"used": True}}
        )
        return res # Returns the doc if valid

    async def update_config(self, key, value):
        """Updates global bot settings like shortlink URL."""
        await self.settings.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

    async def get_config(self, key, default=None):
        """Gets global config value."""
        res = await self.settings.find_one({"key": key})
        return res["value"] if res else default

    # ==========================================
    # ANALYTICS TRACKING
    # ==========================================
        # FIX: Updated to match ad_manager signature (user_id and ad_id support)
    async def track_event(self, user_id: int, event_type: str, ad_id: str = None, **kwargs):
        """Non-blocking analytics tracking for Ads and Shortlinks."""
        try:
            update_query = {"$inc": {"count": 1}}
            
            # Agar Ad view/click hai to Ads collection bhi update karein
            if event_type.startswith("ad_") and ad_id:
                 await self.ads.update_one(
                     {"ad_id": ad_id}, 
                     {"$inc": {"views" if "view" in event_type else "clicks": 1}}
                 )
            
            # Global Analytics Update
            await self.analytics.update_one(
                {"type": event_type, "date": datetime.now(timezone.utc).strftime("%Y-%m-%d")},
                update_query,
                upsert=True
            )
        except Exception as e:
            logger.error(f"Analytics Error: {e}")

    # --- NAYE FUNCTIONS: Cross-Process Lock ---
    async def check_if_lock_exists(self, lock_name: str) -> bool:
        """FIX for AttributeError: Checks if a non-expired lock exists। (Used for Webhook setup skip)"""
        if not await self.is_ready(): return False
        try:
            # Check if an unexpired lock exists
            count = await self.locks.count_documents({
                "lock_name": lock_name,
                "expires_at": {"$gt": datetime.now(timezone.utc)}
            })
            return count > 0
        except Exception as e:
            logger.warning(f"Error checking lock existence for {lock_name}: {e}")
            return False

    async def acquire_cross_process_lock(self, lock_name: str, timeout_sec: int) -> bool:
        """MongoDB ka istemaal karke distributed lock acquire karta hai।"""
        if not await self.is_ready(): return False
        
        try:
            now = datetime.now(timezone.utc)
            # Try to insert the lock document
            result = await self.locks.insert_one({
                "lock_name": lock_name,
                "worker_pid": os.getpid(),
                "acquired_at": now,
                "expires_at": now + timedelta(seconds=timeout_sec)
            })
            return result.inserted_id is not None
        except DuplicateKeyError:
            # Lock pehle se exists karta hai, check karein ki kya expired hai
            try:
                # Agar lock expired hai (< now), toh usko update karke acquire karein
                expired_doc = await self.locks.find_one_and_update(
                    {
                        "lock_name": lock_name,
                        "expires_at": {"$lt": datetime.now(timezone.utc)}
                    },
                    {"$set": {"acquired_at": datetime.now(timezone.utc), "expires_at": datetime.now(timezone.utc) + timedelta(seconds=timeout_sec), "worker_pid": os.getpid()}}
                )
                return expired_doc is not None
            except Exception as e:
                logger.warning(f"Lock renewal check failed: {e}")
                return False
        except Exception as e:
            logger.error(f"Error acquiring lock {lock_name}: {e}", exc_info=True)
            return False

    async def release_cross_process_lock(self, lock_name: str) -> bool:
        """Distributed lock release karta hai।"""
        if not await self.is_ready(): return False
        try:
            # Release lock, without checking PID (relying on TTL and acquisition update for robustness)
            result = await self.locks.delete_one({"lock_name": lock_name})
            if result.deleted_count > 0:
                 logger.info(f"MongoDB Lock '{lock_name}' released.")
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error releasing lock {lock_name}: {e}", exc_info=True)
            return False
    # --- END NAYE FUNCTIONS ---
    
    async def _handle_db_error(self, e: Exception) -> bool:
        if isinstance(e, (ConnectionFailure, asyncio.TimeoutError)):
             logger.error(f"DB connection error detected: {type(e).__name__}. Will try to reconnect।", exc_info=False)
             self.client = None
             return True
        elif isinstance(e, DuplicateKeyError):
             logger.warning(f"DB DuplicateKeyError: {e.details}")
             return False
        else:
             logger.error(f"Unhandled DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False

    # --- EXACT SEARCH LOGIC (Mongo) ---
    async def mongo_primary_search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        MongoDB text search (primary)।
        Yeh function 'clean_title' par $text search karega।
        """
        if not await self.is_ready():
            logger.error("mongo_primary_search: DB not ready।")
            return []
        
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
            return results
        except Exception as e:
            logger.error(f"mongo_primary_search failed for '{query}': {e}", exc_info=True)
            await self._handle_db_error(e)
            return []
            
    async def mongo_fallback_search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        MongoDB text search (fallback)।
        Yeh M1 (primary) ke logic ki duplicate hai।
        """
        return await self.mongo_primary_search(query, limit)
    
    # --- User Methods (Redis Wrapper Hooks) ---
    async def add_user(self, user_id, username, first_name, last_name):
        if not await self.is_ready(): await self._connect()
        try:
            # --- HOOK 1: Redis ko bhi update karo (Non-blocking I/O) ---
            if redis_cache.is_ready():
                await redis_cache.update_user_activity(user_id)
            # --- END HOOK 1 ---
            
            # --- ORIGINAL MongoDB update_one logic ---
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
            logger.info(f"Deactivated user {user_id}।")
        except Exception as e:
            logger.error(f"deactivate_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        # --- HOOK 2: Pehle Redis se check karo (Fast, low-latency) ---
        if redis_cache.is_ready():
            redis_count = await redis_cache.get_concurrent_user_count()
            if redis_count is not None:
                return redis_count
        # --- END HOOK 2 (FALLBACK to MongoDB if Redis is down) ---
        
        if not await self.is_ready(): await self._connect()
        try:
            # --- ORIGINAL MongoDB count logic ---
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
                logger.info(f"Deactivated {result.modified_count} inactive users।")
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

    async def get_all_user_details(self) -> List[Dict]:
        """
        NYA FEATURE 1: Exports all active user details for Admin Export.
        """
        if not await self.is_ready(): await self._connect()
        try:
            users_cursor = self.users.find(
                {"is_active": True},
                {"user_id": 1, "username": 1, "first_name": 1, "last_name": 1, "joined_date": 1, "last_active": 1, "_id": 0}
            )
            
            user_list = []
            async for user in users_cursor:
                # Convert datetime objects to string for JSON serialization
                user['joined_date'] = user.get('joined_date', datetime.min.replace(tzinfo=timezone.utc)).isoformat()
                user['last_active'] = user.get('last_active', datetime.min.replace(tzinfo=timezone.utc)).isoformat()
                user_list.append(user)
                
            return user_list
        except Exception as e:
            logger.error(f"get_all_user_details error: {e}", exc_info=False)
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

    # --- Ban Methods (NYA FEATURE 2) ---
    async def is_user_banned(self, user_id: int) -> bool:
        """Checks if a user is currently banned."""
        if not await self.is_ready(): return False
        try:
            count = await self.bans.count_documents({"user_id": user_id})
            return count > 0
        except Exception as e:
            logger.error(f"is_user_banned check failed for {user_id}: {e}", exc_info=False)
            return False

    async def ban_user(self, user_id: int, reason: str | None) -> bool:
        """Bans a user by adding them to the ban list."""
        if not await self.is_ready(): return False
        try:
            result = await self.bans.update_one(
                {"user_id": user_id},
                {"$set": {
                    "banned_at": datetime.now(timezone.utc),
                    "reason": reason or "No reason provided."
                }},
                upsert=True
            )
            return result.upserted_id is not None or result.modified_count > 0
        except Exception as e:
            logger.error(f"ban_user failed for {user_id}: {e}", exc_info=False)
            return False

    async def unban_user(self, user_id: int) -> bool:
        """Removes a user from the ban list."""
        if not await self.is_ready(): return False
        try:
            result = await self.bans.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"unban_user failed for {user_id}: {e}", exc_info=False)
            return False
    # --- End Ban Methods ---

    # --- Movie Methods (File Retrieval) ---
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
            logger.warning(f"add_movie DuplicateKeyError: {title} ({imdb_id})। Error: {e.details}")
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
            logger.info(f"Removed {result.deleted_count} entries from JSON imports।")
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
            # FIX: Prevent OOM on large datasets by allowing disk usage
            async for group in self.movies.aggregate(pipeline, allowDiskUse=True):
                duplicates_found_pass += (group['count'] - 1)
                
                sorted_docs = sorted(
                    group['docs'],
                    key=lambda x: x.get('added_date', datetime.min.replace(tzinfo=timezone.utc)),
                    reverse=True
                )
                
                # Keep the latest one (index 0), delete the rest
                ids_to_delete.extend([doc['_id'] for doc in sorted_docs[1:]])
                
                if len(ids_to_delete) >= batch_limit:
                    break
            
            if not ids_to_delete:
                return (0, 0)
            
            ids_to_delete = ids_to_delete[:batch_limit]
            
            result = await self.movies.delete_many({"_id": {"$in": ids_to_delete}})
            
            deleted_count = result.deleted_count
            logger.info(f"Successfully deleted {deleted_count} Mongo duplicates (by imdb_id)।")
            
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
            # Find documents missing clean_title
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
                logger.info(f"rebuild_clean_titles: Bulk updated {updated_count} titles।")
            return (updated_count, total_count)
        except Exception as e:
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return (updated_count, total_count)
            
    # --- NAYA COMMAND FUNCTION: Clean Titles ---
    async def cleanup_movie_titles(self) -> Tuple[int, int]:
        if not await self.is_ready(): await self._connect()
        updated_count, total_count = 0, 0
        logger.info("Starting movie title cleanup for @usernames/links...")
        
        # Regex to find documents that MIGHT contain unwanted data
        junk_regex = re.compile(r'(@[a-zA-Z0-9_]+|t\.me/|https?://)', re.IGNORECASE)
        
        try:
            total_count = await self.movies.count_documents({})
            if total_count == 0: return (0, 0)
            
            # Filter for documents containing potential junk in the 'title' field
            # Use batching for efficiency on large DBs
            BATCH_SIZE = 500
            cursor = self.movies.find(
                {"title": {"$regex": junk_regex}},
                {"title": 1}
            ).batch_size(BATCH_SIZE)
            
            bulk_ops = []
            processed_cursor_count = 0
            
            async for movie in cursor:
                processed_cursor_count += 1
                
                original_title = movie["title"]
                cleaned_title = remove_junk_from_title(original_title)
                
                if cleaned_title != original_title:
                    # Title has been modified, update both title and clean_title
                    new_clean_title = clean_text_for_search(cleaned_title)
                    bulk_ops.append(
                        pymongo.UpdateOne(
                            {"_id": movie["_id"]},
                            {"$set": {
                                "title": cleaned_title,
                                "clean_title": new_clean_title
                            }}
                        )
                    )
                
                # Batch execution
                if len(bulk_ops) >= BATCH_SIZE:
                    result = await self.movies.bulk_write(bulk_ops, ordered=False)
                    updated_count += result.modified_count
                    bulk_ops = []
                    logger.info(f"cleanup_movie_titles: Batch updated {result.modified_count} titles.")

            # Final batch execute karein
            if bulk_ops:
                result = await self.movies.bulk_write(bulk_ops, ordered=False)
                updated_count += result.modified_count
                logger.info(f"cleanup_movie_titles: Final batch updated {result.modified_count} titles.")
            
            # Title cleanup ke baad, bache hue missing clean_titles ko rebuild karein
            rebuilt, _ = await self.rebuild_clean_titles(clean_text_for_search)
            updated_count += rebuilt
            
            return (updated_count, total_count)
        except Exception as e:
            logger.error(f"cleanup_movie_titles error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return (updated_count, total_count)


    # --- YEH FUNCTION 'get_all_movies_for_sync' SE ALAG HAI ---
    async def get_all_movies_for_neon_sync(self) -> List[Dict] | None:
        """NeonDB sync ke liye MongoDB se data nikalta hai।"""
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

    # --- NAYA FUNCTION: 'rapidfuzz' ke liye data load karega (Redis Hook) ---
    async def get_all_movies_for_fuzzy_cache(self) -> List[Dict]:
        """
        Python in-memory fuzzy search ke liye sabhi unique movie titles load karta hai।
        Pehle Redis check karta hai, phir Mongo se load karke Redis mein save karta hai।
        """
        # --- HOOK 3: Pehle Redis se load karne ki koshish karein ---
        if redis_cache.is_ready():
            cached_data_dict = await redis_cache.load_fuzzy_cache()
            if cached_data_dict:
                # Redis se dictionary aayegi
                return list(cached_data_dict.values())
        # --- END HOOK 3 (FALLBACK to MongoDB) ---
        
        if not await self.is_ready(): 
            return []
        
        try:
            # FIX: Memory Optimized Fetch (No Aggregation)
            # Hum sirf raw data layenge aur Python mein dedup karenge (Faster for Free Tier)
            cursor = self.movies.find(
                {}, 
                {"imdb_id": 1, "title": 1, "year": 1, "clean_title": 1, "_id": 0}
            )
            
            raw_movies = []
            async for m in cursor:
                 raw_movies.append(m)

            # Python Deduplication (Last one stays logic not guaranteed here but safer for RAM)
            # Agar exact 'latest' chahiye to client side sort karein, par fuzzy cache ke liye zaroori nahi
            movies_dict = {}
            for m in raw_movies:
                if not m.get('clean_title'):
                    m['clean_title'] = clean_text_for_search(m.get('title', ''))
                
                # Dictionary key override handles duplicates automatically
                movies_dict[m['imdb_id']] = {
                    'imdb_id': m["imdb_id"],
                    'title': m.get("title", "N/A"),
                    'year': m.get("year"),
                    'clean_title': m.get("clean_title")
                }
            
            movies = list(movies_dict.values())

            # --- HOOK 4: Agar Mongo se load hua, toh Redis mein save karein ---
            if movies and redis_cache.is_ready():
                # Dictionary banana jiske keys 'clean_title' hon
                cache_dict = {m['clean_title']: m for m in movies if m.get('clean_title')}
                asyncio.create_task(redis_cache.save_fuzzy_cache(cache_dict))
            # --- END HOOK 4 ---

            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_fuzzy_cache error: {e}", exc_info=True)
            return []

    # --- NAYA DIAGNOSTIC FUNCTION ---
    async def check_mongo_clean_title(self) -> Dict | None:
        """Checks if clean_title exists in Mongo।"""
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

    #
    # ==================================================
    # +++++ NAYA 'FORCE' FUNCTION (Database ke liye) +++++
    # ==================================================
    #
    async def force_rebuild_all_clean_titles(self, clean_title_func: Callable[[str], str], progress_callback: Callable[[int, int], Any] | None = None) -> Tuple[int, int]:
        """
        ZABARDASTI sabhi 'clean_title' fields ko title se rebuild karta hai।
        Progress callback har batch ke baad chalta hai।
        """
        if not await self.is_ready(): await self._connect()
        updated_count, total_count = 0, 0
        logger.warning("--- FORCE REBUILD (M1) SHURU ---")
        try:
            total_count = await self.movies.count_documents({})
            if total_count == 0:
                return (0, 0)
            
            # Use batching for cursor iteration to avoid memory issues and enable progress tracking
            BATCH_SIZE = 500 
            
            # Cursor timeout set kiya gaya (30 minutes in milliseconds)
            cursor = self.movies.find({}, {"title": 1}).batch_size(BATCH_SIZE).max_time_ms(1800000)
            
            bulk_ops = []
            processed_cursor_count = 0

            # anext() का उपयोग करके सुरक्षित रूप से iterate करें
            try:
                # anext() Python 3.10+ में async iterators के लिए built-in hai
                # agar aapka Python version isse kam hai, toh yeh async for loop ke barabar hi kaam karega.
                while True:
                    movie = await anext(cursor) 
                    processed_cursor_count += 1

                    if "title" in movie and movie["title"]:
                        raw_title = movie["title"] 
                        cleaned_title_for_db = remove_junk_from_title(raw_title) 
                        new_clean_title = clean_title_func(cleaned_title_for_db)
                        
                        update_fields = {"clean_title": new_clean_title}
                        
                        # Check if raw title had junk that was cleaned
                        if cleaned_title_for_db != raw_title:
                             # Title field ko bhi clean kiya gaya title se update kare
                             update_fields["title"] = cleaned_title_for_db
                             
                        bulk_ops.append(
                            pymongo.UpdateOne(
                                {"_id": movie["_id"]},
                                {"$set": update_fields}
                            )
                        )
                    
                    # Batch execution and progress update
                    if len(bulk_ops) >= BATCH_SIZE:
                        logger.info(f"force_rebuild: Executing bulk write for {len(bulk_ops)} operations.")
                        result = await self.movies.bulk_write(bulk_ops, ordered=False)
                        updated_count += result.modified_count
                        bulk_ops = []
                        
                        if progress_callback:
                            # Non-blocking call to the bot handler
                            asyncio.create_task(progress_callback(processed_cursor_count, total_count))
            except StopAsyncIteration:
                pass # Cursor iteration finished
            except ExecutionTimeout as e:
                 logger.error(f"MongoDB Cursor timed out after 30 mins: {e}")
                 # Fall through to final batch execution
            
            # Final batch execute karein
            if bulk_ops:
                logger.info(f"force_rebuild: Executing final bulk write for {len(bulk_ops)} operations.")
                result = await self.movies.bulk_write(bulk_ops, ordered=False)
                updated_count += result.modified_count
            
            # Final progress update (guarantee)
            if progress_callback:
                # Processed count is used as total processed at this point
                await progress_callback(processed_cursor_count, total_count)
            
            logger.info(f"force_rebuild: Bulk updated total {updated_count} titles।")
            
            return (updated_count, total_count)
        except Exception as e:
            logger.error(f"force_rebuild_all_clean_titles error: {e}", exc_info=True)
            await self._handle_db_error(e)
            # Total count ko dobara fetch karein agar error execution ke beech mein aaya
            final_total = await self.movies.count_documents({}) if self.movies else 0
            return (updated_count, final_total)

    async def close(self):
        """Closes the MongoDB connection."""
        if self.client:
            try:
                self.client.close()
                logger.info("MongoDB connection closed.")
            except Exception as e:
                logger.error(f"Error closing MongoDB connection: {e}")
            finally:
                self.client = None
                self.db = None

    # --- FINAL FIX: MongoDB Index Rebuild ---
    async def force_rebuild_text_index(self):
        """Zabaradasti MongoDB Text Index ko drop k करके rebuild karta hai।"""
        if not await self.is_ready(): await self._connect()
        try:
            # Pehle purana index drop karein
            await self.movies.drop_index("title_text_index")
            logger.warning("MongoDB text index 'title_text_index' dropped।")
        except OperationFailure as e:
            if "index not found" in str(e):
                logger.info("MongoDB: Index drop karte waqt index mila nahi (ignore) ।")
            else:
                logger.error(f"Failed to drop text index: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to drop text index: {e}")
            raise
        
        # Ab naya index banayein
        await self.create_mongo_text_index()
        return True
