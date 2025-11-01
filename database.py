# database.py
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone # <--- FIX: `timezone` import karein
from typing import List, Dict, Tuple, Any, Literal
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
import certifi # SSL Fix ke liye import karein

logger = logging.getLogger("bot.database")

# --- FINAL FIX (v3): Smart clean_text_for_search ---
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing, preserving transliterations."""
    if not text: return ""
    
    # 1. Lowercase
    text = text.lower()
    
    # 2. Remove year patterns like (2023) or [1998]
    text = re.sub(r"[\(\[]\s*(\d{4})\s*[\)\]]", " ", text, flags=re.UNICODE)

    # 3. Remove square brackets and their content [Hindi], [1080p] etc.
    # Yeh aksar non-title tags hote hain.
    text = re.sub(r"\[.*?\]", " ", text, flags=re.UNICODE)
    
    # 4. Remove common keywords (jo ab brackets ke bahar ho sakte hain)
    text = re.sub(r"\b(web-rip|org|hindi|dd 5.1|english|480p|720p|1080p|web-dl|hdrip|bluray|dual audio|hevc)\b", " ", text, flags=re.IGNORECASE | re.UNICODE)

    # 5. Ab, sirf letters (Unicode), numbers, aur spaces rakhein.
    # Yeh bache hue parens '()' aur emojis '🌸' ko hata dega.
    # Yeh (Sakti) ko 'sakti' bana dega.
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    
    # 6. Remove "season" and "s01" type patterns
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text, flags=re.IGNORECASE | re.UNICODE)
    
    # 7. Normalize whitespace
    text = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip()
    
    # logger.debug(f"DB Cleaned text: '{text}'") # Debugging ke liye
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
        # FIX: Check using 'is None'
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
            
            # FIX: Check using 'is None'
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
        # FIX: Check using 'is None'
        if self.users is None: await self._connect()
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "last_active": datetime.now(timezone.utc), # FIX: Use timezone-aware datetime
                    "is_active": True
                },
                "$setOnInsert": {
                    "joined_date": datetime.now(timezone.utc) # FIX: Use timezone-aware datetime
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        # FIX: Check using 'is None'
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
        # FIX: Check using 'is None'
        if self.users is None: await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes) # FIX: Use timezone-aware datetime
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
        # FIX: Check using 'is None'
        if self.users is None: await self._connect()
        try:
            count = await self.users.count_documents({"is_active": True})
            return count
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        # FIX: Check using 'is None'
        if self.users is None: await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days) # FIX: Use timezone-aware datetime
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
        # FIX: Check using 'is None'
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
        # FIX: Check using 'is None'
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
        # FIX: Check using 'is None'
        if self.movies is None: await self._connect()
        try:
            count = await self.movies.count_documents({})
            return count
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        # FIX: Check using 'is None'
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
        # FIX: Check using 'is None'
        if self.movies is None: await self._connect()
        
        movie_doc = {
            "imdb_id": imdb_id,
            "title": title,
            "clean_title": clean_title,
            "year": year,
            "file_id": file_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "added_date": datetime.now(timezone.utc) # FIX: Use timezone-aware datetime
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
                # Agar duplicate hai, toh update karne ki koshish karein (IMDB ID se)
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
        # FIX: Check using 'is None'
        if self.movies is None: await self._connect()
        try:
            result = await self.movies.delete_one({"imdb_id": imdb_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def rebuild_clean_titles(self, clean_title_func) -> Tuple[int, int]:
        """
        Rebuilds clean_title for movies missing it.
        Yeh 'clean_title_func' ko (jo bot.py se aata hai) istemaal karta hai.
        """
        if self.movies is None: await self._connect()
        updated_count, total_count = 0, 0
        try:
            total_count = await self.movies.count_documents({})
            if total_count == 0:
                return (0, 0)

            # Un sabhi movies ko dhoondhein jinka clean_title ya toh hai nahi, ya empty hai
            # YA jinka title () brackets abhi bhi rakhta hai (puraane buggy code se)
            cursor = self.movies.find(
                {"$or": [
                    {"clean_title": {"$exists": False}}, 
                    {"clean_title": ""}, 
                    {"clean_title": None},
                    {"clean_title": {"$regex": "[\(\)]"}} # Puraane bug ko pakadne ke liye
                ]},
                {"title": 1}
            )
            
            bulk_ops = []
            async for movie in cursor:
                if "title" in movie and movie["title"]:
                    # Naya, sahi (v3) function (bot.py se) yahaan call hoga
                    new_clean_title = clean_title_func(movie["title"])
                    if new_clean_title: # Sirf tabhi update karein agar naya title empty nahi hai
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
        """
        Gets all movies formatted for Algolia sync.
        Yeh *database.py* ke (ab fix ho chuke) clean_text_for_search() ko fallback ke liye use karega.
        """
        if self.movies is None: await self._connect()
        try:
            cursor = self.movies.find(
                {},
                {"imdb_id": 1, "title": 1, "year": 1, "clean_title": 1, "_id": 0} # Projection
            )
            movies = []
            async for m in cursor:
                title = m.get("title", "N/A")
                
                # Yahaan critical fix hai:
                # 1. Pehle database se clean_title lene ki koshish karein
                clean_title_db = m.get("clean_title")
                
                # 2. Agar woh empty ya None hai, toh *isi file* ke (v3) function se generate karein
                #    Kyunki ab yeh function bhi fix ho gaya hai, yeh sahi kaam karega.
                final_clean_title = clean_title_db or clean_text_for_search(title)
                
                movies.append({
                    'objectID': m["imdb_id"], # Required by Algolia
                    'imdb_id': m["imdb_id"],
                    'title': title,
                    'year': m.get("year"),
                    'clean_title': final_clean_title 
                })
            
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        # FIX: Check using 'is None'
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
