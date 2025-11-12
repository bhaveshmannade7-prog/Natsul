import logging
import re
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import ConnectionFailure, DuplicateKeyError, OperationFailure
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
        """Connects to the Secondary MongoDB."""
        if self.client is not None and self.db is not None:
            try:
                await self.client.admin.command('ping')
                return True
            except Exception:
                self.client = None

        try:
            logger.info("Connecting to Secondary MongoDB...")
            ca = certifi.where()
            self.client = AsyncIOMotorClient(
                self.database_url,
                serverSelectionTimeoutMS=10000,
                tls=True,
                tlsCAFile=ca
            )
            await self.client.admin.command('ping')
            # Hum alag naam use kar rahe hain taaki confusion na ho
            self.db = self.client["MovieBotSecondaryDB"]
            self.movies = self.db["movies_index"]
            logger.info("Connected to Secondary MongoDB.")
            return True
        except Exception as e:
            logger.critical(f"Failed to connect to Secondary MongoDB: {e}")
            self.client = None
            return False

    async def is_ready(self) -> bool:
        if self.client is None or self.db is None:
            return False
        try:
            await self.client.admin.command('ping')
            return True
        except:
            return False

    async def init_db(self):
        """Initialize indexes for fast search."""
        if not await self._connect():
            logger.warning("Secondary DB connection failed on startup.")
            return

        try:
            # Unique Index on IMDB ID
            await self.movies.create_index("imdb_id", unique=True)
            
            # Text Index for Search (Title & Clean Title)
            await self.movies.create_index(
                [("clean_title", "text"), ("title", "text")],
                name="search_index",
                default_language="none"
            )
            logger.info("Secondary DB indexes created.")
        except Exception as e:
            logger.error(f"Error creating indexes in Secondary DB: {e}")

    async def add_movie(self, data: dict) -> bool:
        """Adds or updates a movie in the secondary search DB."""
        if not await self.is_ready(): await self._connect()
        try:
            # Hum sirf search ke liye zaruri data rakhenge
            doc = {
                "imdb_id": data.get("imdb_id"),
                "title": data.get("title"),
                "year": data.get("year"),
                "clean_title": data.get("clean_title") or clean_text_for_search(data.get("title")),
                "score": 0 # Placeholder for sorting if needed
            }
            await self.movies.update_one(
                {"imdb_id": doc["imdb_id"]},
                {"$set": doc},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Secondary DB add_movie failed: {e}")
            return False

    async def add_batch_movies(self, movies_list: list) -> bool:
        """Batch insert for sync."""
        if not await self.is_ready(): await self._connect()
        if not movies_list: return True
        
        try:
            ops = []
            for m in movies_list:
                doc = {
                    "imdb_id": m.get("imdb_id"),
                    "title": m.get("title"),
                    "year": m.get("year"),
                    "clean_title": m.get("clean_title"),
                }
                ops.append(pymongo.UpdateOne(
                    {"imdb_id": doc["imdb_id"]},
                    {"$set": doc},
                    upsert=True
                ))
            
            if ops:
                await self.movies.bulk_write(ops, ordered=False)
            return True
        except Exception as e:
            logger.error(f"Secondary DB batch insert error: {e}")
            return False

    async def remove_movie(self, imdb_id: str) -> bool:
        if not await self.is_ready(): await self._connect()
        try:
            res = await self.movies.delete_one({"imdb_id": imdb_id})
            return res.deleted_count > 0
        except Exception as e:
            logger.error(f"Secondary DB remove error: {e}")
            return False

    async def search_movies(self, query: str, limit: int = 20) -> list:
        """Performs text search on the Secondary DB."""
        if not await self.is_ready(): return []
        
        try:
            clean_query = clean_text_for_search(query)
            if not clean_query: return []

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

            # Fallback: Regex Search if text search gives no results
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
            logger.error(f"Secondary DB search error: {e}")
            return []

    async def get_count(self) -> int:
        if not await self.is_ready(): await self._connect()
        try:
            return await self.movies.count_documents({})
        except:
            return -1
