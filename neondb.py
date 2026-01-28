# neondb.py - Hybrid Database Wrapper (PostgreSQL + MongoDB)
# Fixed for Render Deployment & Parsing Errors

import logging
import asyncio
import os
import re
from typing import List, Dict, Any, Tuple, Union

# --- Imports for Postgres (Neon) ---
try:
    import asyncpg
except ImportError:
    asyncpg = None

# --- Imports for MongoDB ---
try:
    from motor.motor_asyncio import AsyncIOMotorClient
    from pymongo import UpdateOne, IndexModel, ASCENDING, DESCENDING, TEXT
    from pymongo.errors import DuplicateKeyError, ConnectionFailure, OperationFailure
    import certifi
except ImportError:
    AsyncIOMotorClient = None

logger = logging.getLogger("bot.neondb")

class NeonDB:
    def __init__(self, database_url: str, db_primary_instance=None):
        """
        Initializes the Hybrid Database connection.
        Automatically detects if the URL is for PostgreSQL (Neon) or MongoDB.
        """
        # Strip whitespace to prevent parsing errors
        self.database_url = database_url.strip() if database_url else ""
        self.db_primary = db_primary_instance 
        self.mode = self._detect_mode()
        
        # storage handles
        self.pool = None        # for Postgres
        self.client = None      # for Mongo
        self.db = None          # for Mongo
        self.collection = None  # for Mongo collection 'videos'
        
        logger.info(f"Initialized NeonDB Wrapper. Detected Mode: {self.mode.upper()}")

    def _detect_mode(self) -> str:
        """Helper to determine DB type from URL string safely."""
        if not self.database_url:
            return "none"
        
        url_lower = self.database_url.lower()
        
        # Explicit MongoDB check
        if url_lower.startswith("mongodb") or "mongodb.net" in url_lower:
            return "mongo"
        
        # Explicit Postgres check
        if url_lower.startswith("postgres") or "neon.tech" in url_lower:
            return "postgres"
            
        # Fallback based on content (Safe check)
        if "sslmode" in url_lower: # Postgres usually has sslmode
            return "postgres"
            
        return "unknown"

    async def init_db(self):
        """Establishes connection based on detected mode."""
        if self.mode == "none":
            logger.warning("⚠️ No Backup Database URL provided. Skipping Backup DB.")
            return

        # --- POSTGRES MODE ---
        if self.mode == "postgres":
            if not asyncpg:
                logger.critical("❌ CRITICAL: 'asyncpg' library is missing. Install it via pip.")
                return
            try:
                # Create connection pool
                self.pool = await asyncpg.create_pool(
                    self.database_url, 
                    min_size=1, 
                    max_size=20,
                    command_timeout=60
                )
                logger.info("✅ Connected to NeonDB (PostgreSQL). Verifying Schema...")
                await self._create_tables_postgres()
            except Exception as e:
                # Catch invalid DSN errors specifically to stop crash loop
                logger.error(f"❌ NeonDB (Postgres) Connection Failed: {e}")
                logger.error("💡 Check your NEON_DATABASE_URL. It seems invalid for Postgres.")
                self.mode = "error" # Disable further operations

        # --- MONGO MODE ---
        elif self.mode == "mongo":
            if not AsyncIOMotorClient:
                logger.critical("❌ CRITICAL: 'motor' or 'pymongo' is missing. Install via pip.")
                return
            try:
                # SSL Context for Atlas (Required for Render)
                ca = certifi.where()
                self.client = AsyncIOMotorClient(
                    self.database_url,
                    serverSelectionTimeoutMS=5000,
                    tls=True,
                    tlsCAFile=ca
                )
                # Ping check
                await self.client.admin.command('ping')
                
                # Setup DB and Collection
                db_name = "NeonBackupDB"
                self.db = self.client[db_name]
                self.collection = self.db["videos"]
                
                logger.info(f"✅ Connected to MongoDB (Hybrid Backup). DB: {db_name}")
                await self._create_indexes_mongo()
            except Exception as e:
                logger.error(f"❌ MongoDB (Backup) Connection Failed: {e}")
                self.mode = "error" # Disable further operations
        
        else:
            logger.warning(f"⚠️ Unknown Database URL format: {self.database_url[:10]}... check your .env")

    # --- Internal Schema Setup ---

    async def _create_tables_postgres(self):
        """Creates the SQL schema required for the bot."""
        if not self.pool: return
        query = """
        CREATE TABLE IF NOT EXISTS videos (
            id SERIAL PRIMARY KEY,
            message_id INTEGER NOT NULL,
            channel_id BIGINT NOT NULL,
            file_id TEXT NOT NULL,
            file_unique_id TEXT NOT NULL UNIQUE,
            imdb_id TEXT,
            title TEXT,
            search_vector tsvector
        );
        CREATE INDEX IF NOT EXISTS idx_videos_search_vector ON videos USING gin(search_vector);
        CREATE INDEX IF NOT EXISTS idx_videos_file_unique_id ON videos(file_unique_id);
        CREATE INDEX IF NOT EXISTS idx_videos_imdb_id ON videos(imdb_id);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query)

    async def _create_indexes_mongo(self):
        """Creates NoSQL indexes."""
        if self.collection is None: return
        try:
            # Unique ID Index
            await self.collection.create_index("file_unique_id", unique=True)
            
            # Text Index for Search
            await self.collection.create_index(
                [("title", TEXT), ("imdb_id", TEXT)],
                name="title_imdb_text_index"
            )
            # Standard Index
            await self.collection.create_index("imdb_id")
            logger.info("✅ MongoDB Backup Indexes verified.")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

    # --- PUBLIC METHODS (Safe Wrapped) ---
    
    async def is_ready(self) -> bool:
        """Health check method."""
        if self.mode == "postgres" and self.pool:
            return not self.pool._closed
        elif self.mode == "mongo" and self.client:
            return True # Client doesn't track state easily, assumes alive if init passed
        return False

    async def close(self):
        """Gracefully close connections."""
        if self.mode == "postgres" and self.pool:
            await self.pool.close()
        elif self.mode == "mongo" and self.client:
            self.client.close()

    async def get_movie_count(self) -> int:
        try:
            if self.mode == "postgres" and self.pool:
                async with self.pool.acquire() as conn:
                    return await conn.fetchval("SELECT COUNT(*) FROM videos")
            elif self.mode == "mongo" and self.collection is not None:
                return await self.collection.count_documents({})
        except Exception as e:
            logger.error(f"Error getting movie count (Backup DB): {e}")
            return 0
        return 0

    async def add_movie(self, message_id, channel_id, file_id, file_unique_id, imdb_id, title):
        """Adds or Updates a movie."""
        if self.mode == "error" or self.mode == "none": return False

        clean_title = re.sub(r"[._\-]+", " ", title).strip() if title else ""
        
        # --- POSTGRES LOGIC ---
        if self.mode == "postgres" and self.pool:
            sql = """
            INSERT INTO videos (message_id, channel_id, file_id, file_unique_id, imdb_id, title, search_vector)
            VALUES ($1, $2, $3, $4, $5, $6, to_tsvector('english', $6 || ' ' || COALESCE($5, '')))
            ON CONFLICT (file_unique_id) DO UPDATE 
            SET message_id = EXCLUDED.message_id,
                channel_id = EXCLUDED.channel_id,
                title = EXCLUDED.title,
                imdb_id = EXCLUDED.imdb_id,
                search_vector = to_tsvector('english', EXCLUDED.title || ' ' || COALESCE(EXCLUDED.imdb_id, ''));
            """
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(sql, message_id, channel_id, file_id, file_unique_id, imdb_id, clean_title)
                    return True
            except Exception as e:
                logger.error(f"Postgres add_movie error: {e}")
                return False

        # --- MONGO LOGIC ---
        elif self.mode == "mongo" and self.collection is not None:
            doc = {
                "message_id": message_id,
                "channel_id": channel_id,
                "file_id": file_id,
                "file_unique_id": file_unique_id,
                "imdb_id": imdb_id,
                "title": clean_title,
                "title_lower": clean_title.lower() if clean_title else ""
            }
            try:
                await self.collection.update_one(
                    {"file_unique_id": file_unique_id},
                    {"$set": doc},
                    upsert=True
                )
                return True
            except Exception as e:
                logger.error(f"Mongo add_movie error: {e}")
                return False
        return False

    async def search_video(self, query):
        if self.mode == "error" or self.mode == "none": return []

        clean_query = re.sub(r"[._\-]+", " ", query).strip()
        
        if self.mode == "postgres" and self.pool:
            sql = """
            SELECT message_id, channel_id, file_id, title 
            FROM videos 
            WHERE search_vector @@ plainto_tsquery('english', $1)
            LIMIT 10;
            """
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(sql, clean_query)
                    return [dict(row) for row in rows]
            except Exception:
                return []

        elif self.mode == "mongo" and self.collection is not None:
            try:
                # Text Search
                cursor = self.collection.find(
                    {"$text": {"$search": clean_query}},
                    {"score": {"$meta": "textScore"}}
                ).sort([("score", {"$meta": "textScore"})]).limit(10)
                
                results = await cursor.to_list(length=10)
                
                # Regex Fallback
                if not results:
                    regex_query = re.compile(re.escape(clean_query), re.IGNORECASE)
                    cursor = self.collection.find({"title": regex_query}).limit(10)
                    results = await cursor.to_list(length=10)
                return results
            except Exception:
                return []
        return []

    async def remove_movie_by_imdb(self, imdb_id):
        if self.mode == "postgres" and self.pool:
            try:
                async with self.pool.acquire() as conn:
                    res = await conn.execute("DELETE FROM videos WHERE imdb_id = $1", imdb_id)
                    return "DELETE 0" not in res
            except Exception:
                return False
        
        elif self.mode == "mongo" and self.collection is not None:
            try:
                res = await self.collection.delete_many({"imdb_id": imdb_id})
                return res.deleted_count > 0
            except Exception:
                return False
        return False

    # --- ADMIN / MAINTENANCE ---

    async def check_neon_clean_title(self):
        """Diagnostic."""
        if self.mode == "postgres" and self.pool:
            try:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT title FROM videos LIMIT 1")
                    if row: return {"title": row['title'], "clean_title": "✅ Postgres Active"}
                    return {"title": "Empty", "clean_title": "⚠️ No Data"}
            except Exception as e:
                return {"title": "Error", "clean_title": str(e)}
        
        elif self.mode == "mongo" and self.collection is not None:
            try:
                doc = await self.collection.find_one({}, {"title": 1})
                if doc: return {"title": doc.get('title'), "clean_title": "✅ Mongo Active"}
                return {"title": "Empty", "clean_title": "⚠️ No Data"}
            except Exception as e:
                return {"title": "Error", "clean_title": str(e)}
        return {"title": "Offline", "clean_title": "Check Log/Env"}

    async def rebuild_fts_vectors(self):
        if self.mode == "postgres" and self.pool:
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE videos 
                        SET search_vector = to_tsvector('english', title || ' ' || COALESCE(imdb_id, ''))
                    """)
                    return await self.get_movie_count()
            except Exception:
                return -1
        
        elif self.mode == "mongo" and self.collection is not None:
            try:
                await self.collection.drop_index("title_imdb_text_index")
                await self._create_indexes_mongo()
                return await self.get_movie_count()
            except Exception:
                return -1
        return -1

    async def sync_from_mongo(self, mongo_movies_list: List[Dict]):
        if not mongo_movies_list or self.mode in ["error", "none"]: return 0
        
        if self.mode == "postgres" and self.pool:
            data_tuples = []
            for m in mongo_movies_list:
                title = re.sub(r"[._\-]+", " ", m.get('title', '')).strip()
                imdb = m.get('imdb_id') or f"auto_{m.get('message_id')}"
                data_tuples.append((
                    m.get('message_id'), m.get('channel_id'), m.get('file_id'),
                    m.get('file_unique_id') or m.get('file_id'),
                    imdb, title
                ))
            
            sql = """
            INSERT INTO videos (message_id, channel_id, file_id, file_unique_id, imdb_id, title, search_vector)
            VALUES ($1, $2, $3, $4, $5, $6, to_tsvector('english', $6 || ' ' || COALESCE($5, '')))
            ON CONFLICT (file_unique_id) DO NOTHING
            """
            try:
                async with self.pool.acquire() as conn:
                    await conn.executemany(sql, data_tuples)
                return len(data_tuples)
            except Exception as e:
                logger.error(f"Postgres Sync Error: {e}")
                return 0

        elif self.mode == "mongo" and self.collection is not None:
            bulk_ops = []
            for m in mongo_movies_list:
                fid_unique = m.get('file_unique_id') or m.get('file_id')
                title = re.sub(r"[._\-]+", " ", m.get('title', '')).strip()
                doc = {
                    "message_id": m.get('message_id'),
                    "channel_id": m.get('channel_id'),
                    "file_id": m.get('file_id'),
                    "file_unique_id": fid_unique,
                    "imdb_id": m.get('imdb_id'),
                    "title": title,
                    "title_lower": title.lower()
                }
                bulk_ops.append(UpdateOne({"file_unique_id": fid_unique}, {"$set": doc}, upsert=True))
            
            if bulk_ops:
                try:
                    res = await self.collection.bulk_write(bulk_ops, ordered=False)
                    return res.modified_count + res.upserted_count
                except Exception as e:
                    logger.error(f"Mongo Sync Error: {e}")
                    return 0
            return 0

    async def find_and_delete_duplicates(self, batch_limit=100):
        if self.mode == "postgres" and self.pool:
            sql = """
            DELETE FROM videos a USING videos b
            WHERE a.id > b.id AND a.file_unique_id = b.file_unique_id
            RETURNING a.id
            """
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(sql)
                    return [], len(rows)
            except Exception:
                return [], 0

        elif self.mode == "mongo" and self.collection is not None:
            pipeline = [
                {"$group": {
                    "_id": "$file_unique_id",
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"}
                }},
                {"$match": {"count": {"$gt": 1}}}
            ]
            try:
                duplicates = await self.collection.aggregate(pipeline).to_list(length=None)
                deleted_total = 0
                for group in duplicates:
                    ids_to_delete = group['ids'][1:]
                    if ids_to_delete:
                        res = await self.collection.delete_many({"_id": {"$in": ids_to_delete}})
                        deleted_total += res.deleted_count
                return [], deleted_total
            except Exception:
                return [], 0
        return [], 0
