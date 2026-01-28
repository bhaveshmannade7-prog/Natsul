# neondb.py - Hybrid Database Wrapper (PostgreSQL + MongoDB)
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
        self.database_url = database_url
        self.db_primary = db_primary_instance # Used for potential cross-DB locks
        self.mode = self._detect_mode()
        
        # storage handles
        self.pool = None        # for Postgres
        self.client = None      # for Mongo
        self.db = None          # for Mongo
        self.collection = None  # for Mongo collection 'videos'
        
        logger.info(f"Initialized NeonDB Wrapper. Detected Mode: {self.mode.upper()}")

    def _detect_mode(self) -> str:
        """Helper to determine DB type from URL string."""
        if not self.database_url:
            return "none"
        if self.database_url.startswith("mongodb") or "mongo" in self.database_url:
            return "mongo"
        if self.database_url.startswith("postgres") or "neon.tech" in self.database_url:
            return "postgres"
        return "unknown"

    async def init_db(self):
        """Establishes connection and creates Tables (SQL) or Indexes (NoSQL)."""
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
                logger.error(f"❌ NeonDB (Postgres) Connection Failed: {e}", exc_info=True)

        elif self.mode == "mongo":
            if not AsyncIOMotorClient:
                logger.critical("❌ CRITICAL: 'motor' or 'pymongo' is missing. Install via pip.")
                return
            try:
                # SSL Context for Atlas
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
                db_name = "NeonBackupDB" # Fixed name for the backup functionality
                self.db = self.client[db_name]
                self.collection = self.db["videos"]
                
                logger.info(f"✅ Connected to MongoDB (Hybrid Mode). DB: {db_name}")
                await self._create_indexes_mongo()
            except Exception as e:
                logger.error(f"❌ MongoDB Connection Failed: {e}", exc_info=True)
        else:
            logger.warning("⚠️ No valid Database URL provided for NeonDB Backup System.")

    # --- Internal Schema Setup ---

    async def _create_tables_postgres(self):
        """Creates the SQL schema required for the bot."""
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
        -- Indexes for speed
        CREATE INDEX IF NOT EXISTS idx_videos_search_vector ON videos USING gin(search_vector);
        CREATE INDEX IF NOT EXISTS idx_videos_file_unique_id ON videos(file_unique_id);
        CREATE INDEX IF NOT EXISTS idx_videos_imdb_id ON videos(imdb_id);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query)

    async def _create_indexes_mongo(self):
        """Creates NoSQL indexes to mimic SQL performance."""
        # 1. Unique constraint on file_unique_id (Just like SQL PRIMARY/UNIQUE)
        await self.collection.create_index("file_unique_id", unique=True)
        
        # 2. Compound Text Index for Search
        # This allows: db.collection.find({$text: {$search: "query"}})
        await self.collection.create_index(
            [("title", TEXT), ("imdb_id", TEXT)],
            name="title_imdb_text_index"
        )
        
        # 3. Standard index for IMDB lookups
        await self.collection.create_index("imdb_id")
        logger.info("✅ MongoDB Indexes verified.")

    # --- PUBLIC METHODS (API) ---
    
    async def is_ready(self) -> bool:
        """Health check method."""
        if self.mode == "postgres":
            return self.pool is not None and not self.pool._closed
        elif self.mode == "mongo":
            return self.client is not None
        return False

    async def close(self):
        """Gracefully close connections."""
        if self.mode == "postgres" and self.pool:
            await self.pool.close()
            logger.info("NeonDB (Postgres) pool closed.")
        elif self.mode == "mongo" and self.client:
            self.client.close()
            logger.info("MongoDB (Hybrid) client closed.")

    async def get_movie_count(self) -> int:
        """Returns total number of files in DB."""
        try:
            if self.mode == "postgres":
                async with self.pool.acquire() as conn:
                    return await conn.fetchval("SELECT COUNT(*) FROM videos")
            elif self.mode == "mongo":
                return await self.collection.count_documents({})
        except Exception as e:
            logger.error(f"Error getting movie count: {e}")
            return 0

    async def add_movie(self, message_id, channel_id, file_id, file_unique_id, imdb_id, title):
        """
        Adds or Updates a movie.
        SQL: Uses ON CONFLICT DO UPDATE.
        NoSQL: Uses update_one(upsert=True).
        """
        clean_title = re.sub(r"[._\-]+", " ", title).strip() if title else ""
        
        if self.mode == "postgres":
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

        elif self.mode == "mongo":
            # Prepare document
            doc = {
                "message_id": message_id,
                "channel_id": channel_id,
                "file_id": file_id,
                "file_unique_id": file_unique_id,
                "imdb_id": imdb_id,
                "title": clean_title,
                # Adding lowercase fields for better regex search fallback
                "title_lower": clean_title.lower() if clean_title else ""
            }
            try:
                # Upsert based on unique ID
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
        """
        Full Text Search.
        SQL: Uses to_tsquery.
        NoSQL: Uses $text index, falls back to Regex.
        """
        clean_query = re.sub(r"[._\-]+", " ", query).strip()
        
        if self.mode == "postgres":
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
            except Exception as e:
                logger.error(f"Postgres search error: {e}")
                return []

        elif self.mode == "mongo":
            try:
                results = []
                # 1. Try Text Search (Fastest)
                cursor = self.collection.find(
                    {"$text": {"$search": clean_query}},
                    {"score": {"$meta": "textScore"}}
                ).sort([("score", {"$meta": "textScore"})]).limit(10)
                
                results = await cursor.to_list(length=10)
                
                # 2. Fallback: Regex Search (If text search fails or returns nothing)
                if not results:
                    regex_query = re.compile(re.escape(clean_query), re.IGNORECASE)
                    cursor = self.collection.find({"title": regex_query}).limit(10)
                    results = await cursor.to_list(length=10)

                return results
            except Exception as e:
                logger.error(f"Mongo search error: {e}")
                return []
        return []

    async def remove_movie_by_imdb(self, imdb_id):
        """Deletes all entries with specific IMDb ID."""
        if self.mode == "postgres":
            try:
                async with self.pool.acquire() as conn:
                    res = await conn.execute("DELETE FROM videos WHERE imdb_id = $1", imdb_id)
                    # res format is usually "DELETE count"
                    return "DELETE 0" not in res
            except Exception as e:
                logger.error(f"Postgres remove error: {e}")
                return False
        
        elif self.mode == "mongo":
            try:
                res = await self.collection.delete_many({"imdb_id": imdb_id})
                return res.deleted_count > 0
            except Exception as e:
                logger.error(f"Mongo remove error: {e}")
                return False
        return False

    # --- ADMIN / MAINTENANCE COMMANDS ---

    async def check_neon_clean_title(self):
        """Diagnostic: Check if DB has valid data."""
        if self.mode == "postgres":
            try:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT title FROM videos LIMIT 1")
                    if row: return {"title": row['title'], "clean_title": "✅ Postgres Active"}
                    return {"title": "Empty", "clean_title": "⚠️ No Data"}
            except Exception as e:
                return {"title": "Error", "clean_title": str(e)}
        
        elif self.mode == "mongo":
            try:
                doc = await self.collection.find_one({}, {"title": 1})
                if doc: return {"title": doc.get('title'), "clean_title": "✅ Mongo Active"}
                return {"title": "Empty", "clean_title": "⚠️ No Data"}
            except Exception as e:
                return {"title": "Error", "clean_title": str(e)}
        return {"title": "Offline", "clean_title": "Check ENV"}

    async def rebuild_fts_vectors(self):
        """
        Rebuilds Search Index.
        SQL: Updates tsvector column.
        NoSQL: Drops and Recreates Text Index.
        """
        if self.mode == "postgres":
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE videos 
                        SET search_vector = to_tsvector('english', title || ' ' || COALESCE(imdb_id, ''))
                    """)
                    return await self.get_movie_count()
            except Exception as e:
                logger.error(f"Postgres Rebuild Error: {e}")
                return -1
        
        elif self.mode == "mongo":
            try:
                logger.info("Dropping Mongo Text Index...")
                # Try dropping the index (names might vary, using wildcard logic internally handled by recreate)
                await self.collection.drop_index("title_imdb_text_index")
            except OperationFailure:
                logger.warning("Index didn't exist, creating new one.")
            except Exception as e:
                logger.error(f"Index drop failed: {e}")
            
            # Recreate
            await self._create_indexes_mongo()
            return await self.get_movie_count()
        return -1

    async def sync_from_mongo(self, mongo_movies_list: List[Dict]):
        """
        Imports a large list of movies.
        Optimized for bulk operations in both DBs.
        """
        if not mongo_movies_list: return 0
        
        count = 0
        if self.mode == "postgres":
            # Postgres: Use executemany for high speed insertion
            data_tuples = []
            for m in mongo_movies_list:
                title = re.sub(r"[._\-]+", " ", m.get('title', '')).strip()
                imdb = m.get('imdb_id') or f"auto_{m.get('message_id')}"
                # Tuple order must match SQL params
                data_tuples.append((
                    m.get('message_id'), 
                    m.get('channel_id'), 
                    m.get('file_id'),
                    m.get('file_unique_id') or m.get('file_id'),
                    imdb, 
                    title
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

        elif self.mode == "mongo":
            # Mongo: Use bulk_write with UpdateOne (Upsert)
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
                
                bulk_ops.append(
                    UpdateOne({"file_unique_id": fid_unique}, {"$set": doc}, upsert=True)
                )
            
            if bulk_ops:
                try:
                    res = await self.collection.bulk_write(bulk_ops, ordered=False)
                    return res.modified_count + res.upserted_count
                except Exception as e:
                    logger.error(f"Mongo Sync Error: {e}")
                    return 0
            return 0

    async def find_and_delete_duplicates(self, batch_limit=100):
        """
        Identifies and removes duplicate files.
        Logic: Keeps the oldest/first entry, removes subsequent ones with same ID.
        """
        if self.mode == "postgres":
            # Complex Self-Join to find duplicates in SQL
            sql = """
            DELETE FROM videos a USING videos b
            WHERE a.id > b.id AND a.file_unique_id = b.file_unique_id
            RETURNING a.id
            """
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(sql)
                    return [], len(rows) # Format: (ignored_list, count)
            except Exception as e:
                logger.error(f"Postgres Dedupe Error: {e}")
                return [], 0

        elif self.mode == "mongo":
            # Aggregation Pipeline to find duplicates
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
                    # Keep the first ID, delete the rest
                    all_ids = group['ids']
                    ids_to_delete = all_ids[1:] # Skip first one
                    
                    if ids_to_delete:
                        res = await self.collection.delete_many({"_id": {"$in": ids_to_delete}})
                        deleted_total += res.deleted_count
                        
                return [], deleted_total
            except Exception as e:
                logger.error(f"Mongo Dedupe Error: {e}")
                return [], 0
        return [], 0
