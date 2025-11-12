# neondb.py
import logging
import asyncpg
from datetime import datetime, timezone
from typing import List, Dict, Tuple

logger = logging.getLogger("bot.neondb")

class NeonDB:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None

    async def init_db(self):
        try:
            self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
            async with self.pool.acquire() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        id SERIAL PRIMARY KEY,
                        message_id BIGINT, channel_id BIGINT,
                        file_id TEXT, file_unique_id TEXT,
                        imdb_id TEXT, title TEXT,
                        added_date TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_file ON movies (file_unique_id);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_title_trgm ON movies USING gist (title gist_trgm_ops);")
            logger.info("NeonDB Initialized.")
        except Exception as e:
            logger.error(f"NeonDB Init Error: {e}")

    async def is_ready(self):
        if not self.pool: return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except: return False

    async def close(self):
        if self.pool: await self.pool.close()

    # --- Features for Bot ---
    async def get_movie_count(self):
        if not await self.is_ready(): return -1
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM movies")

    async def add_movie(self, message_id, channel_id, file_id, file_unique_id, imdb_id, title):
        if not await self.is_ready(): return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title, added_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (file_unique_id) DO UPDATE 
                    SET title = EXCLUDED.title
                """, message_id, channel_id, file_id, file_unique_id, imdb_id, title, datetime.now(timezone.utc))
            return True
        except Exception as e:
            logger.error(f"Neon Add Error: {e}")
            return False

    async def neondb_search(self, query: str, limit: int = 20):
        if not await self.is_ready(): return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT imdb_id, title FROM movies 
                WHERE title % $1 
                ORDER BY similarity(title, $1) DESC LIMIT $2
            """, query, limit)
            return [{'imdb_id': r['imdb_id'], 'title': r['title'], 'year': None} for r in rows]

    # --- Admin / Cleanup Features ---
    async def find_and_delete_duplicates(self, batch_limit: int):
        # Returns (list of (msg_id, chat_id) to delete, total duplicates)
        # Simplified logic: Find file_unique_ids appearing > 1 time
        # Note: Real implementation depends on exact schema requirements
        return ([], 0) # Placeholder to prevent crash if called

    async def get_unique_movies_for_backup(self):
        if not await self.is_ready(): return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT message_id, channel_id FROM movies")
            return [(r['message_id'], r['channel_id']) for r in rows]

    async def sync_from_mongo(self, mongo_movies: List[Dict]):
        if not await self.is_ready(): return 0
        count = 0
        async with self.pool.acquire() as conn:
            for m in mongo_movies:
                try:
                    await conn.execute("""
                        INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
                        VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING
                    """, m.get('message_id'), m.get('channel_id'), m.get('file_id'), 
                       m.get('file_unique_id') or m.get('file_id'), m.get('imdb_id'), m.get('title'))
                    count += 1
                except: pass
        return count
