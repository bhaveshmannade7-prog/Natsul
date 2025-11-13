# -*- coding: utf-8 -*-
import os
import logging
import asyncpg
import re
from typing import List, Tuple, Dict, Any

logger = logging.getLogger("bot.neondb")

class NeonDB:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None

    async def init_db(self):
        """Initializes the connection pool, updates schema, and backfills data."""
        if not self.database_url:
            logger.warning("NEON_DATABASE_URL is not set.")
            return

        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.database_url,
                min_size=1,
                max_size=10,
                command_timeout=30 # Increased timeout for migrations
            )
            
            async with self.pool.acquire() as conn:
                # 1. Ensure Table Exists
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        imdb_id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        year TEXT,
                        message_id BIGINT,
                        channel_id BIGINT,
                        file_id TEXT,
                        file_unique_id TEXT,
                        search_text TSVECTOR
                    );
                """)
                
                # 2. Add Columns Safe Mode (Fixes 'UndefinedColumnError')
                # We run these separately to ensure one failure doesn't stop others
                try:
                    await conn.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS search_text TSVECTOR;")
                except Exception as e: logger.debug(f"Migration (search_text): {e}")

                try:
                    await conn.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS year TEXT;")
                except Exception as e: logger.debug(f"Migration (year): {e}")
                
                # 3. Create Index (Only after columns ensure)
                try:
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_movies_search_text ON movies USING GIN(search_text);")
                except Exception as e: logger.warning(f"Index creation warning: {e}")

                # 4. CRITICAL FIX: Backfill NULL search_text for existing 9000+ movies
                # This makes old data searchable immediately
                try:
                    # Update rows where search_text is NULL but title exists
                    await conn.execute("""
                        UPDATE movies 
                        SET search_text = to_tsvector('english', regexp_replace(title, '[^a-zA-Z0-9\s]', ' ', 'g')) 
                        WHERE search_text IS NULL AND title IS NOT NULL;
                    """)
                    logger.info("NeonDB: Auto-repaired missing search indices for old data.")
                except Exception as e:
                    logger.warning(f"NeonDB Backfill warning: {e}")

            logger.info("NeonDB (Postgres) initialized and verified.")
        except Exception as e:
            logger.critical(f"Failed to initialize NeonDB: {e}", exc_info=True)
            self.pool = None

    async def is_ready(self) -> bool:
        return self.pool is not None

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("NeonDB pool closed.")

    async def add_movie(self, message_id: int, channel_id: int, file_id: str, file_unique_id: str, imdb_id: str, title: str, year: str = None) -> bool:
        if not self.pool: return False
        try:
            # Generate search vector from title
            clean_title = re.sub(r"[^a-zA-Z0-9\s]", " ", title).strip()
            if not clean_title: clean_title = "untitled"
            
            query = """
                INSERT INTO movies (imdb_id, title, year, message_id, channel_id, file_id, file_unique_id, search_text)
                VALUES ($1, $2, $3, $4, $5, $6, $7, to_tsvector('english', $8))
                ON CONFLICT (imdb_id) DO UPDATE 
                SET title = EXCLUDED.title,
                    year = EXCLUDED.year,
                    message_id = EXCLUDED.message_id,
                    channel_id = EXCLUDED.channel_id,
                    file_id = EXCLUDED.file_id,
                    file_unique_id = EXCLUDED.file_unique_id,
                    search_text = to_tsvector('english', $8);
            """
            async with self.pool.acquire() as conn:
                await conn.execute(query, imdb_id, title, year, message_id, channel_id, file_id, file_unique_id, clean_title)
            return True
        except Exception as e:
            logger.error(f"NeonDB add_movie error: {e}")
            return False

    async def neondb_search(self, query: str, limit: int = 20) -> List[Dict]:
        """Robust Search: Tries Index first, Falls back to ILIKE."""
        if not self.pool: return []
        try:
            clean_q = re.sub(r"[^a-zA-Z0-9\s]", " ", query).strip()
            if not clean_q: return []
            
            rows = []
            async with self.pool.acquire() as conn:
                # Attempt 1: Full Text Search (Fast & Smart)
                ts_query_str = " & ".join(clean_q.split()) + ":*"
                sql_fts = """
                    SELECT imdb_id, title, year
                    FROM movies
                    WHERE search_text @@ to_tsquery('english', $1)
                    LIMIT $2;
                """
                rows = await conn.fetch(sql_fts, ts_query_str, limit)

                # Attempt 2: Fallback to ILIKE if Index fails (Slow but Guaranteed for partial matches)
                if not rows:
                    sql_like = """
                        SELECT imdb_id, title, year
                        FROM movies
                        WHERE title ILIKE $1
                        LIMIT $2;
                    """
                    rows = await conn.fetch(sql_like, f"%{clean_q}%", limit)

            return [
                {'imdb_id': r['imdb_id'], 'title': r['title'], 'year': r['year']} 
                for r in rows
            ]
        except Exception as e:
            logger.error(f"NeonDB neondb_search error for '{query}': {e}", exc_info=True)
            return []

    async def get_movie_count(self) -> int:
        if not self.pool: return -1
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval("SELECT COUNT(*) FROM movies;")
        except:
            return -1

    async def sync_from_mongo(self, movies_list: List[Dict]) -> int:
        if not self.pool or not movies_list: return 0
        
        data_tuples = []
        for m in movies_list:
            if isinstance(m, dict):
                t_imdb = m.get('imdb_id')
                t_title = m.get('title')
                if t_imdb and t_title:
                    clean_t = re.sub(r"[^a-zA-Z0-9\s]", " ", t_title).strip()
                    if not clean_t: clean_t = "untitled"
                    data_tuples.append((
                        t_imdb, t_title, m.get('year'), m.get('message_id'), m.get('channel_id'), 
                        m.get('file_id'), m.get('file_unique_id'), clean_t
                    ))

        if not data_tuples: return 0

        query = """
            INSERT INTO movies (imdb_id, title, year, message_id, channel_id, file_id, file_unique_id, search_text)
            VALUES ($1, $2, $3, $4, $5, $6, $7, to_tsvector('english', $8))
            ON CONFLICT (imdb_id) DO UPDATE 
            SET title = EXCLUDED.title,
                year = EXCLUDED.year,
                message_id = EXCLUDED.message_id,
                channel_id = EXCLUDED.channel_id,
                file_id = EXCLUDED.file_id,
                file_unique_id = EXCLUDED.file_unique_id,
                search_text = EXCLUDED.search_text;
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(query, data_tuples)
            return len(data_tuples)
        except Exception as e:
            logger.error(f"NeonDB Sync Error: {e}")
            return 0

    # --- Cleanup Helpers ---
    async def find_and_delete_duplicates(self, batch_limit: int = 100) -> Tuple[List[Tuple[int, int]], int]:
        if not self.pool: return ([], 0)
        try:
            async with self.pool.acquire() as conn:
                duplicates = await conn.fetch("""
                    SELECT file_unique_id, COUNT(*) FROM movies GROUP BY file_unique_id HAVING COUNT(*) > 1
                """)
                total_duplicates = sum([r['count'] - 1 for r in duplicates])
                to_delete = []
                for row in duplicates:
                    entries = await conn.fetch("""
                        SELECT message_id, channel_id FROM movies WHERE file_unique_id = $1 ORDER BY message_id DESC
                    """, row['file_unique_id'])
                    for e in entries[1:]:
                        to_delete.append((e['message_id'], e['channel_id']))
                        if len(to_delete) >= batch_limit: break
                    if len(to_delete) >= batch_limit: break
                return to_delete, total_duplicates
        except Exception: return ([], 0)

    async def get_unique_movies_for_backup(self) -> List[Tuple[int, int]]:
        if not self.pool: return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT DISTINCT ON (file_unique_id) message_id, channel_id FROM movies;")
                return [(r['message_id'], r['channel_id']) for r in rows]
        except Exception: return []
