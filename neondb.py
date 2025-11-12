# neondb.py
import logging
import os
import asyncio
import asyncpg
from typing import List, Dict, Tuple
from datetime import datetime, timezone

logger = logging.getLogger("bot.neondb")

class NeonDB:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None

    async def init_db(self):
        """Connection pool banata hai aur FTS features ke saath table ensure karta hai."""
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            async with self.pool.acquire() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        id SERIAL PRIMARY KEY,
                        message_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        file_id TEXT NOT NULL,
                        file_unique_id TEXT NOT NULL,
                        imdb_id TEXT,
                        title TEXT,
                        added_date TIMESTAMPTZ DEFAULT NOW(),
                        title_search_vector TSVECTOR
                    );
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON movies (file_unique_id);")
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_channel ON movies (message_id, channel_id);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_search_vector ON movies USING GIN(title_search_vector);")

                await conn.execute("""
                    CREATE OR REPLACE FUNCTION update_movie_search_vector()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.title_search_vector :=
                            to_tsvector('simple', COALESCE(NEW.title, ''));
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                
                await conn.execute("""
                    DROP TRIGGER IF EXISTS ts_movie_title_update ON movies;
                    CREATE TRIGGER ts_movie_title_update
                    BEFORE INSERT OR UPDATE ON movies
                    FOR EACH ROW
                    EXECUTE FUNCTION update_movie_search_vector();
                """)

            logger.info("NeonDB (Postgres) initialized.")
        except Exception as e:
            logger.critical(f"Failed to initialize NeonDB: {e}", exc_info=True)
            self.pool = None
            
    async def is_ready(self) -> bool:
        if self.pool is None: return False
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return True
        except Exception:
            return False

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("NeonDB closed.")

    async def get_movie_count(self) -> int:
        if not await self.is_ready(): return -1
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval("SELECT COUNT(*) FROM movies")
        except Exception:
            return -1

    async def add_movie(self, message_id: int, channel_id: int, file_id: str, file_unique_id: str, imdb_id: str, title: str) -> bool:
        if not await self.is_ready(): return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title, added_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (message_id, channel_id) DO NOTHING
                    """,
                    message_id, channel_id, file_id, file_unique_id, imdb_id, title, datetime.now(timezone.utc)
                )
            return True
        except Exception as e:
            logger.error(f"NeonDB add_movie error: {e}")
            return False

    async def neondb_search(self, query: str, limit: int = 20) -> List[Dict]:
        """NeonDB Full-Text Search."""
        if not await self.is_ready(): return []
        
        search_query = """
            SELECT imdb_id, title, year
            FROM movies, plainto_tsquery('simple', $1) AS q
            WHERE title_search_vector @@ q
            ORDER BY ts_rank(title_search_vector, q) DESC
            LIMIT $2;
        """
        try:
            async with self.pool.acquire() as conn:
                # Note: year column is actually not in table schema above, handling gracefully
                # If you want year, add it to CREATE TABLE. For now, sending None.
                rows = await conn.fetch(search_query, query, limit)
                if not rows:
                    trigram_query = """
                        SELECT imdb_id, title, year
                        FROM movies
                        ORDER BY similarity(title, $1) DESC
                        LIMIT $2;
                    """
                    rows = await conn.fetch(trigram_query, query, limit)

                return [{'imdb_id': r['imdb_id'], 'title': r['title'], 'year': None} for r in rows]
        except Exception as e:
            logger.error(f"NeonDB search error: {e}")
            return []

    async def find_and_delete_duplicates(self, batch_limit: int) -> Tuple[List[Tuple[int, int]], int]:
        if not await self.is_ready(): return ([], 0)
        query = """
        WITH ranked_movies AS (
            SELECT id, message_id, channel_id, file_unique_id, added_date,
                ROW_NUMBER() OVER(PARTITION BY file_unique_id ORDER BY added_date DESC, message_id DESC) as rn
            FROM movies
        ),
        to_delete AS (SELECT id, message_id, channel_id FROM ranked_movies WHERE rn > 1),
        total_duplicates AS (SELECT COUNT(*) as total_count FROM to_delete)
        SELECT (SELECT total_count FROM total_duplicates), 
            array_agg(t.id) as ids_to_delete_db,
            array_agg(ARRAY[t.message_id, t.channel_id]) as messages_to_delete_tg
        FROM (SELECT * FROM to_delete LIMIT $1) t;
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, batch_limit)
                if not result or not result['ids_to_delete_db']: return ([], 0)
                
                db_ids = result['ids_to_delete_db']
                tg_msgs = [tuple(msg) for msg in result['messages_to_delete_tg']]
                
                async with conn.transaction():
                    await conn.execute("DELETE FROM movies WHERE id = ANY($1)", db_ids)
                
                return (tg_msgs, result['total_count'])
        except Exception as e:
            logger.error(f"Neon cleanup error: {e}")
            return ([], 0)

    async def get_unique_movies_for_backup(self) -> List[Tuple[int, int]]:
        if not await self.is_ready(): return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT DISTINCT ON (file_unique_id) message_id, channel_id FROM movies ORDER BY file_unique_id, added_date DESC;")
                return [(r['message_id'], r['channel_id']) for r in rows]
        except Exception:
            return []

    async def sync_from_mongo(self, mongo_movies: List[Dict]) -> int:
        if not await self.is_ready() or not mongo_movies: return 0
        data = []
        for m in mongo_movies:
            if not m.get('file_id'): continue
            data.append((
                m.get('message_id'), m.get('channel_id'), m.get('file_id'),
                m.get('file_unique_id') or m.get('file_id'),
                m.get('imdb_id'), m.get('title')
            ))
        if not data: return 0
        
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany("""
                    INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (message_id, channel_id) DO NOTHING
                """, data)
                return len(data)
        except Exception as e:
            logger.error(f"Neon Sync error: {e}")
            return 0
