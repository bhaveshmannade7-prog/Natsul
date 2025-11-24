# neondb.py
import logging
import asyncio
import asyncpg
import re
from typing import List, Dict, Tuple
from datetime import datetime, timezone

logger = logging.getLogger("bot.neondb")

# --- Helper function ---
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing."""
    if not text: return ""
    text = text.lower()
    text = re.sub(r"[._\-]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]+", "", text)
    text = re.sub(r"\b(s|season)\s*\d{1,2}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

class NeonDB:
    """
    NeonDB (Postgres) ke liye Async Interface Class।
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: asyncpg.Pool | None = None

    async def init_db(self):
        """
        Connection pool banata hai aur zaroori tables ensure karta hai.
        Conflict handling add kiya gaya hai.
        """
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=60,
                server_settings={'application_name': 'MovieBot-Backup'}
            )
            if self.pool is None:
                raise Exception("Pool creation returned None")

            async with self.pool.acquire() as conn:
                # 1. Extensions create karein
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                
                # 2. Tables create karein
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        id SERIAL PRIMARY KEY,
                        message_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        file_id TEXT NOT NULL,
                        file_unique_id TEXT NOT NULL,
                        imdb_id TEXT,
                        title TEXT,
                        clean_title TEXT,
                        added_date TIMESTAMPTZ DEFAULT NOW(),
                        title_search_vector TSVECTOR
                    );
                """)
                
                # 3. Columns verify karein
                try:
                    await conn.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS clean_title TEXT;")
                    await conn.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS title_search_vector TSVECTOR;")
                except Exception as e:
                    logger.warning(f"ALTER TABLE skipped (likely exists): {e}")

                # 4. Indexes create karein (Safer method)
                await conn.execute("DROP INDEX IF EXISTS idx_unique_file;")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON movies (file_unique_id);")
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_channel ON movies (message_id, channel_id);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON movies (imdb_id);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_search_vector ON movies USING GIN(title_search_vector);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_clean_title_trgm ON movies USING GIN (clean_title gin_trgm_ops);")

                # 5. Functions aur Triggers (Split execution to reduce locking)
                try:
                    await conn.execute("""
                        CREATE OR REPLACE FUNCTION f_clean_text_for_search(text TEXT)
                        RETURNS TEXT AS $$
                        BEGIN
                            IF text IS NULL THEN RETURN ''; END IF;
                            RETURN trim(regexp_replace(regexp_replace(regexp_replace(lower(text), '[._\-]+', ' ', 'g'), '[^a-z0-9\s]+', '', 'g'), '\y(s|season)\s*\d{1,2}\y', '', 'g'));
                        END;
                        $$ LANGUAGE plpgsql IMMUTABLE;
                    """)
                    
                    await conn.execute("""
                        CREATE OR REPLACE FUNCTION update_movie_search_vector()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.clean_title := f_clean_text_for_search(NEW.title);
                            NEW.title_search_vector := setweight(to_tsvector('simple', COALESCE(NEW.clean_title, '')), 'A');
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
                except asyncpg.exceptions.InternalServerError as e:
                    # Ignore 'tuple concurrently updated' errors during startup
                    if "concurrently updated" in str(e):
                        logger.warning("DB Init race condition detected (ignoring as DB is likely initializing in another worker).")
                    else:
                        raise e

            logger.info("NeonDB (Postgres) initialized successfully.")
        except Exception as e:
            logger.critical(f"NeonDB init failed: {e}", exc_info=True)
            self.pool = None
            raise

    async def is_ready(self) -> bool:
        if self.pool is None:
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return True
        except (asyncpg.exceptions.PoolError, OSError, asyncio.TimeoutError) as e:
            logger.warning(f"NeonDB is_ready check fail: {e}")
            await self.close() 
            self.pool = None
            return False
        except Exception:
            return False

    async def close(self):
        if self.pool:
            try:
                await self.pool.close()
                logger.info("NeonDB (Postgres) pool close ho gaya।")
            except Exception as e:
                logger.error(f"NeonDB pool close karte waqt error: {e}")
            finally:
                self.pool = None

    async def get_movie_count(self) -> int:
        if not await self.is_ready():
            return -1
        try:
            async with self.pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM movies")
                return count if count is not None else 0
        except Exception as e:
            logger.error(f"NeonDB get_movie_count error: {e}", exc_info=True)
            return -1

    async def add_movie(self, message_id: int, channel_id: int, file_id: str, file_unique_id: str, imdb_id: str, title: str) -> bool:
        if not await self.is_ready(): 
            return False
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title, added_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (message_id, channel_id) DO UPDATE SET
                        file_id = EXCLUDED.file_id,
                        file_unique_id = EXCLUDED.file_unique_id,
                        imdb_id = EXCLUDED.imdb_id,
                        title = EXCLUDED.title,
                        added_date = EXCLUDED.added_date
                    """,
                    message_id, channel_id, file_id, file_unique_id, imdb_id, title, datetime.now(timezone.utc)
                )
            return True
        except asyncpg.exceptions.UniqueViolationError:
            return False
        except Exception as e:
            logger.error(f"NeonDB add_movie error: {e}", exc_info=True)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        if not await self.is_ready(): 
            return False
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute("DELETE FROM movies WHERE imdb_id = $1", imdb_id)
                deleted_count = int(result.split()[-1]) if result else 0
                return deleted_count > 0
        except Exception as e:
            logger.error(f"NeonDB remove_movie_by_imdb error: {e}", exc_info=True)
            return False

    async def rebuild_fts_vectors(self) -> int:
        if not await self.is_ready():
            return -1
        
        query = """
            UPDATE movies
            SET 
                clean_title = f_clean_text_for_search(title),
                title_search_vector = setweight(to_tsvector('simple', COALESCE(f_clean_text_for_search(title), '')), 'A')
            WHERE clean_title IS NULL OR clean_title != f_clean_text_for_search(title);
        """
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(query)
                updated_count = int(result.split()[-1]) if result else 0
                return updated_count
        except Exception as e:
            logger.error(f"NeonDB rebuild_fts_vectors error: {e}", exc_info=True)
            return -1

    async def find_and_delete_duplicates(self, batch_limit: int) -> Tuple[List[Tuple[int, int]], int]:
        if not await self.is_ready(): return ([], 0)
        
        query = """
        WITH ranked_movies AS (
            SELECT id, message_id, channel_id,
                ROW_NUMBER() OVER(PARTITION BY file_unique_id ORDER BY added_date DESC, message_id DESC) as rn
            FROM movies
        ),
        to_delete AS (
            SELECT id, message_id, channel_id FROM ranked_movies WHERE rn > 1
        ),
        delete_batch AS (
            SELECT id, message_id, channel_id FROM to_delete LIMIT $1
        ),
        deleted_rows AS (
            DELETE FROM movies WHERE id IN (SELECT id FROM delete_batch)
            RETURNING message_id, channel_id, imdb_id
        )
        SELECT 
            (SELECT COUNT(*) FROM to_delete) as total_duplicates_remaining,
            (SELECT array_agg(ARRAY[message_id, channel_id]) FROM deleted_rows) as messages_deleted_tg,
            (SELECT array_agg(imdb_id) FROM deleted_rows) as imdb_ids_deleted
        """
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, batch_limit)
                if not result or not result['messages_deleted_tg']:
                    return ([], 0)
                total_duplicates_found = result['total_duplicates_remaining']
                tg_messages_to_delete = [tuple(msg) for msg in result['messages_deleted_tg']]
                return (tg_messages_to_delete, total_duplicates_found)
        except Exception as e:
            logger.error(f"NeonDB find_and_delete_duplicates error: {e}", exc_info=True)
            return ([], 0)

    async def get_unique_movies_for_backup(self) -> List[Tuple[int, int]]:
        if not await self.is_ready(): return []
        query = "SELECT DISTINCT ON (file_unique_id) message_id, channel_id FROM movies ORDER BY file_unique_id, added_date DESC, message_id DESC;"
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [(row['message_id'], row['channel_id']) for row in rows]
        except Exception as e:
            logger.error(f"NeonDB get_unique_movies_for_backup error: {e}", exc_info=True)
            return []

    async def sync_from_mongo(self, mongo_movies: List[Dict]) -> int:
        if not await self.is_ready() or not mongo_movies: return 0
        data_to_insert = []
        for movie in mongo_movies:
            unique_id = movie.get('file_unique_id') or movie.get('file_id')
            if unique_id and movie.get('file_id') and movie.get('title'):
                data_to_insert.append((
                    movie.get('message_id'), movie.get('channel_id'), movie.get('file_id'),
                    unique_id, movie.get('imdb_id'), movie.get('title')
                ))
        if not data_to_insert: return 0
        query = """
        INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (message_id, channel_id) DO NOTHING
        """
        try:
            async with self.pool.acquire() as conn:
                status = await conn.executemany(query, data_to_insert)
                return int(status.split()[-1]) if status else 0
        except Exception as e:
            logger.error(f"NeonDB sync_from_mongo error: {e}", exc_info=True)
            return 0

    async def check_neon_clean_title(self) -> Dict | None:
        if not await self.is_ready(): return None
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT title, clean_title FROM movies WHERE clean_title IS NOT NULL AND clean_title != '' LIMIT 1")
                if row: return {"title": row['title'], "clean_title": row['clean_title']}
                row_bad = await conn.fetchrow("SELECT title, clean_title FROM movies WHERE clean_title IS NULL OR clean_title = '' LIMIT 1")
                if row_bad: return {"title": row_bad['title'], "clean_title": "--- KHAALI HAI ---"}
                return {"title": "N/A", "clean_title": "DB Khaali Hai"}
        except Exception as e:
            return {"title": "Error", "clean_title": str(e)}
