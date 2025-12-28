# neondb.py
import logging
import asyncio
import asyncpg
import re
from typing import List, Dict, Tuple, Any
from datetime import datetime, timezone
import os # Naya import

logger = logging.getLogger("bot.neondb")

# --- Helper function (FUZZY SEARCH ke saath SYNCHRONIZED kiya gaya) ---
def clean_text_for_search(text: str) -> str:
    """Cleans text for search indexing (Synchronized with bot.py's safer version)।"""
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

class NeonDB:
    """
    NeonDB (Postgres) ke liye Async Interface Class।
    """
    
    def __init__(self, database_url: str, db_primary_instance: Any = None):
        # db_primary_instance: MongoDB instance (for cross-process lock)
        self.database_url = database_url
        self.pool: asyncpg.Pool | None = None
        self._init_lock = asyncio.Lock() # Internal Lock (Same Process)
        self._initialized = False 
        self.db_primary = db_primary_instance # MongoDB reference

    async def init_db(self):
        """
        Connection pool banata hai aur zaroori tables, extensions, 
        aur FTS/Trigram triggers ko ensure karta hai।
        """
        if self._initialized:
             logger.debug("NeonDB already initialized by this worker.")
             return
             
        # --- FIX: Cross-Process Lock (MongoDB se Lock lenge) ---
        lock_name = "neon_schema_init_lock"
        
        if self.db_primary:
            # Rule: Har error possibility ko dimag me rakh ke kam karo.
            logger.info("Attempting to acquire cross-process lock via MongoDB...")
            
            # Wait loop: Agar lock nahi mila to thoda wait karke dobara check karein
            max_attempts = 30
            for attempt in range(max_attempts):
                # safe_db_call is intentionally avoided here as it can mask critical startup issues.
                # FIX: Timeout 120s badhaya taaki slow startup handle ho sake
                lock_acquired = await self.db_primary.acquire_cross_process_lock(lock_name, 120) 
                
                if lock_acquired:
                    logger.warning("✅ MongoDB Lock acquired. Proceeding with NeonDB Schema setup.")
                    break
                
                if attempt == max_attempts - 1:
                    logger.critical(f"❌ Failed to acquire lock after {max_attempts} attempts. Another process might be stuck.")
                    # Startup fail na ho isliye hum continue karte hain (Race condition risk hai par bot chalega)
                    logger.warning("⚠️ Forcing initialization without lock due to timeout.")
                    break
                    
                await asyncio.sleep(1.0) # Wait for the other worker to finish or die
        
        # --- END FIX ---
        
        # Internal Lock (Safety inside this worker)
        async with self._init_lock:
            if self._initialized:
                 # Lock jaldi release karein agar pehle se initialized ho
                 if self.db_primary: await self.db_primary.release_cross_process_lock(lock_name)
                 return
            
            try:
                # FIX: Connection pool settings for Free Tier
                self.pool = await asyncpg.create_pool(
                    self.database_url,
                    min_size=0, # Idle connections close hone do
                    max_size=3, # Max 3 connections for Free Tier safety
                    command_timeout=60,
                    server_settings={'application_name': 'MovieBot-Worker'}
                )
                if self.pool is None:
                    raise Exception("Pool creation returned None")

                async with self.pool.acquire() as conn:
                    # Table and index creation must be sequential
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
                            clean_title TEXT,
                            added_date TIMESTAMPTZ DEFAULT NOW(),
                            title_search_vector TSVECTOR
                        );
                    """)
                    
                    # --- Purane table mein 'clean_title' column jodein ---
                    try:
                        await conn.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS clean_title TEXT;")
                        await conn.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS title_search_vector TSVECTOR;")
                        logger.info("Verified 'clean_title' & 'title_search_vector' columns exist in NeonDB।")
                    except Exception as e:
                        logger.warning(f"ALTER TABLE command mein mamooli error (shayad pehle se tha): {e}")

                    # --- Python logic ko SQL FUNCTION ke roop mein banayein ---
                    await conn.execute("""
                        CREATE OR REPLACE FUNCTION f_clean_text_for_search(text TEXT)
                        RETURNS TEXT AS $$
                        DECLARE
                            cleaned_text TEXT;
                        BEGIN
                            IF text IS NULL THEN
                                RETURN '';
                            END IF;
                            
                            cleaned_text := lower(text);
                            -- Dots/Underscores ko space se badle (database.py jaisa)
                            cleaned_text := regexp_replace(cleaned_text, '[._\-]+', ' ', 'g');
                            -- Sirf a-z aur 0-9 rakhein
                            cleaned_text := regexp_replace(cleaned_text, '[^a-z0-9\s]+', '', 'g');
                            -- Season info remove karein
                            cleaned_text := regexp_replace(cleaned_text, '\y(s|season)\s*\d{1,2}\y', '', 'g');
                            -- Extra space hatayein
                            cleaned_text := trim(regexp_replace(cleaned_text, '\s+', ' ', 'g'));
                            
                            RETURN cleaned_text;
                        END;
                        $$ LANGUAGE plpgsql IMMUTABLE;
                    """)
                    
                    await conn.execute("DROP INDEX IF EXISTS idx_unique_file;")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON movies (file_unique_id);")
                    await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_channel ON movies (message_id, channel_id);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON movies (imdb_id);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_search_vector ON movies USING GIN(title_search_vector);")
                    await conn.execute("CREATE INDEX IF NOT EXISTS idx_clean_title_trgm ON movies USING GIN (clean_title gin_trgm_ops);")

                    # --- Trigger ko naye SQL FUNCTION ka istemal karayein ---
                    await conn.execute("""
                        CREATE OR REPLACE FUNCTION update_movie_search_vector()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.clean_title := f_clean_text_for_search(NEW.title);
                            NEW.title_search_vector :=
                                setweight(to_tsvector('simple', COALESCE(f_clean_text_for_search(NEW.title), '')), 'A');
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

                logger.info("NeonDB (Postgres) connection pool, table, aur FUZZY/Trigram initialize ho gaya।")
                self._initialized = True
            except Exception as e:
                logger.critical(f"NeonDB pool initialize nahi ho paya: {e}", exc_info=True)
                self.pool = None
                raise
            finally:
                 # Lock release karna bahut zaroori hai, chahe error aaye ya na aaye
                 if self.db_primary: await self.db_primary.release_cross_process_lock(lock_name)
                 
        # --- END FIX: Lock Released ---

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
            logger.error("NeonDB pool (get_movie_count) ready nahi hai।")
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
            logger.error("NeonDB pool (add_movie) ready nahi hai।")
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
            logger.warning(f"NeonDB: Message {message_id} pehle se exists hai (UniqueViolation)।")
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
        """
        Purane data ke liye 'clean_title' aur 'title_search_vector' ko update karta hai।
        """
        if not await self.is_ready():
            logger.error("NeonDB pool (rebuild_fts_vectors) ready nahi hai।")
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
                logger.info(f"NeonDB: {updated_count} NULL/old FTS/CleanTitle vectors ko rebuild kiya।")
                return updated_count
        except Exception as e:
            logger.error(f"NeonDB rebuild_fts_vectors error: {e}", exc_info=True)
            return -1

    async def find_and_delete_duplicates(self, batch_limit: int) -> Tuple[List[Tuple[int, int]], int]:
        if not await self.is_ready(): return ([], 0)
        
        query = """
        WITH ranked_movies AS (
            SELECT
                id,
                message_id,
                channel_id,
                ROW_NUMBER() OVER(
                    PARTITION BY file_unique_id 
                    ORDER BY added_date DESC, message_id DESC
                ) as rn
            FROM movies
        ),
        to_delete AS (
            SELECT id, message_id, channel_id
            FROM ranked_movies
            WHERE rn > 1
        ),
        delete_batch AS (
            SELECT id, message_id, channel_id
            FROM to_delete
            LIMIT $1
        ),
        deleted_rows AS (
            DELETE FROM movies
            WHERE id IN (SELECT id FROM delete_batch)
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
                
                logger.info(f"NeonDB: {len(tg_messages_to_delete)} duplicate entries DB se clean kiye।")
                
                return (tg_messages_to_delete, total_duplicates_found)
                
        except Exception as e:
            logger.error(f"NeonDB find_and_delete_duplicates error: {e}", exc_info=True)
            return ([], 0)

    async def get_unique_movies_for_backup(self) -> List[Tuple[int, int]]:
        if not await self.is_ready(): return []
        
        query = """
        SELECT DISTINCT ON (file_unique_id)
            message_id,
            channel_id
        FROM movies
        ORDER BY file_unique_id, added_date DESC, message_id DESC;
        """
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [(row['message_id'], row['channel_id']) for row in rows]
        except Exception as e:
            logger.error(f"NeonDB get_unique_movies_for_backup error: {e}", exc_info=True)
            return []

    async def sync_from_mongo(self, mongo_movies: List[Dict]) -> int:
        if not await self.is_ready() or not mongo_movies:
            if not mongo_movies: logger.warning("NeonDB Sync: Sync ke liye koi data nahi mila।")
            return 0
            
        data_to_insert = []
        for movie in mongo_movies:
            unique_id_for_db = movie.get('file_unique_id') or movie.get('file_id')
            
            if not all([
                unique_id_for_db,
                movie.get('file_id'),
                movie.get('message_id') is not None,
                movie.get('channel_id') is not None,
                movie.get('title') # Title hona zaroori hai
            ]):
                logger.warning(f"NeonDB Sync: Movie skip (missing data): {movie.get('title')}")
                continue

            data_to_insert.append((
                movie.get('message_id'),
                movie.get('channel_id'),
                movie.get('file_id'),
                unique_id_for_db,
                movie.get('imdb_id'),
                movie.get('title')
                # clean_title ab DB mein trigger se banega
            ))

        if not data_to_insert:
            logger.warning("NeonDB Sync: Mongo data se koi valid entry nahi mili।")
            return 0
            
        query = """
        INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (message_id, channel_id) DO NOTHING
        """
        
        try:
            async with self.pool.acquire() as conn:
                status = await conn.executemany(query, data_to_insert)
                
                if status:
                    inserted_count = int(status.split()[-1])
                    logger.info(f"NeonDB Sync: {inserted_count} naye records insert kiye।")
                
                processed_count = len(data_to_insert)
                return processed_count
        except Exception as e:
            logger.error(f"NeonDB sync_from_mongo error: {e}", exc_info=True)
            return 0

    # --- NAYA DIAGNOSTIC FUNCTION ---
    async def check_neon_clean_title(self) -> Dict | None:
        if not await self.is_ready(): return None
        try:
            async with self.pool.acquire() as conn:
                # Find one row that *has* a clean_title
                row = await conn.fetchrow(
                    "SELECT title, clean_title FROM movies WHERE clean_title IS NOT NULL AND clean_title != '' LIMIT 1"
                )
                if row:
                    return {"title": row['title'], "clean_title": row['clean_title']}
                
                # If none found, find one that *doesn't*
                row_bad = await conn.fetchrow(
                    "SELECT title, clean_title FROM movies WHERE clean_title IS NULL OR clean_title = '' LIMIT 1"
                )
                if row_bad:
                    return {"title": row_bad['title'], "clean_title": "--- KHAALI HAI (Run /rebuild_neon_vectors) ---"}
                return {"title": "N/A", "clean_title": "DB Khaali Hai"}
        except Exception as e:
            return {"title": "Error", "clean_title": str(e)}
