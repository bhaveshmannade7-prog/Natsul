# neondb.py
import logging
import asyncio
import asyncpg
from typing import List, Dict, Tuple
from datetime import datetime, timezone

logger = logging.getLogger("bot.neondb")

class NeonDB:
    """NeonDB (Postgres) ke liye Async Interface Class."""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: asyncpg.Pool | None = None

    async def init_db(self):
        """
        Connection pool banata hai aur zaroori tables, extensions, 
        aur FTS triggers ko ensure karta hai.
        """
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=60,
                server_settings={'application_name': 'MovieBot'}
            )
            if self.pool is None:
                raise Exception("Pool creation returned None")

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
                
                # --- YEH NAYA FIX HAI ---
                # 1. Purane/Galat UNIQUE index ko hatayein (agar maujood hai)
                logger.info("Purane 'idx_unique_file' index ko drop kiya ja raha hai (agar hai)...")
                await conn.execute("DROP INDEX IF EXISTS idx_unique_file;")
                
                # 2. Sahi (NON-UNIQUE) index banayein
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON movies (file_unique_id);")
                # --- END FIX ---
                
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_channel ON movies (message_id, channel_id);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_search_vector ON movies USING GIN(title_search_vector);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON movies (imdb_id);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_title_trgm ON movies USING GIN (title gin_trgm_ops);")


                await conn.execute("""
                    CREATE OR REPLACE FUNCTION update_movie_search_vector()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.title_search_vector :=
                            setweight(to_tsvector('simple', COALESCE(NEW.title, '')), 'A');
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

            logger.info("NeonDB (Postgres) connection pool, table, aur FTS/Trigram initialize ho gaya.")
        except Exception as e:
            logger.critical(f"NeonDB pool initialize nahi ho paya: {e}", exc_info=True)
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
                logger.info("NeonDB (Postgres) pool close ho gaya.")
            except Exception as e:
                logger.error(f"NeonDB pool close karte waqt error: {e}")
            finally:
                self.pool = None

    async def get_movie_count(self) -> int:
        if not await self.is_ready():
            logger.error("NeonDB pool (get_movie_count) ready nahi hai.")
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
            logger.error("NeonDB pool (add_movie) ready nahi hai.")
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
            logger.warning(f"NeonDB: Message {message_id} pehle se exists hai (UniqueViolation).")
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

    # --- YEH FUNCTION REPLACE KIYA GAYA HAI ---
    async def neondb_search(self, query: str, limit: int = 20) -> List[Dict]:
        if not await self.is_ready(): 
            logger.error("NeonDB pool (neondb_search) ready nahi hai.")
            return []
        
        # --- NAYA LOGIC: FTS aur Trigram ko combine karein ---
        
        # 1. Full-Text Search (FTS) - Sahi words aur multiple terms ke liye
        search_query_fts = """
            SELECT imdb_id, title, ts_rank(title_search_vector, query) as rank
            FROM movies, websearch_to_tsquery('simple', $1) AS query
            WHERE title_search_vector @@ query
            ORDER BY rank DESC
            LIMIT $2;
        """
        
        # 2. Trigram (pg_trgm) Search - Typos aur galat spellings ke liye
        trigram_query = """
            SELECT imdb_id, title, similarity(title, $1) AS sml
            FROM movies
            WHERE title % $1
            AND similarity(title, $1) > 0.2  -- Threshold 0.25 se 0.2 kar diya hai (zyada typos ke liye)
            ORDER BY sml DESC, title
            LIMIT $2;
        """
        
        fts_rows = []
        trg_rows = []
        
        try:
            async with self.pool.acquire() as conn:
                # Dono queries ko parallel (ek saath) run karein
                # return_exceptions=True taaki ek fail hone par doosra crash na ho
                results = await asyncio.gather(
                    conn.fetch(search_query_fts, query, limit),
                    conn.fetch(trigram_query, query, limit),
                    return_exceptions=True
                )
                
                if isinstance(results[0], list):
                    fts_rows = results[0]
                else:
                    logger.error(f"NeonDB FTS search fail hua '{query}': {results[0]}", exc_info=False)

                if isinstance(results[1], list):
                    trg_rows = results[1]
                else:
                    logger.error(f"NeonDB trigram search fail hua '{query}': {results[1]}", exc_info=False)

        except Exception as e:
            logger.error(f"NeonDB connection pool (gather) fail hua '{query}': {e}", exc_info=True)
            return [] # Pool fail hua toh kuch nahi kar sakte

        # 3. Results ko Merge (de-duplicate) karein
        merged_results = {}
        
        # Pehle FTS results daalein (yeh zyada relevant maane jayenge)
        if fts_rows:
            logger.debug(f"FTS ne {len(fts_rows)} results diye.")
            for row in fts_rows:
                # Hum 'imdb_id' ko key bana rahe hain taaki duplicates na aayein
                if row['imdb_id'] not in merged_results:
                    merged_results[row['imdb_id']] = {
                        'imdb_id': row['imdb_id'], 
                        'title': row['title'], 
                        'year': None # NeonDB se year nahi aa raha
                    }
                    
        # Phir Trigram results daalein (jo FTS mein nahi mile)
        if trg_rows:
            logger.debug(f"Trigram ne {len(trg_rows)} results diye.")
            for row in trg_rows:
                 if row['imdb_id'] not in merged_results:
                    merged_results[row['imdb_id']] = {
                        'imdb_id': row['imdb_id'], 
                        'title': row['title'], 
                        'year': None
                    }

        # Final list banayein, limit yahaan apply karein
        final_list = list(merged_results.values())[:limit]
        
        if not final_list:
             logger.debug(f"FTS aur Trigram, dono ko '{query}' ke liye kuch nahi mila.")

        return final_list
    # --- END REPLACED FUNCTION ---

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
                
                logger.info(f"NeonDB: {len(tg_messages_to_delete)} duplicate entries DB se clean kiye.")
                
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
            if not mongo_movies: logger.warning("NeonDB Sync: Sync ke liye koi data nahi mila.")
            return 0
            
        data_to_insert = []
        for movie in mongo_movies:
            unique_id_for_db = movie.get('file_unique_id') or movie.get('file_id')
            
            if not all([
                unique_id_for_db,
                movie.get('file_id'),
                movie.get('message_id') is not None,
                movie.get('channel_id') is not None
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
            ))

        if not data_to_insert:
            logger.warning("NeonDB Sync: Mongo data se koi valid entry nahi mili.")
            return 0
            
        query = """
        INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (message_id, channel_id) DO NOTHING
        """
        
        try:
            async with self.pool.acquire() as conn:
                # Ab yeh 'executemany' kaam karega kyonki galat index hata diya gaya hai
                status = await conn.executemany(query, data_to_insert)
                
                if status:
                    inserted_count = int(status.split()[-1])
                    logger.info(f"NeonDB Sync: {inserted_count} naye records insert kiye.")
                
                processed_count = len(data_to_insert)
                return processed_count
        except Exception as e:
            logger.error(f"NeonDB sync_from_mongo error: {e}", exc_info=True)
            return 0
