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
                # 1. Trigram extension (fuzzy matching ke liye)
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                
                # 2. Movies table
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
                
                # 3. Indexes
                # Duplicate files (channel backup/cleanup) ke liye
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON movies (file_unique_id);")
                # Duplicate messages (sync) ke liye
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_channel ON movies (message_id, channel_id);")
                # FTS search ke liye
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_search_vector ON movies USING GIN(title_search_vector);")

                # 4. FTS Trigger Function (title update hone par vector update kare)
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
                
                # 5. FTS Trigger (INSERT ya UPDATE par function call kare)
                await conn.execute("""
                    DROP TRIGGER IF EXISTS ts_movie_title_update ON movies;
                    CREATE TRIGGER ts_movie_title_update
                    BEFORE INSERT OR UPDATE ON movies
                    FOR EACH ROW
                    EXECUTE FUNCTION update_movie_search_vector();
                """)

            logger.info("NeonDB (Postgres) connection pool, table, aur FTS initialize ho gaya.")
        except Exception as e:
            logger.critical(f"NeonDB pool initialize nahi ho paya: {e}", exc_info=True)
            self.pool = None
            raise

    async def is_ready(self) -> bool:
        """Check karein ki connection pool active hai."""
        if self.pool is None:
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return True
        except (asyncpg.exceptions.PoolError, OSError, asyncio.TimeoutError) as e:
            logger.warning(f"NeonDB is_ready check fail: {e}")
            # Pool kharaab ho sakta hai, close karein taaki agla call naya banaye
            await self.close() 
            self.pool = None
            return False
        except Exception:
            return False

    async def close(self):
        """Connection pool ko gracefully close karta hai."""
        if self.pool:
            try:
                await self.pool.close()
                logger.info("NeonDB (Postgres) pool close ho gaya.")
            except Exception as e:
                logger.error(f"NeonDB pool close karte waqt error: {e}")
            finally:
                self.pool = None

    async def get_movie_count(self) -> int:
        """NeonDB mein total entries count karta hai."""
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
        """
        NeonDB mein ek movie entry add karta hai.
        FTS trigger 'title_search_vector' ko automatically update kar dega.
        """
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
            # Yeh 'ON CONFLICT' ke kaaran nahi hona chahiye, par safety ke liye hai
            logger.warning(f"NeonDB: Message {message_id} pehle se exists hai (UniqueViolation).")
            return False
        except Exception as e:
            logger.error(f"NeonDB add_movie error: {e}", exc_info=True)
            return False

    async def neondb_search(self, query: str, limit: int = 20) -> List[Dict]:
        """NeonDB mein Full-Text Search (FTS) aur Trigram (Fuzzy) Search karta hai."""
        if not await self.is_ready(): 
            logger.error("NeonDB pool (neondb_search) ready nahi hai.")
            return []
        
        results = []
        
        # 1. Koshish: Full-Text Search (FTS)
        # 'simple' dictionary (stemming nahi) aur 'websearch_to_tsquery' (complex queries ke liye)
        search_query_fts = """
            SELECT imdb_id, title, ts_rank(title_search_vector, query) as rank
            FROM movies, websearch_to_tsquery('simple', $1) AS query
            WHERE title_search_vector @@ query
            ORDER BY rank DESC
            LIMIT $2;
        """
        
        try:
            async with self.pool.acquire() as conn:
                rows_fts = await conn.fetch(search_query_fts, query, limit)
                if rows_fts:
                    results = [
                        {'imdb_id': row['imdb_id'], 'title': row['title'], 'year': None} 
                        for row in rows_fts
                    ]
        except Exception as e:
            logger.warning(f"NeonDB FTS search fail hua '{query}': {e}")
            results = [] # FTS fail, trigram try karein

        # 2. Fallback: Trigram (pg_trgm) Search (Fuzzy 'LIKE')
        if not results:
            logger.debug(f"Neon FTS fail hua '{query}', ab trigram (similarity) search...")
            trigram_query = """
                SELECT imdb_id, title, similarity(title, $1) AS sml
                FROM movies
                WHERE title % $1 -- 'similarity' check (index use karega agar bana ho)
                ORDER BY sml DESC, title
                LIMIT $2;
            """
            try:
                 async with self.pool.acquire() as conn:
                    # Trigram index ke liye GIN/GIST index ki zaroorat hoti hai title par
                    # Abhi hum FTS index par focus kar rahe hain, yeh thoda slow ho sakta hai
                    rows_trg = await conn.fetch(trigram_query, query, limit)
                    results = [
                        {'imdb_id': row['imdb_id'], 'title': row['title'], 'year': None} 
                        for row in rows_trg
                    ]
            except Exception as e:
                logger.error(f"NeonDB trigram search bhi fail hua '{query}': {e}", exc_info=True)
                return []
        
        # Note: NeonDB table mein 'year' column nahi hai, isliye None bhej rahe hain
        return results


    async def find_and_delete_duplicates(self, batch_limit: int) -> Tuple[List[Tuple[int, int]], int]:
        """
        file_unique_id ke basis par duplicates dhoondta hai.
        Har group mein sabse naya message (by added_date) rakhta hai aur purane delete karta hai.
        Returns: (List of (msg_id, chat_id) to delete from Telegram, total duplicates found)
        """
        if not await self.is_ready(): return ([], 0)
        
        # Yeh query pehle duplicates ko rank karta hai (rn > 1 waale duplicates hain)
        # Phir unhe DB se delete karta hai aur unki (msg_id, channel_id) list return karta hai
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
            RETURNING message_id, channel_id
        )
        SELECT 
            (SELECT COUNT(*) FROM to_delete) as total_duplicates_remaining,
            (SELECT array_agg(ARRAY[message_id, channel_id]) FROM deleted_rows) as messages_deleted_tg
        """
        
        try:
            async with self.pool.acquire() as conn:
                # Query ek hi transaction mein find aur delete karti hai
                result = await conn.fetchrow(query, batch_limit)
                
                if not result or not result['messages_deleted_tg']:
                    return ([], 0) # Koi duplicates nahi mile/delete hue

                total_duplicates_found = result['total_duplicates_remaining']
                tg_messages_to_delete = [tuple(msg) for msg in result['messages_deleted_tg']]
                
                logger.info(f"NeonDB: {len(tg_messages_to_delete)} duplicate entries DB se clean kiye.")
                
                return (tg_messages_to_delete, total_duplicates_found)
                
        except Exception as e:
            logger.error(f"NeonDB find_and_delete_duplicates error: {e}", exc_info=True)
            return ([], 0)

    async def get_unique_movies_for_backup(self) -> List[Tuple[int, int]]:
        """
        Backup ke liye unique files ki list nikalta hai (har file ka sabse naya message_id).
        Returns: List of (message_id, channel_id)
        """
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
        """MongoDB se data ko bulk-insert/update karta hai."""
        if not await self.is_ready() or not mongo_movies:
            if not mongo_movies: logger.warning("NeonDB Sync: Sync ke liye koi data nahi mila.")
            return 0
            
        data_to_insert = []
        for movie in mongo_movies:
            # FIX: Agar 'file_unique_id' nahi hai, toh 'file_id' ko fallback ki tarah use karein
            unique_id_for_db = movie.get('file_unique_id') or movie.get('file_id')
            
            # Zaroori fields check karein
            if not all([
                unique_id_for_db,
                movie.get('file_id'),
                movie.get('message_id'),
                movie.get('channel_id')
            ]):
                logger.warning(f"NeonDB Sync: Movie skip (missing data): {movie.get('title')}")
                continue

            data_to_insert.append((
                movie.get('message_id'),
                movie.get('channel_id'),
                movie.get('file_id'),
                unique_id_for_db, # Yahan fallback value use hogi
                movie.get('imdb_id'),
                movie.get('title')
            ))

        if not data_to_insert:
            logger.warning("NeonDB Sync: Mongo data se koi valid entry nahi mili.")
            return 0
            
        # ON CONFLICT (message_id, channel_id) DO NOTHING
        # Hum 'DO NOTHING' use kar rahe hain kyonki yeh 'DO UPDATE' se tez hai
        # Agar nayi file aati hai toh 'add_movie' use 'DO UPDATE' se handle kar lega
        query = """
        INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (message_id, channel_id) DO NOTHING
        """
        
        try:
            async with self.pool.acquire() as conn:
                # executemany 'None' return karta hai, status se count milega
                status = await conn.executemany(query, data_to_insert)
                
                # 'INSERT 0 100' jaisa status parse karein
                if status:
                    inserted_count = int(status.split()[-1])
                    logger.info(f"NeonDB Sync: {inserted_count} naye records insert kiye.")
                
                processed_count = len(data_to_insert)
                return processed_count
        except Exception as e:
            logger.error(f"NeonDB sync_from_mongo error: {e}", exc_info=True)
            return 0
