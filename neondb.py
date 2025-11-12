# neondb.py
# YEH AAPKA NEON DB HAI (NEON_DATABASE_URL)
# Yeh duplicate detection aur search fallback ke liye hai.
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
                # Full-Text Search ke liye extension (fuzzy matching ke liye)
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                
                # Table create karein
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
                        title_search_vector TSVECTOR -- FTS ke liye naya column
                    );
                """)
                
                # Indexes
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON movies (file_unique_id);")
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_channel ON movies (message_id, channel_id);")
                
                # FTS Index
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_search_vector ON movies USING GIN(title_search_vector);")

                # FTS Trigger Function
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
                
                # FTS Trigger
                await conn.execute("""
                    DROP TRIGGER IF EXISTS ts_movie_title_update ON movies;
                    CREATE TRIGGER ts_movie_title_update
                    BEFORE INSERT OR UPDATE ON movies
                    FOR EACH ROW
                    EXECUTE FUNCTION update_movie_search_vector();
                """)

            logger.info("NeonDB (Postgres) connection pool, table, and FTS triggers initialized.")
        except Exception as e:
            logger.critical(f"Failed to initialize NeonDB pool: {e}", exc_info=True)
            self.pool = None # Init fail hua toh pool ko None set karein
            raise
            
    async def is_ready(self) -> bool:
        """Check karein ki connection pool initialize hua hai aur active hai."""
        if self.pool is None:
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return True
        except Exception:
            return False

    async def close(self):
        """Connection pool ko close karta hai."""
        if self.pool:
            await self.pool.close()
            logger.info("NeonDB (Postgres) pool closed.")

    async def get_movie_count(self) -> int:
        """NeonDB mein total entries count karta hai."""
        if not await self.is_ready():
            logger.error("NeonDB pool not initialized for get_movie_count.")
            return -1
        try:
            async with self.pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM movies")
                return count
        except Exception as e:
            logger.error(f"NeonDB get_movie_count error: {e}", exc_info=True)
            return -1

    async def add_movie(self, message_id: int, channel_id: int, file_id: str, file_unique_id: str, imdb_id: str, title: str) -> bool:
        """NeonDB mein ek movie entry add karta hai. FTS trigger automatically index karega."""
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
        except asyncpg.exceptions.UniqueViolationError:
            logger.warning(f"NeonDB: Message {message_id} pehle se exists hai (UniqueViolation).")
            return False
        except Exception as e:
            logger.error(f"NeonDB add_movie error: {e}", exc_info=True)
            return False

    async def neondb_search(self, query: str, limit: int = 20) -> List[Dict]:
        """NeonDB mein Full-Text Search (FTS) karta hai."""
        if not await self.is_ready(): return []
        
        search_query = """
            SELECT imdb_id, title, NULL AS year
            FROM movies, plainto_tsquery('simple', $1) AS q
            WHERE title_search_vector @@ q
            ORDER BY ts_rank(title_search_vector, q) DESC
            LIMIT $2;
        """
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(search_query, query, limit)
                
                if not rows:
                    logger.debug(f"Neon FTS failed for '{query}', trying trigram (LIKE) search...")
                    trigram_query = """
                        SELECT imdb_id, title, NULL AS year
                        FROM movies
                        ORDER BY similarity(title, $1) DESC
                        LIMIT $2;
                    """
                    rows = await conn.fetch(trigram_query, query, limit)

                # Results ko standard format mein convert karein
                return [
                    {'imdb_id': row['imdb_id'], 'title': row['title'], 'year': row['year']} 
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"NeonDB neondb_search error for '{query}': {e}", exc_info=True)
            return []


    async def find_and_delete_duplicates(self, batch_limit: int) -> Tuple[List[Tuple[int, int]], int]:
        """
        file_unique_id ke basis par duplicates dhoondta hai.
        Har group mein sabse naya message (by added_date) rakhta hai aur purane delete karta hai.
        Returns: (List of (msg_id, chat_id) to delete from Telegram, total duplicates found)
        """
        if not await self.is_ready(): return ([], 0)
        
        query = """
        WITH ranked_movies AS (
            SELECT
                id,
                message_id,
                channel_id,
                file_unique_id,
                added_date,
                ROW_NUMBER() OVER(PARTITION BY file_unique_id ORDER BY added_date DESC, message_id DESC) as rn
            FROM movies
        ),
        to_delete AS (
            SELECT id, message_id, channel_id
            FROM ranked_movies
            WHERE rn > 1
        ),
        total_duplicates AS (
            SELECT COUNT(*) as total_count FROM to_delete
        )
        SELECT 
            (SELECT total_count FROM total_duplicates), 
            array_agg(t.id) as ids_to_delete_db,
            array_agg(ARRAY[t.message_id, t.channel_id]) as messages_to_delete_tg
        FROM (
            SELECT * FROM to_delete LIMIT $1
        ) t;
        """
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, batch_limit)
                
                if not result or not result['ids_to_delete_db']:
                    return ([], 0)

                total_duplicates_found = result['total_count']
                db_ids_to_delete = result['ids_to_delete_db']
                tg_messages_to_delete = [tuple(msg) for msg in result['messages_to_delete_tg']]

                if not db_ids_to_delete:
                    return ([], 0)

                async with conn.transaction():
                    await conn.execute("DELETE FROM movies WHERE id = ANY($1)", db_ids_to_delete)
                
                logger.info(f"NeonDB: Cleaned up {len(db_ids_to_delete)} duplicate entries from NeonDB.")
                
                return (tg_messages_to_delete, total_duplicates_found)
                
        except Exception as e:
            logger.error(f"NeonDB find_and_delete_duplicates error: {e}", exc_info=True)
            return ([], 0)

    async def get_unique_movies_for_backup(self) -> List[Tuple[int, int]]:
        """
        Backup ke liye unique files ki list nikalta hai (sabse naya message_id har file ka).
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
        """MongoDB se data ko bulk-insert karta hai."""
        if not await self.is_ready() or not mongo_movies:
            return 0
            
        data_to_insert = []
        for movie in mongo_movies:
            # --- FIX: Agar 'file_unique_id' nahi hai, toh 'file_id' ko fallback ki tarah use karein ---
            unique_id_for_db = movie.get('file_unique_id') or movie.get('file_id')
            
            # Agar dono nahi hain, toh skip karein
            if not unique_id_for_db or not movie.get('file_id'):
                logger.warning(f"NeonDB Sync: Skipping movie (no unique_id or file_id): {movie.get('title')}")
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
            logger.warning("NeonDB Sync: Mongo data se koi valid entry nahi mili (ho sakta hai file_id bhi missing ho).")
            return 0
            
        query = """
        INSERT INTO movies (message_id, channel_id, file_id, file_unique_id, imdb_id, title)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (message_id, channel_id) DO NOTHING
        """
        
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(query, data_to_insert)
                
                processed_count = len(data_to_insert)
                return processed_count
        except Exception as e:
            logger.error(f"NeonDB sync_from_mongo error: {e}", exc_info=True)
            return 0
