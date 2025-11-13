# -*- coding: utf-8 -*-
import logging
import asyncpg
from typing import List, Dict

logger = logging.getLogger("bot.neondb")

class NeonDB:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None

    async def init_db(self):
        if not self.database_url: return
        try:
            self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
            async with self.pool.acquire() as conn:
                # Enable Trigram Extension for Fuzzy Search
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        id SERIAL PRIMARY KEY,
                        imdb_id TEXT, title TEXT, year TEXT,
                        message_id BIGINT, channel_id BIGINT,
                        file_id TEXT, file_unique_id TEXT, search_text TEXT
                    );
                """)
                # Create Index for Speed
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_trgm ON movies USING gin (title gin_trgm_ops);")
                
                # Auto-fix columns if missing
                try: await conn.execute("ALTER TABLE movies ADD COLUMN search_text TEXT;")
                except: pass
                try: await conn.execute("ALTER TABLE movies ADD COLUMN year TEXT;")
                except: pass
                
        except Exception as e: 
            logger.error(f"Neon Init Fail: {e}")

    async def is_ready(self):
        if not self.pool: return False
        try:
            async with self.pool.acquire() as conn: await conn.execute("SELECT 1")
            return True
        except: return False

    async def add_movie(self, mid, cid, fid, fuid, imdb, title, year):
        if not await self.is_ready(): await self.init_db()
        if not self.pool: return
        
        # Combine title + year for better search text
        st = f"{title} {year or ''}".lower()
        
        try:
            async with self.pool.acquire() as conn:
                exist = await conn.fetchval("SELECT 1 FROM movies WHERE file_unique_id=$1", fuid)
                if exist:
                    await conn.execute("""
                        UPDATE movies 
                        SET message_id=$1, channel_id=$2, imdb_id=$3, title=$4, search_text=$5, year=$6 
                        WHERE file_unique_id=$7
                    """, mid, cid, imdb, title, st, year, fuid)
                else:
                    await conn.execute("""
                        INSERT INTO movies 
                        (message_id, channel_id, file_id, file_unique_id, imdb_id, title, search_text, year) 
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """, mid, cid, fid, fuid, imdb, title, st, year)
        except Exception as e:
            logger.error(f"Neon Add Error: {e}")

    async def neondb_search(self, query: str, limit: int = 20):
        """
        Advanced Fuzzy Search.
        If 'Web Series Mode' regex is detected, use Regex.
        Otherwise, use strict Trigram Similarity (handles Typos like 'Katra' -> 'Kantara').
        """
        if not await self.is_ready(): await self.init_db()
        if not self.pool: return []
        
        try:
            async with self.pool.acquire() as conn:
                # 1. Web Series Regex Logic (passed from bot.py)
                if "(" in query and "|" in query:
                     rows = await conn.fetch("SELECT imdb_id, title, year FROM movies WHERE title ~* $1 LIMIT $2", query, limit)
                
                else:
                     # 2. Super Fuzzy Search for Movies
                     # Lower threshold means more lenient search (0.3 = 30% match required)
                     await conn.execute("SET pg_trgm.similarity_threshold = 0.3;")
                     
                     # Uses % operator for similarity
                     rows = await conn.fetch("""
                        SELECT imdb_id, title, year 
                        FROM movies 
                        WHERE title % $1 
                        ORDER BY similarity(title, $1) DESC 
                        LIMIT $2
                     """, query, limit)
                     
                return [{"imdb_id": r["imdb_id"], "title": r["title"], "year": r["year"]} for r in rows]
        except Exception as e: 
            logger.error(f"Neon Search Error: {e}")
            return []

    async def get_movie_count(self):
        if not await self.is_ready(): return 0
        try:
            async with self.pool.acquire() as conn: return await conn.fetchval("SELECT COUNT(*) FROM movies")
        except: return 0
    
    async def sync_from_mongo(self, batch):
        if not await self.is_ready(): await self.init_db()
        if not self.pool: return
        async with self.pool.acquire() as conn:
            for m in batch:
                try: 
                    await self.add_movie(
                        m['message_id'], m['channel_id'], m['file_id'], m['file_unique_id'], 
                        m['imdb_id'], m['title'], m.get('year')
                    )
                except: pass

    async def get_unique_movies_for_backup(self):
        if not await self.is_ready(): return []
        async with self.pool.acquire() as conn: return await conn.fetch("SELECT message_id, channel_id FROM movies")

    async def find_and_delete_duplicates(self, limit=100):
        if not await self.is_ready(): return [], 0
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT message_id, channel_id 
                FROM movies 
                WHERE id NOT IN (SELECT MIN(id) FROM movies GROUP BY file_unique_id) 
                LIMIT $1
            """, limit)
            return rows, len(rows)
            
    async def close(self):
        if self.pool: await self.pool.close() to be 
