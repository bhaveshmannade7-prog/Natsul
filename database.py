import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, and_, text, delete, update, UniqueConstraint
from sqlalchemy.exc import OperationalError, DisconnectionError, IntegrityError

logger = logging.getLogger("database")
Base = declarative_base()

# Placeholder ID for JSON imports
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

def clean_text_for_search(text: str) -> str:
    """Cleans text for saving to the clean_title column."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text) # Keep only letters and numbers
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text) # Remove season tags like s01, season 2
    text = re.sub(r'\s+', ' ', text) # Collapse multiple spaces
    return text.strip()

# ============ DATABASE MODELS (PostgreSQL) ============
class User(Base):
    __tablename__ = 'users'
    user_id = Column(BigInteger, primary_key=True)
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    joined_date = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True, index=True)
    last_active = Column(DateTime, default=datetime.utcnow, index=True)

class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    imdb_id = Column(String(50), unique=True, nullable=False, index=True) # Used as Algolia objectID
    title = Column(String, nullable=False) # For display & Algolia 'title'
    clean_title = Column(String, nullable=False, index=True) # Indexed in DB for potential future use
    year = Column(String(10), nullable=True) # For Algolia 'year'

    # DB only fields
    file_id = Column(String, nullable=False, index=True)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint('file_id', name='uq_file_id'),) # Ensure file_id is unique too


class Database:
    def __init__(self, database_url: str):
        connect_args = {}
        if '.com' in database_url or '.co' in database_url:
             connect_args['ssl'] = 'require'
             logger.info("External database URL detected, setting ssl='require'.")
        else:
             logger.info("Internal database URL detected, using default SSL (none).")

        # Previous attempt to put statement_cache_size here was incorrect place for SQLAlchemy 2.0+
        # connect_args['statement_cache_size'] = 0 # REMOVED FROM HERE

        if database_url.startswith('postgresql://'):
             database_url_mod = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            database_url_mod = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
        else:
            database_url_mod = database_url

        # Remove the URL parameter fix as well, it caused TypeError
        # database_url_mod = re.sub(r'[?&]statement_cache_size=\d+', '', database_url_mod)


        self.database_url = database_url_mod

        # --- YAHI HAI MUKHYA FIX (Database - Attempt 3) ---
        # Disable SQLAlchemy's compiled statement cache, which conflicts with pgbouncer
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            connect_args=connect_args, # SSL settings here
            pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300, pool_timeout=8,
            execution_options={"compiled_cache": None} # Disable cache here
        )
        logger.info("SQLAlchemy engine created with compiled_cache=None for pgbouncer compatibility.")
        # --- FIX END ---

        self.SessionLocal = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        logger.info(f"Database engine initialized (SSL: {connect_args.get('ssl', 'default')})")

    async def _handle_db_error(self, e: Exception) -> bool:
        """Handle connection errors by trying to dispose the engine."""
        if isinstance(e, (OperationalError, DisconnectionError)):
            logger.error(f"Critical DB connection error detected: {type(e).__name__}. Disposing engine pool.", exc_info=False) # Reduced log noise
            try:
                await self.engine.dispose()
                logger.info("DB engine pool disposed. New connections will be created on next request.")
                return True # Indicate that a retry might be worthwhile
            except Exception as re_e:
                logger.critical(f"Failed to dispose DB engine pool: {re_e}", exc_info=True)
                return False # Something is seriously wrong
        return False # Not a connection error we can handle this way

    async def init_db(self):
        """Initialize database tables, retrying on connection errors."""
        max_retries = 3
        last_exception = None
        for attempt in range(max_retries):
            try:
                async with self.engine.begin() as conn:
                    # Check connection explicitly first before creating tables
                    await conn.execute(text("SELECT 1"))
                    logger.info("Database connection successful.")
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized/verified successfully.")
                return # Success
            except Exception as e:
                last_exception = e
                logger.error(f"Failed to initialize DB (attempt {attempt+1}/{max_retries}): {e}", exc_info=False) # Less verbose log
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying DB initialization after {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue # Retry the loop
                else:
                    # If it's not a connection error we handle, or retries exhausted
                    logger.critical("DB initialization failed permanently.")
                    raise last_exception # Reraise the last exception

    async def add_user(self, user_id, username, first_name, last_name):
        """Add or update user, handling potential transient errors."""
        max_retries = 2
        last_exception = None
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    # Using merge for simplicity in upserting user data
                    user_obj = User(
                        user_id=user_id,
                        username=username,
                        first_name=first_name,
                        last_name=last_name,
                        last_active=datetime.utcnow(),
                        is_active=True
                    )
                    await session.merge(user_obj)
                    await session.commit()
                    return # Success
            except Exception as e:
                last_exception = e
                if session: await session.rollback()
                logger.warning(f"add_user attempt {attempt+1} failed for {user_id}: {e}", exc_info=False)
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(0.5); continue # Quick retry for connection issues
                else:
                    break # Don't retry other errors or after max retries
        logger.error(f"add_user failed permanently for {user_id}: {last_exception}", exc_info=False)
        return

    async def deactivate_user(self, user_id: int):
        """Mark user as inactive (e.g., bot blocked)."""
        try:
            async with self.SessionLocal() as session:
                stmt = update(User).where(User.user_id == user_id).values(is_active=False)
                await session.execute(stmt)
                await session.commit()
                logger.info(f"Deactivated user {user_id} (likely bot blocked).")
        except Exception as e:
            logger.error(f"Failed to deactivate user {user_id}: {e}", exc_info=False)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        """Count active users within the specified time window."""
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                stmt = select(func.count(User.user_id)).where(User.last_active >= cutoff, User.is_active == True)
                result = await session.execute(stmt)
                return result.scalar_one()
        except Exception as e:
            await self._handle_db_error(e) # Attempt to handle if it's a connection error
            logger.error(f"get_concurrent_user_count error: {e}", exc_info=True)
            return 0 # Return 0 on error

    async def get_user_count(self) -> int:
        """Count total active users."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(func.count(User.user_id)).where(User.is_active == True)
                result = await session.execute(stmt)
                return result.scalar_one()
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"get_user_count error: {e}", exc_info=True)
            return 0

    async def get_movie_count(self) -> int:
        """Count total movies in the database."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(func.count(Movie.id))
                result = await session.execute(stmt)
                return result.scalar_one()
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"get_movie_count error: {e}", exc_info=True)
            return -1 # Indicate error for health check

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        """Fetch movie details by IMDB ID (for callback)."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie).filter(Movie.imdb_id == imdb_id)
                result = await session.execute(stmt)
                movie = result.scalar_one_or_none()
                if movie:
                    return {
                        'imdb_id': movie.imdb_id, 'title': movie.title, 'year': movie.year,
                        'file_id': movie.file_id, 'channel_id': movie.channel_id, 'message_id': movie.message_id,
                    }
                return None
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"get_movie_by_imdb error for {imdb_id}: {e}", exc_info=True)
            return None

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int):
        """Add or update a movie (Upsert logic)."""
        session = None
        last_exception = None
        for attempt in range(2): # Retry once on potential connection error
            try:
                async with self.SessionLocal() as session:
                    # Check for existing movie by unique constraints (imdb_id or file_id)
                    stmt = select(Movie).where(or_(Movie.imdb_id == imdb_id, Movie.file_id == file_id))
                    result = await session.execute(stmt)
                    movie = result.scalar_one_or_none()

                    clean_title_val = clean_text_for_search(title)

                    if movie:
                        # Update existing movie
                        movie.imdb_id = imdb_id # Update imdb_id if it changed (e.g., from auto_ to real)
                        movie.title = title
                        movie.clean_title = clean_title_val
                        movie.year = year
                        movie.message_id = message_id
                        movie.channel_id = channel_id
                        movie.file_id = file_id # Update file_id if needed
                        await session.commit()
                        return "updated"
                    else:
                        # Add new movie
                        new_movie = Movie(
                            imdb_id=imdb_id, title=title, clean_title=clean_title_val, year=year,
                            file_id=file_id, message_id=message_id, channel_id=channel_id
                        )
                        session.add(new_movie)
                        await session.commit()
                        return True # Added successfully
            except IntegrityError as e:
                 if session: await session.rollback()
                 # This usually means a unique constraint violation (imdb_id or file_id exists)
                 # which *should* have been caught by the select, but handle just in case.
                 logger.warning(f"add_movie IntegrityError (likely race condition or unexpected duplicate): {title} ({imdb_id}/{file_id}). Error: {e}")
                 return "duplicate" # Treat as duplicate
            except Exception as e:
                 last_exception = e
                 if session: await session.rollback()
                 logger.warning(f"add_movie attempt {attempt+1} failed for {title} ({imdb_id}): {e}", exc_info=False)
                 if await self._handle_db_error(e) and attempt == 0:
                     await asyncio.sleep(0.5); continue # Retry connection errors once
                 else:
                     break # Don't retry other errors

        logger.error(f"add_movie failed permanently for {title} ({imdb_id}): {last_exception}", exc_info=True)
        return False # Failed


    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        """Remove a movie by its IMDB ID."""
        try:
            async with self.SessionLocal() as session:
                stmt = delete(Movie).where(Movie.imdb_id == imdb_id)
                result = await session.execute(stmt)
                await session.commit()
                return result.rowcount > 0 # Return True if one or more rows were deleted
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=True)
            return False

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        """Mark users inactive if last_active is older than 'days'."""
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(days=days)
                # Count first (optional, but good for logging)
                count_stmt = select(func.count(User.user_id)).where(User.last_active < cutoff, User.is_active == True)
                count_result = await session.execute(count_stmt)
                count = count_result.scalar_one()

                if count > 0:
                    update_stmt = update(User).where(User.last_active < cutoff, User.is_active == True).values(is_active=False)
                    await session.execute(update_stmt)
                    await session.commit()
                    logger.info(f"Deactivated {count} users inactive for over {days} days.")
                else:
                    logger.info("No inactive users found to cleanup.")
                return count
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"cleanup_inactive_users error: {e}", exc_info=True)
            return 0

    async def rebuild_clean_titles(self) -> Tuple[int, int]:
        """Re-generates 'clean_title' for all movies. DB intensive."""
        updated_count = 0
        total_count = 0
        try:
            async with self.SessionLocal() as session:
                # Get total count first
                count_stmt = select(func.count(Movie.id))
                total_result = await session.execute(count_stmt)
                total_count = total_result.scalar_one()

                # Iterate and update in batches if needed, but simple update for now
                # This might lock the table for a while on large databases
                update_query = text(r"""
                    UPDATE movies
                    SET clean_title = trim(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'),
                                '\y(s|season)\s*\d{1,2}\y', '', 'g' -- Using \y for word boundaries
                            ),
                            '\s+', ' ', 'g'
                        )
                    )
                    WHERE clean_title IS NULL OR clean_title = '' OR clean_title != trim(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'),
                                '\y(s|season)\s*\d{1,2}\y', '', 'g'
                            ),
                            '\s+', ' ', 'g'
                        )
                    );
                """)
                # Execute returns a result proxy
                result_proxy = await session.execute(update_query)
                updated_count = result_proxy.rowcount # Get number of rows affected
                await session.commit()
                logger.info(f"Rebuilt clean_titles: Updated {updated_count} of {total_count} movie titles.")
                return (updated_count, total_count)
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=True)
            return (0, total_count) # Return 0 updated, but potentially correct total

    async def get_all_users(self) -> List[int]:
        """Get IDs of all active users for broadcast."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(User.user_id).where(User.is_active == True)
                result = await session.execute(stmt)
                return [row[0] for row in result.all()] # Fetch all results
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"get_all_users error: {e}", exc_info=True)
            return []

    async def get_all_movies_for_sync(self) -> List[Dict] | None:
        """Fetch all movies (essential fields) for Algolia sync."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie.imdb_id, Movie.title, Movie.year)
                result = await session.execute(stmt)
                movies = result.all() # Fetch all results as tuples

                # Convert to list of dicts required by Algolia
                return [
                    {
                        'objectID': m.imdb_id, # Must match imdb_id
                        'imdb_id': m.imdb_id,
                        'title': m.title,
                        'year': m.year
                    }
                    for m in movies # Iterate through the fetched tuples
                ]
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=True)
            return None # Indicate error by returning None


    # --- Export Functions (Remain largely the same) ---

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        """Fetch users for CSV export."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(User).limit(limit)
                result = await session.execute(stmt)
                users = result.scalars().all()
                return [{'user_id': u.user_id, 'username': u.username, 'first_name': u.first_name, 'last_name': u.last_name,
                         'joined_date': u.joined_date.isoformat() if u.joined_date else '',
                         'last_active': u.last_active.isoformat() if u.last_active else '', 'is_active': u.is_active} for u in users]
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"export_users error: {e}", exc_info=True)
            return []

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        """Fetch movies for CSV export."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie).limit(limit)
                result = await session.execute(stmt)
                movies = result.scalars().all()
                return [{'imdb_id': m.imdb_id, 'title': m.title, 'year': m.year, 'channel_id': m.channel_id,
                         'message_id': m.message_id, 'added_date': m.added_date.isoformat() if m.added_date else ''} for m in movies]
        except Exception as e:
            await self._handle_db_error(e)
            logger.error(f"export_movies error: {e}", exc_info=True)
            return []
