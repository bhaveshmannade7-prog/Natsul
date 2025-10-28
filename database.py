import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Integer, func, select, or_, and_, text, delete, update, UniqueConstraint
from sqlalchemy.exc import OperationalError, DisconnectionError, IntegrityError, ProgrammingError

logger = logging.getLogger("database")
Base = declarative_base()

AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090

def clean_text_for_search(text: str) -> str:
    if not text: return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

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
    # IMPORTANT: Ensure 'id' column exists in your actual DB table now.
    # If not, you MUST run `ALTER TABLE movies ADD COLUMN id SERIAL PRIMARY KEY;`
    # or DROP and recreate the table.
    id = Column(Integer, primary_key=True, autoincrement=True)
    imdb_id = Column(String(50), unique=True, nullable=False, index=True)
    title = Column(String, nullable=False)
    clean_title = Column(String, nullable=False, index=True)
    year = Column(String(10), nullable=True)
    file_id = Column(String, nullable=False, index=True)
    channel_id = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    added_date = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint('file_id', name='uq_file_id'),)

class Database:
    def __init__(self, database_url: str):
        connect_args = {}
        if '.com' in database_url or '.co' in database_url:
             connect_args['ssl'] = 'require'
             logger.info("External DB URL: setting ssl='require'.")
        else:
             logger.info("Internal DB URL: using default SSL.")

        # --- FINAL FIX for pgbouncer ---
        # Pass statement_cache_size=0 directly in connect_args
        # This is the recommended way per asyncpg/SQLAlchemy docs for pgbouncer
        connect_args['statement_cache_size'] = 0
        logger.info("Setting connect_args['statement_cache_size'] = 0 for pgbouncer compatibility.")
        # --- END FIX ---

        # URL modification for asyncpg driver
        if database_url.startswith('postgresql://'):
             database_url_mod = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            database_url_mod = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
        else:
            database_url_mod = database_url # Assume already correct or different driver

        self.database_url = database_url_mod

        # Create engine WITHOUT execution_options (keep it simple)
        try:
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                connect_args=connect_args, # Includes statement_cache_size=0 and potentially ssl='require'
                pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300, pool_timeout=10, # Increased timeout slightly
            )
            self.SessionLocal = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
            logger.info(f"Database engine created (SSL: {connect_args.get('ssl', 'default')}, StmtCache: {connect_args.get('statement_cache_size')})")
        except Exception as e:
            logger.critical(f"Failed to create SQLAlchemy engine: {e}", exc_info=True)
            raise # Reraise critical error

    async def _handle_db_error(self, e: Exception) -> bool:
        """Handle connection errors."""
        # Check for specific asyncpg/SQLAlchemy connection errors
        if isinstance(e, (OperationalError, DisconnectionError, ConnectionRefusedError, asyncio.TimeoutError)):
             # Check if it's the specific prepared statement error - indicates cache fix didn't work
             if isinstance(e, ProgrammingError) and "DuplicatePreparedStatementError" in str(e):
                  logger.critical(f"Persistent DuplicatePreparedStatementError detected! statement_cache_size=0 fix is not working. Error: {e}")
                  # Cannot recover from this automatically if the fix isn't working
                  return False
             else:
                  logger.error(f"DB connection/operational error detected: {type(e).__name__}. Disposing engine pool.", exc_info=False)
                  try:
                      if self.engine: await self.engine.dispose()
                      logger.info("DB engine pool disposed. Will reconnect on next request.")
                      return True # Can retry
                  except Exception as re_e:
                      logger.critical(f"Failed to dispose DB engine pool: {re_e}", exc_info=True)
                      return False # Cannot recover
        # Log other ProgrammingErrors (like missing column) but don't dispose pool
        elif isinstance(e, ProgrammingError):
             logger.error(f"DB Programming Error: {e}", exc_info=False) # Log less verbosely
             return False # Cannot recover by disposing pool
        # Log other general errors
        else:
             logger.error(f"Unhandled DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False # Cannot recover

    async def init_db(self):
        """Initialize DB tables with retries."""
        max_retries = 3
        last_exception = None
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting DB initialization (attempt {attempt+1}/{max_retries})...")
                async with self.engine.begin() as conn:
                    # Explicitly check connection with simple query
                    await conn.execute(text("SELECT 1"))
                    logger.info("DB connection test successful.")
                    # Create tables if they don't exist
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized/verified.")
                return # Success
            except Exception as e:
                last_exception = e
                logger.error(f"DB init failed (attempt {attempt+1}/{max_retries}): {type(e).__name__} - {e}", exc_info=False)
                # Check if we should retry based on error type
                can_retry = await self._handle_db_error(e)
                if can_retry and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retrying DB initialization in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    # If cannot retry or retries exhausted
                    logger.critical("DB initialization failed permanently.")
                    raise last_exception # Reraise the last exception

    # --- User Methods ---
    async def add_user(self, user_id, username, first_name, last_name):
        try:
            async with self.SessionLocal() as session:
                user_obj = User(user_id=user_id, username=username, first_name=first_name, last_name=last_name, last_active=datetime.utcnow(), is_active=True)
                await session.merge(user_obj)
                await session.commit()
        except Exception as e:
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e) # Log and potentially handle

    async def deactivate_user(self, user_id: int):
        try:
            async with self.SessionLocal() as session:
                stmt = update(User).where(User.user_id == user_id).values(is_active=False)
                await session.execute(stmt)
                await session.commit()
                logger.info(f"Deactivated user {user_id}.")
        except Exception as e:
            logger.error(f"deactivate_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                stmt = select(func.count(User.user_id)).where(User.last_active >= cutoff, User.is_active == True)
                result = await session.execute(stmt)
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_concurrent_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return CURRENT_CONC_LIMIT + 1 # Return high number on error to trigger capacity limit

    async def get_user_count(self) -> int:
        try:
            async with self.SessionLocal() as session:
                stmt = select(func.count(User.user_id)).where(User.is_active == True)
                result = await session.execute(stmt)
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(days=days)
                count_stmt = select(func.count(User.user_id)).where(User.last_active < cutoff, User.is_active == True)
                count_result = await session.execute(count_stmt)
                count = count_result.scalar_one()
                if count > 0:
                    update_stmt = update(User).where(User.last_active < cutoff, User.is_active == True).values(is_active=False)
                    await session.execute(update_stmt)
                    await session.commit()
                    logger.info(f"Deactivated {count} inactive users.")
                return count
        except Exception as e:
            logger.error(f"cleanup_inactive_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def get_all_users(self) -> List[int]:
        try:
            async with self.SessionLocal() as session:
                stmt = select(User.user_id).where(User.is_active == True)
                result = await session.execute(stmt)
                return [row[0] for row in result.all()]
        except Exception as e:
            logger.error(f"get_all_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        try:
            async with self.SessionLocal() as session:
                stmt = select(User).limit(limit)
                result = await session.execute(stmt)
                users = result.scalars().all()
                return [{'user_id': u.user_id, 'username': u.username, 'first_name': u.first_name, 'last_name': u.last_name, 'joined_date': u.joined_date.isoformat() if u.joined_date else '', 'last_active': u.last_active.isoformat() if u.last_active else '', 'is_active': u.is_active} for u in users]
        except Exception as e:
            logger.error(f"export_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    # --- Movie Methods ---
    async def get_movie_count(self) -> int:
        """Counts movies. Returns -1 on error."""
        try:
            async with self.SessionLocal() as session:
                stmt = select(func.count(Movie.id)) # Count using the primary key 'id'
                result = await session.execute(stmt)
                return result.scalar_one()
        except ProgrammingError as pe: # Catch specific error for missing column
             if "column movies.id does not exist" in str(pe):
                 logger.critical("DATABASE SCHEMA ERROR: 'movies' table is missing the 'id' column! Please DROP and recreate the table or add the column manually (ALTER TABLE movies ADD COLUMN id SERIAL PRIMARY KEY;)")
                 return -1 # Indicate specific schema error
             else:
                 logger.error(f"get_movie_count ProgrammingError: {pe}", exc_info=False)
                 await self._handle_db_error(pe)
                 return -1 # Indicate general programming error
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1 # Indicate general error


    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie).filter(Movie.imdb_id == imdb_id)
                result = await session.execute(stmt)
                movie = result.scalar_one_or_none()
                if movie: return {'imdb_id': movie.imdb_id, 'title': movie.title, 'year': movie.year, 'file_id': movie.file_id, 'channel_id': movie.channel_id, 'message_id': movie.message_id}
                return None
        except Exception as e:
            logger.error(f"get_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int):
        session = None
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie).where(or_(Movie.imdb_id == imdb_id, Movie.file_id == file_id))
                result = await session.execute(stmt)
                movie = result.scalar_one_or_none()
                clean_title_val = clean_text_for_search(title)
                if movie:
                    movie.imdb_id = imdb_id; movie.title = title; movie.clean_title = clean_title_val; movie.year = year
                    movie.message_id = message_id; movie.channel_id = channel_id; movie.file_id = file_id
                    await session.commit(); return "updated"
                else:
                    new_movie = Movie(imdb_id=imdb_id, title=title, clean_title=clean_title_val, year=year, file_id=file_id, message_id=message_id, channel_id=channel_id)
                    session.add(new_movie); await session.commit(); return True
        except IntegrityError as e:
            if session: await session.rollback()
            logger.warning(f"add_movie IntegrityError: {title} ({imdb_id}/{file_id}). Error: {e}")
            return "duplicate"
        except Exception as e:
            if session: await session.rollback()
            logger.error(f"add_movie failed for {title} ({imdb_id}): {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        try:
            async with self.SessionLocal() as session:
                stmt = delete(Movie).where(Movie.imdb_id == imdb_id)
                result = await session.execute(stmt)
                await session.commit()
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return False

    async def rebuild_clean_titles(self) -> Tuple[int, int]:
        updated_count, total_count = 0, 0
        try:
            async with self.SessionLocal() as session:
                count_stmt = select(func.count(Movie.id))
                total_result = await session.execute(count_stmt)
                total_count = total_result.scalar_one_or_none() or 0
                if total_count == 0: return (0, 0) # Nothing to update
                # Use word boundaries (\y in PostgreSQL regex)
                update_query = text(r"""UPDATE movies SET clean_title = trim(regexp_replace(regexp_replace(regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'), '\y(s|season)\s*\d{1,2}\y', '', 'g'),'\s+', ' ', 'g')) WHERE clean_title IS NULL OR clean_title = '' OR clean_title != trim(regexp_replace(regexp_replace(regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'), '\y(s|season)\s*\d{1,2}\y', '', 'g'),'\s+', ' ', 'g'));""")
                result_proxy = await session.execute(update_query)
                updated_count = result_proxy.rowcount
                await session.commit()
                return (updated_count, total_count)
        except ProgrammingError as pe: # Catch specific error for missing column
             if "column movies.id does not exist" in str(pe):
                 logger.critical("SCHEMA ERROR: Cannot rebuild_clean_titles, 'movies' table missing 'id' column!")
                 return (0,0)
             else: raise pe # Reraise other programming errors
        except Exception as e:
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return (0, total_count)

    async def get_all_movies_for_sync(self) -> List[Dict] | None:
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie.imdb_id, Movie.title, Movie.year)
                result = await session.execute(stmt)
                movies = result.all()
                return [{'objectID': m.imdb_id, 'imdb_id': m.imdb_id, 'title': m.title, 'year': m.year} for m in movies]
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        try:
            async with self.SessionLocal() as session:
                stmt = select(Movie).limit(limit)
                result = await session.execute(stmt)
                movies = result.scalars().all()
                return [{'imdb_id': m.imdb_id, 'title': m.title, 'year': m.year, 'channel_id': m.channel_id, 'message_id': m.message_id, 'added_date': m.added_date.isoformat() if m.added_date else ''} for m in movies]
        except Exception as e:
            logger.error(f"export_movies error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []
