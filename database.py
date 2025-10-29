# database.py

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
    # ZAROORI: Aapko yeh column manually add karna hoga.
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
        
        # 1. SSL (Yeh sahi hai)
        if '.com' in database_url or '.co' in database_url:
             connect_args['ssl'] = 'require'
             logger.info("External DB URL: setting ssl='require'.")
        else:
             logger.info("Internal DB URL: using default SSL.")

        # 2. URL modification
        if database_url.startswith('postgresql://'):
             database_url_mod = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            database_url_mod = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
        else:
            database_url_mod = database_url

        self.database_url = database_url_mod

        try:
            # --- YEH HAI AAKHRI FIX ---
            # Hum parameter ko seedhe engine ko de rahe hain,
            # 'connect_args' ke andar nahi.
            # Yeh SQLAlchemy ke 'asyncpg' dialect ko configure karta hai.
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                connect_args=connect_args, # Yahan ab sirf SSL hai
                pool_size=5, 
                max_overflow=10, 
                pool_pre_ping=True, 
                pool_recycle=300, 
                pool_timeout=10,
                statement_cache_size=0 # <-- YEH HAI ASLI FIX
            )
            
            self.SessionLocal = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
            logger.info(f"Database engine created (SSL: {connect_args.get('ssl', 'default')}, Dialect Stmt Cache: 0)")
        
        except TypeError as te:
            # Agar yeh error aata hai ki 'statement_cache_size' invalid hai
            # (jaisa pichhli baar 'prepared_statement_cache_size' ke saath hua tha),
            # toh humara 'connect_args' wala tareeka hi sahi tha aur problem Pgbouncer cache ki thi.
            logger.critical(f"Failed to create engine due to TypeError: {te}. Fallback needed.", exc_info=True)
            # Fallback to connect_args method (just in case)
            del connect_args['statement_cache_size'] # remove if added by mistake
            connect_args['statement_cache_size'] = 0 # add it back correctly
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                connect_args=connect_args,
                pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300, pool_timeout=10
            )
            self.SessionLocal = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
            logger.info("Fallback activated: Using connect_args={'statement_cache_size': 0}")
            
        except Exception as e:
            logger.critical(f"Failed to create SQLAlchemy engine: {e}", exc_info=True)
            raise 

    async def _handle_db_error(self, e: Exception) -> bool:
        """Handle connection errors."""
        if isinstance(e, (OperationalError, DisconnectionError, ConnectionRefusedError, asyncio.TimeoutError)):
             logger.error(f"DB connection/operational error detected: {type(e).__name__}. Disposing engine pool.", exc_info=False)
             try:
                 if self.engine: await self.engine.dispose()
                 logger.info("DB engine pool disposed. Will reconnect on next request.")
                 return True 
             except Exception as re_e:
                 logger.critical(f"Failed to dispose DB engine pool: {re_e}", exc_info=True)
                 return False 
        elif isinstance(e, ProgrammingError):
             logger.error(f"DB Programming Error: {e}", exc_info=True) 
             return False 
        else:
             logger.error(f"Unhandled DB Exception: {type(e).__name__}: {e}", exc_info=True)
             return False 

    async def init_db(self):
        """Initialize DB tables with retries."""
        max_retries = 3
        last_exception = None
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting DB initialization (attempt {attempt+1}/{max_retries})...")
                async with self.engine.begin() as conn:
                    # Yeh query ab fail nahi honi chahiye
                    await conn.execute(text("select pg_catalog.version()"))
                    logger.info("DB connection test successful.")
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized/verified.")
                return 
            except Exception as e:
                last_exception = e
                logger.error(f"DB init failed (attempt {attempt+1}/{max_retries}): {type(e).__name__} - {e}", exc_info=False)
                can_retry = await self._handle_db_error(e)
                if can_retry and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retrying DB initialization in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.critical("DB initialization failed permanently.")
                    raise last_exception 

    # --- User Methods (Koi badlaav nahi) ---
    async def add_user(self, user_id, username, first_name, last_name):
        try:
            async with self.SessionLocal() as session:
                await session.merge(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name, last_active=datetime.utcnow(), is_active=True))
                await session.commit()
        except Exception as e:
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
        try:
            async with self.SessionLocal() as session:
                await session.execute(update(User).where(User.user_id == user_id).values(is_active=False))
                await session.commit()
                logger.info(f"Deactivated user {user_id}.")
        except Exception as e:
            logger.error(f"deactivate_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                result = await session.execute(select(func.count(User.user_id)).where(User.last_active >= cutoff, User.is_active == True))
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_concurrent_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 9999 

    async def get_user_count(self) -> int:
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(func.count(User.user_id)).where(User.is_active == True))
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 0

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(days=days)
                count = (await session.execute(select(func.count(User.user_id)).where(User.last_active < cutoff, User.is_active == True))).scalar_one()
                if count > 0:
                    await session.execute(update(User).where(User.last_active < cutoff, User.is_active == True).values(is_active=False))
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
                result = await session.execute(select(User.user_id).where(User.is_active == True))
                return [r[0] for r in result.all()]
        except Exception as e:
            logger.error(f"get_all_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        try:
            async with self.SessionLocal() as session:
                users = (await session.execute(select(User).limit(limit))).scalars().all()
                return [{'user_id': u.user_id, 'username': u.username, 'first_name': u.first_name, 'last_name': u.last_name, 'joined_date': u.joined_date.isoformat() if u.joined_date else '', 'last_active': u.last_active.isoformat() if u.last_active else '', 'is_active': u.is_active} for u in users]
        except Exception as e:
            logger.error(f"export_users error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []

    # --- Movie Methods (Koi badlaav nahi) ---
    async def get_movie_count(self) -> int:
        try:
            async with self.SessionLocal() as session:
                return (await session.execute(select(func.count(Movie.id)))).scalar_one() 
        except ProgrammingError as pe:
             if "column movies.id does not exist" in str(pe):
                 logger.critical("DATABASE SCHEMA ERROR: 'movies' table missing 'id' column! DROP/recreate table or run: ALTER TABLE movies ADD COLUMN id SERIAL PRIMARY KEY;")
                 return -1
             else:
                 logger.error(f"get_movie_count ProgrammingError: {pe}", exc_info=False)
                 await self._handle_db_error(pe)
                 return -1
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return -1

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict | None:
        try:
            async with self.SessionLocal() as session:
                movie = (await session.execute(select(Movie).filter(Movie.imdb_id == imdb_id))).scalar_one_or_none()
                return {'imdb_id': movie.imdb_id, 'title': movie.title, 'year': movie.year, 'file_id': movie.file_id, 'channel_id': movie.channel_id, 'message_id': movie.message_id} if movie else None
        except Exception as e:
            logger.error(f"get_movie_by_imdb error for {imdb_id}: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def add_movie(self, imdb_id: str, title: str, year: str | None, file_id: str, message_id: int, channel_id: int):
        session = None
        try:
            async with self.SessionLocal() as session:
                movie = (await session.execute(select(Movie).where(or_(Movie.imdb_id == imdb_id, Movie.file_id == file_id)))).scalar_one_or_none()
                clean_title_val = clean_text_for_search(title)
                if movie:
                    movie.imdb_id = imdb_id
                    movie.title = title
                    movie.clean_title = clean_title_val
                    movie.year = year
                    movie.message_id = message_id
                    movie.channel_id = channel_id
                    movie.file_id = file_id
                    await session.commit()
                    return "updated"
                else:
                    session.add(Movie(imdb_id=imdb_id, title=title, clean_title=clean_title_val, year=year, file_id=file_id, message_id=message_id, channel_id=channel_id))
                    await session.commit()
                    return True
        except IntegrityError as e:
            logger.warning(f"add_movie IntegrityError: {title} ({imdb_id}/{file_id}). Error: {e}")
            if session: await session.rollback()
            return "duplicate"
        except Exception as e:
            logger.error(f"add_movie failed for {title} ({imdb_id}): {e}", exc_info=False)
            if session: await session.rollback()
            await self._handle_db_error(e)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str) -> bool:
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(delete(Movie).where(Movie.imdb_id == imdb_id))
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
                total_count = (await session.execute(select(func.count(Movie.id)))).scalar_one_or_none() or 0
                if total_count == 0: return (0, 0)
                update_query = text(r"""UPDATE movies SET clean_title = trim(regexp_replace(regexp_replace(regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'), '\y(s|season)\s*\d{1,2}\y', '', 'g'),'\s+', ' ', 'g')) WHERE clean_title IS NULL OR clean_title = '' OR clean_title != trim(regexp_replace(regexp_replace(regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'), '\y(s|season)\s*\d{1,2}\y', '', 'g'),'\s+', ' ', 'g'));""")
                result_proxy = await session.execute(update_query)
                updated_count = result_proxy.rowcount
                await session.commit()
                return (updated_count, total_count)
        except ProgrammingError as pe:
             if "column movies.id does not exist" in str(pe):
                 logger.critical("SCHEMA ERROR: Cannot rebuild_clean_titles, 'movies' table missing 'id' column!")
                 return (0,0)
             else:
                 logger.error(f"rebuild_clean_titles ProgrammingError: {pe}", exc_info=False)
                 await self._handle_db_error(pe)
                 return (0, total_count)
        except Exception as e:
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return (0, total_count)

    async def get_all_movies_for_sync(self) -> List[Dict] | None:
        try:
            async with self.SessionLocal() as session:
                movies = (await session.execute(select(Movie.imdb_id, Movie.title, Movie.year))).all()
                return [{'objectID': m.imdb_id, 'imdb_id': m.imdb_id, 'title': m.title, 'year': m.year} for m in movies]
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return None

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        try:
            async with self.SessionLocal() as session:
                movies = (await session.execute(select(Movie).limit(limit))).scalars().all()
                return [{'imdb_id': m.imdb_id, 'title': m.title, 'year': m.year, 'channel_id': m.channel_id, 'message_id': m.message_id, 'added_date': m.added_date.isoformat() if m.added_date else ''} for m in movies]
        except Exception as e:
            logger.error(f"export_movies error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return []
