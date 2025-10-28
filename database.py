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

# Yeh placeholder ID JSON se import ki gayi files ke liye hai
AUTO_MESSAGE_ID_PLACEHOLDER = 9090909090 

def clean_text_for_search(text: str) -> str:
    """Search ke liye text ko saaf karne wala function (ab DB mein save karne ke liye use hoga)."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = re.sub(r'\b(s|season)\s*\d{1,2}\b', '', text)
    text = re.sub(r'\s+', ' ', text)
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
    imdb_id = Column(String(50), unique=True, nullable=False, index=True) # Yeh Algolia ka 'objectID' banega
    title = Column(String, nullable=False) # Yeh Algolia mein 'title' banega
    clean_title = Column(String, nullable=False, index=True) # Yeh DB mein indexing ke liye
    year = Column(String(10), nullable=True) # Yeh Algolia mein 'year' banega
    
    # Yeh details sirf DB mein rahengi, Algolia ko inki zaroorat nahi
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
             logger.info("External database URL detected, setting ssl='require'.")
        else:
             logger.info("Internal database URL detected, using default SSL (none).")
        
        # --- YAHI HAI MUKHYA FIX ---
        # Render/pgbouncer ke saath compatibility ke liye prepared statement cache ko disable karein
        connect_args['statement_cache_size'] = 0
        logger.info("Setting statement_cache_size=0 for pgbouncer compatibility.")
        # --- FIX END ---

        if database_url.startswith('postgresql://'):
             database_url_mod = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgres://'):
            database_url_mod = database_url.replace('postgres://', 'postgresql+asyncpg://', 1)
        else:
            database_url_mod = database_url 

        self.database_url = database_url_mod
        
        self.engine = create_async_engine(
            self.database_url, 
            echo=False, 
            connect_args=connect_args, # Ab 'connect_args' mein fix shaamil hai
            pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300, pool_timeout=8,
        )
        
        self.SessionLocal = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        # Behtar logging ke liye
        logger.info(f"Database engine initialized (SSL: {connect_args.get('ssl', 'default')}, Cache: {connect_args.get('statement_cache_size')})")
        
    async def _handle_db_error(self, e: Exception) -> bool:
        """Connection errors ko handle karein."""
        if isinstance(e, (OperationalError, DisconnectionError)):
            logger.error(f"Critical DB error detected: {type(e).__name__}. Re-initializing engine.", exc_info=True)
            try:
                await self.engine.dispose()
                logger.info("DB engine disposed. New connections will be created.")
                return True
            except Exception as re_e:
                logger.critical(f"Failed to dispose DB engine: {re_e}", exc_info=True)
                return False
        return False 
        
    async def init_db(self):
        """Database table banayein."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables initialized successfully.")
                return
            except Exception as e:
                logger.critical(f"Failed to initialize DB (attempt {attempt+1}/{max_retries}).", exc_info=True)
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                if attempt == max_retries - 1:
                    raise
    
    async def add_user(self, user_id, username, first_name, last_name):
        """User ko add ya update karein."""
        max_retries = 2
        for attempt in range(max_retries):
            session = None
            try:
                async with self.SessionLocal() as session:
                    result = await session.execute(select(User).filter(User.user_id == user_id))
                    user = result.scalar_one_or_none()
                    if user:
                        user.last_active = datetime.utcnow()
                        user.is_active = True
                        user.username, user.first_name, user.last_name = username, first_name, last_name
                    else:
                        session.add(User(user_id=user_id, username=username, first_name=first_name, last_name=last_name))
                    await session.commit()
                    return
            except Exception as e:
                if session: await session.rollback()
                if await self._handle_db_error(e) and attempt < max_retries - 1:
                    await asyncio.sleep(1); continue
                logger.error(f"add_user error for {user_id}: {e}", exc_info=False)
                return

    async def deactivate_user(self, user_id: int):
        """Broadcast fail hone par user ko inactive karein."""
        try:
            async with self.SessionLocal() as session:
                await session.execute(update(User).where(User.user_id == user_id).values(is_active=False))
                await session.commit()
                logger.info(f"Deactivated user {user_id} (bot blocked).")
        except Exception as e:
            logger.error(f"deactivate_user error for {user_id}: {e}", exc_info=False)

    async def get_concurrent_user_count(self, minutes: int) -> int:
        """Active users ki ginti karein."""
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                result = await session.execute(select(func.count(User.user_id)).where(User.last_active >= cutoff, User.is_active == True))
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_concurrent_user_count error: {e}", exc_info=True)
            return 0

    async def get_user_count(self) -> int:
        """Total active users ki ginti karein."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(func.count(User.user_id)).where(User.is_active == True))
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_user_count error: {e}", exc_info=True)
            return 0

    async def get_movie_count(self) -> int:
        """Total movies ki ginti karein."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(func.count(Movie.id)))
                return result.scalar_one()
        except Exception as e:
            logger.error(f"get_movie_count error: {e}", exc_info=True)
            return 0

    async def get_movie_by_imdb(self, imdb_id: str) -> Dict:
        """IMDB ID se movie search karein (Callback ke liye)."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(Movie).filter(Movie.imdb_id == imdb_id))
                movie = result.scalar_one_or_none()
                if movie:
                    return {
                        'imdb_id': movie.imdb_id, 'title': movie.title, 'year': movie.year,
                        'file_id': movie.file_id, 'channel_id': movie.channel_id, 'message_id': movie.message_id,
                    }
                return None
        except Exception as e:
            logger.error(f"get_movie_by_imdb error: {e}", exc_info=True)
            return None

    async def add_movie(self, imdb_id: str, title: str, year: str, file_id: str, message_id: int, channel_id: int):
        """Movie ko DB mein add ya update karein (Upsert logic)."""
        session = None
        try:
            async with self.SessionLocal() as session:
                # Pehle check karein ki movie file_id ya imdb_id se exist karti hai ya nahi
                stmt = select(Movie).where(or_(Movie.imdb_id == imdb_id, Movie.file_id == file_id))
                result = await session.execute(stmt)
                movie = result.scalar_one_or_none()
                
                clean = clean_text_for_search(title)
                
                if movie:
                    # Movie mili - Update karein
                    movie.imdb_id = imdb_id
                    movie.title = title
                    movie.clean_title = clean
                    movie.year = year
                    movie.message_id = message_id
                    movie.channel_id = channel_id
                    movie.file_id = file_id
                    await session.commit()
                    return "updated"
                else:
                    # Movie nahi mili - Nayi add karein
                    movie = Movie(
                        imdb_id=imdb_id, title=title, clean_title=clean, year=year,
                        file_id=file_id, message_id=message_id, channel_id=channel_id
                    )
                    session.add(movie)
                    await session.commit()
                    return True 
        except IntegrityError as e:
            if session: await session.rollback()
            logger.warning(f"Duplicate entry skipped (IntegrityError): {title} (IMDB: {imdb_id} or FileID: {file_id}).")
            return "duplicate" 
        except Exception as e:
            if session: await session.rollback()
            if await self._handle_db_error(e): 
                await asyncio.sleep(1)
                return await self.add_movie(imdb_id, title, year, file_id, message_id, channel_id) 
            logger.error(f"add_movie error: {e}", exc_info=True)
            return False

    async def remove_movie_by_imdb(self, imdb_id: str):
        """Movie ko DB se remove karein."""
        try:
            async with self.SessionLocal() as session:
                await session.execute(delete(Movie).where(Movie.imdb_id == imdb_id))
                await session.commit()
                return True
        except Exception as e:
            logger.error(f"remove_movie_by_imdb error: {e}", exc_info=True)
            return False

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        """Inactive users ko 'is_active = False' set karein."""
        try:
            async with self.SessionLocal() as session:
                cutoff = datetime.utcnow() - timedelta(days=days)
                result = await session.execute(select(func.count(User.user_id)).where(User.last_active < cutoff, User.is_active == True))
                count = result.scalar_one()
                if count > 0:
                    await session.execute(update(User).where(User.last_active < cutoff, User.is_active == True).values(is_active=False))
                    await session.commit()
                return count
        except Exception as e:
            logger.error(f"cleanup_inactive_users error: {e}", exc_info=True)
            return 0

    async def rebuild_clean_titles(self) -> Tuple[int, int]:
        """(Sirf DB ke liye) 'clean_title' ko dobara banayein."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(func.count(Movie.id)))
                total = result.scalar_one()
                
                # PostgreSQL-specific regex update
                update_query = text(r"""
                    UPDATE movies 
                    SET clean_title = trim(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(lower(title), '[^a-z0-9]+', ' ', 'g'),
                                '\y(s|season)\s*\d{1,2}\y', '', 'g'
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
                    )
                """)
                update_result = await session.execute(update_query)
                await session.commit()
                return (update_result.rowcount, total)
        except Exception as e:
            logger.error(f"rebuild_clean_titles error: {e}", exc_info=True)
            return (0, 0)

    async def get_all_users(self) -> List[int]:
        """Broadcast ke liye sabhi active users ki ID lein."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(User.user_id).where(User.is_active == True))
                return [row[0] for row in result.all()]
        except Exception as e:
            logger.error(f"get_all_users error: {e}", exc_info=True)
            return []

    async def get_all_movies_for_sync(self) -> List[Dict]:
        """
        Algolia Sync ke liye sabhi movies fetch karein.
        Sirf zaroori data (objectID, title, year) hi select karein.
        """
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(Movie.imdb_id, Movie.title, Movie.year))
                movies = result.all()
                
                # Data ko Algolia ke format mein badlein
                return [
                    {
                        'objectID': m.imdb_id, # Sabse zaroori
                        'imdb_id': m.imdb_id,
                        'title': m.title,
                        'year': m.year
                    }
                    for m in movies
                ]
        except Exception as e:
            logger.error(f"get_all_movies_for_sync error: {e}", exc_info=True)
            return None # Error ke case mein None return karein


    # --- Export Functions (DB se data export karein) ---

    async def export_users(self, limit: int = 2000) -> List[Dict]:
        """Users ko CSV export ke liye fetch karein."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(User).limit(limit))
                users = result.scalars().all()
                return [{'user_id': u.user_id, 'username': u.username, 'first_name': u.first_name, 'last_name': u.last_name,
                         'joined_date': u.joined_date.isoformat() if u.joined_date else '',
                         'last_active': u.last_active.isoformat() if u.last_active else '', 'is_active': u.is_active} for u in users]
        except Exception as e:
            logger.error(f"export_users error: {e}", exc_info=True)
            return []

    async def export_movies(self, limit: int = 2000) -> List[Dict]:
        """Movies ko CSV export ke liye fetch karein."""
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(Movie).limit(limit))
                movies = result.scalars().all()
                return [{'imdb_id': m.imdb_id, 'title': m.title, 'year': m.year, 'channel_id': m.channel_id,
                         'message_id': m.message_id, 'added_date': m.added_date.isoformat() if m.added_date else ''} for m in movies]
        except Exception as e:
            logger.error(f"export_movies error: {e}", exc_info=True)
            return []
