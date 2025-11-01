# algolia_client.py

import os
import logging
from typing import List, Dict, Tuple, Any # Any import kiya hai
import algoliasearch

from algoliasearch.search.client import SearchClient
# FIX: ModuleNotFoundError se bachne ke liye SearchIndex ka direct import hata diya
# from algoliasearch.search.search_index import SearchIndex 
from dotenv import load_dotenv
import asyncio

load_dotenv()

logger = logging.getLogger("bot.algolia")

try:
    logger.info(f"Detected algoliasearch version: {algoliasearch.__version__}")
except Exception as e:
    logger.warning(f"Could not detect algoliasearch version: {e}")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_ADMIN_KEY = os.getenv("ALGOLIA_ADMIN_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME")

# Variable names ko clear kiya (search_client, index)
search_client: SearchClient | None = None
# FIX: Type hint ko Any kar diya taaki import error na aaye
index: Any | None = None 
_is_ready = False

async def initialize_algolia():
    """Algolia client aur index ko initialize karta hai."""
    global search_client, index, _is_ready
    if _is_ready:
        logger.info("Algolia pehle se hi initialized hai.")
        return True

    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia environment variables missing. Initialize nahi ho sakta.")
        _is_ready = False
        return False

    logger.info(f"Algolia client initialize karne ki koshish (index: {ALGOLIA_INDEX_NAME})")
    try:
        # 1. SearchClient initialize karein
        search_client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        logger.info(f"Algolia Async client initialized.")
        
        # 2. Index object ko initialize karein (yeh runtime mein sahi object return karega)
        index = search_client.init_index(ALGOLIA_INDEX_NAME)

        # --- Aggressive Typo-Tolerance Settings ---
        settings_to_apply = {
            'searchableAttributes': ['clean_title', 'title', 'imdb_id', 'year'], 
            'hitsPerPage': 20,
            
            # Aggressive Typo-Tolerance
            'typoTolerance': 'true', 
            'minWordSizefor1Typo': 2, 
            'minWordSizefor2Typos': 4, 
            
            'queryType': 'prefixLast',
            'attributesForFaceting': ['searchable(year)'],
            'removeStopWords': True,
            'ignorePlurals': True,
        }
        
        # 3. Index object ka upyog karke settings apply karein
        await index.set_settings(
            settings_to_apply
        )
        logger.info(f"Algolia settings index '{ALGOLIA_INDEX_NAME}' par apply ho gayi.")

        _is_ready = True
        logger.info("Algolia initialization aur settings apply successful.")
        return True

    except AttributeError as ae:
        logger.critical(f"ALGOLIA VERSION/API ERROR: {ae}. API mismatch!", exc_info=True)
        search_client = None
        index = None
        _is_ready = False
        return False
    except Exception as e:
        logger.critical(f"Algolia client ya settings apply karne mein fail: {e}", exc_info=True)
        search_client = None
        index = None
        _is_ready = False
        return False


def is_algolia_ready():
    """Check karein ki Algolia client aur index dono initialize ho chuke hain."""
    return _is_ready and search_client is not None and index is not None


async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    """Algolia mein search karta hai aur hits return karta hai."""
    if not is_algolia_ready() or index is None:
        logger.error("Algolia search ke liye taiyar nahi hai.")
        return []
    try:
        # Index object ka direct search method use karein (Robust)
        result = await index.search(
            query=query,
            request_options={
                "hitsPerPage": limit
            }
        )
        # Hits ko seedhe result se extract karein
        hits = result.get('hits', []) 

        return [
            {
                'imdb_id': hit['objectID'],
                'title': hit.get('title') or 'Title Missing',
                'year': hit.get('year')
            }
            for hit in hits if 'objectID' in hit 
        ]
    except Exception as e:
        logger.error(f"Algolia search fail hua '{query}' ke liye: {e}", exc_info=True)
        return []


async def algolia_add_movie(movie_data: dict) -> bool:
    """Ek single movie ko Algolia mein add/update karta hai."""
    if not is_algolia_ready() or index is None:
        logger.warning("Algolia movie add ke liye taiyar nahi hai.")
        return False
    if 'objectID' not in movie_data or not movie_data['objectID']:
        if 'imdb_id' in movie_data and movie_data['imdb_id']:
            movie_data['objectID'] = movie_data['imdb_id']
        else:
            logger.error(f"Algolia add ke liye objectID/imdb_id missing hai: {movie_data}")
            return False
    try:
        # Index object ka direct save_object method use karein
        await index.save_object(movie_data)
        return True
    except Exception as e:
        logger.error(f"Algolia save_object fail hua {movie_data.get('objectID', 'N/A')} ke liye: {e}", exc_info=True)
        return False


async def algolia_add_batch_movies(movies_list: List[dict]) -> bool:
    """Multiple movies ko Algolia mein batch mein add/update karta hai."""
    if not is_algolia_ready() or index is None:
        logger.warning("Algolia batch add ke liye taiyar nahi hai.")
        return False
    if not movies_list:
        return True
    valid_movies = []
    for m in movies_list:
        if 'objectID' not in m or not m['objectID']:
            if 'imdb_id' in m and m['imdb_id']:
                m['objectID'] = m['imdb_id']
            else:
                logger.warning(f"Batch item skip kiya (no ID): {m}")
                continue
        valid_movies.append(m)
    if not valid_movies:
        logger.warning("Batch mein koi valid item nahi hai.")
        return False
    try:
        # Index object ka direct save_objects method use karein
        await index.save_objects(valid_movies)
        logger.info(f"Algolia batch mein {len(valid_movies)} items process hue.")
        return True
    except Exception as e:
        logger.error(f"Algolia save_objects fail hua: {e}", exc_info=True)
        return False


async def algolia_remove_movie(imdb_id: str) -> bool:
    """Algolia se movie remove karta hai."""
    if not is_algolia_ready() or index is None:
        logger.warning("Algolia movie remove ke liye taiyar nahi hai.")
        return False
    if not imdb_id:
        return False
    try:
        # Index object ka direct delete_object method use karein
        await index.delete_object(object_id=imdb_id)
        logger.info(f"Algolia delete request {imdb_id} ke liye bheja gaya.")
        return True
    except Exception as e:
        if 'ObjectID does not exist' in str(e):
            logger.info(f"Algolia: {imdb_id} pehle se hi delete ho chuka hai.")
            return True
        logger.error(f"Algolia delete_object fail hua {imdb_id} ke liye: {e}", exc_info=True)
        return False


async def algolia_clear_index() -> bool:
    """Algolia index ko poora khali (clear) karta hai."""
    if not is_algolia_ready() or index is None:
        logger.warning("Algolia index clear ke liye taiyar nahi hai.")
        return False
    try:
        # Index object ka direct clear_objects method use karein
        await index.clear_objects()
        logger.info(f"Algolia index '{ALGOLIA_INDEX_NAME}' clear ho gaya.")
        return True
    except Exception as e:
        logger.error(f"Algolia clear_objects fail hua: {e}", exc_info=True)
        return False


async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """MongoDB ke saare data se Algolia index ko replace (sync) karta hai."""
    if not is_algolia_ready() or index is None:
        logger.error("Algolia sync ke liye taiyar nahi hai.")
        return False, 0
    valid_movies = []
    for m in all_movies_data:
        if 'objectID' not in m or not m['objectID']:
            if 'imdb_id' in m and m['imdb_id']:
                m['objectID'] = m['imdb_id']
            else:
                logger.warning(f"Sync item skip kiya (no ID): {m}")
                continue
        valid_movies.append(m)
    count = len(valid_movies)
    if not valid_movies:
        logger.info("Sync: DB se koi valid data nahi mila, index clear kar rahe hain.")
        return await algolia_clear_index(), 0
    try:
        logger.info(f"Sync: Algolia index ko {count:,} objects se replace kar rahe hain...")
        await algolia_clear_index() 
        # Index object ka direct save_objects method use karein
        await index.save_objects(valid_movies)
        logger.info(f"Sync poora hua.")
        return True, count
    except Exception as e:
        logger.error(f"Algolia sync fail hua: {e}", exc_info=True)
        return False, 0
