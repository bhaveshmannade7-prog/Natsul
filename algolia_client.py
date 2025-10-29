# algolia_client.py

import os
import logging
from typing import List, Dict, Tuple
import algoliasearch # Import the base library to check version
from algoliasearch.search.client import SearchClient # v4+ import
from dotenv import load_dotenv
import asyncio

# Load dotenv VERY early
load_dotenv()

logger = logging.getLogger("bot.algolia")

# Log the detected version
try:
    logger.info(f"Detected algoliasearch version: {algoliasearch.__version__}")
except Exception as e:
    logger.warning(f"Could not detect algoliasearch version: {e}")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_ADMIN_KEY = os.getenv("ALGOLIA_ADMIN_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME")

client = None
index = None
_is_ready = False # Internal flag

async def initialize_algolia():
    """Tries to initialize the Algolia client and index asynchronously. Should be called from lifespan."""
    global client, index, _is_ready
    if _is_ready:
        logger.info("Algolia already initialized.")
        return True # Already initialized

    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia environment variables missing (APP_ID, ADMIN_KEY, or INDEX_NAME). Cannot initialize.")
        _is_ready = False
        return False

    logger.info("Attempting to initialize Algolia client (v4+)...")
    try:
        # Client 'SearchClient(...)' se banta hai (bina .create)
        client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        
        # +++++ YEH HAI ASLI FIX +++++
        # Method ka naam 'init_index' hai, 'index' nahi.
        index = client.init_index(ALGOLIA_INDEX_NAME) 
        # ++++++++++++++++++++++++++++
        
        logger.info(f"Algolia client and index initialized for: {ALGOLIA_INDEX_NAME}")

        # Check connection and apply settings
        logger.info("Fetching/Applying Algolia index settings...")
        settings_to_apply = {
            'minWordSizefor1Typo': 3, 'minWordSizefor2Typos': 7, 'hitsPerPage': 20,
            'searchableAttributes': ['title', 'imdb_id', 'year'],
            'queryType': 'prefixLast', 'attributesForFaceting': ['searchable(year)'],
            'typoTolerance': 'min', 'removeStopWords': True, 'ignorePlurals': True,
        }
        await index.set_settings_async(settings_to_apply, request_options={'timeout': 20}) 
        await index.get_settings_async(request_options={'timeout': 15}) 
        logger.info("Applied settings using async methods.")

        _is_ready = True
        logger.info("Algolia initialization and settings apply successful.")
        return True

    except AttributeError as ae:
        # Yeh error ab nahi aana chahiye
        logger.critical(f"ALGOLIA VERSION ERROR: {ae}. API mismatch!", exc_info=True)
        client = None
        index = None
        _is_ready = False
        return False
    except Exception as e:
        # Catch other errors like connection issues, bad credentials
        logger.critical(f"Failed to initialize Algolia client or apply settings: {e}", exc_info=True)
        client = None
        index = None
        _is_ready = False
        return False


def is_algolia_ready():
    """Check if Algolia client and index were successfully initialized."""
    return _is_ready and client is not None and index is not None

# --- Baaki file waise hi rahegi ---

async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    if not is_algolia_ready(): logger.error("Algolia not ready for search."); return []
    try:
        results = await index.search_async(query, {'hitsPerPage': limit})
        hits = results.get('hits', [])
        return [{'imdb_id': hit['objectID'], 'title': hit.get('title', 'N/A')} for hit in hits if 'objectID' in hit]
    except Exception as e: logger.error(f"Algolia search failed for '{query}': {e}", exc_info=True); return []

async def algolia_add_movie(movie_data: dict) -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for add_movie."); return False
    if 'objectID' not in movie_data or not movie_data['objectID']:
         if 'imdb_id' in movie_data and movie_data['imdb_id']: movie_data['objectID'] = movie_data['imdb_id']
         else: logger.error(f"Missing objectID/imdb_id for Algolia add: {movie_data}"); return False
    try:
        await index.save_object_async(movie_data)
        return True
    except Exception as e: logger.error(f"Algolia save_object failed for {movie_data.get('objectID', 'N/A')}: {e}", exc_info=True); return False

async def algolia_add_batch_movies(movies_list: List[dict]) -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for add_batch."); return False
    if not movies_list: return True
    valid_movies = []
    for m in movies_list:
        if 'objectID' not in m or not m['objectID']:
            if 'imdb_id' in m and m['imdb_id']: m['objectID'] = m['imdb_id']
            else: logger.warning(f"Skipping batch item (no ID): {m}"); continue
        valid_movies.append(m)
    if not valid_movies: logger.warning("No valid items in batch."); return False
    try:
        await index.save_objects_async(valid_movies, {"batchSize": 1000})
        logger.info(f"Algolia batch processed {len(valid_movies)} items.")
        return True
    except Exception as e: logger.error(f"Algolia save_objects failed: {e}", exc_info=True); return False

async def algolia_remove_movie(imdb_id: str) -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for remove_movie."); return False
    if not imdb_id: return False
    try:
        await index.delete_object_async(imdb_id)
        logger.info(f"Algolia delete request for {imdb_id} submitted.")
        return True
    except Exception as e:
        if 'ObjectID does not exist' in str(e): logger.info(f"Algolia: {imdb_id} already deleted."); return True
        logger.error(f"Algolia delete_object failed for {imdb_id}: {e}", exc_info=True); return False

async def algolia_clear_index() -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for clear_index."); return False
    try:
        await index.clear_objects_async()
        logger.info(f"Algolia index '{ALGOLIA_INDEX_NAME}' cleared.")
        return True
    except Exception as e: logger.error(f"Algolia clear_objects failed: {e}", exc_info=True); return False

async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    if not is_algolia_ready(): logger.error("Algolia not ready for sync."); return False, 0
    valid_movies = []
    for m in all_movies_data:
        if 'objectID' not in m or not m['objectID']:
            if 'imdb_id' in m and m['imdb_id']: m['objectID'] = m['imdb_id']
            else: logger.warning(f"Skipping sync item (no ID): {m}"); continue
        valid_movies.append(m)
    count = len(valid_movies)
    if not valid_movies: logger.info("Sync: No valid data from DB, clearing index."); return await algolia_clear_index(), 0
    try:
        logger.info(f"Sync: Replacing Algolia index with {count:,} objects...")
        await index.replace_all_objects_async(valid_movies, {"batchSize": 1000})
        logger.info(f"Sync completed (async replace).")
        return True, count
    except Exception as e: logger.error(f"Algolia replace_all_objects failed: {e}", exc_info=True); return False, 0
