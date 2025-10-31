# algolia_client.py

import os
import logging
from typing import List, Dict, Tuple
import algoliasearch

from algoliasearch.search.client import SearchClient
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

client = None
index = None
_is_ready = False

async def initialize_algolia():
    """Tries to initialize the Algolia client and index asynchronously. Should be called from lifespan."""
    global client, index, _is_ready
    if _is_ready:
        logger.info("Algolia already initialized.")
        return True

    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia environment variables missing (APP_ID, ADMIN_KEY, or INDEX_NAME). Cannot initialize.")
        _is_ready = False
        return False

    logger.info("Attempting to initialize Algolia client (v4 Async)...")
    try:
        client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        
        logger.info(f"Algolia Async client initialized for: {ALGOLIA_INDEX_NAME}")

        # --- FIX: Aggressive Typo-Tolerance Settings ---
        settings_to_apply = {
            'searchableAttributes': ['clean_title', 'title', 'imdb_id', 'year'], # clean_title ko pehle rakhein
            'hitsPerPage': 20,
            
            # Aggressive Typo-Tolerance
            'typoTolerance': 'true', # Sabhi queries par typos allow karein
            'minWordSizefor1Typo': 2, # 2-letter words par 1 typo
            'minWordSizefor2Typos': 4, # 4-letter words par 2 typos ("ktra" yahan match hoga)
            
            'queryType': 'prefixLast',
            'attributesForFaceting': ['searchable(year)'],
            'removeStopWords': True,
            'ignorePlurals': True,
        }
        # --- END FIX ---
        
        await client.set_settings(
            index_name=ALGOLIA_INDEX_NAME,
            index_settings=settings_to_apply
        )
        logger.info(f"Applied settings (with aggressive typo-tolerance) using async methods.")

        _is_ready = True
        logger.info("Algolia initialization and settings apply successful.")
        return True

    except AttributeError as ae:
        logger.critical(f"ALGOLIA VERSION/API ERROR: {ae}. API mismatch!", exc_info=True)
        client = None
        index = None
        _is_ready = False
        return False
    except Exception as e:
        logger.critical(f"Failed to initialize Algolia client or apply settings: {e}", exc_info=True)
        client = None
        index = None
        _is_ready = False
        return False


def is_algolia_ready():
    """Check if Algolia client was successfully initialized."""
    return _is_ready and client is not None


async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    if not is_algolia_ready():
        logger.error("Algolia not ready for search.")
        return []
    try:
        result = await client.search(
            search_method_params={
                "requests": [{
                    "indexName": ALGOLIA_INDEX_NAME,
                    "query": query,
                    "hitsPerPage": limit
                }]
            }
        )
        
        # --- FIX for algoliasearch v4 (AttributeError Fix) ---
        # `result.results[0]` ek dict-like object hai. Use .hits nahi, ['hits'] se access karein.
        hits = []
        if result.results and len(result.results) > 0 and 'hits' in result.results[0]:
            hits = result.results[0]['hits']
        # --- END FIX ---

        return [
            {
                'imdb_id': hit['objectID'], # 'hit' is a dict, so this is correct
                'title': hit.get('title') or 'Title Missing',
                'year': hit.get('year')
            }
            for hit in hits if 'objectID' in hit
        ]
    except Exception as e:
        logger.error(f"Algolia search failed for '{query}': {e}", exc_info=True)
        return []


async def algolia_add_movie(movie_data: dict) -> bool:
    if not is_algolia_ready():
        logger.warning("Algolia not ready for add_movie.")
        return False
    if 'objectID' not in movie_data or not movie_data['objectID']:
        if 'imdb_id' in movie_data and movie_data['imdb_id']:
            movie_data['objectID'] = movie_data['imdb_id']
        else:
            logger.error(f"Missing objectID/imdb_id for Algolia add: {movie_data}")
            return False
    try:
        await client.save_object(
            index_name=ALGOLIA_INDEX_NAME,
            body=movie_data
        )
        return True
    except Exception as e:
        logger.error(f"Algolia save_object failed for {movie_data.get('objectID', 'N/A')}: {e}", exc_info=True)
        return False


async def algolia_add_batch_movies(movies_list: List[dict]) -> bool:
    if not is_algolia_ready():
        logger.warning("Algolia not ready for add_batch.")
        return False
    if not movies_list:
        return True
    valid_movies = []
    for m in movies_list:
        if 'objectID' not in m or not m['objectID']:
            if 'imdb_id' in m and m['imdb_id']:
                m['objectID'] = m['imdb_id']
            else:
                logger.warning(f"Skipping batch item (no ID): {m}")
                continue
        valid_movies.append(m)
    if not valid_movies:
        logger.warning("No valid items in batch.")
        return False
    try:
        await client.save_objects(
            index_name=ALGOLIA_INDEX_NAME,
            objects=valid_movies
        )
        logger.info(f"Algolia batch processed {len(valid_movies)} items.")
        return True
    except Exception as e:
        logger.error(f"Algolia save_objects failed: {e}", exc_info=True)
        return False


async def algolia_remove_movie(imdb_id: str) -> bool:
    if not is_algolia_ready():
        logger.warning("Algolia not ready for remove_movie.")
        return False
    if not imdb_id:
        return False
    try:
        await client.delete_object(
            index_name=ALGOLIA_INDEX_NAME,
            object_id=imdb_id
        )
        logger.info(f"Algolia delete request for {imdb_id} submitted.")
        return True
    except Exception as e:
        if 'ObjectID does not exist' in str(e):
            logger.info(f"Algolia: {imdb_id} already deleted.")
            return True
        logger.error(f"Algolia delete_object failed for {imdb_id}: {e}", exc_info=True)
        return False


async def algolia_clear_index() -> bool:
    if not is_algolia_ready():
        logger.warning("Algolia not ready for clear_index.")
        return False
    try:
        await client.clear_objects(index_name=ALGOLIA_INDEX_NAME)
        logger.info(f"Algolia index '{ALGOLIA_INDEX_NAME}' cleared.")
        return True
    except Exception as e:
        logger.error(f"Algolia clear_objects failed: {e}", exc_info=True)
        return False


async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    if not is_algolia_ready():
        logger.error("Algolia not ready for sync.")
        return False, 0
    valid_movies = []
    for m in all_movies_data:
        if 'objectID' not in m or not m['objectID']:
            if 'imdb_id' in m and m['imdb_id']:
                m['objectID'] = m['imdb_id']
            else:
                logger.warning(f"Skipping sync item (no ID): {m}")
                continue
        valid_movies.append(m)
    count = len(valid_movies)
    if not valid_movies:
        logger.info("Sync: No valid data from DB, clearing index.")
        return await algolia_clear_index(), 0
    try:
        logger.info(f"Sync: Replacing Algolia index with {count:,} objects...")
        await algolia_clear_index()
        await client.save_objects(
            index_name=ALGOLIA_INDEX_NAME,
            objects=valid_movies
        )
        logger.info(f"Sync completed.")
        return True, count
    except Exception as e:
        logger.error(f"Algolia sync failed: {e}", exc_info=True)
        return False, 0
