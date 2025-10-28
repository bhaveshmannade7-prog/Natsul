import os
import logging
from typing import List, Dict, Tuple
from algoliasearch.search.client import SearchClient # Correct import for v4+
from dotenv import load_dotenv
import asyncio

# Load dotenv VERY early, although it should be loaded by bot.py first
load_dotenv()

logger = logging.getLogger("bot.algolia")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_ADMIN_KEY = os.getenv("ALGOLIA_ADMIN_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME")

client = None
index = None
_is_ready = False # Internal flag

async def _initialize_algolia_async():
    """Asynchronous function to initialize Algolia."""
    global client, index, _is_ready
    if _is_ready: # Avoid re-initializing
        return True

    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia environment variables missing (APP_ID, ADMIN_KEY, or INDEX_NAME). Cannot initialize.")
        _is_ready = False
        return False

    logger.info("Attempting to initialize Algolia client...")
    try:
        # Correct initialization for v4+
        client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        index = client.init_index(ALGOLIA_INDEX_NAME)
        logger.info(f"Algolia client created for App ID: {ALGOLIA_APP_ID}, Index: {ALGOLIA_INDEX_NAME}")

        # Basic check to confirm connection and permissions by getting settings
        logger.info("Fetching Algolia index settings to confirm connection...")
        await index.get_settings_async()
        logger.info("Successfully fetched index settings.")

        # Apply desired index settings asynchronously
        logger.info("Applying Algolia index settings...")
        await index.set_settings_async({
            'minWordSizefor1Typo': 3,
            'minWordSizefor2Typos': 7,
            'hitsPerPage': 20,
            'searchableAttributes': [
                'title', # Make title primary
                'imdb_id',
                'year'
            ],
            'queryType': 'prefixLast', # Allow searching prefixes
            'attributesForFaceting': ['searchable(year)'], # Make year searchable for filtering
            'typoTolerance': 'min',
            'removeStopWords': True,
            'ignorePlurals': True,
        }, request_options={'timeout': 10}) # Add timeout for settings apply
        logger.info("Successfully applied Algolia index settings.")

        _is_ready = True
        logger.info("Algolia initialization successful.")
        return True

    except Exception as e:
        logger.critical(f"Failed to initialize Algolia client or apply settings: {e}", exc_info=True)
        client = None
        index = None
        _is_ready = False
        return False

# --- Synchronous Initialization Block ---
# Try to run the async initialization synchronously when the module is loaded.
# This makes the startup depend on Algolia connection success.
try:
    logger.info("Running synchronous Algolia initialization...")
    # Get a running loop or create a new one for initialization
    try:
        loop = asyncio.get_running_loop()
        # If loop is running, create task (might happen during testing/specific setups)
        # However, typically at import time, no loop is running.
        # This path is less common during standard startup.
        logger.warning("Event loop already running during Algolia init. Creating task.")
        init_task = loop.create_task(_initialize_algolia_async())
        # Note: We don't wait here as the loop is already managed elsewhere.
        # _is_ready will be set later by the task. This might lead to race conditions.
        # Preferring asyncio.run() below.
    except RuntimeError: # No running event loop
        logger.info("No running event loop found, using asyncio.run() for Algolia init.")
        asyncio.run(_initialize_algolia_async())

except Exception as e:
    logger.critical(f"Critical error during synchronous Algolia initialization setup: {e}", exc_info=True)
    _is_ready = False
# --- End Initialization Block ---


def is_algolia_ready():
    """Check if Algolia client and index were successfully initialized."""
    if not _is_ready:
        # Avoid logging this warning every time if init failed permanently
        # logger.warning("is_algolia_ready called, but initialization failed or hasn't completed.")
        pass
    return _is_ready and client is not None and index is not None

async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    """Search Algolia asynchronously."""
    if not is_algolia_ready():
        logger.error("Algolia not ready, cannot perform search.")
        return []

    try:
        results = await index.search_async(query, {'hitsPerPage': limit})
        formatted_hits = []
        for hit in results.get('hits', []):
            if 'objectID' in hit:
                 formatted_hits.append({
                     'imdb_id': hit['objectID'],
                     'title': hit.get('title', 'N/A')
                 })
            else:
                 logger.warning(f"Algolia hit missing objectID: {hit}")
        return formatted_hits
    except Exception as e:
        logger.error(f"Failed to search Algolia for query '{query}': {e}", exc_info=True)
        return []

async def algolia_add_movie(movie_data: dict) -> bool:
    """Add/update a single movie in Algolia (Upsert)."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping add_movie")
        return False
    if 'objectID' not in movie_data or not movie_data['objectID']:
        if 'imdb_id' in movie_data and movie_data['imdb_id']:
            movie_data['objectID'] = movie_data['imdb_id']
        else:
            logger.error(f"Cannot add movie to Algolia: Missing objectID/imdb_id. Data: {movie_data}")
            return False

    try:
        await index.save_object_async(movie_data)
        return True
    except Exception as e:
        logger.error(f"Failed to add/update object {movie_data.get('objectID', 'N/A')} to Algolia: {e}", exc_info=True)
        return False

async def algolia_add_batch_movies(movies_list: List[dict]) -> bool:
    """Add/update multiple movies in Algolia (Batch Upsert)."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping add_batch_movies")
        return False
    if not movies_list:
        logger.info("algolia_add_batch_movies called with empty list.")
        return True

    valid_movies = []
    for movie in movies_list:
        if 'objectID' not in movie or not movie['objectID']:
             if 'imdb_id' in movie and movie['imdb_id']:
                 movie['objectID'] = movie['imdb_id']
             else:
                 logger.warning(f"Skipping movie in batch: Missing objectID/imdb_id. Data: {movie}")
                 continue
        valid_movies.append(movie)

    if not valid_movies:
         logger.warning("No valid movies found in batch after checking for objectID.")
         return False

    try:
        await index.save_objects_async(valid_movies, {"batchSize": 1000})
        logger.info(f"Successfully processed {len(valid_movies)} objects in Algolia batch.")
        return True
    except Exception as e:
        logger.error(f"Failed to process batch in Algolia: {e}", exc_info=True)
        return False

async def algolia_remove_movie(imdb_id: str) -> bool:
    """Delete a movie from Algolia index by its objectID (imdb_id)."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping remove_movie")
        return False
    if not imdb_id:
        logger.warning("Algolia remove_movie called with empty imdb_id.")
        return False

    try:
        await index.delete_object_async(imdb_id)
        logger.info(f"Successfully submitted delete request for object from Algolia: {imdb_id}")
        return True
    except Exception as e:
        if 'ObjectID does not exist' in str(e):
             logger.info(f"Object {imdb_id} already deleted or never existed in Algolia.")
             return True
        logger.error(f"Failed to delete object {imdb_id} from Algolia: {e}", exc_info=True)
        return False

async def algolia_clear_index() -> bool:
    """Clear all objects from the Algolia index."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping clear_index")
        return False

    try:
        await index.clear_objects_async()
        logger.info(f"Successfully cleared Algolia index: {ALGOLIA_INDEX_NAME}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear Algolia index {ALGOLIA_INDEX_NAME}: {e}", exc_info=True)
        return False

async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """Fully synchronize the Algolia index using replace_all_objects."""
    if not is_algolia_ready():
        logger.error("Algolia not ready, cannot perform sync.")
        return False, 0

    valid_movies = []
    for movie in all_movies_data:
        if 'objectID' not in movie or not movie['objectID']:
             if 'imdb_id' in movie and movie['imdb_id']:
                 movie['objectID'] = movie['imdb_id']
             else:
                 logger.warning(f"Skipping movie in sync: Missing objectID/imdb_id. Data: {movie}")
                 continue
        valid_movies.append(movie)

    total_to_upload = len(valid_movies)

    if not valid_movies:
        logger.info("Sync: DB data is empty or invalid, clearing Algolia index...")
        cleared = await algolia_clear_index()
        return cleared, 0

    try:
        logger.info(f"Sync: Starting full replacement of Algolia index with {total_to_upload:,} objects...")
        await index.replace_all_objects_async(valid_movies, {"batchSize": 1000})
        logger.info(f"Sync: Full sync to Algolia complete ({total_to_upload:,} objects).")
        return True, total_to_upload

    except Exception as e:
        logger.error(f"Failed during Algolia sync (replace_all_objects): {e}", exc_info=True)
        return False, 0
