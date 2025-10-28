import os
import logging
from typing import List, Dict, Tuple
import algoliasearch # Import the base library to check version
from algoliasearch.search.client import SearchClient # v4+ import
# For older versions (v2/v3), the client was often imported differently,
# but SearchClient might exist with different methods. We'll handle via try/except.
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
    """Tries to initialize the Algolia client and index asynchronously."""
    global client, index, _is_ready
    if _is_ready: return True # Already initialized

    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia environment variables missing (APP_ID, ADMIN_KEY, or INDEX_NAME). Cannot initialize.")
        _is_ready = False
        return False

    logger.info("Attempting to initialize Algolia client...")
    try:
        # --- Compatibility Fix ---
        # Try v4 initialization first
        try:
            client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
            index = client.init_index(ALGOLIA_INDEX_NAME)
            logger.info(f"Algolia client initialized using v4+ syntax (SearchClient). App ID: {ALGOLIA_APP_ID}")
        except AttributeError:
             logger.warning("'SearchClient' object has no attribute 'init_index'. Falling back to older Algolia client syntax (v2/v3). Ensure algoliasearch version >= 4.0 is installed for best results.")
             # Fallback for older versions (v2/v3 might use this - untested directly but plausible)
             # Note: The async client might have been separate in older versions.
             # This fallback assumes a sync client was available at the top level import.
             # If using v2/v3 async, the import and usage would be different.
             # This primarily handles the case where Render installs an old SYNC version unexpectedly.
             from algoliasearch import algoliasearch as old_algoliasearch
             sync_client = old_algoliasearch.Client(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
             index = sync_client.init_index(ALGOLIA_INDEX_NAME)
             client = sync_client # Store the sync client reference
             logger.info(f"Algolia client initialized using v2/v3 fallback syntax. App ID: {ALGOLIA_APP_ID}")
             # Add a warning that async methods might fail if using fallback
             logger.warning("Using older Algolia client fallback - async methods may not be available or may behave differently.")
        except Exception as init_err:
             logger.critical(f"Unexpected error during Algolia client creation: {init_err}", exc_info=True)
             _is_ready = False
             return False
        # --- End Compatibility Fix ---


        logger.info(f"Using Algolia Index: {ALGOLIA_INDEX_NAME}")

        # Check connection and apply settings (use async methods if possible)
        logger.info("Fetching/Applying Algolia index settings...")
        settings_to_apply = {
            'minWordSizefor1Typo': 3, 'minWordSizefor2Typos': 7, 'hitsPerPage': 20,
            'searchableAttributes': ['title', 'imdb_id', 'year'],
            'queryType': 'prefixLast', 'attributesForFaceting': ['searchable(year)'],
            'typoTolerance': 'min', 'removeStopWords': True, 'ignorePlurals': True,
        }
        try:
            # Prefer async methods
            if hasattr(index, 'set_settings_async'):
                await index.set_settings_async(settings_to_apply, request_options={'timeout': 10})
                await index.get_settings_async(request_options={'timeout': 5}) # Confirm connection
                logger.info("Applied settings using async methods.")
            # Fallback to sync methods if using older client
            elif hasattr(index, 'set_settings'):
                 logger.warning("Using synchronous set_settings fallback.")
                 index.set_settings(settings_to_apply)
                 index.get_settings() # Sync confirmation
                 logger.info("Applied settings using sync methods.")
            else:
                 logger.error("Could not find set_settings method on index object.")
                 _is_ready = False
                 return False

            _is_ready = True
            logger.info("Algolia initialization and settings apply successful.")
            return True

        except Exception as settings_err:
            logger.critical(f"Failed to apply/verify Algolia settings: {settings_err}", exc_info=True)
            _is_ready = False
            return False

    except Exception as e:
        # Catch errors during client creation itself
        logger.critical(f"Critical failure during Algolia client initialization: {e}", exc_info=True)
        client = None
        index = None
        _is_ready = False
        return False


def is_algolia_ready():
    """Check if Algolia client and index were successfully initialized."""
    # This function now just returns the flag set by initialize_algolia
    return _is_ready and client is not None and index is not None

# --- Async Wrappers for Algolia Operations ---
# These wrappers check _is_ready and prefer async methods, with sync fallbacks if needed

async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    if not is_algolia_ready(): logger.error("Algolia not ready for search."); return []
    try:
        if hasattr(index, 'search_async'):
            results = await index.search_async(query, {'hitsPerPage': limit})
        elif hasattr(index, 'search'): # Sync fallback
             logger.warning("Using synchronous search fallback.")
             results = index.search(query, {'hitsPerPage': limit})
        else: raise RuntimeError("No search method found")

        hits = results.get('hits', [])
        return [{'imdb_id': hit['objectID'], 'title': hit.get('title', 'N/A')} for hit in hits if 'objectID' in hit]
    except Exception as e:
        logger.error(f"Algolia search failed for '{query}': {e}", exc_info=True)
        return []

async def algolia_add_movie(movie_data: dict) -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for add_movie."); return False
    if 'objectID' not in movie_data or not movie_data['objectID']:
         if 'imdb_id' in movie_data and movie_data['imdb_id']: movie_data['objectID'] = movie_data['imdb_id']
         else: logger.error(f"Missing objectID/imdb_id for Algolia add: {movie_data}"); return False
    try:
        if hasattr(index, 'save_object_async'):
            await index.save_object_async(movie_data)
        elif hasattr(index, 'save_object'): # Sync fallback
             logger.warning("Using synchronous save_object fallback.")
             index.save_object(movie_data)
        else: raise RuntimeError("No save_object method found")
        return True
    except Exception as e:
        logger.error(f"Algolia save_object failed for {movie_data.get('objectID', 'N/A')}: {e}", exc_info=True)
        return False

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
        if hasattr(index, 'save_objects_async'):
            await index.save_objects_async(valid_movies, {"batchSize": 1000})
        elif hasattr(index, 'save_objects'): # Sync fallback
             logger.warning("Using synchronous save_objects fallback.")
             index.save_objects(valid_movies, {"batchSize": 1000})
        else: raise RuntimeError("No save_objects method found")
        logger.info(f"Algolia batch processed {len(valid_movies)} items.")
        return True
    except Exception as e:
        logger.error(f"Algolia save_objects failed: {e}", exc_info=True)
        return False

async def algolia_remove_movie(imdb_id: str) -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for remove_movie."); return False
    if not imdb_id: return False
    try:
        if hasattr(index, 'delete_object_async'):
            await index.delete_object_async(imdb_id)
        elif hasattr(index, 'delete_object'): # Sync fallback
             logger.warning("Using synchronous delete_object fallback.")
             index.delete_object(imdb_id)
        else: raise RuntimeError("No delete_object method found")
        logger.info(f"Algolia delete request for {imdb_id} submitted.")
        return True
    except Exception as e:
        if 'ObjectID does not exist' in str(e): logger.info(f"Algolia: {imdb_id} already deleted."); return True
        logger.error(f"Algolia delete_object failed for {imdb_id}: {e}", exc_info=True)
        return False

async def algolia_clear_index() -> bool:
    if not is_algolia_ready(): logger.warning("Algolia not ready for clear_index."); return False
    try:
        if hasattr(index, 'clear_objects_async'):
            await index.clear_objects_async()
        elif hasattr(index, 'clear_objects'): # Sync fallback
             logger.warning("Using synchronous clear_objects fallback.")
             index.clear_objects()
        else: raise RuntimeError("No clear_objects method found")
        logger.info(f"Algolia index '{ALGOLIA_INDEX_NAME}' cleared.")
        return True
    except Exception as e:
        logger.error(f"Algolia clear_objects failed: {e}", exc_info=True)
        return False

async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """Syncs DB data to Algolia using replace_all_objects if available."""
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
        if hasattr(index, 'replace_all_objects_async'):
            await index.replace_all_objects_async(valid_movies, {"batchSize": 1000})
            logger.info("Sync completed (async replace).")
        elif hasattr(index, 'replace_all_objects'): # Sync fallback
             logger.warning("Using synchronous replace_all_objects fallback.")
             index.replace_all_objects(valid_movies, {"batchSize": 1000})
             logger.info("Sync completed (sync replace).")
        else: raise RuntimeError("No replace_all_objects method found")
        return True, count
    except Exception as e:
        logger.error(f"Algolia replace_all_objects failed: {e}", exc_info=True)
        return False, 0
