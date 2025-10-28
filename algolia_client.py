import os
import logging
from typing import List, Dict, Tuple
from algoliasearch.search.client import SearchClient # Correct import for v4+
from dotenv import load_dotenv
import asyncio # Import asyncio for potential retries

load_dotenv()

logger = logging.getLogger("bot.algolia")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_ADMIN_KEY = os.getenv("ALGOLIA_ADMIN_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME")

client = None
index = None
_is_ready = False # Internal flag to track initialization status

async def initialize_algolia():
    """Tries to initialize the Algolia client and index."""
    global client, index, _is_ready
    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia APP_ID, ADMIN_KEY, or INDEX_NAME environment variables missing!")
        _is_ready = False
        return

    try:
        # Correct initialization for v4+
        client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        index = client.init_index(ALGOLIA_INDEX_NAME)

        # Basic check to see if connection works (optional but recommended)
        # Try getting settings asynchronously
        await index.get_settings_async()

        # Apply desired index settings asynchronously
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
            'typoTolerance': 'min', # Stricter typo tolerance might be faster
            'removeStopWords': True, # Remove common English stop words
            'ignorePlurals': True, # Treat singular/plural the same
        })
        _is_ready = True
        logger.info(f"Algolia client initialized and settings applied. Index: '{ALGOLIA_INDEX_NAME}'")

    except Exception as e:
        # Log the specific error during initialization
        logger.critical(f"Failed to initialize Algolia client or apply settings: {e}", exc_info=True)
        client = None
        index = None
        _is_ready = False

# Run initialization when the module is loaded
# Use asyncio.create_task if running within an async context already,
# or handle potential loop issues if run standalone.
# For simplicity in this structure, we rely on FastAPI's lifespan to ensure an event loop exists.
# If run outside FastAPI, you'd need asyncio.run(initialize_algolia())
asyncio.create_task(initialize_algolia())

def is_algolia_ready():
    """Check if Algolia client and index were successfully initialized."""
    if not _is_ready:
        # Maybe attempt re-initialization? Or just log.
        logger.warning("is_algolia_ready called, but initialization failed or hasn't completed.")
    return _is_ready and client is not None and index is not None

async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    """Search Algolia asynchronously."""
    if not is_algolia_ready():
        logger.error("Algolia not ready, cannot perform search.")
        return []

    try:
        results = await index.search_async(query, {'hitsPerPage': limit})
        formatted_hits = []
        # Ensure hits exist and objectID is present
        for hit in results.get('hits', []):
            if 'objectID' in hit:
                 formatted_hits.append({
                     'imdb_id': hit['objectID'], # Use objectID as the reliable ID
                     'title': hit.get('title', 'N/A') # Provide default title
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
        # Ensure objectID is present, try using imdb_id as fallback
        if 'imdb_id' in movie_data and movie_data['imdb_id']:
            movie_data['objectID'] = movie_data['imdb_id']
        else:
            logger.error(f"Cannot add movie to Algolia: Missing objectID/imdb_id. Data: {movie_data}")
            return False

    try:
        # Use save_object_async for upsert
        await index.save_object_async(movie_data)
        # logger.debug(f"Successfully added/updated object in Algolia: {movie_data['objectID']}") # Debug level
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
        logger.info("algolia_add_batch_movies called with empty list, nothing to do.")
        return True # Technically successful

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
         return False # Indicate failure if all items were invalid

    try:
        # Use save_objects_async for batch upsert
        await index.save_objects_async(valid_movies, {"batchSize": 1000}) # Use batchSize option
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
        # Use delete_object_async
        await index.delete_object_async(imdb_id)
        logger.info(f"Successfully submitted delete request for object from Algolia: {imdb_id}")
        return True
    except Exception as e:
        # Check if it's a "not found" error, which isn't a failure in this context
        if 'ObjectID does not exist' in str(e):
             logger.info(f"Object {imdb_id} already deleted or never existed in Algolia.")
             return True # Still considered successful deletion attempt
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
    """
    Fully synchronize the Algolia index with the provided data from the DB.
    Uses replace_all_objects for efficiency and atomicity.
    """
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
        # If DB data (after validation) is empty, clear the index
        logger.info("Sync: DB data is empty or invalid, clearing Algolia index...")
        cleared = await algolia_clear_index()
        return cleared, 0

    try:
        logger.info(f"Sync: Starting full replacement of Algolia index with {total_to_upload:,} objects...")
        # Use replace_all_objects_async for a full atomic replacement
        await index.replace_all_objects_async(valid_movies, {"batchSize": 1000})

        logger.info(f"Sync: Full sync to Algolia complete ({total_to_upload:,} objects).")
        return True, total_to_upload

    except Exception as e:
        logger.error(f"Failed during Algolia sync (replace_all_objects): {e}", exc_info=True)
        return False, 0
