# algolia_client.py

import os
import logging
from typing import List, Dict, Tuple, Any 
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

client: SearchClient | None = None 
_is_ready = False


async def initialize_algolia():
    """Initializes the Algolia client and applies settings."""
    global client, _is_ready
    
    if _is_ready:
        logger.info("Algolia pehle se hi initialized hai.")
        return True

    if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
        logger.critical("Algolia environment variables missing. Initialize nahi ho sakta.")
        _is_ready = False
        return False

    logger.info(f"Attempting to initialize Algolia client (v4 Async) for index: {ALGOLIA_INDEX_NAME}")
    try:
        client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        logger.info(f"Algolia Async client initialized.")
        
        # Settings apply karne ke liye client.set_settings ka upyog karein
        settings_to_apply = {
            'searchableAttributes': ['clean_title', 'title', 'imdb_id', 'year', 'unordered(title)', 'unordered(clean_title)'], 
            'hitsPerPage': 20,
            'typoTolerance': 'true', 
            'minWordSizefor1Typo': 2, 
            'minWordSizefor2Typos': 4, 
            'queryType': 'prefixLast', 
            'attributesForFaceting': ['searchable(year)'],
            'removeStopWords': True,
            'ignorePlurals': True,
        }
        
        # client.set_settings ko index_name aur settings ke saath call karein
        await client.set_settings(
            index_name=ALGOLIA_INDEX_NAME, 
            index_settings=settings_to_apply
        )
        logger.info(f"Algolia settings applied for index '{ALGOLIA_INDEX_NAME}'.")

        _is_ready = True
        logger.info("Algolia initialization aur settings apply successful.")
        return True

    except Exception as e:
        logger.critical(f"Failed to initialize Algolia client ya settings apply karne mein: {e}", exc_info=True)
        client = None
        _is_ready = False
        return False


def is_algolia_ready():
    """Check karein ki Algolia client initialize ho chuka hai."""
    return _is_ready and client is not None


async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    """Algolia mein search karta hai aur hits return karta hai."""
    if not is_algolia_ready():
        logger.error("Algolia search ke liye taiyar nahi hai.")
        return []
    try:
        # CRITICAL FIX: client.search() ko ab seedhe list of requests chhodkar
        #  search method parameters ke saath call kiya jayega.
        result = await client.search_for_facet_values( # search_for_facet_values() ka upyog karna compatible hai
            index_name=ALGOLIA_INDEX_NAME,
            facet_name='title', # Koi bhi searchable attribute
            facet_query=query,
            request_options={
                'hitsPerPage': limit,
                # Simple search ke liye sirf query aur index name kafi hai
            }
        )
        
        # search_for_facet_values se hits nahi, balki facet hits milte hain, isliye
        # hum sabse compatible method client.search() ke liye use karenge aur list se object banayenge
        
        # FINAL ATTEMPT ON client.search() with required parameter structure
        # NOTE: Agar yeh fail hua to iska matlab hai ki Algolia keys galat hain.
        search_requests = {
            "indexName": ALGOLIA_INDEX_NAME,
            "query": query,
            "hitsPerPage": limit,
            "restrictSearchableAttributes": ['clean_title', 'title', 'year', 'imdb_id']
        }
        
        # Hum client.search() ko seedhe object (dictionary) bhejenge, na ki list.
        # client.search() expects list of search requests, isliye hum isko ek final tarike se wrap karenge:
        
        # --- Search Request Logic ---
        result = await client.search([
            {
                "indexName": ALGOLIA_INDEX_NAME,
                "query": query,
                "hitsPerPage": limit,
                "restrictSearchableAttributes": ['clean_title', 'title', 'year', 'imdb_id']
            }
        ])
        
        # Hits ko extract karein
        hits = []
        if result.results and len(result.results) > 0 and 'hits' in result.results[0]:
            hits = result.results[0]['hits']
        
        logger.info(f"Algolia returned {len(hits)} hits for query: '{query}'")

        return [
            {
                'imdb_id': hit.get('objectID'),
                'title': hit.get('title') or 'Title Missing',
                'year': hit.get('year')
            }
            for hit in hits if hit.get('objectID') 
        ]
    except Exception as e:
        logger.error(f"Algolia search fail hua '{query}' ke liye: {e}", exc_info=True)
        return []


async def algolia_add_movie(movie_data: dict) -> bool:
    """Ek single movie ko Algolia mein add/update karta hai."""
    if not is_algolia_ready():
        logger.warning("Algolia movie add ke liye taiyar nahi hai.")
        return False
    if 'objectID' not in movie_data or not movie_data['objectID']:
        if 'imdb_id' in movie_data and movie_data['imdb_id']:
            movie_data['objectID'] = movie_data['imdb_id']
        else:
            logger.error(f"Algolia add ke liye objectID/imdb_id missing hai: {movie_data}")
            return False
    try:
        await client.save_object(
            index_name=ALGOLIA_INDEX_NAME, 
            body=movie_data
        )
        return True
    except Exception as e:
        logger.error(f"Algolia save_object fail hua {movie_data.get('objectID', 'N/A')} ke liye: {e}", exc_info=True)
        return False


async def algolia_add_batch_movies(movies_list: List[dict]) -> bool:
    """Multiple movies ko Algolia mein batch mein add/update karta hai."""
    if not is_algolia_ready():
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
        await client.save_objects(
            index_name=ALGOLIA_INDEX_NAME, 
            objects=valid_movies
        )
        logger.info(f"Algolia batch mein {len(valid_movies)} items process hue.")
        return True
    except Exception as e:
        logger.error(f"Algolia save_objects fail hua: {e}", exc_info=True)
        return False


async def algolia_remove_movie(imdb_id: str) -> bool:
    """Algolia se movie remove karta hai."""
    if not is_algolia_ready():
        logger.warning("Algolia movie remove ke liye taiyar nahi hai.")
        return False
    if not imdb_id:
        return False
    try:
        await client.delete_object(
            index_name=ALGOLIA_INDEX_NAME, 
            object_id=imdb_id
        )
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
    if not is_algolia_ready():
        logger.warning("Algolia index clear ke liye taiyar nahi hai.")
        return False
    try:
        await client.clear_objects(index_name=ALGOLIA_INDEX_NAME)
        logger.info(f"Algolia index '{ALGOLIA_INDEX_NAME}' clear ho gaya.")
        return True
    except Exception as e:
        logger.error(f"Algolia clear_objects failed: {e}", exc_info=True)
        return False


async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """MongoDB ke saare data se Algolia index ko replace (sync) karta hai."""
    if not is_algolia_ready():
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
        await client.save_objects(
            index_name=ALGOLIA_INDEX_NAME,
            objects=valid_movies
        )
        logger.info(f"Sync poora hua.")
        return True, count
    except Exception as e:
        logger.error(f"Algolia sync fail hua: {e}", exc_info=True)
        return False, 0
