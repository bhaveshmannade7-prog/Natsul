# typesense_client.py

import os
import logging
from typing import List, Dict, Tuple
import typesense
from typesense.exceptions import ObjectNotFound
from httpx import HTTPStatusError
from dotenv import load_dotenv
import asyncio
import certifi # <--- Certifi ko import karna zaroori hai

load_dotenv()

logger = logging.getLogger("bot.typesense")

def run_sync(func):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, func)

TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY")
TYPESENSE_HOST = os.getenv("TYPESENSE_HOST")
TYPESENSE_PORT = os.getenv("TYPESENSE_PORT", "443")
TYPESENSE_PROTOCOL = os.getenv("TYPESENSE_PROTOCOL", "HTTPS") # Default "HTTPS" hi rakhein

COLLECTION_NAME = "movies" # Collection ka naam

client = None
_is_ready = False

# === SCHEMA DEFINITION ===
movie_schema = {
    'name': COLLECTION_NAME,
    'fields': [
        {'name': 'imdb_id', 'type': 'string', 'facet': False, 'sort': True},
        {'name': 'title', 'type': 'string', 'facet': False, 'sort': True}, 
        {'name': 'clean_title', 'type': 'string', 'facet': False},
        {'name': 'year', 'type': 'string', 'facet': True, 'optional': True, 'sort': True}, 
    ],
    'default_sorting_field': 'title'
}
# === END SCHEMA ===


async def initialize_typesense():
    """Typesense client ko initialize karta hai aur collection check karta hai."""
    global client, _is_ready
    if _is_ready:
        logger.info("Typesense already initialized.")
        return True

    if not all([TYPESENSE_API_KEY, TYPESENSE_HOST, TYPESENSE_PORT, TYPESENSE_PROTOCOL]):
        logger.critical("Typesense environment variables missing. Cannot initialize.")
        _is_ready = False
        return False

    logger.info(f"Initializing Typesense client for {TYPESENSE_PROTOCOL}://{TYPESENSE_HOST}:{TYPESENSE_PORT}")
    
    try:
        # --- YEH HAI ASLI FIX (The Correct Fix) ---

        # 1. Pehle certifi ka path lein
        ca_path = certifi.where()
        logger.info(f"Using certifi CA bundle for Typesense at: {ca_path}")

        # 2. Typesense config banayein
        config = {
            'nodes': [{
                'host': TYPESENSE_HOST,
                'port': TYPESENSE_PORT,
                'protocol': TYPESENSE_PROTOCOL.lower() # 'https' ya 'http' hona chahiye
            }],
            'api_key': TYPESENSE_API_KEY,
            'connection_timeout_seconds': 5,
            'retry_interval_seconds': 1,
            'num_retries': 3,
            
            # 3. SSL settings ko 'httpx_client_options' ke andar pass karein
            'httpx_client_options': {
                'verify': ca_path
            }
            # --- END FIX ---
        }
        
        # 4. Client ko sirf 'config' object pass karein
        client = typesense.Client(config)
        
        # --- END OF FIX ---


        # 1. Check karein ki collection pehle se hai ya nahi
        try:
            logger.info(f"Checking for Typesense collection '{COLLECTION_NAME}'...")
            await run_sync(lambda: client.collections[COLLECTION_NAME].retrieve())
            logger.info(f"Typesense collection '{COLLECTION_NAME}' found.")

        except ObjectNotFound:
            # 2. Agar nahi hai, toh naya banayein
            logger.warning(f"Collection '{COLLECTION_NAME}' not found. Creating...")
            try:
                await run_sync(lambda: client.collections.create(movie_schema))
                logger.info(f"Successfully created Typesense collection '{COLLECTION_NAME}'.")
            except HTTPStatusError as e:
                if e.response.status_code == 409:
                    logger.warning(f"Collection creation conflict (409), assuming it exists now.")
                else:
                    logger.error(f"Failed to create collection (HTTPError): {e}", exc_info=True)
                    raise
            except Exception as e:
                logger.error(f"Failed to create collection (Unknown Error): {e}", exc_info=True)
                raise
        
        _is_ready = True
        logger.info("Typesense initialization successful.")
        return True

    except Exception as e:
        logger.critical(f"Failed to initialize Typesense client: {e}", exc_info=True)
        client = None
        _is_ready = False
        return False


def is_typesense_ready():
    """Check karein ki Typesense client initialize hua ya nahi."""
    return _is_ready and client is not None


async def typesense_search(query: str, limit: int = 20) -> List[Dict]:
    """Typesense mein movies search karein."""
    if not is_typesense_ready():
        logger.error("Typesense not ready for search.")
        return []

    search_params = {
        'q': query,
        'query_by': 'clean_title, title',
        'per_page': limit,
        'sort_by': '_text_match:desc, year:desc',
        'num_typos': 2,
        'drop_tokens_threshold': 1,
    }

    try:
        result = await run_sync(lambda: client.collections[COLLECTION_NAME].documents.search(search_params))

        hits = result.get('hits', [])
        return [
            {
                'imdb_id': hit['document']['imdb_id'],
                'title': hit['document']['title'],
                'year': hit['document'].get('year')
            }
            for hit in hits if 'document' in hit
        ]
    except Exception as e:
        logger.error(f"Typesense search failed for '{query}': {e}", exc_info=True)
        return []


async def typesense_add_movie(movie_data: dict) -> bool:
    """Typesense mein ek movie add/update karein."""
    if not is_typesense_ready():
        logger.warning("Typesense not ready for add_movie.")
        return False

    document = movie_data.copy()
    if 'id' not in document:
         document['id'] = document['imdb_id']

    try:
        await run_sync(lambda: client.collections[COLLECTION_NAME].documents.upsert(document))
        return True
    except Exception as e:
        logger.error(f"Typesense upsert failed for {document.get('id', 'N/A')}: {e}", exc_info=True)
        return False


async def typesense_add_batch_movies(movies_list: List[dict]) -> bool:
    """Typesense mein movies ko batch mein add karein."""
    if not is_typesense_ready():
        logger.warning("Typesense not ready for add_batch.")
        return False
    if not movies_list:
        return True

    # Batch ke liye documents ko format karein (sab mein 'id' field hona zaroori hai)
    formatted_list = []
    for item in movies_list:
        # 'imdb_id' ko 'id' ki tarah copy karein
        if 'imdb_id' in item:
            item['id'] = item['imdb_id']
        elif 'id' not in item:
             logger.warning(f"Skipping batch item (no ID): {item.get('title')}")
             continue # ID ke bina skip karein
        formatted_list.append(item)

    if not formatted_list:
        logger.warning("No valid items in batch.")
        return False

    try:
        results = await run_sync(lambda: client.collections[COLLECTION_NAME].documents.import_(formatted_list, {'action': 'upsert'}))

        failed_items = [res for res in results if not res.get('success', True)]
        if failed_items:
            logger.error(f"Typesense batch failed for {len(failed_items)} items. First error: {failed_items[0].get('error')}")
            return False

        logger.info(f"Typesense batch processed {len(formatted_list)} items.")
        return True
    except Exception as e:
        logger.error(f"Typesense batch import failed: {e}", exc_info=True)
        return False


async def typesense_remove_movie(imdb_id: str) -> bool:
    """Typesense se ek movie delete karein."""
    if not is_typesense_ready():
        logger.warning("Typesense not ready for remove_movie.")
        return False
    if not imdb_id:
        return False

    try:
        await run_sync(lambda: client.collections[COLLECTION_NAME].documents[imdb_id].delete())
        logger.info(f"Typesense delete request for {imdb_id} successful.")
        return True
    except ObjectNotFound:
        logger.info(f"Typesense: {imdb_id} pehle se deleted hai (404).")
        return True
    except Exception as e:
        logger.error(f"Typesense delete failed for {imdb_id}: {e}", exc_info=True)
        return False


async def typesense_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """Poore DB ko Typesense se sync karein (Collection delete karke naya banayein)."""
    if not is_typesense_ready():
        logger.error("Typesense not ready for sync.")
        return False, 0
    
    count = len(all_movies_data)
    
    try:
        # 1. Purana collection delete karein
        try:
            logger.info(f"Sync: Deleting old collection '{COLLECTION_NAME}'...")
            await run_sync(lambda: client.collections[COLLECTION_NAME].delete())
        except ObjectNotFound:
            logger.info("Sync: Old collection not found (404), skipping delete.")
        except Exception as e:
            logger.error(f"Sync: Failed to delete old collection: {e}", exc_info=True)
            return False, 0

        # 2. Naya collection banayein (schema ke saath)
        logger.info("Sync: Creating new collection...")
        await run_sync(lambda: client.collections.create(movie_schema))
        
        # 3. Naya data batch mein import karein
        if not all_movies_data:
            logger.info("Sync: No data from DB, empty collection created.")
            return True, 0
            
        logger.info(f"Sync: Importing {count:,} documents into Typesense...")
        if await typesense_add_batch_movies(all_movies_data):
            logger.info("Sync completed successfully.")
            return True, count
        else:
            logger.error("Sync: Batch import failed during sync.")
            return False, 0

    except Exception as e:
        logger.error(f"Typesense sync failed: {e}", exc_info=True)
        return False, 0
