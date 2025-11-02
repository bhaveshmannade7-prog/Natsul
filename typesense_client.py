# typesense_client.py

import os
import logging
from typing import List, Dict, Tuple
import typesense
from typesense.exceptions import ObjectNotFound
from httpx import HTTPStatusError # 409 Conflict ke liye
from dotenv import load_dotenv
import asyncio

load_dotenv()

logger = logging.getLogger("bot.typesense")

TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY")
TYPESENSE_HOST = os.getenv("TYPESENSE_HOST")
TYPESENSE_PORT = os.getenv("TYPESENSE_PORT", "443")
TYPESENSE_PROTOCOL = os.getenv("TYPESENSE_PROTOCOL", "https")

COLLECTION_NAME = "movies" # Collection ka naam

client = None
_is_ready = False

# === SCHEMA DEFINITION ===
# Yahi woh schema hai jise hum Typesense mein banayenge
movie_schema = {
    'name': COLLECTION_NAME,
    'fields': [
        {'name': 'imdb_id', 'type': 'string', 'facet': False, 'sort': True},
        # --- FIX: 'title' ko sortable banaya ---
        {'name': 'title', 'type': 'string', 'facet': False, 'sort': True},
        # --- END FIX ---
        {'name': 'clean_title', 'type': 'string', 'facet': False},
        # --- FIX: 'year' ko bhi sortable banaya ---
        {'name': 'year', 'type': 'string', 'facet': True, 'optional': True, 'sort': True},
        # --- END FIX ---
    ],
    # Ab yeh valid hai, kyunki 'title' sortable hai
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
        # Async client ka istemal karein
        client = typesense.Client(
            nodes=[{
                'host': TYPESENSE_HOST,
                'port': TYPESENSE_PORT,
                'protocol': TYPESENSE_PROTOCOL
            }],
            api_key=TYPESENSE_API_KEY,
            connection_timeout_seconds=5,
            retry_interval_seconds=1,
            num_retries=3,
            client_factory=typesense.AiohttpAdapter() # Async adapter
        )

        # 1. Check karein ki collection pehle se hai ya nahi
        try:
            logger.info(f"Checking for Typesense collection '{COLLECTION_NAME}'...")
            await client.collections[COLLECTION_NAME].retrieve()
            logger.info(f"Typesense collection '{COLLECTION_NAME}' found.")
            
        except ObjectNotFound:
            # 2. Agar nahi hai, toh naya banayein
            logger.warning(f"Collection '{COLLECTION_NAME}' not found. Creating...")
            try:
                await client.collections.create(movie_schema)
                logger.info(f"Successfully created Typesense collection '{COLLECTION_NAME}'.")
            except HTTPStatusError as e:
                # Agar create karte waqt 409 (Conflict) aata hai (race condition)
                if e.response.status_code == 409:
                    logger.warning(f"Collection creation conflict (409), assuming it exists now.")
                else:
                    logger.error(f"Failed to create collection (HTTPError): {e}", exc_info=True)
                    raise # Asli error ko raise karein
            except Exception as e:
                logger.error(f"Failed to create collection (Unknown Error): {e}", exc_info=True)
                raise # Asli error ko raise karein
        
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
        'query_by': 'clean_title, title', # Pehle clean_title, fir title
        'per_page': limit,
        'sort_by': '_text_match:desc, year:desc', # Best match upar, fir naya saal
        'num_typos': 2, # Typo tolerance
        'drop_tokens_threshold': 1, # Thode words drop kar sakte hain
    }
    
    try:
        result = await client.collections[COLLECTION_NAME].documents.search(search_params)
        
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
    
    # Typesense 'id' field ko 'imdb_id' se alag chahta hai
    doc_id = movie_data['imdb_id']
    document = movie_data.copy()
    
    try:
        # 'upsert' ka matlab hai: agar hai toh update karo, nahi toh naya banao
        await client.collections[COLLECTION_NAME].documents.upsert(document, {'action': 'upsert'})
        return True
    except Exception as e:
        logger.error(f"Typesense upsert failed for {doc_id}: {e}", exc_info=True)
        return False


async def typesense_add_batch_movies(movies_list: List[dict]) -> bool:
    """Typesense mein movies ko batch mein add karein."""
    if not is_typesense_ready():
        logger.warning("Typesense not ready for add_batch.")
        return False
    if not movies_list:
        return True

    try:
        # Batch import, sabko 'upsert' karein
        results = await client.collections[COLLECTION_NAME].documents.import_(movies_list, {'action': 'upsert'})
        
        # Error check
        failed_items = [res for res in results if not res.get('success', True)]
        if failed_items:
            logger.error(f"Typesense batch failed for {len(failed_items)} items. First error: {failed_items[0].get('error')}")
            return False
            
        logger.info(f"Typesense batch processed {len(movies_list)} items.")
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
        await client.collections[COLLECTION_NAME].documents[imdb_id].delete()
        logger.info(f"Typesense delete request for {imdb_id} successful.")
        return True
    except ObjectNotFound:
        logger.info(f"Typesense: {imdb_id} pehle se deleted hai (404).")
        return True # Delete ho chuka hai
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
            await client.collections[COLLECTION_NAME].delete()
        except ObjectNotFound:
            logger.info("Sync: Old collection not found (404), skipping delete.")
        except Exception as e:
            logger.error(f"Sync: Failed to delete old collection: {e}", exc_info=True)
            return False, 0

        # 2. Naya collection banayein (schema ke saath)
        logger.info("Sync: Creating new collection...")
        await client.collections.create(movie_schema)
        
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
