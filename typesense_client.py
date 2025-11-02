# typesense_client.py
import os
import logging
from typing import List, Dict, Tuple
import typesense
from typesense.api_call import ApiCall
from typesense.exceptions import ObjectNotFound, Conflict

from dotenv import load_dotenv
import asyncio

load_dotenv()

logger = logging.getLogger("bot.typesense")

TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY")
TYPESENSE_HOST = os.getenv("TYPESENSE_HOST")
TYPESENSE_PORT = os.getenv("TYPESENSE_PORT", "443")
TYPESENSE_PROTOCOL = os.getenv("TYPESENSE_PROTOCOL", "https")

COLLECTION_NAME = os.getenv("TYPESENSE_COLLECTION_NAME", "movies")

client = None
_is_ready = False

# Movie schema definition
movie_schema = {
    'name': COLLECTION_NAME,
    'fields': [
        # `imdb_id` ya `objectID` ko primary key banayein
        {'name': 'imdb_id', 'type': 'string'},
        {'name': 'title', 'type': 'string'},
        # `clean_title` search ke liye bahut zaroori hai
        {'name': 'clean_title', 'type': 'string'},
        # `year` ko filter/facet ke liye use kar sakte hain
        {'name': 'year', 'type': 'string', 'optional': True, 'facet': True}
    ],
    'default_sorting_field': 'title' # Optional: fallback sorting
}


async def initialize_typesense():
    """Typesense client ko initialize karta hai aur collection schema ensure karta hai."""
    global client, _is_ready
    if _is_ready:
        logger.info("Typesense already initialized.")
        return True

    if not TYPESENSE_API_KEY or not TYPESENSE_HOST:
        logger.critical("Typesense environment variables missing (TYPESENSE_API_KEY, TYPESENSE_HOST). Cannot initialize.")
        _is_ready = False
        return False

    logger.info(f"Attempting to initialize Typesense client for {TYPESENSE_HOST}:{TYPESENSE_PORT}...")
    try:
        # AiohttpAdapter ka istemal karein async operations ke liye
        api_call = ApiCall(
            num_retries=3,
            nodes=[{
                'host': TYPESENSE_HOST,
                'port': TYPESENSE_PORT,
                'protocol': TYPESENSE_PROTOCOL
            }],
            api_key=TYPESENSE_API_KEY,
            connection_timeout_seconds=5,
            healthcheck_interval_seconds=15,
            retry_interval_seconds=1,
            send_healthy_nodes_only=True,
            adapter_class_name='typesense.aiohttp_adapter.AiohttpAdapter'
        )
        client = typesense.Client(api_call)

        # Check health
        health = await client.health.retrieve()
        if not health.get('ok', False) and not health.get('status', '') == 'ok':
             raise Exception(f"Typesense health check failed: {health}")

        logger.info(f"Typesense client initialized successfully. Ensuring collection '{COLLECTION_NAME}'...")

        # Collection (schema) create/ensure karein
        try:
            await client.collections.create(movie_schema)
            logger.info(f"Typesense collection '{COLLECTION_NAME}' created.")
        except Conflict:
            logger.info(f"Typesense collection '{COLLECTION_NAME}' already exists. Updating schema...")
            # Schema update (agar zaroori ho)
            await client.collections[COLLECTION_NAME].update(movie_schema.get('fields', []))
        
        _is_ready = True
        logger.info("Typesense initialization successful.")
        return True

    except Exception as e:
        logger.critical(f"Failed to initialize Typesense client: {e}", exc_info=True)
        client = None
        _is_ready = False
        return False


def is_typesense_ready():
    """Check karein ki Typesense client successfully initialize hua ya nahi."""
    return _is_ready and client is not None


async def typesense_search(query: str, limit: int = 20) -> List[Dict]:
    if not is_typesense_ready():
        logger.error("Typesense not ready for search.")
        return []
    
    # Typesense ke liye search parameters
    search_params = {
        'q': query,
        'query_by': 'clean_title,title', # `clean_title` ko pehle rakhein
        'per_page': limit,
        'num_typos': '2', # Typo tolerance
        'typo_tokens_threshold': 1, # 1-word query pe bhi typo check karein
        'drop_tokens_threshold': 1, # Words drop karein agar match na mile
        'sort_by': '_text_match:desc', # Best match ko upar rakhein
    }
    
    try:
        result = await client.collections[COLLECTION_NAME].documents.search(search_params)
        
        hits = result.get('hits', [])
        return [
            {
                'imdb_id': hit['document']['imdb_id'],
                'title': hit['document'].get('title', 'Title Missing'),
                'year': hit['document'].get('year')
            }
            for hit in hits if hit.get('document') and hit['document'].get('imdb_id')
        ]
    except Exception as e:
        logger.error(f"Typesense search failed for '{query}': {e}", exc_info=True)
        return []


async def typesense_add_movie(movie_data: dict) -> bool:
    if not is_typesense_ready():
        logger.warning("Typesense not ready for add_movie.")
        return False
    
    # Typesense 'id' field ko string expect karta hai, 'objectID' nahi.
    # Hum 'imdb_id' ko primary key ('id') ki tarah use kar rahe hain
    if 'objectID' in movie_data:
        del movie_data['objectID']
    if 'imdb_id' not in movie_data:
         logger.error(f"Missing imdb_id for Typesense add: {movie_data}")
         return False
         
    # Ensure id field is set
    movie_data['id'] = movie_data['imdb_id']

    try:
        # `upsert` action ka use karein (add ya update)
        await client.collections[COLLECTION_NAME].documents.upsert(movie_data)
        return True
    except Exception as e:
        logger.error(f"Typesense upsert failed for {movie_data.get('id', 'N/A')}: {e}", exc_info=True)
        return False


async def typesense_add_batch_movies(movies_list: List[dict]) -> bool:
    if not is_typesense_ready():
        logger.warning("Typesense not ready for add_batch.")
        return False
    if not movies_list:
        return True
    
    valid_movies = []
    for m in movies_list:
        if 'objectID' in m:
            del m['objectID']
        if 'imdb_id' not in m:
            logger.warning(f"Skipping batch item (no imdb_id): {m}")
            continue
        m['id'] = m['imdb_id'] # Primary key set karein
        valid_movies.append(m)
        
    if not valid_movies:
        logger.warning("No valid items in batch.")
        return False
        
    try:
        # `import_` method ka istemal karein batch ke liye
        # action='upsert' ka matlab hai create new or update existing
        results = await client.collections[COLLECTION_NAME].documents.import_(valid_movies, {'action': 'upsert'})
        
        # Check results for errors
        failed_items = [res for res in results if not res.get('success', False)]
        if failed_items:
            logger.error(f"Typesense batch import failed for {len(failed_items)} items. Example error: {failed_items[0].get('error')}")
            return False
            
        logger.info(f"Typesense batch processed {len(valid_movies)} items.")
        return True
    except Exception as e:
        logger.error(f"Typesense documents.import_ failed: {e}", exc_info=True)
        return False


async def typesense_remove_movie(imdb_id: str) -> bool:
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
         logger.info(f"Typesense: {imdb_id} already deleted or not found.")
         return True # Agar pehle se deleted hai toh bhi success maanein
    except Exception as e:
        logger.error(f"Typesense delete failed for {imdb_id}: {e}", exc_info=True)
        return False


async def typesense_clear_index() -> bool:
    """Poore collection ko delete karke recreate karta hai."""
    if not is_typesense_ready():
        logger.warning("Typesense not ready for clear_index.")
        return False
    try:
        logger.warning(f"Attempting to clear Typesense index by DELETING collection '{COLLECTION_NAME}'...")
        await client.collections[COLLECTION_NAME].delete()
        logger.info(f"Collection '{COLLECTION_NAME}' deleted. Recreating...")
        await client.collections.create(movie_schema)
        logger.info(f"Collection '{COLLECTION_NAME}' recreated successfully.")
        return True
    except ObjectNotFound:
         logger.warning(f"Collection '{COLLECTION_NAME}' not found during clear. Recreating...")
         try:
            await client.collections.create(movie_schema)
            logger.info(f"Collection '{COLLECTION_NAME}' recreated successfully.")
            return True
         except Exception as e_create:
            logger.error(f"Failed to recreate collection after ObjectNotFound: {e_create}", exc_info=True)
            return False
    except Exception as e:
        logger.error(f"Typesense clear_index (delete/recreate) failed: {e}", exc_info=True)
        return False


async def typesense_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    if not is_typesense_ready():
        logger.error("Typesense not ready for sync.")
        return False, 0
        
    valid_movies = []
    for m in all_movies_data:
        # objectID ko 'id' se replace karein aur imdb_id ensure karein
        if 'objectID' in m:
            del m['objectID']
        if 'imdb_id' not in m:
            logger.warning(f"Skipping sync item (no imdb_id): {m}")
            continue
        m['id'] = m['imdb_id']
        valid_movies.append(m)
        
    count = len(valid_movies)
    if not valid_movies:
        logger.info("Sync: No valid data from DB, clearing index.")
        return await typesense_clear_index(), 0
        
    try:
        logger.info(f"Sync: Replacing Typesense index with {count:,} objects...")
        # Index clear karein (delete + recreate)
        if not await typesense_clear_index():
            raise Exception("Failed to clear index before sync")
            
        # Naya data batch mein import karein
        # Yahaan action 'create' use kar sakte hain kyunki index naya hai
        results = await client.collections[COLLECTION_NAME].documents.import_(valid_movies, {'action': 'create', 'batch_size': 500})
        
        failed_items = [res for res in results if not res.get('success', False)]
        if failed_items:
            logger.error(f"Typesense sync import failed for {len(failed_items)} items. Example error: {failed_items[0].get('error')}")
            return False, (count - len(failed_items))

        logger.info(f"Sync completed. {count} items imported.")
        return True, count
    except Exception as e:
        logger.error(f"Typesense sync (import) failed: {e}", exc_info=True)
        return False, 0
