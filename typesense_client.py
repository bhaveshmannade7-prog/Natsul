# typesense_client.py

import os
import logging
from typing import List, Dict, Tuple
from dotenv import load_dotenv
import asyncio

# typesense library import
import typesense
# --- FIX (v1.1.1): 'RequestError' ko 'typesense.exceptions' se import kiya ---
from typesense.exceptions import ObjectNotFound, RequestError 
# --- END FIX ---

load_dotenv()

logger = logging.getLogger("bot.typesense")

# --- Configuration ---
TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY")
TYPESENSE_HOST = os.getenv("TYPESENSE_HOST")
TYPESENSE_PORT = os.getenv("TYPESENSE_PORT", "443")
TYPESENSE_PROTOCOL = os.getenv("TYPESENSE_PROTOCOL", "https")

COLLECTION_NAME = os.getenv("TYPESENSE_INDEX_NAME", "movies")

client = None
_is_ready = False

# Movie collection ka schema (structure)
movie_schema = {
    'name': COLLECTION_NAME,
    'fields': [
        {'name': 'title', 'type': 'string'},
        {'name': 'clean_title', 'type': 'string'}, # Search ke liye
        {'name': 'year', 'type': 'string', 'optional': True, 'facet': True}, # Facet se filter kar sakte hain
        {'name': 'imdb_id', 'type': 'string', 'optional': True}
    ],
    'default_sorting_field': 'title' # Default sort
}

async def initialize_typesense():
    """Typesense client ko initialize aur schema create karta hai."""
    global client, _is_ready
    if _is_ready:
        logger.info("Typesense already initialized.")
        return True

    if not all([TYPESENSE_API_KEY, TYPESENSE_HOST, TYPESENSE_PORT, TYPESENSE_PROTOCOL]):
        logger.critical("Typesense environment variables missing! Cannot initialize.")
        _is_ready = False
        return False

    logger.info(f"Initializing Typesense client for {TYPESENSE_HOST}:{TYPESENSE_PORT}...")
    try:
        # Client configuration
        client = typesense.Client({
            'nodes': [{
                'host': TYPESENSE_HOST,
                'port': TYPESENSE_PORT,
                'protocol': TYPESENSE_PROTOCOL
            }],
            'api_key': TYPESENSE_API_KEY,
            'connection_timeout_seconds': 5
        })

        # Check if collection (index) already exists
        try:
            await client.collections[COLLECTION_NAME].retrieve()
            logger.info(f"Typesense collection '{COLLECTION_NAME}' already exists.")
        except ObjectNotFound:
            # Collection nahi hai, toh banayein
            logger.info(f"Collection '{COLLECTION_NAME}' not found. Creating...")
            await client.collections.create(movie_schema)
            logger.info(f"Successfully created collection '{COLLECTION_NAME}'.")
        
        except RequestError as e:
            # Agar collection pehle se hai (HTTP 409 Conflict)
            if e.status_code == 409:
                logger.warning(f"Collection '{COLLECTION_NAME}' creation conflict (likely exists). Proceeding.")
            else:
                raise e # Doosra error hai, toh fail karein

        _is_ready = True
        logger.info("Typesense initialization successful.")
        return True

    except Exception as e:
        logger.critical(f"Failed to initialize Typesense client: {e}", exc_info=True)
        client = None
        _is_ready = False
        return False

def is_typesense_ready():
    """Check if Typesense client was successfully initialized."""
    return _is_ready and client is not None

async def typesense_search(query: str, limit: int = 20) -> List[Dict]:
    if not is_typesense_ready():
        logger.error("Typesense not ready for search.")
        return []
    try:
        # Search parameters
        search_params = {
            'q': query,
            'query_by': 'clean_title, title', # Pehle clean_title, fir title
            'per_page': limit,
            'num_typos': 2, # 2 typo tak allow
            'typo_tokens_threshold': 1, # 1-word query pe bhi typo check
            'drop_tokens_threshold': 1 # Ek word drop karke bhi search kare
        }
        
        result = await client.collections[COLLECTION_NAME].documents.search(search_params)
        
        hits = result.get('hits', [])
        
        # Format results
        return [
            {
                'imdb_id': hit['document']['imdb_id'],
                'title': hit['document']['title'],
                'year': hit['document'].get('year')
            }
            for hit in hits if 'document' in hit and 'imdb_id' in hit['document']
        ]
    except Exception as e:
        logger.error(f"Typesense search failed for '{query}': {e}", exc_info=True)
        return []

async def typesense_add_movie(movie_data: dict) -> bool:
    """Ek movie ko add ya update (upsert) karta hai."""
    if not is_typesense_ready():
        logger.warning("Typesense not ready for add_movie.")
        return False
    
    # Typesense ko 'id' field ki zaroorat hoti hai
    # Hum 'imdb_id' ko 'id' ki tarah use karenge
    if 'imdb_id' not in movie_data:
        logger.error("Missing 'imdb_id' for Typesense add.")
        return False
        
    # 'id' field ko 'imdb_id' se set karein
    movie_data['id'] = movie_data['imdb_id']
    
    try:
        # Upsert ka matlab: agar id hai toh update, nahi toh create
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
        if 'imdb_id' in m:
            m['id'] = m['imdb_id'] # 'id' field set karein
            valid_movies.append(m)
        else:
            logger.warning(f"Skipping batch item (no imdb_id): {m.get('title')}")

    if not valid_movies:
        logger.warning("No valid items in batch.")
        return False
        
    try:
        # Batch import (upsert)
        results = await client.collections[COLLECTION_NAME].documents.import_(valid_movies, {'action': 'upsert'})
        
        # Error check (optional)
        failed_imports = [res for res in results if not res.get('success', False)]
        if failed_imports:
            logger.warning(f"Typesense batch had {len(failed_imports)} failures.")
            logger.debug(f"First failed item: {failed_imports[0]}")
            
        logger.info(f"Typesense batch processed {len(valid_movies)} items.")
        return True
        
    except Exception as e:
        logger.error(f"Typesense batch import failed: {e}", exc_info=True)
        return False

async def typesense_remove_movie(imdb_id: str) -> bool:
    if not is_typesense_ready():
        logger.warning("Typesense not ready for remove_movie.")
        return False
    if not imdb_id:
        return False
    try:
        # Delete by ID (imdb_id ko hum 'id' ki tarah use kar rahe hain)
        await client.collections[COLLECTION_NAME].documents[imdb_id].delete()
        logger.info(f"Typesense delete request for {imdb_id} submitted.")
        return True
    except ObjectNotFound:
        logger.info(f"Typesense: {imdb_id} already deleted or not found.")
        return True # Fail nahi hua, bas tha nahi
    except Exception as e:
        logger.error(f"Typesense delete failed for {imdb_id}: {e}", exc_info=True)
        return False

async def typesense_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """Poore DB ko Typesense se sync karta hai (sab delete karke naya data dalta hai)."""
    if not is_typesense_ready():
        logger.error("Typesense not ready for sync.")
        return False, 0

    logger.info("Starting full Typesense sync...")
    
    # 1. Poore collection ko delete karein (zyaada tez hai)
    try:
        await client.collections[COLLECTION_NAME].delete()
        logger.info(f"Sync: Deleted old collection '{COLLECTION_NAME}'.")
    except ObjectNotFound:
        logger.info(f"Sync: Collection '{COLLECTION_NAME}' did not exist, skipping delete.")
    except Exception as e:
        logger.error(f"Sync: Failed to delete old collection: {e}", exc_info=True)
        return False, 0
        
    # 2. Naya collection schema ke saath banayein
    try:
        await client.collections.create(movie_schema)
        logger.info(f"Sync: Re-created collection '{COLLECTION_NAME}'.")
    except Exception as e:
        logger.error(f"Sync: Failed to re-create collection: {e}", exc_info=True)
        return False, 0
        
    # 3. Naya data batch mein import karein
    valid_movies = []
    for m in all_movies_data:
        if 'imdb_id' in m:
            m['id'] = m['imdb_id'] # 'id' field set karein
            valid_movies.append(m)
            
    count = len(valid_movies)
    if not valid_movies:
        logger.info("Sync: No valid data from DB. Empty collection created.")
        return True, 0
        
    logger.info(f"Sync: Importing {count:,} new documents...")
    try:
        # Batch import (create)
        results = await client.collections[COLLECTION_NAME].documents.import_(valid_movies, {'action': 'create'})
        
        failed_imports = [res for res in results if not res.get('success', False)]
        if failed_imports:
            logger.warning(f"Sync: Batch import had {len(failed_imports)} failures.")
            logger.debug(f"First failed item: {failed_imports[0]}")
            
        logger.info(f"Sync: Successfully imported {count - len(failed_imports)} / {count} documents.")
        return True, (count - len(failed_imports))
        
    except Exception as e:
        logger.error(f"Sync: Batch import failed: {e}", exc_info=True)
        return False, 0
