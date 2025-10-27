import os
import logging
from typing import List, Dict, Tuple
from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("bot.algolia")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_ADMIN_KEY = os.getenv("ALGOLIA_ADMIN_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME")

if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
    logger.critical("Algolia APP_ID, ADMIN_KEY, ya INDEX_NAME environment variables missing!")
    client = None
    index = None
else:
    try:
        # Client aur Index ko initialize karein
        client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        index = client.init_index(ALGOLIA_INDEX_NAME)
        
        # Index settings (Typo tolerance enable karein)
        # Yeh settings Algolia dashboard se bhi ki ja sakti hain
        index.set_settings({
            'minWordSizefor1Typo': 4,      # 4 akshar ke baad 1 typo maaf
            'minWordSizefor2Typos': 8,     # 8 akshar ke baad 2 typo maaf
            'hitsPerPage': 20,             # Default 20 result
            'attributesForFaceting': ['year'], # 'year' ke hisaab se filter kar sakte hain
            'searchableAttributes': ['title', 'imdb_id'] # In do cheezon par search karein
        })
        logger.info(f"Algolia client initialized. Index: '{ALGOLIA_INDEX_NAME}'")
    except Exception as e:
        logger.critical(f"Failed to initialize Algolia client: {e}")
        client = None
        index = None

def is_algolia_ready():
    """Check karein ki Algolia client aur index dono ready hain ya nahi"""
    return client is not None and index is not None

async def algolia_search(query: str, limit: int = 20) -> List[Dict]:
    """Algolia mein search karein."""
    if not is_algolia_ready():
        logger.error("Algolia not ready, cannot perform search.")
        return []
        
    try:
        # Algolia ka async search function use karein
        results = await index.search_async(query, {'hitsPerPage': limit})
        
        formatted_hits = []
        for hit in results.get('hits', []):
            formatted_hits.append({
                'imdb_id': hit['objectID'], # objectID hi hamara imdb_id hai
                'title': hit.get('title', 'N/A') # Title extract karein
            })
        return formatted_hits
    except Exception as e:
        logger.error(f"Failed to search Algolia: {e}", exc_info=True)
        return []

async def algolia_add_movie(movie_data: dict):
    """Ek movie ko Algolia mein add/update karein."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping add_movie")
        return False
    
    try:
        # movie_data mein 'objectID' hona zaroori hai
        if 'objectID' not in movie_data:
            movie_data['objectID'] = movie_data['imdb_id']
            
        await index.save_object_async(movie_data)
        logger.info(f"Successfully added/updated object in Algolia: {movie_data['objectID']}")
        return True
    except Exception as e:
        logger.error(f"Failed to add object to Algolia: {e}", exc_info=True)
        return False

async def algolia_add_batch_movies(movies_list: List[dict]):
    """Bahut saari movies ko ek saath Algolia mein add karein (JSON import ke liye)."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping add_batch_movies")
        return False
        
    if not movies_list:
        return True # Kuch add karne ko nahi hai
        
    try:
        # Ensure objectID is set for all
        for movie in movies_list:
            if 'objectID' not in movie:
                movie['objectID'] = movie.get('imdb_id')
        
        await index.save_objects_async(movies_list)
        logger.info(f"Successfully added {len(movies_list)} objects to Algolia in batch.")
        return True
    except Exception as e:
        logger.error(f"Failed to add batch objects to Algolia: {e}", exc_info=True)
        return False

async def algolia_remove_movie(imdb_id: str):
    """Movie ko Algolia index se delete karein."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping remove_movie")
        return False
        
    try:
        # 'imdb_id' hi hamara 'objectID' hai
        await index.delete_object_async(imdb_id)
        logger.info(f"Successfully deleted object from Algolia: {imdb_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete object from Algolia: {e}", exc_info=True)
        return False

async def algolia_clear_index():
    """Poore Algolia index ko khaali karein."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping clear_index")
        return False
    try:
        await index.clear_objects_async()
        logger.info(f"Successfully cleared Algolia index: {ALGOLIA_INDEX_NAME}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear Algolia index: {e}", exc_info=True)
        return False

async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """
    Sabse important function (/sync_algolia ke liye).
    Yeh DB se liye gaye poore data ko Algolia par upload karega.
    """
    if not is_algolia_ready():
        logger.error("Algolia not ready, cannot perform sync.")
        return False, 0
        
    if not all_movies_data:
        # Agar DB khaali hai, toh index clear kar dein
        await algolia_clear_index()
        return True, 0

    try:
        total_uploaded = 0
        batch_size = 1000 # Algolia ki recommendation
        
        logger.info(f"Sync: Starting upload of {len(all_movies_data)} objects in batches of {batch_size}...")
        
        for i in range(0, len(all_movies_data), batch_size):
            batch = all_movies_data[i:i + batch_size]
            
            # Pehla batch index ko clear karega, baaki ke add honge
            # NOTE: Algolia 'replace_all_objects' ki jagah 'save_objects' use karta hai
            # Pehle batch ke liye 'clearExistingIndex: True' bhejte hain
            
            if i == 0:
                # Pehla batch: Purana data delete karega aur naya daalega
                # *** BUG FIX YAHAN HAI ***
                # Parameter {'clearExistingIndex': True} ke bajaye clear_existing_index=True hona chahiye
                await index.save_objects_async(batch, clear_existing_index=True)
            else:
                # Baaki ke batch: Sirf naya data daalenge
                # *** BUG FIX YAHAN HAI ***
                await index.save_objects_async(batch, clear_existing_index=False)

            total_uploaded += len(batch)
            logger.info(f"Sync: Uploaded {total_uploaded}/{len(all_movies_data)} objects...")
        
        logger.info("Sync: Full sync to Algolia complete.")
        return True, total_uploaded
        
    except Exception as e:
        logger.error(f"Failed during Algolia sync (algolia_sync_data): {e}", exc_info=True)
        return False, 0
