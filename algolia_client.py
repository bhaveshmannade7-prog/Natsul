import os
import logging
from typing import List, Dict, Tuple
from algoliasearch.search.client import SearchClient # ✅ Correct import
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("bot.algolia")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_ADMIN_KEY = os.getenv("ALGOLIA_ADMIN_KEY")
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME")

client = None
index = None

if not ALGOLIA_APP_ID or not ALGOLIA_ADMIN_KEY or not ALGOLIA_INDEX_NAME:
    logger.critical("Algolia APP_ID, ADMIN_KEY, ya INDEX_NAME environment variables missing!")
else:
    try:
        # ✅ FIX: SearchClient को सही तरीके से Initialize करना
        client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        index = client.init_index(ALGOLIA_INDEX_NAME)
        
        # Index settings (Fuzzy Search और Performance के लिए)
        index.set_settings({
            # Typo Tolerance (Kantra -> Kantara) के लिए
            'minWordSizefor1Typo': 3, # 3 अक्षरों के शब्द पर 1 typo की अनुमति
            'minWordSizefor2Typos': 7, # 7 अक्षरों के शब्द पर 2 typos की अनुमति
            'hitsPerPage': 20,
            
            # Title को main search attribute बनाएं (ताकि "ktr" भी काम करे)
            'searchableAttributes': [
                'title',
                'imdb_id',
                'year'
            ],
            
            # Words के बीच partial matching को अनुमति दें (जैसे 'batman' के लिए 'bat' search)
            'queryType': 'prefixLast', 
            
            'attributesForFaceting': ['year'],
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
        # Asynchronous search call
        results = await index.search_async(query, {'hitsPerPage': limit})
        formatted_hits = []
        for hit in results.get('hits', []):
            formatted_hits.append({
                'imdb_id': hit['objectID'],
                'title': hit.get('title', 'N/A')
            })
        return formatted_hits
    except Exception as e:
        logger.error(f"Failed to search Algolia: {e}", exc_info=True)
        return []

async def algolia_add_movie(movie_data: dict):
    """Ek movie ko Algolia mein add/update karein (Yeh 'upsert' hai)."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping add_movie")
        return False
    
    try:
        if 'objectID' not in movie_data:
            movie_data['objectID'] = movie_data['imdb_id']
        # Asynchronous save call (yeh add bhi karta hai aur update bhi)
        await index.save_object_async(movie_data)
        logger.info(f"Successfully added/updated object in Algolia: {movie_data['objectID']}")
        return True
    except Exception as e:
        logger.error(f"Failed to add object to Algolia: {e}", exc_info=True)
        return False

async def algolia_add_batch_movies(movies_list: List[dict]):
    """Bahut saari movies ko ek saath Algolia mein add/update karein."""
    if not is_algolia_ready():
        logger.warning("Algolia not ready, skipping add_batch_movies")
        return False
    
    if not movies_list:
        return True
    
    try:
        for movie in movies_list:
            if 'objectID' not in movie:
                movie['objectID'] = movie.get('imdb_id')
        # Asynchronous save batch call (yeh add/update dono karega)
        await index.save_objects_async(movies_list)
        logger.info(f"Successfully added/updated {len(movies_list)} objects to Algolia in batch.")
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
        # Asynchronous delete call
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
        # Asynchronous clear call
        await index.clear_objects_async()
        logger.info(f"Successfully cleared Algolia index: {ALGOLIA_INDEX_NAME}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear Algolia index: {e}", exc_info=True)
        return False

# FIX 4: /sync_algolia ke liye behtar logic
async def algolia_sync_data(all_movies_data: List[Dict]) -> Tuple[bool, int]:
    """
    Sabse important function (/sync_algolia ke liye).
    Yeh DB se liye gaye poore data ko Algolia par overwrite karega.
    """
    if not is_algolia_ready():
        logger.error("Algolia not ready, cannot perform sync.")
        return False, 0
    
    try:
        if not all_movies_data:
            await algolia_clear_index()
            logger.info("Sync: DB is empty, cleared Algolia index.")
            return True, 0
        
        # Algolia v4 ka sabse behtar tareeka: replace_all_objects_async
        # Yeh automatically index ko clear karta hai aur naya data batch mein upload karta hai.
        logger.info(f"Sync: Starting full replacement of Algolia index with {len(all_movies_data)} objects...")
        
        await index.replace_all_objects_async(all_movies_data, {"batchSize": 1000})
        
        total_uploaded = len(all_movies_data)
        logger.info(f"Sync: Full sync to Algolia complete ({total_uploaded} objects).")
        return True, total_uploaded
        
    except Exception as e:
        logger.error(f"Failed during Algolia sync (algolia_sync_data): {e}", exc_info=True)
        return False, 0
