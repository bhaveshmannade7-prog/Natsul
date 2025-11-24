# database.py
# ... existing imports ...
# --- ADD Redis Import ---
try:
    from redis_cache import redis_cache, RedisCacheLayer
except ImportError:
    class RedisCacheLayer:
        def is_ready(self): return False
    redis_cache = RedisCacheLayer()
# --- ADD Redis Import ---
# ... existing code ...

# Helper function (FUZZY SEARCH ke saath SYNCHRONIZED kiya gaya)
def clean_text_for_search(text: str) -> str:
# ... existing code ...
# ... existing code ...

class Database:
    def __init__(self, database_url: str):
# ... existing code ...
        self.movies = None

    async def _connect(self):
# ... existing code ...

    # ... existing methods ...

    # --- User Methods (Redis Wrapper Hooks) ---
    
    async def add_user(self, user_id, username, first_name, last_name):
        if not await self.is_ready(): await self._connect()
        try:
            # --- HOOK 1: Redis ko bhi update karo ---
            if redis_cache.is_ready():
                await redis_cache.update_user_activity(user_id)
            # --- END HOOK 1 ---
            
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "last_active": datetime.now(timezone.utc),
                    "is_active": True
                },
                "$setOnInsert": {
                    "joined_date": datetime.now(timezone.utc)
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"add_user failed for {user_id}: {e}", exc_info=False)
            await self._handle_db_error(e)

    async def deactivate_user(self, user_id: int):
# ... existing code ...

    async def get_concurrent_user_count(self, minutes: int) -> int:
        # --- HOOK 2: Pehle Redis se check karo ---
        if redis_cache.is_ready():
            redis_count = await redis_cache.get_concurrent_user_count()
            if redis_count is not None:
                # Redis ka count high-traffic ke liye zyada accurate aur fast hoga
                return redis_count
        # --- END HOOK 2 (FALLBACK to MongoDB) ---

        if not await self.is_ready(): await self._connect()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            count = await self.users.count_documents({
                "last_active": {"$gte": cutoff},
                "is_active": True
            })
            return count
        except Exception as e:
            logger.error(f"get_concurrent_user_count error: {e}", exc_info=False)
            await self._handle_db_error(e)
            return 9999 
            
# ... existing methods ...

    # --- NAYA FUNCTION: 'rapidfuzz' ke liye data load karega (Redis Hook) ---
    async def get_all_movies_for_fuzzy_cache(self) -> List[Dict]:
        """
        Python in-memory fuzzy search ke liye sabhi unique movie titles load karta hai।
        Pehle Redis check karta hai, phir Mongo se load karke Redis mein save karta hai।
        """
        # --- HOOK 3: Pehle Redis se load karne ki koshish karein ---
        if redis_cache.is_ready():
            cached_data = await redis_cache.load_fuzzy_cache()
            if cached_data:
                return cached_data
        # --- END HOOK 3 (FALLBACK to MongoDB) ---
        
        if not await self.is_ready():
            return []
        
        # --- Original Mongo Logic ---
        try:
            pipeline = [
                # ... existing aggregation pipeline ...
            ]
            
            # ... existing data processing ...
            
            # --- HOOK 4: Agar Mongo se load hua, toh Redis mein save karein ---
            if movies and redis_cache.is_ready():
                # Dictionary banana jiske keys 'clean_title' hon
                cache_dict = {m['clean_title']: m for m in movies if m.get('clean_title')}
                await redis_cache.save_fuzzy_cache(cache_dict)
            # --- END HOOK 4 ---
            
            return movies
        except Exception as e:
            logger.error(f"get_all_movies_for_fuzzy_cache error: {e}", exc_info=True)
            return []

    # ... existing methods ...
