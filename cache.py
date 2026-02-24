from redis import asyncio as aioredis
from redis import Redis
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache as fastapi_cache_decorator
# from langchain_openai import OpenAIEmbeddings # @TODO: Add support for different types of embeddings based on environment variables
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_redis import RedisSemanticCache
import os
from dotenv import load_dotenv
import json
import hashlib
import pickle
import logging
import inspect

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global Redis URL, can be overridden by environment variable
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Global Redis clients
redis_client_async = None
redis_client_sync = None

async def init_cache():
    global redis_client_async, redis_client_sync
    redis_client_async = await aioredis.from_url(REDIS_URL)
    # Créer aussi un client synchrone pour les fonctions sync (sans decode_responses pour pickle)
    redis_client_sync = Redis.from_url(REDIS_URL, decode_responses=False)
    FastAPICache.init(RedisBackend(redis_client_async), prefix="fastapi-cache")
    logger.info("✅ Cache RedisBackend initialized successfully")

def redis_cache(expire: int = 3600):
    """
    Cache wrapper that can be used for both async and sync functions, using Redis as the backend.
     - expire: Time in seconds for cache expiration (default: 1 hour)
     - The cache key is generated based on the function name and its parameters (using a hash of the parameters for uniqueness).
     - The decorator automatically detects if the function is async or sync and uses the appropriate Redis client.
     - It also includes error handling to log any issues with cache retrieval or storage without breaking the
    """
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                cache_key = f"{func.__name__}:{hashlib.md5(json.dumps(kwargs, sort_keys=True, default=str).encode()).hexdigest()}"
                
                try:
                    if redis_client_async:
                        cached = await redis_client_async.get(cache_key)
                        if cached:
                            logger.debug(f"💾 Found hit in cache")
                            return pickle.loads(cached)
                except Exception as e:
                    logger.warning(f"⚠️ Error while reading cache (async): {e}")
                
                result = await func(*args, **kwargs)
                
                # Stocker en cache
                try:
                    if redis_client_async:
                        await redis_client_async.setex(cache_key, expire, pickle.dumps(result))
                        logger.debug(f"✅ Result stored in cache for {cache_key}")
                except Exception as e:
                    logger.warning(f"⚠️ Error while writing cache (async): {e}")
                
                return result
            return async_wrapper
        else:
            # Pour les fonctions sync - utiliser le client Redis synchrone
            def sync_wrapper(*args, **kwargs):
                # Créer une clé unique à partir des paramètres
                cache_key = f"{func.__name__}:{hashlib.md5(json.dumps(kwargs, sort_keys=True, default=str).encode()).hexdigest()}"
                
                try:
                    # Essayer de récupérer du cache avec le client sync
                    if redis_client_sync:
                        cached = redis_client_sync.get(cache_key)
                        if cached:
                            logger.debug(f"💾 Found hit in cache for {cache_key}")
                            return pickle.loads(cached)
                except Exception as e:
                    logger.warning(f"⚠️ Error while reading cache (sync): {e}")
                
                # Exécuter la fonction
                logger.debug(f"🔄 Exécution de {func.__name__} avec {kwargs}")
                result = func(*args, **kwargs)
                
                # Stocker en cache
                try:
                    if redis_client_sync:
                        redis_client_sync.setex(cache_key, expire, pickle.dumps(result))
                        logger.debug(f"✅ Result stored in cache for {cache_key}")
                except Exception as e:
                    logger.warning(f"⚠️ Error while writing cache (sync): {e}")
                
                return result
            return sync_wrapper
    return decorator

def custom_key_builder(
    func,
    namespace: str = "",
    request = None,
    response = None,
    *args,
    **kwargs,
):
    """
    Generate a cache key based on the function name and its parameters.
    Parameters:
    func: The function being decorated.
    namespace: str - A namespace to avoid key collisions between different parts of the application.
    request: The request object (can be a Pydantic model or any serializable object
    response: The response object (not used for key generation but can be logged if needed)
    *args, **kwargs: Additional parameters that can be used for key generation if needed.
     - The key is generated by hashing the function name and its parameters (converted to JSON for consistency).
     - This ensures that different parameter values will result in different cache keys, while the same function with the same parameters will hit the cache. 
     - The namespace allows for further separation of cache keys, which can be useful in larger applications to avoid collisions.
     - The function also includes error handling to log any issues with key generation without breaking the application. 
     - Note: The request and response objects are expected to be serializable (e.g., Pydantic models) for the JSON conversion to work properly. If they are not, the function will fall back to using their string representation, which may lead to less efficient caching due to potential key collisions. 
    """
    fname = func.__name__
    
    # Pour les paramètres Pydantic, utiliser leur représentation JSON
    params_str = ""
    if request:
        try:
            if hasattr(request, "dict"):
                params_str = json.dumps(request.dict(), sort_keys=True)
            else:
                params_str = json.dumps(request, sort_keys=True, default=str)
        except:
            params_str = str(request)
    
    # Créer un hash de la clé
    key_string = f"{namespace}:{fname}:{params_str}"
    key_hash = hashlib.md5(key_string.encode()).hexdigest()
    
    return f"{namespace}:{fname}:{key_hash}"

# Utiliser directement le décorateur de fastapi-cache2 (pour les endpoints si jamais)
def cache(expire: int):
    """Décorateur de cache qui fonctionne avec les modèles Pydantic."""
    return fastapi_cache_decorator(expire=expire, key_builder=custom_key_builder, namespace="fastapi")

embeddings = HuggingFaceEmbeddings(model_name=os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))
semantic_cache = RedisSemanticCache(
    embeddings=embeddings,
    redis_url=REDIS_URL,
    distance_threshold=0.1,
    ttl=1800  # 30 mins for chat answers
)
