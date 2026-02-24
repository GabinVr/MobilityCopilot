from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def pytest_configure(config):
    """Hook that runs before test collection to mock Redis."""
    # Create mock objects for Redis components
    mock_semantic_cache = MagicMock()
    # Make lookup return None (no cache hit) by default
    mock_semantic_cache.lookup.return_value = None
    # Make update do nothing
    mock_semantic_cache.update.return_value = None
    
    mock_redis_client = MagicMock()
    mock_redis_async = MagicMock()
    
    # Patch Redis modules BEFORE any imports
    patcher_redis = patch("redis.Redis", return_value=mock_redis_client)
    patcher_redis_async = patch("redis.asyncio.from_url", return_value=mock_redis_async)
    patcher_semantic_cache = patch("langchain_redis.RedisSemanticCache", return_value=mock_semantic_cache)
    patcher_cache_init = patch("fastapi_cache.FastAPICache.init", return_value=None)
    
    patcher_redis.start()
    patcher_redis_async.start()
    patcher_semantic_cache.start()
    patcher_cache_init.start()
    
    # Store patchers in config for cleanup
    config._patchers = [patcher_redis, patcher_redis_async, patcher_semantic_cache, patcher_cache_init]


def pytest_unconfigure(config):
    """Hook to clean up patches after tests."""
    if hasattr(config, '_patchers'):
        for patcher in config._patchers:
            patcher.stop()
