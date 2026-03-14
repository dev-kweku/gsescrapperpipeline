"""
Redis Cache Dependency
Decorator + helpers for caching FastAPI endpoint responses.
Falls back gracefully to no-cache if Redis is unavailable.
"""

from __future__ import annotations

import functools
import hashlib
from typing import Any, Callable

import orjson
from redis.asyncio import Redis

from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)

# Typed as Redis | None so Pylance knows the exact type on every access
_redis_client: Redis | None = None


async def _get_redis() -> Redis | None:
    global _redis_client
    if not settings.cache_enabled:
        return None
    if _redis_client is None:
        try:
            import redis.asyncio as aioredis
            client: Redis = aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=False,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            # redis-py 5.x: ping() returns bool directly, it is NOT a coroutine.
            # Calling await on it raises "bool is not awaitable".
            # Use execute_command which always returns an awaitable.
            await client.execute_command("PING")
            _redis_client = client
        except Exception as exc:
            log.warning("redis_unavailable", error=str(exc))
            _redis_client = None
    return _redis_client


def _make_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    raw = f"{func_name}:{str(args)}:{str(sorted(kwargs.items()))}"
    return "gse:" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def cache_response(ttl: int | None = None) -> Callable:
    """
    Decorator for caching FastAPI endpoint responses in Redis.

    Usage:
        @router.get("/endpoint")
        @cache_response(ttl=300)
        async def my_endpoint(...):
            ...
    """
    effective_ttl = ttl or settings.cache_ttl_s

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            redis = await _get_redis()
            if redis is None:
                return await func(*args, **kwargs)

            cacheable_kwargs = {k: str(v) for k, v in kwargs.items() if k != "db"}
            cache_key = _make_cache_key(func.__name__, (), cacheable_kwargs)

            # Cache read
            try:
                cached = await redis.get(cache_key)
                if cached:
                    log.debug("cache_hit", key=cache_key, func=func.__name__)
                    return orjson.loads(cached)
            except Exception as exc:
                log.warning("cache_get_failed", error=str(exc))

            # Execute endpoint
            result = await func(*args, **kwargs)

            # Cache write
            try:
                serialisable = (
                    [r.model_dump() if hasattr(r, "model_dump") else r for r in result]
                    if isinstance(result, list)
                    else (result.model_dump() if hasattr(result, "model_dump") else result)
                )
                await redis.setex(
                    cache_key,
                    effective_ttl,
                    orjson.dumps(serialisable, default=str),
                )
                log.debug("cache_set", key=cache_key, ttl=effective_ttl)
            except Exception as exc:
                log.warning("cache_set_failed", error=str(exc))

            return result

        return wrapper
    return decorator


async def invalidate_market_cache() -> None:
    """Invalidate all market data cache keys after a successful scrape."""
    redis = await _get_redis()
    if redis is None:
        return
    try:
        keys = await redis.keys("gse:*")
        if keys:
            await redis.delete(*keys)
            log.info("cache_invalidated", keys_deleted=len(keys))
    except Exception as exc:
        log.warning("cache_invalidation_failed", error=str(exc))


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None