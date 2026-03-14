"""
Async Database Engine & Session Management.
Uses asyncpg driver + SQLAlchemy 2.0 async ORM.
Connection pool is tuned for production throughput.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)

# ── Engine ─────────────────────────────────────────────────────────────────────
def _build_engine() -> AsyncEngine:
    ssl_args: dict = {}
    if settings.db_ssl:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        ssl_args["ssl"] = ctx

    engine = create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_min,
        max_overflow=settings.db_pool_max - settings.db_pool_min,
        pool_timeout=30,
        pool_recycle=1800,          # recycle connections every 30 min
        pool_pre_ping=True,         # test connection before checkout
        echo=settings.debug,
        json_serializer=__import__("orjson").dumps,
        json_deserializer=__import__("orjson").loads,
        connect_args={
            "server_settings": {
                "application_name": "gse-scraper-isedan",
                "search_path": settings.db_schema,
            },
            **ssl_args,
        },
    )
    return engine


engine: AsyncEngine = _build_engine()

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autobegin=True,
    autoflush=False,
)


# ── Dependency injection helper ────────────────────────────────────────────────
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a session, auto-commits on success."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_db_ctx() -> AsyncGenerator[AsyncSession, None]:
    """Context manager version for non-FastAPI code (scheduler, CLI)."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ── Health check ────────────────────────────────────────────────────────────────
async def db_health_check() -> dict[str, int | str | None]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("SELECT NOW() AS ts, version() AS ver, pg_database_size(current_database()) AS db_size")
        )
        row = result.mappings().one()
        return {
            "healthy": True,
            "server_time": str(row["ts"]),
            "version": str(row["ver"]).split(",")[0],
            "db_size_bytes": int(row["db_size"]),
        }


# ── Lifecycle ───────────────────────────────────────────────────────────────────
async def startup_db() -> None:
    try:
        health = await db_health_check()
        log.info("database_connected", **health)
    except Exception as exc:
        log.error("database_connection_failed", error=str(exc))
        raise


async def shutdown_db() -> None:
    await engine.dispose()
    log.info("database_pool_closed")