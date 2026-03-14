"""
FastAPI Application Factory
Wires together: routes, middleware, lifespan, error handlers.
Built for FrimpTradingData — production-hardened.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import orjson
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.api.v1.endpoints.health  import router as health_router
from app.api.v1.endpoints.market  import router as market_router
from app.api.v1.endpoints.scraper import router as scraper_router
from app.api.dependencies.cache   import close_redis
from app.core.config  import settings
from app.core.logging import configure_logging, get_logger
from app.db.database  import shutdown_db, startup_db
from app.scheduler.scheduler import start_scheduler, stop_scheduler
from app.scraper.pipeline import seed_ghana_holidays

log = get_logger(__name__)


# ── ORJson response ────────────────────────────────────────────────────────────
class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY)


# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    log.info("gse_scraper_starting", version=settings.app_version, env=settings.environment)

    await startup_db()

    try:
        await seed_ghana_holidays()
    except Exception as exc:
        log.warning("holiday_seed_on_startup_failed", error=str(exc))

    start_scheduler()

    log.info("gse_scraper_ready")
    yield  # ── app runs ──

    log.info("gse_scraper_shutting_down")
    stop_scheduler()
    await shutdown_db()
    await close_redis()
    log.info("gse_scraper_stopped")


# ── App Factory ────────────────────────────────────────────────────────────────
def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            "Production GSE stock data scraper pipeline with daily scheduling, "
            "PostgreSQL storage, Redis cache, and REST API for FrimpTradingData."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
    )

    # ── Middleware (order matters — outermost first) ────────────────────────
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    # ── Request timing middleware ──────────────────────────────────────────
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        t0 = time.perf_counter()
        response = await call_next(request)
        ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Process-Time-Ms"] = f"{ms:.2f}"
        if ms > 2000:
            log.warning("slow_request", path=request.url.path, ms=round(ms, 2))
        return response

    # ── Exception handlers ─────────────────────────────────────────────────
    @app.exception_handler(Exception)
    async def unhandled_exception(request: Request, exc: Exception):
        log.error("unhandled_exception", path=request.url.path, error=str(exc), exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    # ── Routers ────────────────────────────────────────────────────────────
    app.include_router(health_router)
    app.include_router(market_router,  prefix=settings.api_prefix)
    app.include_router(scraper_router, prefix=settings.api_prefix)

    return app


app = create_app()
