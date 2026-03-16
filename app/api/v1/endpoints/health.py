"""Health endpoints — /health and /ping."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from app.api.v1.schemas      import HealthResponse
from app.core.config         import settings
from app.db.database         import db_health_check
from app.scheduler.scheduler import get_next_run, get_scheduler

router = APIRouter(tags=["Health"])


def _scheduler_running() -> bool:
    """Wrap get_scheduler() so Pylance sees a concrete bool — not Unknown.
    apscheduler ships no type stubs, so AsyncIOScheduler.running is Unknown
    to Pylance, which causes it to mark the entire health.py module as broken
    and report router as an unknown import symbol.
    """
    try:
        return bool(get_scheduler().running)
    except Exception:
        return False


@router.get("/health", response_model=HealthResponse, summary="Full health check")
async def health() -> HealthResponse:
    db_info: dict[str, int | str | None] = {}
    db_ok = True
    try:
        db_info = await db_health_check()
    except Exception as exc:
        db_ok = False
        db_info = {"error": str(exc)}

    raw_version: str | None = db_info.get("version")  # type: ignore[assignment]
    _raw_size = db_info.get("db_size_bytes")
    raw_size:  int | None = int(_raw_size) if _raw_size is not None else None

    return HealthResponse(
        status="ok" if db_ok else "degraded",
        version=settings.app_version,
        environment=settings.environment,
        db_healthy=db_ok,
        db_version=raw_version,
        db_size_bytes=raw_size,
        scheduler_running=_scheduler_running(),   # bool — never Unknown
        next_scheduled_run=get_next_run(),
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/ping", summary="Liveness probe")
async def ping() -> dict[str, object]:
    return {"pong": True, "timestamp": datetime.now(timezone.utc).isoformat()}