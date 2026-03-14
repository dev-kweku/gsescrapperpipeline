"""
Scraper Management Endpoints — trigger scrapes, view jobs, check scheduler.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.schemas import (
    ScrapeJobOut,
    TriggerScrapeRequest,
    TriggerScrapeResponse,
)
from app.db.database import get_db
from app.db.repositories.repositories import JobRepository
from app.scraper.pipeline import run_historical_pipeline, run_pipeline
from app.scheduler.scheduler import get_next_run, get_scheduler

router = APIRouter(prefix="/scraper", tags=["Scraper Management"])


# ── Trigger scrape ─────────────────────────────────────────────────────────────
@router.post(
    "/trigger",
    response_model=TriggerScrapeResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a scrape run (runs synchronously, returns result)",
)
async def trigger_scrape(
    request: TriggerScrapeRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    try:
        result = await run_pipeline(
            mode=request.mode,
            target_date=request.target_date.isoformat() if request.target_date else None,
            force=request.force,
            dry_run=request.dry_run,
        )
        data = result.to_dict()
        return TriggerScrapeResponse(
            status="skipped" if result.skipped else ("dry_run" if result.dry_run else "completed"),
            message=(
                f"Skipped: {result.skip_reason}" if result.skipped
                else f"Scraped {result.total_records} records for {result.date_scraped}"
            ),
            **{k: v for k, v in data.items() if k not in ("skipped", "skip_reason", "dry_run")},
            skipped=result.skipped,
            skip_reason=result.skip_reason,
            dry_run=result.dry_run,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )


# ── Trigger historical scrape (async background) ──────────────────────────────
@router.post(
    "/trigger/historical",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger full historical scrape in background",
)
async def trigger_historical(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_historical_pipeline)
    return {"status": "accepted", "message": "Historical scrape started in background"}


# ── Recent scrape jobs ─────────────────────────────────────────────────────────
@router.get(
    "/jobs",
    response_model=list[ScrapeJobOut],
    summary="Recent scrape job history",
)
async def get_jobs(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = 20,
):
    repo = JobRepository(db)
    jobs = await repo.get_recent(limit=limit)
    return [ScrapeJobOut.model_validate(j) for j in jobs]


# ── Scheduler status ───────────────────────────────────────────────────────────
@router.get(
    "/scheduler",
    summary="Scheduler status and next run time",
)
async def get_scheduler_status():
    s = get_scheduler()
    job = s.get_job("gse_daily_scrape")
    return {
        "running":       s.running,
        "next_run":      get_next_run(),
        "job_id":        job.id if job else None,
        "job_name":      job.name if job else None,
        "trigger":       str(job.trigger) if job else None,
    }


# ── Manual scheduler trigger (dev/ops use) ────────────────────────────────────
@router.post(
    "/scheduler/run-now",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Force the scheduler job to run immediately",
)
async def run_scheduler_now(background_tasks: BackgroundTasks):
    from app.scheduler.scheduler import _daily_job
    background_tasks.add_task(_daily_job)
    return {"status": "accepted", "message": "Scheduled job triggered immediately"}
