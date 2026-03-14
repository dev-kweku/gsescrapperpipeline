"""
Daily Scrape Scheduler — APScheduler 3.x
Runs Mon-Fri at 18:00 Africa/Accra (UTC+0).
Skips Ghana public holidays automatically.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.logging import get_logger
from app.scraper.pipeline import run_pipeline, seed_ghana_holidays

log = get_logger(__name__)

_scheduler: AsyncIOScheduler | None = None
_job_running = False   # guard against overlapping runs


async def _daily_job() -> None:
    global _job_running
    if _job_running:
        log.warning("scheduler_overlap_skipped")
        return

    _job_running = True
    log.info("=" * 60)
    log.info("scheduler_triggered", time=datetime.now(pytz.utc).isoformat())
    try:
        # Refresh holidays at start of each run
        try:
            await seed_ghana_holidays()
        except Exception as exc:
            log.warning("holiday_seed_failed", error=str(exc))

        result = await run_pipeline(mode="daily")

        if result.skipped:
            log.info("scheduler_job_skipped", reason=result.skip_reason, date=result.date_scraped)
        else:
            log.info(
                "scheduler_job_complete",
                date=result.date_scraped,
                inserted=result.inserted,
                updated=result.updated,
                duration_s=result.duration_s,
            )
    except Exception as exc:
        log.error("scheduler_job_failed", error=str(exc), exc_info=True)
    finally:
        _job_running = False


def build_scheduler() -> AsyncIOScheduler:
    tz = pytz.timezone(settings.scheduler_timezone)
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(
        _daily_job,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=settings.scheduler_hour,
            minute=settings.scheduler_minute,
            timezone=tz,
        ),
        id="gse_daily_scrape",
        name="GSE Daily Scrape",
        replace_existing=True,
        max_instances=1,
        coalesce=True,          # if missed, run once (not multiple times)
        misfire_grace_time=3600, # allow up to 1hr late start
    )
    return scheduler


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = build_scheduler()
    return _scheduler


def start_scheduler() -> AsyncIOScheduler:
    s = get_scheduler()
    if not s.running:
        s.start()
        # get_job() returns Job | None — guard before accessing next_run_time
        job      = s.get_job("gse_daily_scrape")
        next_run = job.next_run_time.isoformat() if job and job.next_run_time else "unknown"
        log.info(
            "scheduler_started",
            cron=f"Mon-Fri {settings.scheduler_hour:02d}:{settings.scheduler_minute:02d}",
            timezone=settings.scheduler_timezone,
            next_run=next_run,
        )
    return s


def stop_scheduler() -> None:
    s = get_scheduler()
    if s.running:
        s.shutdown(wait=False)
        log.info("scheduler_stopped")


def get_next_run() -> str | None:
    s = get_scheduler()
    job = s.get_job("gse_daily_scrape")
    if job and job.next_run_time:
        return job.next_run_time.isoformat()
    return None