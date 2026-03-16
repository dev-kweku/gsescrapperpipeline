"""
Scrape Pipeline Orchestrator
Ties together: holiday check → scrape → DB write → alerting.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import holidays as holidays_lib

from app.core.logging import get_logger
from app.db.database import get_db_ctx
from app.db.models.models import ScrapeJob
from app.db.repositories.repositories import (
    HolidayRepository,
    JobRepository,
    PriceRepository,
    StockRepository,
)
from app.scraper.gse_scraper import ScrapeResult, scrape_gse

async def _scrape_with_fallback(mode: str, target_date: str | None) -> ScrapeResult:
    """Try httpx scraper first, fall back to Playwright on 403."""
    import httpx
    try:
        return await scrape_gse(mode=mode, target_date=target_date)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            log.warning("httpx_403_falling_back_to_browser")
            from app.scraper.browser_scraper import scrape_gse_browser
            return await scrape_gse_browser(mode=mode, target_date=target_date)
        raise
from app.utils.alerting import send_failure_alert, send_success_alert

log = get_logger(__name__)

CHUNK_SIZE = 200


# ── Result ─────────────────────────────────────────────────────────────────────
@dataclass
class PipelineResult:
    skipped:       bool
    skip_reason:   str | None
    date_scraped:  str | None
    inserted:      int
    updated:       int
    parse_errors:  int
    total_records: int
    duration_s:    float
    job_id:        str | None
    dry_run:       bool = False

    def to_dict(self) -> dict:
        return {
            "skipped":       self.skipped,
            "skip_reason":   self.skip_reason,
            "date_scraped":  self.date_scraped,
            "inserted":      self.inserted,
            "updated":       self.updated,
            "parse_errors":  self.parse_errors,
            "total_records": self.total_records,
            "duration_s":    round(self.duration_s, 2),
            "job_id":        self.job_id,
            "dry_run":       self.dry_run,
        }


# ── Holiday helpers ────────────────────────────────────────────────────────────
def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def _is_gh_holiday_local(d: date) -> bool:
    return d in holidays_lib.country_holidays("GH", years=d.year)


async def _is_trading_day(
    d: date, repo: HolidayRepository
) -> tuple[bool, str | None]:
    if _is_weekend(d):
        return False, f"weekend ({d.strftime('%A')})"
    if await repo.is_holiday(d):
        return False, "Ghana public holiday (DB)"
    if _is_gh_holiday_local(d):
        return False, "Ghana public holiday (local)"
    return True, None


def _last_trading_day() -> date:
    today = date.today()
    for delta in range(10):
        d = date.fromordinal(today.toordinal() - delta)
        if not _is_weekend(d) and not _is_gh_holiday_local(d):
            return d
    raise RuntimeError("Cannot find a trading day in the last 10 days")


# ── Seed holidays ──────────────────────────────────────────────────────────────
async def seed_ghana_holidays(year: int | None = None) -> int:
    yr = year or date.today().year
    gh = holidays_lib.country_holidays("GH", years=[yr, yr + 1])
    rows = [
        {"holiday_date": d, "name": name, "local_name": name, "source": "holidays-lib"}
        for d, name in gh.items()
    ]
    async with get_db_ctx() as session:
        count = await HolidayRepository(session).upsert_many(rows)
    log.info("holidays_seeded", year=yr, count=count)
    return count


# ── Dry-run path (no DB job record) ───────────────────────────────────────────
async def _run_dry(scrape_date_str: str, t_start: datetime) -> PipelineResult:
    """Scrape and parse only — zero DB writes, no job record created."""
    scrape_mode = "date" if scrape_date_str else "latest"
    result = await scrape_gse(mode=scrape_mode, target_date=scrape_date_str)
    duration = (datetime.now(timezone.utc) - t_start).total_seconds()
    return PipelineResult(
        skipped=False, skip_reason=None, date_scraped=scrape_date_str,
        inserted=0, updated=0, parse_errors=result.error_count,
        total_records=result.parsed_count,
        duration_s=duration, job_id=None, dry_run=True,
    )


# ── Live path (job record always exists) ──────────────────────────────────────
async def _run_live(
    job: ScrapeJob,           # ← concrete ScrapeJob, never None
    scrape_date_str: str,
    mode: str,
    t_start: datetime,
) -> PipelineResult:
    """
    Full scrape → DB write path.
    job is always a real ScrapeJob here — Pylance sees no Optional.
    """
    async with get_db_ctx() as session:
        job_repo   = JobRepository(session)
        stock_repo = StockRepository(session)
        price_repo = PriceRepository(session)

        try:
            scrape_mode = "all" if mode == "historical" else "latest"
            result = await _scrape_with_fallback(mode=scrape_mode, target_date=scrape_date_str)
            log.info("scrape_complete", parsed=result.parsed_count, errors=result.error_count)

            # Log parse errors against the job
            for err in result.errors[:100]:
                await job_repo.add_error(
                    job.job_id,                     # ✅ always safe — job: ScrapeJob
                    "PARSE_ERROR",
                    err.get("reason", "unknown"),
                    {"raw": err.get("raw", [])},
                )

            # Upsert stock listings + prices
            total_inserted = 0
            total_updated  = 0

            if result.records:
                await stock_repo.upsert_many(
                    list({r.share_code for r in result.records})
                )
                for i in range(0, len(result.records), CHUNK_SIZE):
                    chunk  = result.records[i : i + CHUNK_SIZE]
                    counts = await price_repo.bulk_upsert([r.to_dict() for r in chunk])
                    total_inserted += counts["inserted"]
                    total_updated  += counts["updated"]

            stats = {
                "total_pages":   result.total_pages,
                "total_records": result.total_remote_records,
                "inserted":      total_inserted,
                "updated":       total_updated,
                "skipped":       result.error_count,
            }
            await job_repo.complete(job.job_id, stats)  # ✅ always safe

            duration = (datetime.now(timezone.utc) - t_start).total_seconds()
            log.info("pipeline_complete", **stats, duration_s=round(duration, 2))

            await send_success_alert(
                date=scrape_date_str,
                inserted=total_inserted,
                updated=total_updated,
                duration_s=duration,
            )

            return PipelineResult(
                skipped=False, skip_reason=None, date_scraped=scrape_date_str,
                inserted=total_inserted, updated=total_updated,
                parse_errors=result.error_count,
                total_records=len(result.records),
                duration_s=duration,
                job_id=str(job.job_id),             # ✅ always safe
            )

        except Exception as exc:
            log.error("pipeline_failed", error=str(exc), exc_info=True)
            await job_repo.fail(job.job_id, exc)    # ✅ always safe
            await send_failure_alert(exc, date=scrape_date_str, mode=mode)
            raise


# ── Public entry point ────────────────────────────────────────────────────────
async def run_pipeline(
    mode: str = "daily",
    target_date: str | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> PipelineResult:
    t_start = datetime.now(timezone.utc)
    log.info("pipeline_start", mode=mode, target_date=target_date, force=force, dry_run=dry_run)

    scrape_date: date = (
        date.fromisoformat(target_date) if target_date else _last_trading_day()
    )
    scrape_date_str = scrape_date.isoformat()
    log.info("pipeline_date_resolved", date=scrape_date_str)

    # ── Trading day check ────────────────────────────────────────────────────
    if not force and mode == "daily":
        async with get_db_ctx() as session:
            is_trading, reason = await _is_trading_day(
                scrape_date, HolidayRepository(session)
            )
        if not is_trading:
            log.info("pipeline_skipped", reason=reason, date=scrape_date_str)
            return PipelineResult(
                skipped=True, skip_reason=reason, date_scraped=scrape_date_str,
                inserted=0, updated=0, parse_errors=0, total_records=0,
                duration_s=0.0, job_id=None,
            )

    # ── Dry-run: no DB job record, call separate function ────────────────────
    if dry_run:
        return await _run_dry(scrape_date_str, t_start)

    # ── Live run: create job record first, then hand concrete ScrapeJob ──────
    # job is ScrapeJob (not Optional) from here — _run_live never sees None.
    async with get_db_ctx() as session:
        job_repo = JobRepository(session)
        job: ScrapeJob = await job_repo.create(job_type=mode, target_date=scrape_date)
        await session.flush()
        log.info("pipeline_job_created", job_id=str(job.job_id))

    return await _run_live(job, scrape_date_str, mode, t_start)


async def run_historical_pipeline() -> PipelineResult:
    return await run_pipeline(mode="historical", force=True)