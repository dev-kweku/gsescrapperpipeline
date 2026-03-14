"""
Repository Layer — all database operations in one place.
Uses raw asyncpg for high-performance bulk upserts (copy/unnest),
and SQLAlchemy ORM for standard CRUD.
"""

from __future__ import annotations

import traceback
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models.models import (
    DailyPrice,
    GhanaHoliday,
    ScrapeError,
    ScrapeJob,
    ScrapeStatus,
    StockListing,
)

log = get_logger(__name__)


# ── Stock Listings ─────────────────────────────────────────────────────────────
class StockRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_many(self, share_codes: list[str]) -> int:
        """Insert new share codes; update last_seen_at if existing."""
        if not share_codes:
            return 0
        stmt = pg_insert(StockListing).values(
            [{"share_code": c} for c in share_codes]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["share_code"],
            set_={"last_seen_at": func.now(), "is_active": True, "updated_at": func.now()},
        )
        cursor: CursorResult = await self.session.execute(stmt)  # type: ignore[assignment]
        return cursor.rowcount

    async def get_all_active(self) -> list[StockListing]:
        stmt = select(StockListing).where(StockListing.is_active == True)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())


# ── Daily Prices ───────────────────────────────────────────────────────────────
class PriceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def bulk_upsert(self, records: list[dict[str, Any]]) -> dict[str, int]:
        """
        High-performance bulk upsert using PostgreSQL UNNEST.
        Returns counts of inserted vs updated rows.
        """
        if not records:
            return {"inserted": 0, "updated": 0}

        # Build the unnest SQL — fastest path for large datasets
        sql = text("""
            WITH input AS (
                SELECT
                    UNNEST(:wdt_ids       ::INT[])      AS wdt_id,
                    UNNEST(:trade_dates   ::DATE[])     AS trade_date,
                    UNNEST(:share_codes   ::TEXT[])     AS share_code,
                    UNNEST(:year_highs    ::NUMERIC[])  AS year_high,
                    UNNEST(:year_lows     ::NUMERIC[])  AS year_low,
                    UNNEST(:prev_closes   ::NUMERIC[])  AS previous_closing_price_vwap,
                    UNNEST(:opens         ::NUMERIC[])  AS opening_price,
                    UNNEST(:last_txs      ::NUMERIC[])  AS last_transaction_price,
                    UNNEST(:close_vwaps   ::NUMERIC[])  AS closing_price_vwap,
                    UNNEST(:price_changes ::NUMERIC[])  AS price_change,
                    UNNEST(:close_bids    ::NUMERIC[])  AS closing_bid_price,
                    UNNEST(:close_offers  ::NUMERIC[])  AS closing_offer_price,
                    UNNEST(:shares_traded ::BIGINT[])   AS total_shares_traded,
                    UNNEST(:value_traded  ::NUMERIC[])  AS total_value_traded,
                    UNNEST(:raw_datas     ::JSONB[])    AS raw_data
            ),
            upserted AS (
                INSERT INTO gse.daily_prices (
                    wdt_id, trade_date, share_code,
                    year_high, year_low, previous_closing_price_vwap,
                    opening_price, last_transaction_price, closing_price_vwap,
                    price_change, closing_bid_price, closing_offer_price,
                    total_shares_traded, total_value_traded, raw_data
                )
                SELECT * FROM input
                ON CONFLICT (trade_date, share_code) DO UPDATE SET
                    wdt_id                      = EXCLUDED.wdt_id,
                    year_high                   = EXCLUDED.year_high,
                    year_low                    = EXCLUDED.year_low,
                    previous_closing_price_vwap = EXCLUDED.previous_closing_price_vwap,
                    opening_price               = EXCLUDED.opening_price,
                    last_transaction_price      = EXCLUDED.last_transaction_price,
                    closing_price_vwap          = EXCLUDED.closing_price_vwap,
                    price_change                = EXCLUDED.price_change,
                    closing_bid_price           = EXCLUDED.closing_bid_price,
                    closing_offer_price         = EXCLUDED.closing_offer_price,
                    total_shares_traded         = EXCLUDED.total_shares_traded,
                    total_value_traded          = EXCLUDED.total_value_traded,
                    raw_data                    = EXCLUDED.raw_data,
                    updated_at                  = NOW()
                RETURNING (xmax = 0) AS is_insert
            )
            SELECT
                COUNT(*) FILTER (WHERE is_insert)  AS inserted,
                COUNT(*) FILTER (WHERE NOT is_insert) AS updated
            FROM upserted
        """)

        def _list(field: str) -> list:
            return [r.get(field) for r in records]

        result = await self.session.execute(sql, {
            "wdt_ids":       _list("wdt_id"),
            "trade_dates":   _list("trade_date"),
            "share_codes":   _list("share_code"),
            "year_highs":    _list("year_high"),
            "year_lows":     _list("year_low"),
            "prev_closes":   _list("previous_closing_price_vwap"),
            "opens":         _list("opening_price"),
            "last_txs":      _list("last_transaction_price"),
            "close_vwaps":   _list("closing_price_vwap"),
            "price_changes": _list("price_change"),
            "close_bids":    _list("closing_bid_price"),
            "close_offers":  _list("closing_offer_price"),
            "shares_traded": _list("total_shares_traded"),
            "value_traded":  _list("total_value_traded"),
            "raw_datas":     [__import__("orjson").dumps(r.get("raw_data") or {}).decode() for r in records],
        })
        row = result.mappings().one()
        return {"inserted": int(row["inserted"]), "updated": int(row["updated"])}

    async def get_latest_for_date(self, trade_date: date) -> list[DailyPrice]:
        stmt = select(DailyPrice).where(DailyPrice.trade_date == trade_date)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_stock_history(
        self, share_code: str, from_date: date, to_date: date, limit: int = 500
    ) -> list[DailyPrice]:
        stmt = (
            select(DailyPrice)
            .where(
                DailyPrice.share_code == share_code,
                DailyPrice.trade_date >= from_date,
                DailyPrice.trade_date <= to_date,
            )
            .order_by(DailyPrice.trade_date.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_latest_prices(self, limit: int = 200) -> list[dict]:
        """Return latest price per stock using DISTINCT ON (fastest path)."""
        result = await self.session.execute(text("""
            SELECT DISTINCT ON (dp.share_code)
                dp.*, sl.company_name, sl.sector
            FROM gse.daily_prices dp
            LEFT JOIN gse.stock_listings sl ON sl.share_code = dp.share_code
            ORDER BY dp.share_code, dp.trade_date DESC
            LIMIT :limit
        """), {"limit": limit})
        return [dict(r) for r in result.mappings()]

    async def get_stats(self) -> dict:
        result = await self.session.execute(text("""
            SELECT
                COUNT(*)         AS total_records,
                MIN(trade_date)  AS min_date,
                MAX(trade_date)  AS max_date,
                COUNT(DISTINCT share_code) AS unique_stocks
            FROM gse.daily_prices
        """))
        return dict(result.mappings().one())

    async def get_market_summary(self, trade_date: date) -> list[dict]:
        """ISEDAN: top gainers, losers, most active for a given day."""
        result = await self.session.execute(text("""
            WITH day AS (
                SELECT dp.*, sl.company_name
                FROM gse.daily_prices dp
                LEFT JOIN gse.stock_listings sl ON sl.share_code = dp.share_code
                WHERE dp.trade_date = :trade_date
            )
            SELECT
                share_code, company_name, closing_price_vwap,
                price_change,
                CASE WHEN previous_closing_price_vwap > 0
                     THEN ROUND((price_change / previous_closing_price_vwap) * 100, 2)
                        ELSE NULL END AS pct_change,
                total_shares_traded,
                total_value_traded
            FROM day
            ORDER BY pct_change DESC NULLS LAST
        """), {"trade_date": trade_date})
        return [dict(r) for r in result.mappings()]


# ── Scrape Jobs ────────────────────────────────────────────────────────────────
class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, job_type: str = "daily", target_date: date | None = None) -> ScrapeJob:
        job = ScrapeJob(
            job_type=job_type,
            status=ScrapeStatus.RUNNING,
            target_date=target_date,
            started_at=datetime.now(timezone.utc),
        )
        self.session.add(job)
        await self.session.flush()
        return job

    async def complete(self, job_id: UUID, stats: dict) -> None:
        await self.session.execute(
            update(ScrapeJob)
            .where(ScrapeJob.job_id == job_id)
            .values(
                status=ScrapeStatus.COMPLETED,
                total_pages=stats.get("total_pages", 0),
                total_records=stats.get("total_records", 0),
                records_inserted=stats.get("inserted", 0),
                records_updated=stats.get("updated", 0),
                records_skipped=stats.get("skipped", 0),
                completed_at=datetime.now(timezone.utc),
            )
        )

    async def fail(self, job_id: UUID, exc: Exception) -> None:
        await self.session.execute(
            update(ScrapeJob)
            .where(ScrapeJob.job_id == job_id)
            .values(
                status=ScrapeStatus.FAILED,
                error_message=str(exc)[:1000],
                error_stack=traceback.format_exc()[:2000],
                completed_at=datetime.now(timezone.utc),
            )
        )

    async def skip(self, job_id: UUID, reason: str) -> None:
        await self.session.execute(
            update(ScrapeJob)
            .where(ScrapeJob.job_id == job_id)
            .values(status=ScrapeStatus.SKIPPED, error_message=reason, completed_at=datetime.now(timezone.utc))
        )

    async def get_recent(self, limit: int = 20) -> list[ScrapeJob]:
        stmt = select(ScrapeJob).order_by(ScrapeJob.created_at.desc()).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def add_error(self, job_id: UUID, error_type: str, msg: str, raw: dict | None = None) -> None:
        err = ScrapeError(job_id=job_id, error_type=error_type, error_msg=msg, raw_row=raw)
        self.session.add(err)


# ── Holidays ───────────────────────────────────────────────────────────────────
class HolidayRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def is_holiday(self, d: date) -> bool:
        result = await self.session.execute(
            select(GhanaHoliday.id).where(GhanaHoliday.holiday_date == d).limit(1)
        )
        return result.scalar() is not None

    async def upsert_many(self, holidays: list[dict]) -> int:
        if not holidays:
            return 0
        stmt = pg_insert(GhanaHoliday).values(holidays)
        stmt = stmt.on_conflict_do_update(
            index_elements=["holiday_date"],
            set_={"name": stmt.excluded.name, "local_name": stmt.excluded.local_name},
        )
        cursor: CursorResult = await self.session.execute(stmt)  # type: ignore[assignment]
        return cursor.rowcount

    async def get_year(self, year: int) -> list[GhanaHoliday]:
        from sqlalchemy import extract
        stmt = select(GhanaHoliday).where(
            extract("year", GhanaHoliday.holiday_date) == year
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())