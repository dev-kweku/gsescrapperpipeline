"""
Pydantic v2 Schemas — request/response models for the FastAPI layer.
Optimised with model_config for fast serialisation.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# ── Health ─────────────────────────────────────────────────────────────────────
class HealthResponse(_Base):
    status: str
    version: str
    environment: str
    db_healthy: bool
    db_version: str | None = None
    db_size_bytes: int | None = None
    scheduler_running: bool
    next_scheduled_run: str | None = None
    timestamp: datetime


# ── Stock Listing ──────────────────────────────────────────────────────────────
class StockListingOut(_Base):
    id: int
    share_code: str
    company_name: str | None
    sector: str | None
    is_active: bool
    first_seen_at: datetime
    last_seen_at: datetime


# ── Daily Price ────────────────────────────────────────────────────────────────
class DailyPriceOut(_Base):
    id: int
    wdt_id: int | None
    trade_date: date
    share_code: str
    year_high: Decimal | None
    year_low: Decimal | None
    previous_closing_price_vwap: Decimal | None
    opening_price: Decimal | None
    last_transaction_price: Decimal | None
    closing_price_vwap: Decimal | None
    price_change: Decimal | None
    closing_bid_price: Decimal | None
    closing_offer_price: Decimal | None
    total_shares_traded: int | None
    total_value_traded: Decimal | None
    created_at: datetime
    updated_at: datetime


class MarketSummaryItem(_Base):
    share_code: str
    company_name: str | None = None
    closing_price_vwap: Decimal | None
    price_change: Decimal | None
    pct_change: Decimal | None
    total_shares_traded: int | None
    total_value_traded: Decimal | None


class MarketSummaryResponse(_Base):
    trade_date: date
    total_stocks: int
    gainers: list[MarketSummaryItem]
    losers: list[MarketSummaryItem]
    most_active: list[MarketSummaryItem]
    unchanged: list[MarketSummaryItem]


class PaginatedPrices(_Base):
    items: list[DailyPriceOut]
    total: int
    page: int
    page_size: int
    pages: int


# ── Scrape Jobs ────────────────────────────────────────────────────────────────
class ScrapeJobOut(_Base):
    id: int
    job_id: UUID
    job_type: str
    status: str
    target_date: date | None
    total_pages: int
    total_records: int
    records_inserted: int
    records_updated: int
    records_skipped: int
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime


class TriggerScrapeRequest(_Base):
    target_date: date | None = Field(None, description="Specific date to scrape (YYYY-MM-DD)")
    mode: str = Field("manual", description="Scrape mode: manual | historical")
    force: bool = Field(False, description="Skip holiday check")
    dry_run: bool = Field(False, description="Parse only, no DB writes")


class TriggerScrapeResponse(_Base):
    status: str
    job_id: str | None = None
    date_scraped: str | None = None
    inserted: int = 0
    updated: int = 0
    parse_errors: int = 0
    total_records: int = 0
    duration_s: float = 0.0
    skipped: bool = False
    skip_reason: str | None = None
    dry_run: bool = False
    message: str = ""


# ── DB Stats ───────────────────────────────────────────────────────────────────
class DBStatsResponse(_Base):
    total_records: int
    unique_stocks: int
    min_date: date | None
    max_date: date | None
    active_listings: int


# ── Pagination query params ────────────────────────────────────────────────────
class PaginationParams(_Base):
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=500)
