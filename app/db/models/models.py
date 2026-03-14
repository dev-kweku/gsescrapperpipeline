"""
SQLAlchemy ORM Models — async-compatible mapped dataclasses.
All tables live in the 'gse' schema.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, Enum, ForeignKey,
    Integer, Numeric, String, Text, UniqueConstraint, func,
    Index
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ScrapeStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    SKIPPED   = "skipped"


# ── Stock Listings ─────────────────────────────────────────────────────────────
class StockListing(Base):
    __tablename__ = "stock_listings"
    __table_args__ = (
        Index("ix_stock_listings_share_code", "share_code"),
        Index("ix_stock_listings_is_active", "is_active"),
        {"schema": "gse"},
    )

    id:           Mapped[int]      = mapped_column(Integer, primary_key=True, autoincrement=True)
    share_code:   Mapped[str]      = mapped_column(String(20), nullable=False, unique=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    sector:       Mapped[str | None] = mapped_column(String(100))
    is_active:    Mapped[bool]     = mapped_column(Boolean, nullable=False, default=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at:  Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_at:   Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:   Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    prices: Mapped[list["DailyPrice"]] = relationship("DailyPrice", back_populates="stock")


# ── Daily Prices ───────────────────────────────────────────────────────────────
class DailyPrice(Base):
    __tablename__ = "daily_prices"
    __table_args__ = (
        UniqueConstraint("trade_date", "share_code", name="uq_daily_prices_date_code"),
        Index("ix_daily_prices_trade_date", "trade_date"),
        Index("ix_daily_prices_share_code", "share_code"),
        Index("ix_daily_prices_date_code", "trade_date", "share_code"),
        {"schema": "gse"},
    )

    id:                          Mapped[int]            = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    wdt_id:                      Mapped[int | None]     = mapped_column(Integer)
    trade_date:                  Mapped[date]           = mapped_column(Date, nullable=False)
    share_code:                  Mapped[str]            = mapped_column(String(20), ForeignKey("gse.stock_listings.share_code"), nullable=False)
    year_high:                   Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    year_low:                    Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    previous_closing_price_vwap: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    opening_price:               Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    last_transaction_price:      Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    closing_price_vwap:          Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    price_change:                Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    closing_bid_price:           Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    closing_offer_price:         Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    total_shares_traded:         Mapped[int | None]     = mapped_column(BigInteger)
    total_value_traded:          Mapped[Decimal | None] = mapped_column(Numeric(20, 4))
    raw_data:                    Mapped[dict | None]    = mapped_column(JSONB)
    created_at:                  Mapped[datetime]       = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:                  Mapped[datetime]       = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    stock: Mapped["StockListing"] = relationship("StockListing", back_populates="prices")


# ── Scrape Jobs ────────────────────────────────────────────────────────────────
class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"
    __table_args__ = (
        Index("ix_scrape_jobs_job_id", "job_id"),
        Index("ix_scrape_jobs_status", "status"),
        Index("ix_scrape_jobs_target_date", "target_date"),
        Index("ix_scrape_jobs_created_at", "created_at"),
        {"schema": "gse"},
    )

    id:               Mapped[int]            = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id:           Mapped[UUID]           = mapped_column(PG_UUID(as_uuid=True), nullable=False, unique=True, default=uuid4)
    job_type:         Mapped[str]            = mapped_column(String(50), nullable=False, default="daily")
    status:           Mapped[ScrapeStatus]   = mapped_column(Enum(ScrapeStatus, schema="gse"), nullable=False, default=ScrapeStatus.PENDING)
    target_date:      Mapped[date | None]    = mapped_column(Date)
    total_pages:      Mapped[int]            = mapped_column(Integer, default=0)
    total_records:    Mapped[int]            = mapped_column(Integer, default=0)
    records_inserted: Mapped[int]            = mapped_column(Integer, default=0)
    records_updated:  Mapped[int]            = mapped_column(Integer, default=0)
    records_skipped:  Mapped[int]            = mapped_column(Integer, default=0)
    error_message:    Mapped[str | None]     = mapped_column(Text)
    error_stack:      Mapped[str | None]     = mapped_column(Text)
    metadata_:        Mapped[dict]           = mapped_column("metadata", JSONB, default=dict)
    started_at:       Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at:     Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at:       Mapped[datetime]       = mapped_column(DateTime(timezone=True), server_default=func.now())

    errors: Mapped[list["ScrapeError"]] = relationship("ScrapeError", back_populates="job")


# ── Scrape Errors ──────────────────────────────────────────────────────────────
class ScrapeError(Base):
    __tablename__ = "scrape_errors"
    __table_args__ = (
        Index("ix_scrape_errors_job_id", "job_id"),
        {"schema": "gse"},
    )

    id:         Mapped[int]       = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id:     Mapped[UUID]      = mapped_column(PG_UUID(as_uuid=True), ForeignKey("gse.scrape_jobs.job_id", ondelete="CASCADE"))
    error_type: Mapped[str | None] = mapped_column(String(100))
    error_msg:  Mapped[str | None] = mapped_column(Text)
    raw_row:    Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime]  = mapped_column(DateTime(timezone=True), server_default=func.now())

    job: Mapped["ScrapeJob"] = relationship("ScrapeJob", back_populates="errors")


# ── Ghana Public Holidays ──────────────────────────────────────────────────────
class GhanaHoliday(Base):
    __tablename__ = "ghana_public_holidays"
    __table_args__ = (
        Index("ix_ghana_holidays_date", "holiday_date"),
        {"schema": "gse"},
    )

    id:           Mapped[int]      = mapped_column(Integer, primary_key=True, autoincrement=True)
    holiday_date: Mapped[date]     = mapped_column(Date, nullable=False, unique=True)
    name:         Mapped[str]      = mapped_column(String(255), nullable=False)
    local_name:   Mapped[str | None] = mapped_column(String(255))
    source:       Mapped[str]      = mapped_column(String(100), default="holidays-lib")
    created_at:   Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
