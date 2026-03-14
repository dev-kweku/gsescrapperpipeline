"""
Market Data Endpoints — read-only price/stock data for ISEDAN.
All responses are cached in Redis for configurable TTL.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.schemas import (
    DBStatsResponse,
    DailyPriceOut,
    MarketSummaryItem,
    MarketSummaryResponse,
    PaginatedPrices,
    StockListingOut,
)
from app.api.dependencies.cache import cache_response
from app.db.database import get_db
from app.db.models.models import DailyPrice, StockListing
from app.db.repositories.repositories import PriceRepository, StockRepository

router = APIRouter(prefix="/market", tags=["Market Data"])


# ── Latest prices for all stocks ───────────────────────────────────────────────
@router.get(
    "/prices/latest",
    response_model=list[DailyPriceOut],
    summary="Latest price for every active stock",
)
@cache_response(ttl=300)
async def get_latest_prices(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(500, ge=1, le=1000),
):
    repo = PriceRepository(db)
    rows = await repo.get_latest_prices(limit=limit)
    return [DailyPriceOut.model_validate(r) for r in rows]


# ── Prices for a specific trading date ────────────────────────────────────────
@router.get(
    "/prices/date/{trade_date}",
    response_model=list[DailyPriceOut],
    summary="All prices for a given trading date",
)
@cache_response(ttl=3600)
async def get_prices_by_date(
    trade_date: date,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(500, ge=1, le=1000),
):
    repo = PriceRepository(db)
    rows = await repo.get_latest_for_date(trade_date)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for date {trade_date}")
    return [DailyPriceOut.model_validate(r) for r in rows]


# ── Stock history ──────────────────────────────────────────────────────────────
@router.get(
    "/stocks/{share_code}/history",
    response_model=list[DailyPriceOut],
    summary="Price history for a single stock",
)
@cache_response(ttl=600)
async def get_stock_history(
    share_code: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    from_date: date = Query(default_factory=lambda: date.today() - timedelta(days=365)),
    to_date:   date = Query(default_factory=date.today),
    limit:     int  = Query(500, ge=1, le=2000),
):
    share_code = share_code.upper()
    repo = PriceRepository(db)
    rows = await repo.get_stock_history(share_code, from_date, to_date, limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for {share_code}")
    return [DailyPriceOut.model_validate(r) for r in rows]


# ── Market summary (gainers/losers/most active) ────────────────────────────────
@router.get(
    "/summary/{trade_date}",
    response_model=MarketSummaryResponse,
    summary="Market summary: gainers, losers, most active",
)
@cache_response(ttl=900)
async def get_market_summary(
    trade_date: date,
    db: Annotated[AsyncSession, Depends(get_db)],
    top_n: int = Query(10, ge=1, le=50),
):
    repo = PriceRepository(db)
    all_rows = await repo.get_market_summary(trade_date)
    if not all_rows:
        raise HTTPException(status_code=404, detail=f"No market data for {trade_date}")

    from decimal import Decimal

    items = [MarketSummaryItem.model_validate(r) for r in all_rows]

    # Pre-filter into typed sublists so Pylance knows pct_change is Decimal
    # (not Decimal | None) inside each sorted() key lambda.
    gainers_pool:   list[MarketSummaryItem] = [
        i for i in items if i.pct_change is not None and i.pct_change > 0
    ]
    losers_pool:    list[MarketSummaryItem] = [
        i for i in items if i.pct_change is not None and i.pct_change < 0
    ]
    unchanged_list: list[MarketSummaryItem] = [
        i for i in items if i.pct_change is None or i.pct_change == 0
    ]

    # key lambdas return Decimal, not Decimal | None — no overload error.
    gainers:     list[MarketSummaryItem] = sorted(
        gainers_pool, key=lambda x: x.pct_change or Decimal(0), reverse=True
    )[:top_n]
    losers:      list[MarketSummaryItem] = sorted(
        losers_pool,  key=lambda x: x.pct_change or Decimal(0)
    )[:top_n]
    most_active: list[MarketSummaryItem] = sorted(
        items, key=lambda x: x.total_shares_traded or 0, reverse=True
    )[:top_n]

    return MarketSummaryResponse(
        trade_date=trade_date,
        total_stocks=len(items),
        gainers=gainers,
        losers=losers,
        most_active=most_active,
        unchanged=unchanged_list,
    )


# ── All stock listings ─────────────────────────────────────────────────────────
@router.get(
    "/stocks",
    response_model=list[StockListingOut],
    summary="All active stock listings on GSE",
)
@cache_response(ttl=3600)
async def get_stocks(db: Annotated[AsyncSession, Depends(get_db)]):
    repo = StockRepository(db)
    stocks = await repo.get_all_active()
    return [StockListingOut.model_validate(s) for s in stocks]


# ── DB Statistics ──────────────────────────────────────────────────────────────
@router.get(
    "/stats",
    response_model=DBStatsResponse,
    summary="Database statistics",
)
@cache_response(ttl=120)
async def get_stats(db: Annotated[AsyncSession, Depends(get_db)]):
    price_repo = PriceRepository(db)
    stock_repo = StockRepository(db)

    stats  = await price_repo.get_stats()
    active = await stock_repo.get_all_active()

    return DBStatsResponse(
        total_records=int(stats.get("total_records") or 0),
        unique_stocks=int(stats.get("unique_stocks") or 0),
        min_date=stats.get("min_date"),
        max_date=stats.get("max_date"),
        active_listings=len(active),
    )


# ── Trading calendar check ─────────────────────────────────────────────────────
@router.get(
    "/trading-days",
    summary="Check trading day status for a date range",
)
async def get_trading_days(
    from_date: date = Query(default_factory=lambda: date.today() - timedelta(days=30)),
    to_date:   date = Query(default_factory=date.today),
    db: AsyncSession = Depends(get_db),
):
    from app.db.repositories.repositories import HolidayRepository
    import holidays as holidays_lib

    repo = HolidayRepository(db)
    result = []
    d = from_date
    while d <= to_date:
        is_weekend = d.weekday() >= 5
        is_holiday = (not is_weekend) and (await repo.is_holiday(d) or d in holidays_lib.country_holidays("GH",years=d.year))
        result.append({
            "date": d.isoformat(),
            "day_of_week": d.strftime("%A"),
            "is_trading_day": not is_weekend and not is_holiday,
            "reason": "weekend" if is_weekend else ("holiday" if is_holiday else None),
        })
        d = date.fromordinal(d.toordinal() + 1)
    return result