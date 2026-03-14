"""
Market Data Endpoints — all under /api/v1/market
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Annotated

import holidays as holidays_lib
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies.cache           import cache_response
from app.api.v1.schemas                   import (
    DBStatsResponse,
    DailyPriceOut,
    MarketSummaryItem,
    MarketSummaryResponse,
    StockListingOut,
)
from app.db.database                      import get_db
from app.db.repositories.repositories    import (
    HolidayRepository,
    PriceRepository,
    StockRepository,
)

router = APIRouter(prefix="/market", tags=["Market Data"])


@router.get("/prices/latest", response_model=list[DailyPriceOut], summary="Latest price per stock")
@cache_response(ttl=300)
async def get_latest_prices(
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(500, ge=1, le=1000),
):
    return [DailyPriceOut.model_validate(r) for r in await PriceRepository(db).get_latest_prices(limit)]


@router.get("/prices/date/{trade_date}", response_model=list[DailyPriceOut], summary="All prices for a date")
@cache_response(ttl=3600)
async def get_prices_by_date(
    trade_date: date,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(500, ge=1, le=1000),
):
    rows = await PriceRepository(db).get_latest_for_date(trade_date)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {trade_date}")
    return [DailyPriceOut.model_validate(r) for r in rows]


@router.get("/stocks/{share_code}/history", response_model=list[DailyPriceOut], summary="Stock price history")
@cache_response(ttl=600)
async def get_stock_history(
    share_code: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    from_date: date = Query(default_factory=lambda: date.today() - timedelta(days=365)),
    to_date:   date = Query(default_factory=date.today),
    limit:     int  = Query(500, ge=1, le=2000),
):
    rows = await PriceRepository(db).get_stock_history(share_code.upper(), from_date, to_date, limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history for {share_code.upper()}")
    return [DailyPriceOut.model_validate(r) for r in rows]


@router.get("/summary/{trade_date}", response_model=MarketSummaryResponse, summary="Gainers, losers, most active")
@cache_response(ttl=900)
async def get_market_summary(
    trade_date: date,
    db: Annotated[AsyncSession, Depends(get_db)],
    top_n: int = Query(10, ge=1, le=50),
):
    all_rows = await PriceRepository(db).get_market_summary(trade_date)
    if not all_rows:
        raise HTTPException(status_code=404, detail=f"No market data for {trade_date}")

    items = [MarketSummaryItem.model_validate(r) for r in all_rows]

    # Split into typed sublists before sorting so Pylance can verify the
    # key lambda return type is always Decimal, never Decimal | None.
    gainers_pool:   list[MarketSummaryItem] = [
        i for i in items if i.pct_change is not None and i.pct_change > 0
    ]
    losers_pool:    list[MarketSummaryItem] = [
        i for i in items if i.pct_change is not None and i.pct_change < 0
    ]
    unchanged_list: list[MarketSummaryItem] = [
        i for i in items if i.pct_change is None or i.pct_change == 0
    ]

    # Key lambdas return Decimal (not Decimal | None) — no overload error.
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


@router.get("/stocks", response_model=list[StockListingOut], summary="Active GSE stock listings")
@cache_response(ttl=3600)
async def get_stocks(db: Annotated[AsyncSession, Depends(get_db)]):
    return [StockListingOut.model_validate(s) for s in await StockRepository(db).get_all_active()]


@router.get("/stats", response_model=DBStatsResponse, summary="DB statistics")
@cache_response(ttl=120)
async def get_stats(db: Annotated[AsyncSession, Depends(get_db)]):
    stats  = await PriceRepository(db).get_stats()
    active = await StockRepository(db).get_all_active()
    return DBStatsResponse(
        total_records=int(stats.get("total_records") or 0),
        unique_stocks=int(stats.get("unique_stocks") or 0),
        min_date=stats.get("min_date"),
        max_date=stats.get("max_date"),
        active_listings=len(active),
    )


@router.get("/trading-days", summary="Trading day calendar")
async def get_trading_days(
    db: Annotated[AsyncSession, Depends(get_db)],
    from_date: date = Query(default_factory=lambda: date.today() - timedelta(days=30)),
    to_date:   date = Query(default_factory=date.today),
):
    repo = HolidayRepository(db)
    result: list[dict] = []
    d = from_date
    while d <= to_date:
        is_weekend = d.weekday() >= 5
        is_holiday = not is_weekend and (
            await repo.is_holiday(d)
            or d in holidays_lib.country_holidays("GH", years=d.year)
        )
        result.append({
            "date":           d.isoformat(),
            "day_of_week":    d.strftime("%A"),
            "is_trading_day": not is_weekend and not is_holiday,
            "reason":         "weekend" if is_weekend else ("holiday" if is_holiday else None),
        })
        d = date.fromordinal(d.toordinal() + 1)
    return result