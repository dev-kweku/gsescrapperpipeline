"""
GSE Data Scraper — Async, high-performance.

Key optimisations vs the Node.js version:
- httpx AsyncClient with HTTP/2 + connection pooling
- asyncio.gather() for concurrent page fetching (semaphore-controlled)
- Tenacity for exponential back-off retries
- BeautifulSoup only when nonce parsing needs HTML; otherwise pure regex
- All parsing is pure Python — no regex recompilation per row
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)

# Pre-compiled patterns — compiled once at module load
_HTML_TAG_RE   = re.compile(r"<[^>]+>")
_COMMA_RE      = re.compile(r",")
_NONCE_PATTERNS = [
    re.compile(r'var\s+wdtNonce\s*=\s*["\']([a-f0-9]+)["\']', re.I),
    re.compile(r'"wdtNonce"\s*:\s*["\']([a-f0-9]+)["\']', re.I),
    re.compile(r'wdtNonce["\']?\s*:\s*["\']([a-f0-9]+)["\']', re.I),
]
_DATE_MMDDYYYY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_DATE_YYYYMMDD = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_FALLBACK_NONCE = "63d4512098"

COLUMNS = [
    (0, "wdt_ID"),
    (1, "dailydate"),
    (2, "sharecode"),
    (3, "yearhighgh"),
    (4, "yearlowgh"),
    (5, "previousclosingpricevwapgh"),
    (6, "openingpricegh"),
    (7, "lasttransactionpricegh"),
    (8, "closingpricevwapgh"),
    (9, "pricechangegh"),
    (10, "closingbidpricegh"),
    (11, "closingofferpricegh"),
    (12, "totalsharestraded"),
    (13, "totalvaluetradedgh"),
]


# ── Data Classes ───────────────────────────────────────────────────────────────
@dataclass
class PriceRecord:
    wdt_id: int | None
    trade_date: str            # ISO YYYY-MM-DD
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
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "wdt_id": self.wdt_id,
            "trade_date": self.trade_date,
            "share_code": self.share_code,
            "year_high": self.year_high,
            "year_low": self.year_low,
            "previous_closing_price_vwap": self.previous_closing_price_vwap,
            "opening_price": self.opening_price,
            "last_transaction_price": self.last_transaction_price,
            "closing_price_vwap": self.closing_price_vwap,
            "price_change": self.price_change,
            "closing_bid_price": self.closing_bid_price,
            "closing_offer_price": self.closing_offer_price,
            "total_shares_traded": self.total_shares_traded,
            "total_value_traded": self.total_value_traded,
            "raw_data": self.raw_data,
        }


@dataclass
class ScrapeResult:
    records: list[PriceRecord]
    errors: list[dict]
    total_remote_records: int
    total_pages: int
    parsed_count: int

    @property
    def error_count(self) -> int:
        return len(self.errors)


# ── Parser Helpers ─────────────────────────────────────────────────────────────
def _strip(raw: Any) -> str:
    if raw is None:
        return ""
    return _HTML_TAG_RE.sub("", str(raw)).strip()


def _parse_decimal(raw: Any) -> Decimal | None:
    cleaned = _COMMA_RE.sub("", _strip(raw))
    if not cleaned or cleaned in ("-", "N/A", "—"):
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _parse_int(raw: Any) -> int | None:
    cleaned = _COMMA_RE.sub("", _strip(raw))
    if not cleaned or cleaned in ("-", "N/A"):
        return None
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _parse_date(raw: Any) -> str | None:
    cleaned = _strip(raw)
    if not cleaned or cleaned == "-":
        return None
    m = _DATE_MMDDYYYY.match(cleaned)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    m = _DATE_YYYYMMDD.match(cleaned)
    if m:
        return cleaned
    # Fallback
    try:
        return datetime.strptime(cleaned, "%b %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


def _parse_row(row: list) -> tuple[PriceRecord | None, str | None]:
    """Parse a raw DataTables row. Returns (record, error_reason)."""
    try:
        trade_date = _parse_date(row[1] if len(row) > 1 else None)
        share_code = _strip(row[2] if len(row) > 2 else None).upper()

        if not trade_date:
            return None, f"invalid date: {row[1] if len(row) > 1 else 'missing'}"
        if not share_code:
            return None, "missing share_code"

        record = PriceRecord(
            wdt_id=_parse_int(row[0] if len(row) > 0 else None),
            trade_date=trade_date,
            share_code=share_code,
            year_high=_parse_decimal(row[3] if len(row) > 3 else None),
            year_low=_parse_decimal(row[4] if len(row) > 4 else None),
            previous_closing_price_vwap=_parse_decimal(row[5] if len(row) > 5 else None),
            opening_price=_parse_decimal(row[6] if len(row) > 6 else None),
            last_transaction_price=_parse_decimal(row[7] if len(row) > 7 else None),
            closing_price_vwap=_parse_decimal(row[8] if len(row) > 8 else None),
            price_change=_parse_decimal(row[9] if len(row) > 9 else None),
            closing_bid_price=_parse_decimal(row[10] if len(row) > 10 else None),
            closing_offer_price=_parse_decimal(row[11] if len(row) > 11 else None),
            total_shares_traded=_parse_int(row[12] if len(row) > 12 else None),
            total_value_traded=_parse_decimal(row[13] if len(row) > 13 else None),
            raw_data={"cols": [str(c) for c in row]},
        )
        return record, None
    except Exception as exc:
        return None, str(exc)


# ── HTTP Client Builder ────────────────────────────────────────────────────────
def _build_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(settings.scraper_timeout_s),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        headers={
            "User-Agent":       settings.scraper_user_agent,
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "Accept-Language":  "en-US,en;q=0.9",
            "Origin":           "https://gse.com.gh",
            "Referer":          settings.gse_page_url,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        },
        follow_redirects=True,
    )


# ── Nonce Fetcher ──────────────────────────────────────────────────────────────
async def _fetch_nonce(client: httpx.AsyncClient) -> str:
    try:
        resp = await client.get(settings.gse_page_url, headers={"Accept": "text/html"})
        resp.raise_for_status()
        for pattern in _NONCE_PATTERNS:
            m = pattern.search(resp.text)
            if m:
                log.debug("nonce_found", nonce=m.group(1))
                return m.group(1)
    except Exception as exc:
        log.warning("nonce_fetch_failed", error=str(exc))
    log.warning("nonce_using_fallback")
    return _FALLBACK_NONCE


# ── Payload Builder ────────────────────────────────────────────────────────────
def _build_payload(draw: int, start: int, length: int, nonce: str) -> dict[str, str]:
    payload: dict[str, str] = {
        "action":   "get_wdtable",
        "table_id": settings.gse_table_id,
        "draw":     str(draw),
    }
    for i, (data_idx, name) in enumerate(COLUMNS):
        prefix = f"columns[{i}]"
        payload[f"{prefix}[data]"]            = str(data_idx)
        payload[f"{prefix}[name]"]            = name
        payload[f"{prefix}[searchable]"]      = "true"
        payload[f"{prefix}[orderable]"]       = "true"
        payload[f"{prefix}[search][value]"]   = ""
        payload[f"{prefix}[search][regex]"]   = "false"
    payload.update({
        "order[0][column]": "1",
        "order[0][dir]":    "desc",
        "start":            str(start),
        "length":           str(length),
        "search[value]":    "",
        "search[regex]":    "false",
        "wdtNonce":         nonce,
    })
    return payload


# ── Single Page Fetch ──────────────────────────────────────────────────────────
async def _fetch_page(
    client: httpx.AsyncClient,
    nonce: str,
    draw: int,
    start: int,
    length: int,
) -> tuple[list, int]:
    """Returns (rows, records_total). Retries on transient errors."""
    payload = _build_payload(draw, start, length, nonce)

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.scraper_max_retries),
        wait=wait_exponential(
            multiplier=settings.scraper_retry_delay_s,
            min=settings.scraper_retry_delay_s,
            max=60.0,
        ),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    ):
        with attempt:
            resp = await client.post(settings.gse_ajax_url, data=payload)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict) or "data" not in data:
                raise ValueError(f"Unexpected response structure: {str(data)[:200]}")

            rows = data["data"]
            total = int(data.get("recordsTotal", 0))
            return rows, total

    return [], 0  # unreachable but satisfies type checker


# ── Main Scraper ───────────────────────────────────────────────────────────────
async def scrape_gse(
    mode: str = "latest",
    target_date: str | None = None,
) -> ScrapeResult:
    """
    Scrape GSE DataTable endpoint.

    Args:
        mode:        'latest' | 'all' | 'date'
        target_date: ISO date string (YYYY-MM-DD) — used when mode='date'
    """
    async with _build_client() as client:
        nonce = await _fetch_nonce(client)
        batch = settings.scraper_batch_size

        # First page to get total count
        log.info("scrape_first_page", batch_size=batch)
        first_rows, total_remote = await _fetch_page(client, nonce, 1, 0, batch)
        total_pages = max(1, -(-total_remote // batch))  # ceiling division

        log.info("scrape_plan", total_remote=total_remote, total_pages=total_pages)

        all_records: list[PriceRecord] = []
        all_errors: list[dict] = []

        # Process first page immediately
        _process_rows(first_rows, all_records, all_errors)

        if total_pages > 1:
            # Fetch remaining pages concurrently with a semaphore
            sem = asyncio.Semaphore(settings.scraper_concurrency)

            async def fetch_with_sem(page_idx: int) -> tuple[list, int]:
                async with sem:
                    start = page_idx * batch
                    log.debug("scrape_page", page=page_idx + 1, total=total_pages, start=start)
                    try:
                        rows, _ = await _fetch_page(client, nonce, page_idx + 1, start, batch)
                        return rows, page_idx
                    except Exception as exc:
                        log.error("page_fetch_failed", page=page_idx + 1, error=str(exc))
                        return [], page_idx

            tasks = [fetch_with_sem(i) for i in range(1, total_pages)]
            page_results = await asyncio.gather(*tasks, return_exceptions=False)

            for rows, _ in page_results:
                _process_rows(rows, all_records, all_errors)

        log.info("scrape_raw_complete", parsed=len(all_records), errors=len(all_errors))

        # Filter
        filtered = _filter_records(all_records, mode, target_date)

        return ScrapeResult(
            records=filtered,
            errors=all_errors,
            total_remote_records=total_remote,
            total_pages=total_pages,
            parsed_count=len(all_records),
        )


def _process_rows(
    rows: list,
    records: list[PriceRecord],
    errors: list[dict],
) -> None:
    for row in rows:
        rec, err = _parse_row(row)
        if rec:
            records.append(rec)
        else:
            errors.append({"reason": err, "raw": row})


def _filter_records(
    records: list[PriceRecord],
    mode: str,
    target_date: str | None,
) -> list[PriceRecord]:
    if mode == "all":
        return records
    if mode == "date" and target_date:
        return [r for r in records if r.trade_date == target_date]
    if mode == "latest":
        if not records:
            return []
        latest = max(r.trade_date for r in records)
        log.info("latest_date_resolved", date=latest)
        return [r for r in records if r.trade_date == latest]
    return records
