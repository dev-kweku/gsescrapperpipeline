"""
GSE Data Scraper — Async, nonce-resilient.

Key fixes vs previous version:
- Nonce is refreshed automatically when a page returns empty data (nonce expiry)
- Sequential fallback mode for historical scrapes when nonce expires mid-run
- Each page fetch validates the response before accepting it
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)

_HTML_TAG_RE    = re.compile(r"<[^>]+>")
_COMMA_RE       = re.compile(r",")
_NONCE_PATTERNS = [
    re.compile(r'var\s+wdtNonce\s*=\s*["\']([a-f0-9]+)["\']', re.I),
    re.compile(r'"wdtNonce"\s*:\s*["\']([a-f0-9]+)["\']',     re.I),
    re.compile(r'wdtNonce["\'?]\s*:\s*["\']([a-f0-9]+)["\']', re.I),
]
_DATE_MMDDYYYY  = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_DATE_YYYYMMDD  = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_FALLBACK_NONCE = "77ef878a17"  # captured from browser 2026-03-16

COLUMNS = [
    (0,  "wdt_ID"),
    (1,  "dailydate"),
    (2,  "sharecode"),
    (3,  "yearhighgh"),
    (4,  "yearlowgh"),
    (5,  "previousclosingpricevwapgh"),
    (6,  "openingpricegh"),
    (7,  "lasttransactionpricegh"),
    (8,  "closingpricevwapgh"),
    (9,  "pricechangegh"),
    (10, "closingbidpricegh"),
    (11, "closingofferpricegh"),
    (12, "totalsharestraded"),
    (13, "totalvaluetradedgh"),
]


@dataclass
class PriceRecord:
    wdt_id: int | None
    trade_date: str
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
            "wdt_id":                      self.wdt_id,
            "trade_date":                  self.trade_date,
            "share_code":                  self.share_code,
            "year_high":                   self.year_high,
            "year_low":                    self.year_low,
            "previous_closing_price_vwap": self.previous_closing_price_vwap,
            "opening_price":               self.opening_price,
            "last_transaction_price":      self.last_transaction_price,
            "closing_price_vwap":          self.closing_price_vwap,
            "price_change":                self.price_change,
            "closing_bid_price":           self.closing_bid_price,
            "closing_offer_price":         self.closing_offer_price,
            "total_shares_traded":         self.total_shares_traded,
            "total_value_traded":          self.total_value_traded,
            "raw_data":                    self.raw_data,
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


# ── Parsers ────────────────────────────────────────────────────────────────────
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
    try:
        return datetime.strptime(cleaned, "%b %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


def _parse_row(row: list) -> tuple[PriceRecord | None, str | None]:
    try:
        trade_date = _parse_date(row[1] if len(row) > 1 else None)
        share_code = _strip(row[2] if len(row) > 2 else None).upper()

        if not trade_date:
            return None, f"invalid date: {row[1] if len(row) > 1 else 'missing'}"
        if not share_code:
            return None, "missing share_code"

        return PriceRecord(
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
        ), None
    except Exception as exc:
        return None, str(exc)


# ── HTTP ───────────────────────────────────────────────────────────────────────
def _build_client() -> httpx.AsyncClient:
    # HTTP/2 disabled — GSE server drops HTTP/2 connections (RemoteProtocolError).
    # HTTP/1.1 is stable and sufficient for this endpoint.
    return httpx.AsyncClient(
        http2=False,
        timeout=httpx.Timeout(
            connect=15.0,
            read=60.0,
            write=15.0,
            pool=10.0,
        ),
        limits=httpx.Limits(
            max_keepalive_connections=5,
            max_connections=10,
            keepalive_expiry=30.0,
        ),
        headers={
            "User-Agent":       settings.scraper_user_agent,
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "Accept-Language":  "en-US,en;q=0.9",
            "Accept-Encoding":  "gzip, deflate, br",
            "Connection":       "keep-alive",
            "Origin":           "https://gse.com.gh",
            "Referer":          settings.gse_page_url,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        },
        follow_redirects=True,
        verify=True,
    )


async def _fetch_nonce(client: httpx.AsyncClient) -> str:
    """Fetch a fresh nonce from the GSE market-data page."""
    for attempt in range(3):
        try:
            resp = await client.get(
                settings.gse_page_url,
                headers={
                    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control":   "no-cache",
                },
                timeout=30.0,
            )
            resp.raise_for_status()
            for pattern in _NONCE_PATTERNS:
                m = pattern.search(resp.text)
                if m:
                    log.debug("nonce_found", nonce=m.group(1))
                    return m.group(1)
            log.warning("nonce_not_found_in_page", attempt=attempt + 1, page_size=len(resp.text))
        except Exception as exc:
            log.warning("nonce_fetch_failed", error=str(exc), attempt=attempt + 1)
            await asyncio.sleep(3.0 * (attempt + 1))
    log.warning("nonce_using_fallback")
    return _FALLBACK_NONCE


def _build_payload(draw: int, start: int, length: int, nonce: str) -> dict[str, str]:
    payload: dict[str, str] = {
        "action":   "get_wdtable",
        "table_id": settings.gse_table_id,
        "draw":     str(draw),
    }
    for i, (data_idx, name) in enumerate(COLUMNS):
        prefix = f"columns[{i}]"
        payload[f"{prefix}[data]"]           = str(data_idx)
        payload[f"{prefix}[name]"]           = name
        payload[f"{prefix}[searchable]"]     = "true"
        payload[f"{prefix}[orderable]"]      = "true"
        payload[f"{prefix}[search][value]"]  = ""
        payload[f"{prefix}[search][regex]"]  = "false"
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


async def _fetch_page(
    client: httpx.AsyncClient,
    nonce: str,
    draw: int,
    start: int,
    length: int,
) -> tuple[list, int]:
    """Fetch one page. Returns (rows, records_total)."""
    payload = _build_payload(draw, start, length, nonce)

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.scraper_max_retries),
        wait=wait_exponential(
            multiplier=settings.scraper_retry_delay_s,
            min=settings.scraper_retry_delay_s,
            max=60.0,
        ),
        # Include RemoteProtocolError — GSE server sometimes drops connections
        retry=retry_if_exception_type((
            httpx.HTTPError,
            httpx.TimeoutException,
            httpx.RemoteProtocolError,
        )),
        reraise=True,
    ):
        with attempt:
            resp = await client.post(settings.gse_ajax_url, data=payload)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict) or "data" not in data:
                raise ValueError(f"Unexpected response: {str(data)[:200]}")

            rows  = data["data"]
            total = int(data.get("recordsTotal", 0))
            return rows, total

    return [], 0


def _is_nonce_expired(rows: list) -> bool:
    """
    Detect a stale/expired nonce response.
    GSE returns an empty data array or a special error row when the nonce is invalid.
    """
    if not rows:
        return True
    # Some WordPress DataTables plugins return a single row with an error message
    if len(rows) == 1 and isinstance(rows[0], list) and len(rows[0]) == 1:
        val = str(rows[0][0]).lower()
        if "nonce" in val or "invalid" in val or "expired" in val:
            return True
    return False


async def _fetch_page_with_nonce_refresh(
    client: httpx.AsyncClient,
    nonce: str,
    draw: int,
    start: int,
    length: int,
    page_num: int,
    max_nonce_retries: int = 3,
) -> tuple[list, int, str]:
    """
    Fetch a page with automatic nonce refresh on expiry.
    Returns (rows, total, current_nonce) — the nonce may have been refreshed.
    """
    current_nonce = nonce
    for attempt in range(max_nonce_retries):
        rows, total = await _fetch_page(client, current_nonce, draw, start, length)

        if not _is_nonce_expired(rows):
            return rows, total, current_nonce

        # Nonce expired — refresh and retry
        log.warning(
            "nonce_expired_refreshing",
            page=page_num,
            attempt=attempt + 1,
            max=max_nonce_retries,
        )
        await asyncio.sleep(1.0)  # brief pause before re-fetching nonce
        current_nonce = await _fetch_nonce(client)

    # Final attempt with fresh nonce
    rows, total = await _fetch_page(client, current_nonce, draw, start, length)
    return rows, total, current_nonce


# ── Main Scraper ───────────────────────────────────────────────────────────────
async def scrape_gse(
    mode: str = "latest",
    target_date: str | None = None,
) -> ScrapeResult:
    """
    Scrape GSE DataTable endpoint.

    For 'latest' and 'date' modes: fetches concurrently (fast, nonce stays valid).
    For 'all' (historical) mode: fetches sequentially page-by-page with automatic
    nonce refresh whenever a stale nonce is detected. This is slower but reliable.

    Args:
        mode:        'latest' | 'all' | 'date'
        target_date: ISO date string (YYYY-MM-DD) — used when mode='date'
    """
    async with _build_client() as client:
        nonce = await _fetch_nonce(client)
        batch = settings.scraper_batch_size

        # First page — always fetched first to get total count
        log.info("scrape_first_page", batch_size=batch, mode=mode)
        first_rows, total_remote, nonce = await _fetch_page_with_nonce_refresh(
            client, nonce, 1, 0, batch, page_num=1
        )
        total_pages = max(1, -(-total_remote // batch))
        log.info("scrape_plan", total_remote=total_remote, total_pages=total_pages, mode=mode)

        all_records: list[PriceRecord] = []
        all_errors:  list[dict]        = []

        _process_rows(first_rows, all_records, all_errors)

        if total_pages > 1:
            if mode == "all":
                # ── Historical mode: sequential with per-page nonce refresh ──
                # Slower but immune to nonce expiry mid-scrape.
                # A shared nonce variable is updated whenever a page detects expiry.
                log.info("historical_mode_sequential", total_pages=total_pages)
                for page_idx in range(1, total_pages):
                    start = page_idx * batch
                    log.debug("scrape_page", page=page_idx + 1, total=total_pages, start=start)
                    rows, _, nonce = await _fetch_page_with_nonce_refresh(
                        client, nonce, page_idx + 1, start, batch, page_num=page_idx + 1
                    )
                    _process_rows(rows, all_records, all_errors)
                    # Small delay between pages to avoid overwhelming the server
                    await asyncio.sleep(0.5)

            else:
                # ── Daily mode: concurrent (fast, nonce stays valid for few pages) ──
                sem = asyncio.Semaphore(settings.scraper_concurrency)

                async def fetch_with_sem(page_idx: int) -> tuple[list, int]:
                    async with sem:
                        start = page_idx * batch
                        log.debug("scrape_page", page=page_idx + 1, start=start)
                        try:
                            rows, _, _ = await _fetch_page_with_nonce_refresh(
                                client, nonce, page_idx + 1, start, batch,
                                page_num=page_idx + 1
                            )
                            return rows, page_idx
                        except Exception as exc:
                            log.error("page_fetch_failed", page=page_idx + 1, error=str(exc))
                            return [], page_idx

                page_results = await asyncio.gather(
                    *[fetch_with_sem(i) for i in range(1, total_pages)],
                    return_exceptions=False,
                )
                for rows, _ in page_results:
                    _process_rows(rows, all_records, all_errors)

        log.info("scrape_raw_complete", parsed=len(all_records), errors=len(all_errors))

        return ScrapeResult(
            records=_filter_records(all_records, mode, target_date),
            errors=all_errors,
            total_remote_records=total_remote,
            total_pages=total_pages,
            parsed_count=len(all_records),
        )


def _process_rows(rows: list, records: list[PriceRecord], errors: list[dict]) -> None:
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