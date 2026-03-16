"""
GSE Data Scraper
================
Scrapes the Ghana Stock Exchange DataTable endpoint.

Cloudflare handling:
  GSE is protected by Cloudflare. The scraper supports two modes:
  1. Browser session mode (recommended): inject cookies + nonce from Chrome
     DevTools via GSE_COOKIES and GSE_NONCE in .env
  2. Auto-fetch mode: attempts to fetch nonce from the page directly
     (works when Cloudflare is not actively challenging the IP)

Historical scrapes run sequentially (page by page) to handle nonce expiry.
Daily scrapes run concurrently (fast, 1-2 pages).
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
    re.compile(r"wdtNonce\s*[=:]\s*['\"]([a-f0-9]+)['\"]",    re.I),
]
_DATE_MMDDYYYY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_DATE_YYYYMMDD = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_FALLBACK_NONCE = "77ef878a17"

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


# ── Data classes ───────────────────────────────────────────────────────────────
@dataclass
class PriceRecord:
    wdt_id:                      int | None
    trade_date:                  str
    share_code:                  str
    year_high:                   Decimal | None
    year_low:                    Decimal | None
    previous_closing_price_vwap: Decimal | None
    opening_price:               Decimal | None
    last_transaction_price:      Decimal | None
    closing_price_vwap:          Decimal | None
    price_change:                Decimal | None
    closing_bid_price:           Decimal | None
    closing_offer_price:         Decimal | None
    total_shares_traded:         int | None
    total_value_traded:          Decimal | None
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
    records:              list[PriceRecord]
    errors:               list[dict]
    total_remote_records: int
    total_pages:          int
    parsed_count:         int

    @property
    def error_count(self) -> int:
        return len(self.errors)


# ── Parsers ────────────────────────────────────────────────────────────────────
def _strip(raw: Any) -> str:
    return _HTML_TAG_RE.sub("", str(raw)).strip() if raw is not None else ""


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
    if _DATE_YYYYMMDD.match(cleaned):
        return cleaned
    try:
        return datetime.strptime(cleaned, "%b %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
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
            wdt_id=_parse_int(row[0]  if len(row) > 0  else None),
            trade_date=trade_date,
            share_code=share_code,
            year_high=_parse_decimal(row[3]  if len(row) > 3  else None),
            year_low=_parse_decimal(row[4]   if len(row) > 4  else None),
            previous_closing_price_vwap=_parse_decimal(row[5] if len(row) > 5 else None),
            opening_price=_parse_decimal(row[6]          if len(row) > 6  else None),
            last_transaction_price=_parse_decimal(row[7] if len(row) > 7  else None),
            closing_price_vwap=_parse_decimal(row[8]     if len(row) > 8  else None),
            price_change=_parse_decimal(row[9]           if len(row) > 9  else None),
            closing_bid_price=_parse_decimal(row[10]     if len(row) > 10 else None),
            closing_offer_price=_parse_decimal(row[11]   if len(row) > 11 else None),
            total_shares_traded=_parse_int(row[12]       if len(row) > 12 else None),
            total_value_traded=_parse_decimal(row[13]    if len(row) > 13 else None),
            raw_data={"cols": [str(c) for c in row]},
        ), None
    except Exception as exc:
        return None, str(exc)


# ── HTTP client ────────────────────────────────────────────────────────────────
def _build_client() -> httpx.AsyncClient:
    """
    Build an httpx client that mimics a real Chrome browser.
    - HTTP/2 disabled (GSE server drops HTTP/2 connections)
    - Cookies carried automatically across all requests in the session
    - Browser-like headers to pass Cloudflare checks
    """
    ua = settings.gse_user_agent or settings.scraper_user_agent
    return httpx.AsyncClient(
        http2=False,
        timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=10.0),
        limits=httpx.Limits(
            max_keepalive_connections=5,
            max_connections=10,
            keepalive_expiry=30.0,
        ),
        headers={
            "User-Agent":                ua,
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language":           "en-US,en;q=0.9",
            "Accept-Encoding":           "gzip, deflate, br",
            "Connection":                "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua":                 '"Google Chrome";v="134", "Chromium";v="134", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile":          "?0",
            "Sec-Ch-Ua-Platform":        '"Windows"',
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "none",
            "Sec-Fetch-User":            "?1",
        },
        follow_redirects=True,
        verify=True,
    )


# ── Session setup ──────────────────────────────────────────────────────────────
def _inject_cookies(client: httpx.AsyncClient, cookie_str: str) -> None:
    """
    Parse a raw Cookie header string and inject each cookie into the client.
    Handles Cloudflare cookies correctly — uses the exact domain gse.com.gh
    without a leading dot so httpx sends them on every request to that domain.
    """
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            name, _, value = part.partition("=")
            name  = name.strip()
            value = value.strip()
            if name:
                # Set cookie for both the apex domain and www subdomain
                client.cookies.set(name, value, domain="gse.com.gh")
                client.cookies.set(name, value, domain="www.gse.com.gh")


async def _apply_browser_session(client: httpx.AsyncClient) -> str | None:
    """
    Inject browser-captured cookies and nonce into the client.
    Called first — if GSE_COOKIES and GSE_NONCE are set in .env this bypasses
    the automated page fetch entirely (required when Cloudflare blocks the IP).
    Returns the nonce string, or None if config values are not set.
    """
    if not settings.gse_nonce or not settings.gse_cookies:
        return None

    # Override User-Agent to match the browser that solved the CF challenge
    if settings.gse_user_agent:
        client.headers["User-Agent"] = settings.gse_user_agent

    # Inject all four cookies
    _inject_cookies(client, settings.gse_cookies)

    # Switch to AJAX request headers
    client.headers.update({
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding":  "gzip, deflate, br",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin":           "https://gse.com.gh",
        "Referer":          settings.gse_page_url,
        "Sec-Fetch-Dest":   "empty",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Site":   "same-origin",
    })

    log.info(
        "using_browser_session",
        nonce=settings.gse_nonce,
        cookies=len(list(client.cookies.jar)),
    )
    return settings.gse_nonce


async def _fetch_nonce(client: httpx.AsyncClient) -> str:
    """
    Auto-fetch nonce by visiting the GSE market-data page.
    Works when Cloudflare is not actively blocking the IP.
    Falls back to the hardcoded nonce if all attempts fail.
    """
    for attempt in range(3):
        try:
            # Warm up with homepage visit to pick up any CDN cookies
            try:
                await client.get("https://gse.com.gh/", timeout=15.0)
                await asyncio.sleep(1.5)
            except Exception:
                pass

            # Visit market-data page — WordPress sets session cookies here
            resp = await client.get(settings.gse_page_url, timeout=30.0)
            resp.raise_for_status()

            log.debug("nonce_page_loaded", size=len(resp.text), cookies=len(list(client.cookies.jar)))

            # Switch headers to AJAX mode
            client.headers.update({
                "Accept":           "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin":           "https://gse.com.gh",
                "Referer":          settings.gse_page_url,
                "Sec-Fetch-Dest":   "empty",
                "Sec-Fetch-Mode":   "cors",
                "Sec-Fetch-Site":   "same-origin",
            })

            # Extract nonce
            for pattern in _NONCE_PATTERNS:
                m = pattern.search(resp.text)
                if m:
                    log.info("nonce_found", nonce=m.group(1))
                    return m.group(1)

            log.warning("nonce_not_found_in_page", attempt=attempt + 1, size=len(resp.text))

        except Exception as exc:
            log.warning("nonce_fetch_failed", error=str(exc), attempt=attempt + 1)
            await asyncio.sleep(3.0 * (attempt + 1))

    log.warning("nonce_using_fallback", nonce=_FALLBACK_NONCE)
    return _FALLBACK_NONCE


# ── Payload builder ────────────────────────────────────────────────────────────
def _build_payload(draw: int, start: int, length: int, nonce: str) -> dict[str, str]:
    payload: dict[str, str] = {
        "action":   "get_wdtable",
        "table_id": settings.gse_table_id,
        "draw":     str(draw),
    }
    for i, (data_idx, name) in enumerate(COLUMNS):
        p = f"columns[{i}]"
        payload[f"{p}[data]"]          = str(data_idx)
        payload[f"{p}[name]"]          = name
        payload[f"{p}[searchable]"]    = "true"
        payload[f"{p}[orderable]"]     = "true"
        payload[f"{p}[search][value]"] = ""
        payload[f"{p}[search][regex]"] = "false"
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


# ── Page fetcher ───────────────────────────────────────────────────────────────
async def _fetch_page(
    client: httpx.AsyncClient,
    nonce:  str,
    draw:   int,
    start:  int,
    length: int,
) -> tuple[list, int]:
    payload = _build_payload(draw, start, length, nonce)

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.scraper_max_retries),
        wait=wait_exponential(
            multiplier=settings.scraper_retry_delay_s,
            min=settings.scraper_retry_delay_s,
            max=60.0,
        ),
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
            return data["data"], int(data.get("recordsTotal", 0))

    return [], 0


def _is_stale(rows: list) -> bool:
    """Return True if the response looks like a stale/expired nonce."""
    if not rows:
        return True
    if len(rows) == 1 and isinstance(rows[0], list) and len(rows[0]) == 1:
        val = str(rows[0][0]).lower()
        if any(w in val for w in ("nonce", "invalid", "expired", "error")):
            return True
    return False


async def _fetch_page_refreshing(
    client:   httpx.AsyncClient,
    nonce:    str,
    draw:     int,
    start:    int,
    length:   int,
    page_num: int,
    retries:  int = 3,
) -> tuple[list, int, str]:
    """Fetch one page, refreshing nonce automatically if the response is stale."""
    current = nonce
    for attempt in range(retries):
        rows, total = await _fetch_page(client, current, draw, start, length)
        if not _is_stale(rows):
            return rows, total, current
        log.warning("nonce_stale_refreshing", page=page_num, attempt=attempt + 1)
        await asyncio.sleep(2.0)
        current = await _apply_browser_session(client) or await _fetch_nonce(client)
    # Final attempt
    rows, total = await _fetch_page(client, current, draw, start, length)
    return rows, total, current


# ── Main entry point ───────────────────────────────────────────────────────────
async def scrape_gse(
    mode:        str       = "latest",
    target_date: str | None = None,
) -> ScrapeResult:
    """
    Scrape the GSE DataTable endpoint.

    mode='latest'     → fetch only pages needed for latest trading day (fast, concurrent)
    mode='all'        → fetch all pages sequentially (historical, nonce-safe)
    mode='date'       → fetch all pages, filter to target_date
    """
    async with _build_client() as client:
        nonce = await _apply_browser_session(client) or await _fetch_nonce(client)
        batch = settings.scraper_batch_size

        log.info("scrape_first_page", batch_size=batch, mode=mode)
        first_rows, total_remote, nonce = await _fetch_page_refreshing(
            client, nonce, 1, 0, batch, page_num=1
        )
        total_pages = max(1, -(-total_remote // batch))
        log.info("scrape_plan", total_remote=total_remote, total_pages=total_pages, mode=mode)

        all_records: list[PriceRecord] = []
        all_errors:  list[dict]        = []
        _process_rows(first_rows, all_records, all_errors)

        if total_pages > 1:
            if mode == "all":
                # Sequential — safe for long historical runs with nonce expiry
                log.info("historical_mode_sequential", total_pages=total_pages)
                for page_idx in range(1, total_pages):
                    start = page_idx * batch
                    log.info("scrape_page", page=page_idx + 1, total=total_pages)
                    rows, _, nonce = await _fetch_page_refreshing(
                        client, nonce, page_idx + 1, start, batch, page_num=page_idx + 1
                    )
                    _process_rows(rows, all_records, all_errors)
                    await asyncio.sleep(0.8)  # polite delay between pages
            else:
                # Concurrent — fast for daily scrapes (few pages)
                sem = asyncio.Semaphore(settings.scraper_concurrency)

                async def _fetch(page_idx: int) -> tuple[list, int]:
                    async with sem:
                        try:
                            rows, _, _ = await _fetch_page_refreshing(
                                client, nonce, page_idx + 1,
                                page_idx * batch, batch, page_num=page_idx + 1,
                            )
                            return rows, page_idx
                        except Exception as exc:
                            log.error("page_fetch_failed", page=page_idx + 1, error=str(exc))
                            return [], page_idx

                for rows, _ in await asyncio.gather(*[_fetch(i) for i in range(1, total_pages)]):
                    _process_rows(rows, all_records, all_errors)

        log.info("scrape_complete", parsed=len(all_records), errors=len(all_errors))

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
    records:     list[PriceRecord],
    mode:        str,
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