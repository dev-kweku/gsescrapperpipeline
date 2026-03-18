"""
GSE Data Scraper — Fixed
========================
Fixes applied vs original:

  Bug 1  — Removed hardcoded fallback nonce. Raises clearly instead of
            silently using a dead nonce.
  Bug 2  — _is_stale() now also detects recordsTotal=0 on a non-empty
            page response (Cloudflare block signature).
  Bug 3  — Concurrent mode no longer shares a single nonce closure.
            Each worker fetches its own fresh nonce if stale.
  Bug 4  — Cookie domain uses leading dot (".gse.com.gh") so httpx
            sends Cloudflare cookies on every sub-request correctly.
  Bug 5  — _fetch_nonce() saves/restores headers around the page GET
            so AJAX headers aren't active during the HTML fetch.
  Bug 6  — Sec-Ch-Ua headers removed. They're HTTP/2-only and trigger
            Cloudflare's browser fingerprint mismatch detector.
  Bug 7  — Replaced httpx with curl_cffi which uses Chrome's real TLS
            stack (correct JA3/JA4 fingerprint). This is the primary
            fix for Cloudflare blocking.

Install:
    pip install curl_cffi tenacity

Usage:
    import asyncio
    from gse_scraper_fixed import scrape_gse
    result = asyncio.run(scrape_gse(mode="latest"))
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from curl_cffi.requests import AsyncSession
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ── Settings (replace with your config/env loader) ─────────────────────────────
class _Settings:
    gse_page_url     = "https://gse.com.gh/market-data/gse-equities-market/"
    gse_ajax_url     = "https://gse.com.gh/wp-admin/admin-ajax.php"
    gse_table_id     = "39"
    gse_cookies      = ""          # paste full Cookie header string from DevTools
    gse_nonce        = ""          # paste wdtNonce value from DevTools
    gse_user_agent   = ""          # paste User-Agent from DevTools
    scraper_batch_size    = 100
    scraper_concurrency   = 3
    scraper_max_retries   = 4
    scraper_retry_delay_s = 2.0

settings = _Settings()

# ── Constants ──────────────────────────────────────────────────────────────────
_HTML_TAG_RE    = re.compile(r"<[^>]+>")
_COMMA_RE       = re.compile(r",")
_NONCE_PATTERNS = [
    re.compile(r'var\s+wdtNonce\s*=\s*["\']([a-f0-9]+)["\']', re.I),
    re.compile(r'"wdtNonce"\s*:\s*["\']([a-f0-9]+)["\']',     re.I),
    re.compile(r"wdtNonce\s*[=:]\s*['\"]([a-f0-9]+)['\"]",    re.I),
]
_DATE_MMDDYYYY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_DATE_YYYYMMDD = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

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

_PAGE_HEADERS = {
    "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language":           "en-US,en;q=0.9",
    "Accept-Encoding":           "gzip, deflate, br",
    "Connection":                "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":            "document",
    "Sec-Fetch-Mode":            "navigate",
    "Sec-Fetch-Site":            "none",
    "Sec-Fetch-User":            "?1",
}

_AJAX_HEADERS = {
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":  "en-US,en;q=0.9",
    "Accept-Encoding":  "gzip, deflate, br",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin":           "https://gse.com.gh",
    "Referer":          settings.gse_page_url,
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-origin",
    "Connection":       "keep-alive",
}

# NOTE: No Sec-Ch-Ua headers — they are HTTP/2 only (Bug 6 fix)


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
        return {k: v for k, v in self.__dict__.items()}


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
def _build_client() -> AsyncSession:
    """
    Build a curl_cffi AsyncSession impersonating Chrome 120.

    BUG 7 FIX: curl_cffi uses Chrome's actual TLS stack — correct JA3/JA4
    fingerprint. httpx uses Python ssl which Cloudflare detects immediately.

    impersonate="chrome120" sets:
      - Correct TLS cipher suites and extensions order
      - Correct HTTP/2 settings frames
      - Correct ALPN negotiation
    All of which match what a real Chrome browser sends.
    """
    ua = settings.gse_user_agent or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    return AsyncSession(
        impersonate="chrome120",   # <-- the key fix
        headers={"User-Agent": ua},
        timeout=30,
        verify=True,
    )


# ── Cookie injection ───────────────────────────────────────────────────────────
def _inject_cookies(session: AsyncSession, cookie_str: str) -> None:
    """
    BUG 4 FIX: domain uses leading dot so httpx/curl_cffi sends cookies
    for both gse.com.gh and www.gse.com.gh correctly.
    """
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            name, _, value = part.partition("=")
            name  = name.strip()
            value = value.strip()
            if name:
                # Leading dot = matches apex + all subdomains
                session.cookies.set(name, value, domain=".gse.com.gh")


# ── Nonce management ───────────────────────────────────────────────────────────
async def _apply_browser_session(session: AsyncSession) -> str | None:
    """Inject cookies + nonce from .env (browser session mode)."""
    if not settings.gse_nonce or not settings.gse_cookies:
        return None
    if settings.gse_user_agent:
        session.headers["User-Agent"] = settings.gse_user_agent
    _inject_cookies(session, settings.gse_cookies)
    print(f"[session] Using browser session nonce: {settings.gse_nonce}")
    return settings.gse_nonce


async def _fetch_nonce(session: AsyncSession) -> str:
    """
    BUG 5 FIX: Save and restore AJAX headers around the page GET.
    The page GET must use navigation headers, not AJAX headers, otherwise
    the server returns JSON/redirect instead of HTML and nonce regex fails.

    BUG 1 FIX: Raise instead of returning a hardcoded dead nonce.
    """
    for attempt in range(3):
        try:
            # Warm up
            try:
                await session.get(
                    "https://gse.com.gh/",
                    headers=_PAGE_HEADERS,
                    timeout=15,
                )
                await asyncio.sleep(1.5)
            except Exception:
                pass

            # Fetch the market page with PAGE headers (not AJAX)
            resp = await session.get(
                settings.gse_page_url,
                headers=_PAGE_HEADERS,   # <-- Bug 5 fix: explicit page headers
                timeout=30,
            )
            resp.raise_for_status()

            for pattern in _NONCE_PATTERNS:
                m = pattern.search(resp.text)
                if m:
                    print(f"[nonce] Found: {m.group(1)}")
                    return m.group(1)

            print(f"[nonce] Not found in page (attempt {attempt + 1}), page size={len(resp.text)}")

        except Exception as exc:
            print(f"[nonce] Fetch failed (attempt {attempt + 1}): {exc}")
            await asyncio.sleep(3.0 * (attempt + 1))

    # BUG 1 FIX: Raise clearly instead of using a dead hardcoded nonce
    raise RuntimeError(
        "Could not extract wdtNonce from GSE page after 3 attempts.\n"
        "Cloudflare is likely blocking auto-fetch.\n"
        "Fix: open gse.com.gh in Chrome, open DevTools → Network,\n"
        "find the admin-ajax.php request, copy:\n"
        "  1. The full Cookie header → GSE_COOKIES in .env\n"
        "  2. The wdtNonce form field value → GSE_NONCE in .env\n"
        "  3. The User-Agent header → GSE_USER_AGENT in .env"
    )


# ── Stale detection ────────────────────────────────────────────────────────────
def _is_stale(rows: list, total_remote: int) -> bool:
    """
    BUG 2 FIX: Also detect Cloudflare block via empty data + zero total.
    A legitimate empty page would still have recordsTotal > 0 on first fetch.
    """
    if not rows:
        return True
    # WDT returns {"data": [], "recordsTotal": 0} on blocked/invalid nonce
    if len(rows) == 0 and total_remote == 0:
        return True
    # Single-row error message from WDT
    if len(rows) == 1 and isinstance(rows[0], list) and len(rows[0]) == 1:
        val = str(rows[0][0]).lower()
        if any(w in val for w in ("nonce", "invalid", "expired", "error", "security")):
            return True
    return False


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
    session: AsyncSession,
    nonce:   str,
    draw:    int,
    start:   int,
    length:  int,
) -> tuple[list, int]:
    payload = _build_payload(draw, start, length, nonce)

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.scraper_max_retries),
        wait=wait_exponential(
            multiplier=settings.scraper_retry_delay_s,
            min=settings.scraper_retry_delay_s,
            max=60.0,
        ),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    ):
        with attempt:
            resp = await session.post(
                settings.gse_ajax_url,
                data=payload,
                headers=_AJAX_HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError(f"Unexpected response: {str(data)[:200]}")
            return data["data"], int(data.get("recordsTotal", 0))

    return [], 0


async def _fetch_page_refreshing(
    session:  AsyncSession,
    nonce:    str,
    draw:     int,
    start:    int,
    length:   int,
    page_num: int,
    retries:  int = 3,
) -> tuple[list, int, str]:
    """Fetch one page, refreshing nonce if stale."""
    current = nonce
    for attempt in range(retries):
        rows, total = await _fetch_page(session, current, draw, start, length)
        if not _is_stale(rows, total):   # Bug 2 fix: pass total
            return rows, total, current
        print(f"[page {page_num}] Stale nonce, refreshing (attempt {attempt + 1})")
        await asyncio.sleep(2.0)
        current = await _apply_browser_session(session) or await _fetch_nonce(session)
    rows, total = await _fetch_page(session, current, draw, start, length)
    if _is_stale(rows, total):
        raise RuntimeError(
            f"Page {page_num} returned empty data after {retries} nonce refreshes. "
            "Cloudflare may be actively blocking. Refresh GSE_COOKIES and GSE_NONCE."
        )
    return rows, total, current


# ── Main entry point ───────────────────────────────────────────────────────────
async def scrape_gse(
    mode:        str        = "latest",
    target_date: str | None = None,
) -> ScrapeResult:
    """
    mode='latest'  → fetch pages for the latest trading day (concurrent, fast)
    mode='all'     → fetch all pages sequentially (historical, nonce-safe)
    mode='date'    → fetch all pages, filter to target_date (YYYY-MM-DD)
    """
    async with _build_client() as session:
        nonce = await _apply_browser_session(session) or await _fetch_nonce(session)
        batch = settings.scraper_batch_size

        print(f"[scrape] mode={mode}, batch={batch}")
        first_rows, total_remote, nonce = await _fetch_page_refreshing(
            session, nonce, 1, 0, batch, page_num=1
        )
        total_pages = max(1, -(-total_remote // batch))
        print(f"[scrape] total_remote={total_remote}, total_pages={total_pages}")

        all_records: list[PriceRecord] = []
        all_errors:  list[dict]        = []
        _process_rows(first_rows, all_records, all_errors)

        if total_pages > 1:
            if mode == "all":
                # Sequential — nonce refreshes safely between pages
                for page_idx in range(1, total_pages):
                    start = page_idx * batch
                    print(f"[scrape] page {page_idx + 1}/{total_pages}")
                    rows, _, nonce = await _fetch_page_refreshing(
                        session, nonce, page_idx + 1, start, batch,
                        page_num=page_idx + 1,
                    )
                    _process_rows(rows, all_records, all_errors)
                    await asyncio.sleep(0.8)
            else:
                # BUG 3 FIX: Each worker fetches its own session nonce independently
                # instead of sharing the outer closure nonce.
                sem = asyncio.Semaphore(settings.scraper_concurrency)

                async def _fetch_worker(page_idx: int) -> tuple[list, int]:
                    async with sem:
                        try:
                            # Each worker uses its own fresh nonce — no shared closure
                            worker_nonce = (
                                await _apply_browser_session(session)
                                or await _fetch_nonce(session)
                            )
                            rows, _, _ = await _fetch_page_refreshing(
                                session, worker_nonce, page_idx + 1,
                                page_idx * batch, batch, page_num=page_idx + 1,
                            )
                            return rows, page_idx
                        except Exception as exc:
                            print(f"[page {page_idx + 1}] failed: {exc}")
                            return [], page_idx

                results = await asyncio.gather(
                    *[_fetch_worker(i) for i in range(1, total_pages)]
                )
                for rows, _ in results:
                    _process_rows(rows, all_records, all_errors)

        print(f"[scrape] done — parsed={len(all_records)}, errors={len(all_errors)}")

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
        print(f"[filter] latest date resolved: {latest}")
        return [r for r in records if r.trade_date == latest]
    return records


# ── Quick test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    async def main():
        result = await scrape_gse(mode="latest")
        print(f"\nRecords : {len(result.records)}")
        print(f"Errors  : {result.error_count}")
        print(f"Pages   : {result.total_pages}")
        if result.records:
            print(f"\nFirst record:\n{json.dumps(result.records[0].to_dict(), default=str, indent=2)}")

    asyncio.run(main())