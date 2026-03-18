

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "gse_stocks")
DB_USER     = os.getenv("DB_USER",     "gse_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA   = os.getenv("DB_SCHEMA",   "gse")
USER_AGENT  = os.getenv(
    "GSE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36",
)

CHUNK_SIZE    = 500
LOAD_TIMEOUT  = 90_000   # 90s initial page load
TABLE_TIMEOUT = 45_000   # 45s wait for table to appear
PAGE_WAIT_S   = 8        # seconds to wait for table to update after Next click


# ── Logging ────────────────────────────────────────────────────────────────────
def log(msg: str, **kw: Any) -> None:
    ts  = datetime.now(timezone.utc).strftime("%H:%M:%S")
    kws = "  ".join(f"{k}={v}" for k, v in kw.items())
    print(f"[{ts}] {msg}  {kws}", flush=True)


# ── Parsers ────────────────────────────────────────────────────────────────────
def _dec(v: Any) -> Decimal | None:
    s = re.sub(r"[^\d.]", "", str(v or "")).strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _int(v: Any) -> int | None:
    d = _dec(v)
    return int(d) if d is not None else None


def _date(v: Any) -> date | None:
    s = str(v or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# ── Column mapping ─────────────────────────────────────────────────────────────
ALIASES: dict[str, list[str]] = {
    "trade_date":                  ["daily date", "date", "trade date"],
    "share_code":                  ["share code", "code", "ticker", "symbol"],
    "year_high":                   ["year high (gh¢)", "year high", "52w high"],
    "year_low":                    ["year low (gh¢)", "year low", "52w low"],
    "previous_closing_price_vwap": ["previous closing price - vwap (gh¢)", "prev close",
                                    "previous closing price"],
    "opening_price":               ["opening price (gh¢)", "open", "opening price"],
    "last_transaction_price":      ["last transaction price (gh¢)", "last price"],
    "closing_price_vwap":          ["closing price - vwap (gh¢)", "close", "closing price", "vwap"],
    "price_change":                ["price change (gh¢)", "change", "price change"],
    "closing_bid_price":           ["closing bid price (gh¢)", "bid"],
    "closing_offer_price":         ["closing offer price (gh¢)", "offer"],
    "total_shares_traded":         ["total shares traded", "volume"],
    "total_value_traded":          ["total value traded (gh¢)", "value", "turnover"],
}


def map_columns(headers: list[str]) -> dict[str, str]:
    lowered = {h.lower().strip(): h for h in headers}
    mapping: dict[str, str] = {}
    for field, aliases in ALIASES.items():
        for alias in aliases:
            if alias in lowered:
                mapping[field] = lowered[alias]
                break
    return mapping


def parse_row(cells: list[str], mapping: dict[str, str], headers: list[str]) -> dict | None:
    row = dict(zip(headers, cells))

    trade_date = _date(row.get(mapping.get("trade_date", ""), ""))
    share_code = row.get(mapping.get("share_code", ""), "").strip().upper()

    if not trade_date or not share_code:
        return None

    def g(field: str) -> str:
        return row.get(mapping.get(field, ""), "")

    return {
        "trade_date":                  trade_date,
        "share_code":                  share_code,
        "year_high":                   _dec(g("year_high")),
        "year_low":                    _dec(g("year_low")),
        "previous_closing_price_vwap": _dec(g("previous_closing_price_vwap")),
        "opening_price":               _dec(g("opening_price")),
        "last_transaction_price":      _dec(g("last_transaction_price")),
        "closing_price_vwap":          _dec(g("closing_price_vwap")),
        "price_change":                _dec(g("price_change")),
        "closing_bid_price":           _dec(g("closing_bid_price")),
        "closing_offer_price":         _dec(g("closing_offer_price")),
        "total_shares_traded":         _int(g("total_shares_traded")),
        "total_value_traded":          _dec(g("total_value_traded")),
    }


# ── Table reader ───────────────────────────────────────────────────────────────
async def read_table_rows(page: Any, selector: str) -> tuple[list[str], list[list[str]]]:
    """
    Extract headers and all visible data rows from the table via JavaScript.
    Returns (headers, list_of_cell_lists).
    """
    result = await page.evaluate(f"""() => {{
        const table = document.querySelector('{selector}');
        if (!table) return {{ headers: [], rows: [] }};

        const headers = Array.from(
            table.querySelectorAll('thead th, thead td')
        ).map(th => th.innerText.trim());

        const rows = Array.from(table.querySelectorAll('tbody tr'))
            .map(tr => Array.from(tr.querySelectorAll('td'))
                           .map(td => td.innerText.trim()))
            .filter(cells => cells.length > 1);

        return {{ headers, rows }};
    }}""")
    return result.get("headers", []), result.get("rows", [])


async def get_first_cell(page: Any, selector: str) -> str:
    """Return the text of the very first data cell — used to detect page change."""
    try:
        return await page.evaluate(f"""() => {{
            const td = document.querySelector('{selector} tbody tr:first-child td:first-child');
            return td ? td.innerText.trim() : '';
        }}""")
    except Exception:
        return ""


async def wait_for_page_change(page: Any, selector: str, old_first_cell: str) -> bool:
    """
    Poll until the first cell in the table changes — this confirms the
    DataTables AJAX call has completed and the new page is rendered.
    Times out after PAGE_WAIT_S * 2 seconds.
    """
    for _ in range(PAGE_WAIT_S * 4):  # check every 0.5s
        await asyncio.sleep(0.5)
        new_cell = await get_first_cell(page, selector)
        if new_cell and new_cell != old_first_cell:
            return True
    return False


# ── Browser scraper ────────────────────────────────────────────────────────────
async def scrape_html_table(url: str) -> list[dict]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        sys.exit("Run: pip install playwright && playwright install chromium")

    all_records: list[dict] = []
    seen:        set[tuple] = set()
    headers:     list[str]  = []
    mapping:     dict[str, str] = {}
    page_count   = 0
    total_records = 0

    table_selectors = [
        "table.wpDataTable",
        "table.dataTable",
        ".dataTables_wrapper table",
    ]
    next_selectors = [
        ".paginate_button.next:not(.disabled)",
        "#table_1_next:not(.disabled)",
        "a.next:not(.disabled)",
        "li.next:not(.disabled) a",
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--disable-gpu",
                "--disable-background-timer-throttling",
            ],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Accra",
        )
        page = await context.new_page()

        # Block ads/analytics
        async def _block(route: Any) -> None:
            blocked = ["google-analytics", "googletagmanager", "doubleclick",
                       "googlesyndication", "facebook", "hotjar"]
            if any(x in route.request.url for x in blocked):
                await route.abort()
            else:
                await route.continue_()
        await page.route("**/*", _block)

        # ── Load page ──────────────────────────────────────────────────────────
        log("loading_page", url=url)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=LOAD_TIMEOUT)
        except Exception as exc:
            log("page_load_partial", error=str(exc))

        # ── Wait for table ─────────────────────────────────────────────────────
        table_selector = None
        for sel in table_selectors:
            try:
                await page.wait_for_selector(sel, timeout=TABLE_TIMEOUT)
                table_selector = sel
                log("table_found", selector=sel)
                break
            except Exception:
                continue

        if not table_selector:
            await browser.close()
            raise RuntimeError("Data table not found on page.")

        # Extra wait for DataTables JS to fully render all rows
        await asyncio.sleep(5)

        # ── Get total record count ─────────────────────────────────────────────
        try:
            info_text = await page.locator(".dataTables_info").inner_text(timeout=5000)
            m = re.search(r"of ([\d,]+) entries", info_text)
            if m:
                total_records = int(m.group(1).replace(",", ""))
                log("total_records", count=total_records)
        except Exception:
            pass

        # ── Paginate ───────────────────────────────────────────────────────────
        while True:
            page_count += 1

            # Capture first cell so we can detect when next page loads
            first_cell_before = await get_first_cell(page, table_selector)

            # Read current page rows
            page_headers, rows = await read_table_rows(page, table_selector)

            if not rows:
                log("empty_page", page=page_count)
                break

            # Set up column mapping on first page
            if not headers:
                headers = page_headers
                mapping = map_columns(headers)
                log("columns_mapped", headers=headers, mapped=len(mapping))

                missing = [f for f in ["trade_date", "share_code", "closing_price_vwap"]
                           if f not in mapping]
                if missing:
                    await browser.close()
                    raise ValueError(f"Required columns not found: {missing}. Headers: {headers}")

            # Parse rows
            page_records = 0
            for cells in rows:
                rec = parse_row(cells, mapping, headers)
                if rec is None:
                    continue
                key = (rec["trade_date"], rec["share_code"])
                if key in seen:
                    continue
                seen.add(key)
                all_records.append(rec)
                page_records += 1

            log("page_scraped",
                page=page_count,
                rows=page_records,
                total_so_far=len(all_records),
            )

            # Stop if we have all records
            if total_records > 0 and len(all_records) >= total_records:
                log("all_records_collected", total=len(all_records))
                break

            # ── Find and click Next ────────────────────────────────────────────
            next_btn = None
            for sel in next_selectors:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=2000):
                        cls = await btn.get_attribute("class") or ""
                        if "disabled" in cls:
                            next_btn = "disabled"
                        else:
                            next_btn = btn
                        break
                except Exception:
                    continue

            if next_btn == "disabled" or next_btn is None:
                log("last_page_reached", page=page_count)
                break

            await next_btn.click()
            log("next_clicked", going_to_page=page_count + 1)

            # ── Wait for table to actually update ──────────────────────────────
            # This is the critical fix — we wait until the first row changes,
            # confirming the DataTables AJAX response has rendered
            changed = await wait_for_page_change(page, table_selector, first_cell_before)
            if not changed:
                log("page_did_not_change", page=page_count + 1)
                # One more try with a longer wait
                await asyncio.sleep(PAGE_WAIT_S)
                new_cell = await get_first_cell(page, table_selector)
                if new_cell == first_cell_before:
                    log("table_stuck_stopping")
                    break

        await browser.close()

    log("scrape_complete",
        pages=page_count,
        records=len(all_records),
        unique_stocks=len({r["share_code"] for r in all_records}),
    )
    return all_records


# ── Database ───────────────────────────────────────────────────────────────────
async def upsert_stock_listings(conn: asyncpg.Connection, codes: list[str]) -> None:
    await conn.executemany(
        f"""
        INSERT INTO {DB_SCHEMA}.stock_listings (share_code, is_active)
        VALUES ($1, true)
        ON CONFLICT (share_code) DO UPDATE
          SET last_seen_at = NOW(), is_active = true
        """,
        [(c,) for c in codes],
    )


async def bulk_upsert_prices(conn: asyncpg.Connection, records: list[dict]) -> int:
    if not records:
        return 0
    result = await conn.execute(
        f"""
        INSERT INTO {DB_SCHEMA}.daily_prices (
            trade_date, share_code,
            year_high, year_low,
            previous_closing_price_vwap, opening_price,
            last_transaction_price, closing_price_vwap,
            price_change, closing_bid_price,
            closing_offer_price, total_shares_traded,
            total_value_traded
        )
        SELECT * FROM UNNEST(
            $1::DATE[], $2::VARCHAR[],
            $3::NUMERIC[], $4::NUMERIC[],
            $5::NUMERIC[], $6::NUMERIC[],
            $7::NUMERIC[], $8::NUMERIC[],
            $9::NUMERIC[], $10::NUMERIC[],
            $11::NUMERIC[], $12::BIGINT[],
            $13::NUMERIC[]
        )
        ON CONFLICT (trade_date, share_code) DO UPDATE SET
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
            updated_at                  = NOW()
        """,
        [r["trade_date"]                  for r in records],
        [r["share_code"]                  for r in records],
        [r["year_high"]                   for r in records],
        [r["year_low"]                    for r in records],
        [r["previous_closing_price_vwap"] for r in records],
        [r["opening_price"]               for r in records],
        [r["last_transaction_price"]      for r in records],
        [r["closing_price_vwap"]          for r in records],
        [r["price_change"]                for r in records],
        [r["closing_bid_price"]           for r in records],
        [r["closing_offer_price"]         for r in records],
        [r["total_shares_traded"]         for r in records],
        [r["total_value_traded"]          for r in records],
    )
    return int(result.split()[-1]) if result else 0


async def save_to_database(records: list[dict]) -> None:
    log("connecting_to_db", host=DB_HOST, db=DB_NAME)
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    try:
        codes = list({r["share_code"] for r in records})
        await upsert_stock_listings(conn, codes)
        log("stocks_upserted", count=len(codes))

        total_written = 0
        for i in range(0, len(records), CHUNK_SIZE):
            chunk   = records[i:i + CHUNK_SIZE]
            written = await bulk_upsert_prices(conn, chunk)
            total_written += written
            pct = round((i + len(chunk)) / len(records) * 100)
            log("db_progress", chunk=i // CHUNK_SIZE + 1, written=written, pct=f"{pct}%")

        log("db_complete",
            total_written=total_written,
            date_range=(
                f"{min(r['trade_date'] for r in records)} → "
                f"{max(r['trade_date'] for r in records)}"
            ),
        )
    finally:
        await conn.close()


# ── Analysis ───────────────────────────────────────────────────────────────────
def print_analysis(records: list[dict]) -> None:
    if not records:
        return
    codes = sorted({r["share_code"] for r in records})
    dates = sorted({r["trade_date"] for r in records})
    print("\n=== Summary ===")
    print(f"Total records  : {len(records):,}")
    print(f"Unique stocks  : {len(codes)}")
    print(f"Stock codes    : {', '.join(codes)}")
    print(f"Date range     : {dates[0]} → {dates[-1]}")

    vol: dict[str, int] = {}
    for r in records:
        vol[r["share_code"]] = vol.get(r["share_code"], 0) + (r["total_shares_traded"] or 0)
    print("\nTop 10 by volume:")
    for code, v in sorted(vol.items(), key=lambda x: -x[1])[:10]:
        print(f"  {code:<8} {v:>15,}")


# ── Main ───────────────────────────────────────────────────────────────────────
async def run(url: str, dry_run: bool) -> None:
    t0 = datetime.now(timezone.utc)
    log("gse_scraper_start", url=url, dry_run=dry_run)

    records = await scrape_html_table(url)
    if not records:
        log("no_records_found")
        return

    print_analysis(records)

    if dry_run:
        log("dry_run_complete", records=len(records))
    else:
        await save_to_database(records)

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log("finished", elapsed_s=round(elapsed, 1))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape GSE HTML table → PostgreSQL")
    parser.add_argument("--url",      default="https://gse.com.gh/trading-and-data/")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.url, args.dry_run))


if __name__ == "__main__":
    main()