"""
GSE Trading Data Scraper — Selenium + PostgreSQL
=================================================
Reads live DOM via JavaScript after each page click.
Uses DataTables _iDisplayStart to confirm each page advance.

Usage:
    python scrapper.py
    python scrapper.py --resume-from-page 47
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import time
import traceback
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg
from dotenv import load_dotenv
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException

load_dotenv()
import sys as _sys
_sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

# ── Config ─────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "gse_stocks")
DB_USER     = os.getenv("DB_USER",     "gse_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA   = os.getenv("DB_SCHEMA",   "gse")

COL_DATE       = "Daily Date"
COL_CODE       = "Share Code"
COL_YEAR_HIGH  = "Year High (GH\u00a2)"
COL_YEAR_LOW   = "Year Low (GH\u00a2)"
COL_PREV_CLOSE = "Previous Closing Price - VWAP (GH\u00a2)"
COL_OPEN       = "Opening Price (GH\u00a2)"
COL_LAST       = "Last Transaction Price (GH\u00a2)"
COL_CLOSE      = "Closing Price - VWAP (GH\u00a2)"
COL_CHANGE     = "Price Change (GH\u00a2)"
COL_BID        = "Closing Bid Price (GH\u00a2)"
COL_OFFER      = "Closing Offer Price (GH\u00a2)"
COL_VOL        = "Total Shares Traded"
COL_VALUE      = "Total Value Traded (GH\u00a2)"


# ── Parsers ────────────────────────────────────────────────────────────────────
def _dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    s = re.sub(r"[^\d.]", "", str(v)).strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _int(v: Any) -> int | None:
    d = _dec(v)
    return int(d) if d is not None else None


def _parse_date(v: Any) -> date | None:
    s = str(v or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def rows_to_records(
    js_rows:   list[list[str]],
    headers:   list[str],
    last_date: date | None,
) -> tuple[list[dict], date | None]:
    records: list[dict] = []

    try:
        date_idx = headers.index(COL_DATE)
    except ValueError:
        date_idx = 0

    try:
        code_idx = headers.index(COL_CODE)
    except ValueError:
        code_idx = 1

    for cells in js_rows:
        if len(cells) <= max(date_idx, code_idx):
            continue

        raw_d = cells[date_idx].strip()
        if raw_d:
            parsed = _parse_date(raw_d)
            if parsed:
                last_date = parsed

        if not last_date:
            continue

        share_code = str(cells[code_idx] or "").strip().upper()
        if not share_code:
            continue

        row = dict(zip(headers, cells))

        def g(col: str) -> Any:
            return row.get(col)

        records.append({
            "trade_date":                  last_date,
            "share_code":                  share_code,
            "year_high":                   _dec(g(COL_YEAR_HIGH)),
            "year_low":                    _dec(g(COL_YEAR_LOW)),
            "previous_closing_price_vwap": _dec(g(COL_PREV_CLOSE)),
            "opening_price":               _dec(g(COL_OPEN)),
            "last_transaction_price":      _dec(g(COL_LAST)),
            "closing_price_vwap":          _dec(g(COL_CLOSE)),
            "price_change":                _dec(g(COL_CHANGE)),
            "closing_bid_price":           _dec(g(COL_BID)),
            "closing_offer_price":         _dec(g(COL_OFFER)),
            "total_shares_traded":         _int(g(COL_VOL)),
            "total_value_traded":          _dec(g(COL_VALUE)),
        })

    return records, last_date


# ── DOM readers ────────────────────────────────────────────────────────────────
def js_read_rows(driver: ChromeDriver) -> list[list[str]]:
    """Read ALL visible rows from live DOM using JavaScript."""
    return driver.execute_script("""
        var rows = [];
        document.querySelectorAll('table#table_1 tbody tr').forEach(function(tr) {
            var cells = [];
            tr.querySelectorAll('td').forEach(function(td) {
                cells.push(td.innerText.trim());
            });
            if (cells.length > 1) rows.push(cells);
        });
        return rows;
    """) or []


def js_get_display_start(driver: ChromeDriver) -> int:
    """Get DataTables _iDisplayStart — the row offset of the current page."""
    val = driver.execute_script("""
        try {
            return jQuery('#table_1').DataTable().settings()[0]._iDisplayStart;
        } catch(e) { return -1; }
    """)
    try:
        return int(val)
    except Exception:
        return -1


def js_get_total_pages(driver: ChromeDriver) -> int:
    val = driver.execute_script("""
        try {
            var info = jQuery('#table_1').DataTable().page.info();
            return info.pages;
        } catch(e) { return 0; }
    """)
    try:
        return int(val)
    except Exception:
        return 0


def js_get_headers(driver: ChromeDriver) -> list[str]:
    return driver.execute_script("""
        var h = [];
        document.querySelectorAll('table#table_1 thead th').forEach(function(th) {
            h.push(th.innerText.trim());
        });
        return h;
    """) or []


def js_click_next(driver: ChromeDriver) -> bool:
    """Click Next via JS directly on the button element."""
    result = driver.execute_script("""
        var btn = document.querySelector('.paginate_button.next');
        if (!btn) return 'not_found';
        if (btn.classList.contains('disabled')) return 'disabled';
        btn.click();
        return 'clicked';
    """)
    return result == 'clicked'


def wait_for_display_start_change(
    driver:    ChromeDriver,
    old_start: int,
    timeout:   int = 15,
) -> bool:
    """
    Poll until DataTables _iDisplayStart changes.
    This is the most reliable way to confirm a new page has loaded —
    it changes the instant the AJAX response updates the DataTables state.
    """
    for _ in range(timeout * 2):
        time.sleep(0.5)
        new_start = js_get_display_start(driver)
        if new_start != old_start and new_start >= 0:
            return True
    return False


# ── Database ───────────────────────────────────────────────────────────────────
async def create_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.stock_listings (
            id           SERIAL      PRIMARY KEY,
            share_code   VARCHAR(20) NOT NULL UNIQUE,
            is_active    BOOLEAN     DEFAULT true,
            last_seen_at TIMESTAMPTZ DEFAULT NOW(),
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.daily_prices (
            id                          BIGSERIAL   PRIMARY KEY,
            trade_date                  DATE        NOT NULL,
            share_code                  VARCHAR(20) NOT NULL,
            year_high                   NUMERIC(18,4),
            year_low                    NUMERIC(18,4),
            previous_closing_price_vwap NUMERIC(18,4),
            opening_price               NUMERIC(18,4),
            last_transaction_price      NUMERIC(18,4),
            closing_price_vwap          NUMERIC(18,4),
            price_change                NUMERIC(18,4),
            closing_bid_price           NUMERIC(18,4),
            closing_offer_price         NUMERIC(18,4),
            total_shares_traded         BIGINT,
            total_value_traded          NUMERIC(18,4),
            created_at                  TIMESTAMPTZ DEFAULT NOW(),
            updated_at                  TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT uq_daily_prices UNIQUE (trade_date, share_code)
        )
    """)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.scrape_jobs (
            id              SERIAL      PRIMARY KEY,
            status          VARCHAR(50) DEFAULT 'running',
            mode            VARCHAR(50) DEFAULT 'historical',
            started_at      TIMESTAMPTZ DEFAULT NOW(),
            finished_at     TIMESTAMPTZ,
            pages_scraped   INT         DEFAULT 0,
            records_scraped INT         DEFAULT 0,
            resume_from     INT         DEFAULT 1,
            error_message   TEXT
        )
    """)
    for col_sql in [
        f"ALTER TABLE {DB_SCHEMA}.scrape_jobs ADD COLUMN IF NOT EXISTS pages_scraped INT DEFAULT 0",
        f"ALTER TABLE {DB_SCHEMA}.scrape_jobs ADD COLUMN IF NOT EXISTS resume_from INT DEFAULT 1",
        f"ALTER TABLE {DB_SCHEMA}.scrape_jobs ADD COLUMN IF NOT EXISTS mode VARCHAR(50) DEFAULT 'historical'",
    ]:
        await conn.execute(col_sql)
    for sql in [
        f"CREATE INDEX IF NOT EXISTS idx_dp_trade_date ON {DB_SCHEMA}.daily_prices (trade_date DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_dp_share_code ON {DB_SCHEMA}.daily_prices (share_code)",
    ]:
        await conn.execute(sql)
    print("Tables ready.")


async def job_start(conn: asyncpg.Connection, resume_from: int) -> str:
    row = await conn.fetchrow(
        f"INSERT INTO {DB_SCHEMA}.scrape_jobs (status, mode, resume_from) "
        f"VALUES ('running', 'historical', $1) RETURNING id::text",
        resume_from,
    )
    job_id = str(row["id"]) if row else "0"
    print(f"Job started : {job_id}")
    return job_id


async def job_finish(conn: asyncpg.Connection, job_id: str,
                     pages: int, records: int,
                     status: str = "completed",
                     error: str | None = None) -> None:
    try:
        await conn.execute(
            f"UPDATE {DB_SCHEMA}.scrape_jobs "
            f"SET status=$2, finished_at=NOW(), pages_scraped=$3, "
            f"records_scraped=$4, error_message=$5 WHERE id::text=$1",
            job_id, status, pages, records, error,
        )
    except Exception:
        pass
    print(f"Job {status}: pages={pages:,}  records={records:,}")


async def upsert_page(conn: asyncpg.Connection, records: list[dict]) -> int:
    if not records:
        return 0
    codes = list({r["share_code"] for r in records})
    await conn.executemany(
        f"INSERT INTO {DB_SCHEMA}.stock_listings (share_code, is_active) "
        f"VALUES ($1, true) ON CONFLICT (share_code) DO UPDATE "
        f"SET last_seen_at=NOW(), is_active=true",
        [(c,) for c in codes],
    )
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


# ── Main ───────────────────────────────────────────────────────────────────────
async def run_scraper(resume_from: int = 1) -> None:
    t0            = datetime.now(timezone.utc)
    total_written = 0
    page_count    = 0
    last_date:    date | None = None
    job_id        = ""
    final_status  = "completed"
    error_msg:    str | None = None

    print(f"GSE Historical Scraper")
    print(f"Resume from page : {resume_from}")
    print(f"Database         : {DB_NAME} on {DB_HOST}\n")

    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    await create_tables(conn)
    job_id = await job_start(conn, resume_from)

    driver: ChromeDriver | None = None

    try:
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        options.add_argument("--log-level=3")
        options.add_argument("--blink-settings=imagesEnabled=false")

        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        driver  = ChromeDriver(service=service, options=options)
        print(f"ChromeDriver ready")
        print(f"Browser: {driver.capabilities['browserVersion']}")

        driver.get("https://gse.com.gh/trading-and-data/")
        time.sleep(10)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.wpDataTable"))
        )
        print("Table found.")

        # Get total pages
        total_pages = js_get_total_pages(driver)
        total_records_el = driver.execute_script("""
            try { return jQuery('#table_1').DataTable().page.info().recordsTotal; }
            catch(e) { return 0; }
        """)
        print(f"Total records : {total_records_el:,}  (~{total_pages:,} pages)")

        # Get headers
        headers = js_get_headers(driver)
        print(f"Headers: {headers}\n")

        # Jump to resume page
        if resume_from > 1:
            print(f"Jumping to page {resume_from}...")
            driver.execute_script(
                f"jQuery('#table_1').DataTable().page({resume_from - 1}).draw(false);"
            )
            time.sleep(5)
            page_count = resume_from - 1

        consecutive_empty = 0

        while True:
            # Capture _iDisplayStart BEFORE reading rows
            start_before = js_get_display_start(driver)
            page_count  += 1

            # Read rows from LIVE DOM (not page_source)
            js_rows = js_read_rows(driver)

            if not js_rows:
                consecutive_empty += 1
                print(f"Page {page_count:>5}  | empty (consecutive={consecutive_empty})")
                if consecutive_empty >= 3:
                    break
            else:
                consecutive_empty = 0
                records, last_date = rows_to_records(js_rows, headers, last_date)

                if records:
                    unique_dates = sorted({r["trade_date"] for r in records})
                    written        = await upsert_page(conn, records)
                    total_written += written
                    print(
                        f"Page {page_count:>5}  |  "
                        f"rows={len(js_rows)}  "
                        f"upserted={written}  "
                        f"total_db={total_written:,}  "
                        f"dates={unique_dates}"
                    )
                    await conn.execute(
                        f"UPDATE {DB_SCHEMA}.scrape_jobs "
                        f"SET pages_scraped=$2, records_scraped=$3 WHERE id::text=$1",
                        job_id, page_count, total_written,
                    )
                else:
                    print(f"Page {page_count:>5}  |  rows={len(js_rows)}  parsed=0")

            # Check if last page
            if total_pages > 0 and page_count >= total_pages:
                print("All pages complete.")
                break

            # Click Next via JS
            click_result = js_click_next(driver)
            if click_result == 'disabled' or not click_result:
                print("Last page reached.")
                break

            # Wait for _iDisplayStart to change — confirms new data in DOM
            changed = wait_for_display_start_change(driver, start_before, timeout=15)
            if not changed:
                # Retry once
                js_click_next(driver)
                changed = wait_for_display_start_change(driver, start_before, timeout=10)
                if not changed:
                    new_start = js_get_display_start(driver)
                    print(f"  WARNING: page did not advance "
                          f"(start={start_before} → {new_start})")
                    # Continue anyway — read whatever is in DOM
                    continue

    except KeyboardInterrupt:
        final_status = "stopped"
        print(f"\nStopped at page {page_count}.")
        print(f"Resume: python scrapper.py --resume-from-page {page_count}")

    except Exception as exc:
        final_status = "failed"
        error_msg    = str(exc)
        print(f"\nError: {exc}")
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("Browser closed.")
        if job_id:
            await job_finish(conn, job_id, page_count, total_written,
                             final_status, error_msg)
        await conn.close()
        print("DB closed.")

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    mins, secs = divmod(int(elapsed), 60)
    print(f"\nTotal upserted : {total_written:,}")
    print(f"Time elapsed   : {mins}m {secs}s")
    print(f"\nVerify:")
    print(f"  SELECT COUNT(*) FROM {DB_SCHEMA}.daily_prices;")
    print(f"  SELECT DISTINCT trade_date FROM {DB_SCHEMA}.daily_prices ORDER BY 1 DESC LIMIT 10;")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GSE Scraper")
    parser.add_argument("--resume-from-page", type=int, default=1, metavar="N")
    args = parser.parse_args()
    asyncio.run(run_scraper(resume_from=args.resume_from_page))