

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
import traceback
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import asyncpg
import pandas as pd
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "gse_stocks")
DB_USER     = os.getenv("DB_USER",     "gse_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA   = os.getenv("DB_SCHEMA",   "gse")

# ── GSE column names (¢ = U+00A2) ─────────────────────────────────────────────
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
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def clean_numeric_value(value_str: object) -> float:
    s = str(value_str).strip() if value_str is not None else ""
    if s in ("", "nan", "none", "null", "n/a", "-"):
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", s)
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


# ── Row parser ─────────────────────────────────────────────────────────────────
def rows_to_records(
    raw_rows: list[list[str]],
    headers:  list[str],
    seen:     set[tuple],
) -> tuple[list[dict], list[str]]:
    """
    Convert raw HTML rows to DB-ready dicts.
    Returns (records, skip_reasons) so callers can log why rows were skipped.
    """
    records:     list[dict] = []
    skip_reasons: list[str] = []

    for cells in raw_rows:
        if len(cells) < len(headers):
            skip_reasons.append(f"too few cells: got {len(cells)}, expected {len(headers)}")
            continue

        row = dict(zip(headers, cells))

        trade_date: date | None = None
        raw_d = row.get(COL_DATE, "")
        if raw_d:
            try:
                trade_date = pd.Timestamp(raw_d).date()
            except Exception:
                trade_date = _parse_date(raw_d)

        share_code = str(row.get(COL_CODE) or "").strip().upper()

        if not trade_date:
            skip_reasons.append(f"invalid date: {repr(raw_d)}")
            continue
        if not share_code:
            skip_reasons.append("empty share code")
            continue

        key = (trade_date, share_code)
        if key in seen:
            skip_reasons.append(f"duplicate: {share_code} {trade_date}")
            continue
        seen.add(key)

        def g(col: str) -> Any:
            return row.get(col)

        records.append({
            "trade_date":                  trade_date,
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

    return records, skip_reasons


# ── Database ───────────────────────────────────────────────────────────────────
async def create_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.stock_listings (
            id           SERIAL      PRIMARY KEY,
            share_code   VARCHAR(20) NOT NULL UNIQUE,
            company_name VARCHAR(255),
            is_active    BOOLEAN     DEFAULT true,
            listed_at    DATE,
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
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
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
    for sql in [
        f"CREATE INDEX IF NOT EXISTS idx_dp_trade_date ON {DB_SCHEMA}.daily_prices (trade_date DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_dp_share_code ON {DB_SCHEMA}.daily_prices (share_code)",
        f"CREATE INDEX IF NOT EXISTS idx_dp_date_code  ON {DB_SCHEMA}.daily_prices (trade_date DESC, share_code)",
    ]:
        await conn.execute(sql)
    print("Tables ready.")


# ── Improvement 1: scrape_jobs tracking ───────────────────────────────────────
async def job_start(conn: asyncpg.Connection, resume_from: int) -> str:
    """Insert a scrape_jobs row and return the job id."""
    job_id = str(uuid.uuid4())
    await conn.execute(
        f"""
        INSERT INTO {DB_SCHEMA}.scrape_jobs (id, status, mode, resume_from)
        VALUES ($1, 'running', 'historical', $2)
        """,
        job_id, resume_from,
    )
    print(f"Job started  : {job_id}")
    return job_id


async def job_update(
    conn:    asyncpg.Connection,
    job_id:  str,
    pages:   int,
    records: int,
) -> None:
    """Update progress on the current job (called after every page)."""
    await conn.execute(
        f"""
        UPDATE {DB_SCHEMA}.scrape_jobs
        SET pages_scraped = $2, records_scraped = $3
        WHERE id = $1
        """,
        job_id, pages, records,
    )


async def job_finish(
    conn:          asyncpg.Connection,
    job_id:        str,
    pages:         int,
    records:       int,
    status:        str = "completed",
    error_message: str | None = None,
) -> None:
    """Mark the job as completed or failed."""
    await conn.execute(
        f"""
        UPDATE {DB_SCHEMA}.scrape_jobs
        SET status          = $2,
            finished_at     = NOW(),
            pages_scraped   = $3,
            records_scraped = $4,
            error_message   = $5
        WHERE id = $1
        """,
        job_id, status, pages, records, error_message,
    )
    print(f"Job {status}: {job_id}")
    print(f"  Pages scraped  : {pages:,}")
    print(f"  Records saved  : {records:,}")


async def upsert_page(conn: asyncpg.Connection, records: list[dict]) -> int:
    if not records:
        return 0
    codes = list({r["share_code"] for r in records})
    await conn.executemany(
        f"""
        INSERT INTO {DB_SCHEMA}.stock_listings (share_code, is_active)
        VALUES ($1, true)
        ON CONFLICT (share_code) DO UPDATE
          SET last_seen_at = NOW(), is_active = true
        """,
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
            $1::DATE[],    $2::VARCHAR[],
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


# ── Selenium helpers ───────────────────────────────────────────────────────────
def setup_driver() -> WebDriver:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    )
    options.add_argument("--log-level=3")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service: Service = Service(ChromeDriverManager().install())
        driver: WebDriver = WebDriver(service=service, options=options)
        print("ChromeDriver ready")
        return driver
    except Exception as exc:
        print(f"ChromeDriver failed: {exc}")
        raise


def _click_next(driver: WebDriver) -> bool:
    for by, sel in [
        (By.CSS_SELECTOR, ".paginate_button.next"),
        (By.ID,           "table_1_next"),
        (By.XPATH,        "//a[contains(@class,'next') and contains(@class,'paginate_button')]"),
        (By.XPATH,        "//a[contains(text(),'Next')]"),
        (By.XPATH,        "//li[contains(@class,'next')]/a"),
    ]:
        try:
            found: WebElement = driver.find_element(by, sel)
            cls: str = found.get_attribute("class") or ""
            if "disabled" in cls:
                return False
            for name, script in [
                ("direct",      None),
                ("js-click",    "arguments[0].click();"),
                ("js-dispatch", "arguments[0].dispatchEvent(new MouseEvent('click',{bubbles:true}));"),
            ]:
                try:
                    if script is None:
                        found.click()
                    else:
                        driver.execute_script(script, found)
                    print(f"  Next ({name})")
                    return True
                except Exception:
                    continue
        except (NoSuchElementException, StaleElementReferenceException):
            continue
    return False


def _get_text(tag: Tag, selector: str) -> list[str]:
    return [cast(str, td.get_text(strip=True)) for td in tag.find_all(selector)]


# ── Improvement 3: jump to a specific page ────────────────────────────────────
def _jump_to_page(driver: WebDriver, target_page: int) -> None:
    """
    Use the DataTables JS API to jump directly to a page number.
    Much faster than clicking Next N times.
    """
    if target_page <= 1:
        return
    print(f"Jumping to page {target_page} via DataTables API...")
    try:
        driver.execute_script(f"""
            var tables = jQuery('table.wpDataTable');
            if (tables.length && tables.DataTable) {{
                tables.DataTable().page({target_page - 1}).draw(false);
            }}
        """)
        # Wait for the table to update
        time.sleep(3)
        print(f"Jumped to page {target_page}.")
    except Exception as exc:
        print(f"Jump failed: {exc} — will click Next {target_page - 1} times instead")
        for i in range(target_page - 1):
            if not _click_next(driver):
                break
            time.sleep(1)
            print(f"  Navigated to page {i + 2}")


# ── Main ───────────────────────────────────────────────────────────────────────
async def run_scraper(resume_from: int = 1) -> None:
    t0            = datetime.now(timezone.utc)
    ts_start      = t0.strftime("%Y%m%d_%H%M%S")
    csv_path      = f"gse_partial_{ts_start}.csv"
    total_written = 0
    page_count    = 0
    total_records = 0
    all_data:     list[list[str]] = []
    headers_list: list[str]       = []
    seen:         set[tuple]      = set()
    job_id:       str             = ""

    print(f"Starting GSE scraper...")
    print(f"Resume from page  : {resume_from}")
    print(f"Progressive save  : {csv_path}")
    print(f"Database          : {DB_NAME} on {DB_HOST}\n")

    print("Connecting to database...")
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    await create_tables(conn)

    # Improvement 1: record job start
    job_id = await job_start(conn, resume_from)

    driver: WebDriver | None = None
    final_status = "completed"
    error_msg:  str | None = None

    try:
        driver = setup_driver()
        print(f"Browser: {driver.capabilities['browserVersion']}")

        driver.get("https://gse.com.gh/trading-and-data/")
        time.sleep(10)

        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.wpDataTable"))
            )
            print("Table found.")
        except TimeoutException:
            driver.save_screenshot("gse_no_table.png")
            raise RuntimeError("Table not found on page.")

        for sel in [".dataTables_info", ".wpDataTable-info", ".table-info"]:
            try:
                text = driver.find_element(By.CSS_SELECTOR, sel).text
                m    = re.search(r"of ([\d,]+) entries", text)
                if m:
                    total_records = int(m.group(1).replace(",", ""))
                    pages_est     = (total_records + 25) // 26
                    print(f"Total records : {total_records:,}  (~{pages_est} pages)\n")
                    break
            except NoSuchElementException:
                continue

        # Improvement 3: jump to resume page if set
        if resume_from > 1:
            _jump_to_page(driver, resume_from)
            page_count = resume_from - 1

        consecutive_empty = 0

        while consecutive_empty < 3:
            page_count += 1

            soup      = BeautifulSoup(driver.page_source, "html.parser")
            raw_table = soup.find("table", {"class": "wpDataTable"})
            if not isinstance(raw_table, Tag):
                consecutive_empty += 1
                continue
            table: Tag = raw_table

            if not headers_list:
                raw_header = table.find("tr")
                if not isinstance(raw_header, Tag):
                    break
                headers_list = _get_text(raw_header, "th")
                print(f"Headers: {headers_list}\n")

            page_rows: list[list[str]] = []
            for row in table.find_all("tr")[1:]:
                if not isinstance(row, Tag):
                    continue
                cells = _get_text(row, "td")
                if len(cells) > 1:
                    page_rows.append(cells)

            if not page_rows:
                consecutive_empty += 1
                # Improvement 2: log why page has no rows
                print(f"Page {page_count}: no rows found in HTML table.")
                continue

            consecutive_empty = 0
            all_data.extend(page_rows)

            # Parse and save this page
            page_records, skip_reasons = rows_to_records(page_rows, headers_list, seen)

            # Improvement 2: log skip reasons when saved=0
            if not page_records:
                print(f"Page {page_count:>4}  |  rows={len(page_rows)}  saved=0  "
                      f"total_db={total_written:,}  total_scraped={len(all_data):,}")
                if skip_reasons:
                    unique_reasons = list(dict.fromkeys(skip_reasons))[:3]
                    print(f"  Skipped because: {'; '.join(unique_reasons)}")
            else:
                try:
                    written        = await upsert_page(conn, page_records)
                    total_written += written
                    print(
                        f"Page {page_count:>4}  |  "
                        f"rows={len(page_rows)}  "
                        f"saved={written}  "
                        f"total_db={total_written:,}  "
                        f"total_scraped={len(all_data):,}"
                    )
                    # Improvement 1: update job progress after every page
                    await job_update(conn, job_id, page_count, total_written)
                except Exception as db_exc:
                    print(f"Page {page_count} DB error: {db_exc}")

            # CSV checkpoint every 10 pages
            if page_count % 10 == 0:
                pd.DataFrame(all_data, columns=headers_list).to_csv(csv_path, index=False)
                print(f"  CSV checkpoint → {csv_path}")

            if total_records > 0 and len(all_data) >= total_records:
                print("\nAll records collected.")
                break

            if not _click_next(driver):
                print("\nLast page reached.")
                break

            # Wait for table to update
            time.sleep(1)
            try:
                sel_first = "table.wpDataTable tbody tr:first-child td:first-child"
                old_text  = driver.find_element(By.CSS_SELECTOR, sel_first).text
                for _ in range(20):
                    time.sleep(0.5)
                    new_text = driver.find_element(By.CSS_SELECTOR, sel_first).text
                    if new_text != old_text:
                        break
            except Exception:
                time.sleep(3)

    except KeyboardInterrupt:
        final_status = "stopped"
        print(f"\nStopped by user at page {page_count}.")
        print(f"Resume with:  python scrapper.py --resume-from-page {page_count}")

    except Exception as exc:
        final_status = "failed"
        error_msg    = str(exc)
        print(f"\nError: {exc}")
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("Browser closed.")

        # Improvement 1: mark job as finished
        if job_id:
            await job_finish(conn, job_id, page_count, total_written,
                             final_status, error_msg)

        await conn.close()
        print("Database connection closed.")

    # Final CSV
    if all_data and headers_list:
        final_csv = f"gse_complete_{ts_start}.csv"
        pd.DataFrame(all_data, columns=headers_list).to_csv(final_csv, index=False)
        print(f"\nFinal CSV : {final_csv}")

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    mins, secs = divmod(int(elapsed), 60)
    print(f"Total DB rows  : {total_written:,}")
    print(f"Total scraped  : {len(all_data):,}")
    print(f"Time elapsed   : {mins}m {secs}s")
    print(f"\nVerify in psql:")
    print(f"  SELECT COUNT(*) FROM {DB_SCHEMA}.daily_prices;")
    print(f"  SELECT * FROM {DB_SCHEMA}.scrape_jobs ORDER BY started_at DESC LIMIT 5;")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GSE Trading Data Scraper")
    parser.add_argument(
        "--resume-from-page",
        type=int,
        default=1,
        metavar="N",
        help="Resume scraping from page N (default: 1)",
    )
    args = parser.parse_args()
    asyncio.run(run_scraper(resume_from=args.resume_from_page))