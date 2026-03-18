"""
GSE Trading Data Scraper — Selenium + PostgreSQL
=================================================
Saves to PostgreSQL after EVERY page — data is never lost if stopped.
Creates tables automatically on first run.

Reads from .env: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
import traceback
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
CHUNK_SIZE  = 500

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


def get_cell(row: pd.Series, col: str) -> Any:
    v = row.get(col)
    try:
        return None if pd.isna(v) else v
    except (TypeError, ValueError):
        return v


# ── Row parser ─────────────────────────────────────────────────────────────────
def rows_to_records(
    raw_rows:    list[list[str]],
    headers:     list[str],
    seen:        set[tuple],
) -> list[dict]:
    """
    Convert a list of raw HTML cell lists into DB-ready dicts.
    Updates seen in-place to deduplicate across pages.
    """
    records: list[dict] = []
    for cells in raw_rows:
        if len(cells) < len(headers):
            continue
        row = dict(zip(headers, cells))

        trade_date: date | None = None
        raw_d = row.get(COL_DATE)
        if raw_d:
            try:
                trade_date = pd.Timestamp(raw_d).date()
            except Exception:
                trade_date = _parse_date(raw_d)

        share_code = str(row.get(COL_CODE) or "").strip().upper()

        if not trade_date or not share_code:
            continue

        key = (trade_date, share_code)
        if key in seen:
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
    return records


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
            status          VARCHAR(50) DEFAULT 'pending',
            mode            VARCHAR(50),
            target_date     DATE,
            started_at      TIMESTAMPTZ DEFAULT NOW(),
            finished_at     TIMESTAMPTZ,
            records_scraped INT         DEFAULT 0,
            error_message   TEXT
        )
    """)
    for sql in [
        f"CREATE INDEX IF NOT EXISTS idx_dp_trade_date ON {DB_SCHEMA}.daily_prices (trade_date DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_dp_share_code ON {DB_SCHEMA}.daily_prices (share_code)",
        f"CREATE INDEX IF NOT EXISTS idx_dp_date_code  ON {DB_SCHEMA}.daily_prices (trade_date DESC, share_code)",
    ]:
        await conn.execute(sql)


async def upsert_page(conn: asyncpg.Connection, records: list[dict]) -> int:
    """Upsert one page of records using a single UNNEST statement."""
    if not records:
        return 0

    # Register stock codes
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

    # Bulk upsert prices
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
    """Find and click the Next button. Returns False if disabled or not found."""
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
            # Try three click methods
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


# ── Main ───────────────────────────────────────────────────────────────────────
async def run_scraper() -> None:
    """
    Main scraper — opens ONE database connection for the entire run,
    saves to DB and CSV after every page, never loses data if stopped.
    """
    t0            = datetime.now(timezone.utc)
    ts_start      = t0.strftime("%Y%m%d_%H%M%S")
    csv_path      = f"gse_partial_{ts_start}.csv"
    total_written = 0
    page_count    = 0
    total_records = 0
    all_data:     list[list[str]] = []
    headers_list: list[str]       = []
    seen:         set[tuple]      = set()   # deduplication across all pages

    print(f"Starting GSE scraper...")
    print(f"Progressive save  : {csv_path}")
    print(f"Database          : {DB_NAME} on {DB_HOST}")

    # ── Open DB connection once ────────────────────────────────────────────────
    print(f"\nConnecting to database...")
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    await create_tables(conn)
    print("Tables ready.\n")

    driver: WebDriver | None = None

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
            print("Table not found — screenshot saved.")
            return

        # Get total record count
        for sel in [".dataTables_info", ".wpDataTable-info", ".table-info"]:
            try:
                text = driver.find_element(By.CSS_SELECTOR, sel).text
                m    = re.search(r"of ([\d,]+) entries", text)
                if m:
                    total_records = int(m.group(1).replace(",", ""))
                    pages_est = (total_records + 25) // 26
                    print(f"Total records: {total_records:,}  (~{pages_est} pages)")
                    break
            except NoSuchElementException:
                continue

        consecutive_empty = 0

        # ── Page loop ──────────────────────────────────────────────────────────
        while consecutive_empty < 3:
            page_count += 1

            soup      = BeautifulSoup(driver.page_source, "html.parser")
            raw_table = soup.find("table", {"class": "wpDataTable"})
            if not isinstance(raw_table, Tag):
                consecutive_empty += 1
                continue
            table: Tag = raw_table

            # Extract headers on first page
            if page_count == 1:
                raw_header = table.find("tr")
                if not isinstance(raw_header, Tag):
                    break
                headers_list = _get_text(raw_header, "th")
                print(f"Headers: {headers_list}\n")

            # Extract rows
            page_rows: list[list[str]] = []
            for row in table.find_all("tr")[1:]:
                if not isinstance(row, Tag):
                    continue
                cells = _get_text(row, "td")
                if len(cells) > 1:
                    page_rows.append(cells)

            if not page_rows:
                consecutive_empty += 1
                print(f"Page {page_count}: no rows.")
                continue

            consecutive_empty = 0
            all_data.extend(page_rows)

            # ── Save this page to DB immediately ───────────────────────────────
            page_records = rows_to_records(page_rows, headers_list, seen)
            if page_records:
                try:
                    written = await upsert_page(conn, page_records)
                    total_written += written
                    print(
                        f"Page {page_count:>4}  |  "
                        f"rows={len(page_rows)}  "
                        f"saved={written}  "
                        f"total_db={total_written:,}  "
                        f"total_scraped={len(all_data):,}"
                    )
                except Exception as db_exc:
                    print(f"Page {page_count} DB error: {db_exc}")

            # ── Save CSV checkpoint ────────────────────────────────────────────
            if page_count % 10 == 0 or len(all_data) >= total_records > 0:
                pd.DataFrame(all_data, columns=headers_list).to_csv(
                    csv_path, index=False
                )
                print(f"  CSV checkpoint → {csv_path}")

            if total_records > 0 and len(all_data) >= total_records:
                print("\nAll records collected.")
                break

            # ── Click Next ─────────────────────────────────────────────────────
            if not _click_next(driver):
                print("\nLast page reached.")
                break

            # Wait for the table to update after clicking Next.
            # The page content changes via AJAX — we wait until the
            # first row text changes before reading the new HTML.
            time.sleep(1)
            try:
                first_row_sel = "table.wpDataTable tbody tr:first-child td:first-child"
                old_text = driver.find_element(By.CSS_SELECTOR, first_row_sel).text
                for _ in range(20):   # wait up to 10s
                    time.sleep(0.5)
                    new_text = driver.find_element(By.CSS_SELECTOR, first_row_sel).text
                    if new_text != old_text:
                        break
            except Exception:
                time.sleep(3)

    except KeyboardInterrupt:
        print(f"\n\nStopped by user at page {page_count}.")
        print(f"Data already saved: {total_written:,} rows in DB, {len(all_data):,} rows in CSV.")

    except Exception as exc:
        print(f"\nError: {exc}")
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("Browser closed.")
        await conn.close()
        print("Database connection closed.")

    # Final CSV save
    if all_data and headers_list:
        final_csv = f"gse_complete_{ts_start}.csv"
        pd.DataFrame(all_data, columns=headers_list).to_csv(final_csv, index=False)
        print(f"\nFinal CSV saved : {final_csv}")

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    mins, secs = divmod(int(elapsed), 60)
    print(f"Total DB rows   : {total_written:,}")
    print(f"Total scraped   : {len(all_data):,}")
    print(f"Time elapsed    : {mins}m {secs}s")
    print(f"\nVerify in psql:")
    print(f"  SELECT COUNT(*) FROM {DB_SCHEMA}.daily_prices;")
    print(f"  SELECT trade_date, share_code, closing_price_vwap")
    print(f"  FROM {DB_SCHEMA}.daily_prices ORDER BY trade_date DESC LIMIT 10;")


if __name__ == "__main__":
    asyncio.run(run_scraper())