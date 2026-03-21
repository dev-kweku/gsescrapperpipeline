"""
GSE Historical Data — Excel Download + PostgreSQL Import
=========================================================
Sets date filter via Bootstrap datetimepicker, downloads Excel, imports to DB.

Usage:
    python gse_excel_downloader.py --from-date 2020-01-01 --to-date 2026-03-19
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import asyncpg
import pandas as pd
from dotenv import load_dotenv
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException

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

DOWNLOAD_DIR = str(Path.home() / "Downloads" / "gse_data")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ── Helpers ────────────────────────────────────────────────────────────────────
def month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    ranges, cur = [], start.replace(day=1)
    while cur <= end:
        last = (cur.replace(month=cur.month % 12 + 1, day=1) - timedelta(days=1)
                if cur.month < 12 else cur.replace(day=31))
        ranges.append((cur, min(last, end)))
        cur = (cur.replace(month=cur.month % 12 + 1, day=1)
               if cur.month < 12 else cur.replace(year=cur.year + 1, month=1, day=1))
    return ranges


def _dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    s = re.sub(r"[^\d.\-]", "", str(v)).strip()
    if not s or s == "-":
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _int(v: Any) -> int | None:
    d = _dec(v)
    return int(d) if d is not None else None


# ── Selenium ───────────────────────────────────────────────────────────────────
def setup_driver() -> ChromeDriver:
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
    prefs = {
        "download.default_directory":  DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         True,
    }
    options.add_experimental_option("prefs", prefs)
    from webdriver_manager.chrome import ChromeDriverManager
    service = Service(ChromeDriverManager().install())
    driver  = ChromeDriver(service=service, options=options)
    print("ChromeDriver ready")
    return driver


def clear_filter(driver: ChromeDriver) -> None:
    """Click Clear filters button to reset table."""
    try:
        btn = driver.find_element(By.CSS_SELECTOR, ".wdt-clear-filters-button")
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(3)
    except Exception:
        pass


def set_date_range_via_keyboard(driver: ChromeDriver, from_date: date, to_date: date) -> int:
    """
    Type dates directly into the filter inputs using Selenium keyboard input.
    This is more reliable than JS .value = because it triggers the datetimepicker
    internal event listeners naturally.
    Returns the number of filtered records.
    """
    from_str = from_date.strftime("%d/%m/%Y")
    to_str   = to_date.strftime("%d/%m/%Y")

    # First clear existing filter
    clear_filter(driver)

    # Find the two date inputs inside #table_1_1_filter
    inputs = driver.find_elements(
        By.CSS_SELECTOR, "#table_1_1_filter input"
    )

    if not inputs:
        print("  No date inputs found in filter")
        return 0

    print(f"  Found {len(inputs)} date inputs")

    # Type into FROM input (first input)
    from_input = inputs[0]
    from_input.click()
    time.sleep(0.5)
    # Select all and delete existing value
    from_input.send_keys(Keys.CONTROL + "a")
    from_input.send_keys(Keys.DELETE)
    from_input.send_keys(from_str)
    from_input.send_keys(Keys.TAB)
    time.sleep(1)

    # Type into TO input (second input) if it exists
    if len(inputs) >= 2:
        to_input = inputs[1]
        to_input.click()
        time.sleep(0.5)
        to_input.send_keys(Keys.CONTROL + "a")
        to_input.send_keys(Keys.DELETE)
        to_input.send_keys(to_str)
        to_input.send_keys(Keys.TAB)
        time.sleep(1)

    # Wait for table to reload
    time.sleep(5)

    # Get filtered count
    count = driver.execute_script("""
        try {
            return jQuery('#table_1').DataTable().page.info().recordsDisplay;
        } catch(e) { return -1; }
    """)

    return int(count) if count else 0


def get_existing_downloads() -> set[str]:
    """Get set of current xlsx files (excluding temp files)."""
    return {
        f for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
        if not os.path.basename(f).startswith("~$")
    }


def click_excel_and_wait(driver: ChromeDriver, timeout: int = 60) -> str | None:
    """Click Excel button and wait for a NEW file to appear."""
    before = get_existing_downloads()

    # Click the Excel button
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "button.DTTT_button_xls, a.DTTT_button_xls, .buttons-excel")
        driver.execute_script("arguments[0].click();", btn)
        print("  Excel button clicked")
    except NoSuchElementException:
        # Try by text
        try:
            btn = driver.find_element(
                By.XPATH,
                "//*[contains(@class,'excel') or contains(text(),'Excel')]"
            )
            driver.execute_script("arguments[0].click();", btn)
            print("  Excel button clicked (by text)")
        except Exception as e:
            print(f"  Could not find Excel button: {e}")
            return None

    # Wait for a new file to appear
    for _ in range(timeout * 2):
        time.sleep(0.5)
        after = get_existing_downloads()
        new_files = after - before
        if new_files:
            filepath = list(new_files)[0]
            # Wait a bit more to ensure download is complete
            time.sleep(2)
            return filepath

    return None


# ── Parse Excel ────────────────────────────────────────────────────────────────
def parse_excel(filepath: str) -> list[dict]:
    try:
        df = pd.read_excel(filepath, header=0, engine='openpyxl')
    except Exception as e:
        print(f"  Excel read error: {e}")
        return []

    print(f"  Rows={len(df)}  Cols={list(df.columns[:3])}")

    records   = []
    last_date = None

    for _, row in df.iterrows():
        # Parse date
        raw_d = str(row.get("Daily Date", "") or "").strip()
        if raw_d and raw_d not in ("", "nan", "Daily Date"):
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
                try:
                    last_date = datetime.strptime(raw_d, fmt).date()
                    break
                except ValueError:
                    continue

        if not last_date:
            continue

        raw_code   = str(row.get("Share Code", "") or "").strip()
        share_code = re.sub(r"[^A-Z0-9]", "", raw_code.upper())
        if not share_code:
            continue

        def g(col: str) -> Any:
            return row.get(col)

        records.append({
            "trade_date":                  last_date,
            "share_code":                  share_code,
            "year_high":                   _dec(g("Year High (GH\u00a2)")),
            "year_low":                    _dec(g("Year Low (GH\u00a2)")),
            "previous_closing_price_vwap": _dec(g("Previous Closing Price - VWAP (GH\u00a2)")),
            "opening_price":               _dec(g("Opening Price (GH\u00a2)")),
            "last_transaction_price":      _dec(g("Last Transaction Price (GH\u00a2)")),
            "closing_price_vwap":          _dec(g("Closing Price - VWAP (GH\u00a2)")),
            "price_change":                _dec(g("Price Change (GH\u00a2)")),
            "closing_bid_price":           _dec(g("Closing Bid Price (GH\u00a2)")),
            "closing_offer_price":         _dec(g("Closing Offer Price (GH\u00a2)")),
            "total_shares_traded":         _int(g("Total Shares Traded")),
            "total_value_traded":          _dec(g("Total Value Traded (GH\u00a2)")),
        })

    return records


# ── Database ───────────────────────────────────────────────────────────────────
async def create_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.stock_listings (
            id SERIAL PRIMARY KEY, share_code VARCHAR(20) NOT NULL UNIQUE,
            is_active BOOLEAN DEFAULT true, last_seen_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.daily_prices (
            id BIGSERIAL PRIMARY KEY, trade_date DATE NOT NULL,
            share_code VARCHAR(20) NOT NULL,
            year_high NUMERIC(18,4), year_low NUMERIC(18,4),
            previous_closing_price_vwap NUMERIC(18,4), opening_price NUMERIC(18,4),
            last_transaction_price NUMERIC(18,4), closing_price_vwap NUMERIC(18,4),
            price_change NUMERIC(18,4), closing_bid_price NUMERIC(18,4),
            closing_offer_price NUMERIC(18,4), total_shares_traded BIGINT,
            total_value_traded NUMERIC(18,4),
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT uq_daily_prices UNIQUE (trade_date, share_code)
        )
    """)
    for sql in [
        f"CREATE INDEX IF NOT EXISTS idx_dp_trade_date ON {DB_SCHEMA}.daily_prices (trade_date DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_dp_share_code ON {DB_SCHEMA}.daily_prices (share_code)",
    ]:
        await conn.execute(sql)
    print("Tables ready.")


async def upsert_records(conn: asyncpg.Connection, records: list[dict]) -> int:
    if not records:
        return 0
    await conn.executemany(
        f"INSERT INTO {DB_SCHEMA}.stock_listings (share_code, is_active) "
        f"VALUES ($1, true) ON CONFLICT (share_code) DO UPDATE SET last_seen_at=NOW()",
        [(r["share_code"],) for r in records],
    )
    result = await conn.execute(f"""
        INSERT INTO {DB_SCHEMA}.daily_prices (
            trade_date, share_code, year_high, year_low,
            previous_closing_price_vwap, opening_price, last_transaction_price,
            closing_price_vwap, price_change, closing_bid_price,
            closing_offer_price, total_shares_traded, total_value_traded
        )
        SELECT * FROM UNNEST(
            $1::DATE[], $2::VARCHAR[], $3::NUMERIC[], $4::NUMERIC[],
            $5::NUMERIC[], $6::NUMERIC[], $7::NUMERIC[], $8::NUMERIC[],
            $9::NUMERIC[], $10::NUMERIC[], $11::NUMERIC[], $12::BIGINT[], $13::NUMERIC[]
        )
        ON CONFLICT (trade_date, share_code) DO UPDATE SET
            year_high=EXCLUDED.year_high, year_low=EXCLUDED.year_low,
            previous_closing_price_vwap=EXCLUDED.previous_closing_price_vwap,
            opening_price=EXCLUDED.opening_price,
            last_transaction_price=EXCLUDED.last_transaction_price,
            closing_price_vwap=EXCLUDED.closing_price_vwap,
            price_change=EXCLUDED.price_change,
            closing_bid_price=EXCLUDED.closing_bid_price,
            closing_offer_price=EXCLUDED.closing_offer_price,
            total_shares_traded=EXCLUDED.total_shares_traded,
            total_value_traded=EXCLUDED.total_value_traded,
            updated_at=NOW()
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
async def run(from_date: date, to_date: date) -> None:
    t0 = datetime.now(timezone.utc)
    total_saved = 0

    print(f"GSE Excel Downloader")
    print(f"Range    : {from_date} → {to_date}")
    print(f"Download : {DOWNLOAD_DIR}")
    print(f"Database : {DB_NAME} on {DB_HOST}\n")

    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    assert conn is not None
    await create_tables(conn)

    ranges = month_ranges(from_date, to_date)
    driver: ChromeDriver | None = None

    try:
        driver = setup_driver()
        print(f"Browser: {driver.capabilities['browserVersion']}\n")

        driver.get("https://gse.com.gh/trading-and-data/")
        time.sleep(10)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.wpDataTable"))
        )
        print("Table found.\n")

        for i, (range_from, range_to) in enumerate(ranges):
            print(f"[{i+1}/{len(ranges)}] {range_from} → {range_to}")

            # Set date filter via keyboard
            count = set_date_range_via_keyboard(driver, range_from, range_to)
            print(f"  Filtered records: {count}")

            if count == 0:
                print(f"  No data — skipping")
                continue

            # Download Excel
            filepath = click_excel_and_wait(driver, timeout=60)

            if not filepath:
                print(f"  Download failed — skipping")
                continue

            print(f"  Downloaded: {os.path.basename(filepath)}")

            # Parse and import
            records = parse_excel(filepath)
            if records:
                written      = await upsert_records(conn, records)
                total_saved += written
                dates = sorted({str(r["trade_date"]) for r in records})
                print(f"  Saved={written}  total_db={total_saved:,}  "
                      f"dates={dates[0]}→{dates[-1]}")
            else:
                print(f"  No records parsed")

            # Delete downloaded file to avoid confusion on next iteration
            try:
                os.remove(filepath)
            except Exception:
                pass

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as exc:
        print(f"\nError: {exc}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("Browser closed.")
        await conn.close()

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    mins, secs = divmod(int(elapsed), 60)
    print(f"\nTotal saved : {total_saved:,} rows")
    print(f"Time        : {mins}m {secs}s")
    print(f"\nVerify:")
    print(f"  SELECT DISTINCT trade_date FROM {DB_SCHEMA}.daily_prices ORDER BY 1 DESC LIMIT 10;")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", default="2020-01-01")
    parser.add_argument("--to-date",   default=date.today().isoformat())
    args = parser.parse_args()
    asyncio.run(run(
        date.fromisoformat(args.from_date),
        date.fromisoformat(args.to_date),
    ))