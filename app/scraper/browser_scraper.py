"""
Browser-based GSE scraper using Playwright.
Intercepts the network request the DataTables plugin makes naturally.
This is 100% indistinguishable from a real user — we let the page make
its own requests and capture the responses.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from app.core.config  import settings
from app.core.logging import get_logger
from app.scraper.gse_scraper import (
    ScrapeResult,
    _filter_records,
    _process_rows,
    _FALLBACK_NONCE,
    COLUMNS,
)

log = get_logger(__name__)


async def scrape_gse_browser(
    mode:        str        = "latest",
    target_date: str | None = None,
) -> ScrapeResult:
    """
    Scrape GSE by loading the real page in Chromium and intercepting
    the DataTables AJAX requests the page makes naturally.

    Strategy:
    1. Load gse.com.gh/market-data/ in headless Chrome
    2. Intercept every POST to admin-ajax.php
    3. Capture the JSON response for each page
    4. Click 'Next' to trigger subsequent pages OR manipulate DataTables API
    """
    try:
        from playwright.async_api import async_playwright, Response
    except ImportError:
        raise RuntimeError(
            "Playwright not installed. Run: pip install playwright && playwright install chromium"
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = await browser.new_context(
            user_agent=settings.gse_user_agent or settings.scraper_user_agent,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Accra",
        )

        page = await context.new_page()

        # ── Intercept DataTables AJAX responses ────────────────────────────────
        captured_pages: list[dict[str, Any]] = []

        async def handle_response(response: Any) -> None:
            if "admin-ajax.php" in response.url and response.status == 200:
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        if isinstance(data, dict) and "data" in data:
                            captured_pages.append(data)
                            log.info(
                                "intercepted_ajax_page",
                                records=len(data.get("data", [])),
                                total=data.get("recordsTotal", 0),
                                captured=len(captured_pages),
                            )
                except Exception as exc:
                    log.warning("intercept_parse_error", error=str(exc))

        page.on("response", handle_response)

        # ── Load the page ──────────────────────────────────────────────────────
        log.info("browser_scraper_loading_page")
        try:
            await page.goto(
                settings.gse_page_url,
                wait_until="domcontentloaded",
                timeout=90000,
            )
        except Exception as exc:
            log.warning("browser_page_load_partial", error=str(exc))

        # Wait for the first DataTables response to be captured
        log.info("browser_waiting_for_datatable")
        for _ in range(30):   # wait up to 30s
            if captured_pages:
                break
            await asyncio.sleep(1)

        if not captured_pages:
            log.error("browser_no_datatable_response")
            await browser.close()
            raise RuntimeError(
                "No DataTables AJAX response captured after 30s. "
                "The page may not have loaded the table."
            )

        first_page   = captured_pages[0]
        total_remote = int(first_page.get("recordsTotal", 0))
        batch        = int(first_page.get("recordsFiltered", len(first_page.get("data", []))))
        if batch == 0:
            batch = settings.scraper_batch_size
        total_pages  = max(1, -(-total_remote // batch))

        log.info(
            "browser_scrape_plan",
            total_remote=total_remote,
            total_pages=total_pages,
            batch=batch,
        )

        # ── Navigate remaining pages via DataTables JS API ────────────────────
        if total_pages > 1:
            # Get nonce from page for re-use
            nonce = _FALLBACK_NONCE
            try:
                extracted = await page.evaluate(r"""() => {
                    if (typeof wdtNonce !== 'undefined') return String(wdtNonce);
                    return null;
                }""")
                if extracted:
                    nonce = str(extracted)
            except Exception:
                pass

            # Use DataTables API to navigate pages — the page makes the requests
            for page_idx in range(1, total_pages):
                before_count = len(captured_pages)
                try:
                    # Navigate DataTables to next page using its own API
                    await page.evaluate(f"""() => {{
                        const tables = jQuery ? jQuery('.wdt-table').DataTable ? jQuery('.wdt-table') : null : null;
                        if (tables && tables.length) {{
                            tables.DataTable().page({page_idx}).draw(false);
                            return true;
                        }}
                        // Fallback: click the Next button
                        const next = document.querySelector('.dataTables_paginate .next:not(.disabled)');
                        if (next) {{ next.click(); return true; }}
                        return false;
                    }}""")
                except Exception as exc:
                    log.warning("datatable_navigate_error", page=page_idx + 1, error=str(exc))

                # Wait for the new page response to be captured
                for _ in range(15):
                    if len(captured_pages) > before_count:
                        break
                    await asyncio.sleep(1)

                log.info(
                    "browser_scrape_page",
                    page=page_idx + 1,
                    total=total_pages,
                    captured=len(captured_pages),
                )
                await asyncio.sleep(0.5)

        await browser.close()

        # ── Process all captured pages ─────────────────────────────────────────
        all_records: list[Any] = []
        all_errors:  list[Any] = []

        for page_data in captured_pages:
            _process_rows(page_data.get("data", []), all_records, all_errors)

        log.info(
            "browser_scrape_complete",
            pages_captured=len(captured_pages),
            parsed=len(all_records),
            errors=len(all_errors),
        )

        return ScrapeResult(
            records=_filter_records(all_records, mode, target_date),
            errors=all_errors,
            total_remote_records=total_remote,
            total_pages=total_pages,
            parsed_count=len(all_records),
        )