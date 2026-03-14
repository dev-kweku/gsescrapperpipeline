"""Alerting — async email (aiosmtplib) + Slack webhook."""

from __future__ import annotations

import httpx

from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)


async def _send_slack(text: str, color: str = "#36a64f") -> None:
    url = settings.slack_webhook_url
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(url, json={
                "attachments": [{
                    "color": color,
                    "text": text,
                    "footer": "GSE Scraper — FrimpTradingData",
                }]
            })
    except Exception as exc:
        log.warning("slack_alert_failed", error=str(exc))


async def _send_email(subject: str, body: str) -> None:
    if not (settings.smtp_host and settings.alert_email):
        return
    try:
        import aiosmtplib
        from email.message import EmailMessage
        msg = EmailMessage()
        msg["From"]    = settings.smtp_user or "frimpTradingdata"
        msg["To"]      = settings.alert_email
        msg["Subject"] = f"[GSE Scraper] {subject}"
        msg.set_content(body)
        await aiosmtplib.send(
            msg,
            hostname=settings.smtp_host,
            port=settings.smtp_port,
            username=settings.smtp_user,
            password=settings.smtp_pass,
            start_tls=True,
        )
    except Exception as exc:
        log.warning("email_alert_failed", error=str(exc))


async def send_success_alert(date: str, inserted: int, updated: int, duration_s: float) -> None:
    msg = f"✅ GSE scrape SUCCESS\nDate: {date}\nInserted: {inserted} | Updated: {updated}\nDuration: {duration_s:.1f}s"
    import asyncio
    await asyncio.gather(
        _send_slack(msg, "#36a64f"),
        _send_email(f"Scrape SUCCESS – {date}", msg),
        return_exceptions=True,
    )


async def send_failure_alert(exc: Exception, **ctx) -> None:
    msg = f"❌ GSE scrape FAILED\nDate: {ctx.get('date', 'unknown')}\nMode: {ctx.get('mode', '-')}\nError: {exc}"
    import asyncio
    await asyncio.gather(
        _send_slack(msg, "#ff0000"),
        _send_email(f"Scrape FAILED – {ctx.get('date', 'error')}", msg),
        return_exceptions=True,
    )