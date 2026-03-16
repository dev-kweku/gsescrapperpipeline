"""
Application Settings — loaded from environment / .env file.
All configuration validated at startup via Pydantic.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=False,   # allow runtime mutation for session refresh endpoint
    )

    # ── App ────────────────────────────────────────────────────────────
    app_name: str    = "GSE Stock Scraper — ISEDAN"
    app_version: str = "2.0.0"
    environment: Literal["development", "staging", "production"] = "production"
    debug: bool      = False
    api_prefix: str  = "/api/v1"
    allowed_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
    )

    # ── Database ───────────────────────────────────────────────────────
    db_host:     str  = "localhost"
    db_port:     int  = 5432
    db_name:     str  = "gse_stocks"
    db_user:     str  = "gse_user"
    db_password: str  = Field(default="", description="Required — set via DB_PASSWORD env var or .env file")

    @field_validator("db_password")
    @classmethod
    def db_password_must_be_set(cls, v: str) -> str:
        if not v:
            raise ValueError(
                "DB_PASSWORD is required. Set it in your .env file or as an environment variable."
            )
        return v
    db_schema:   str  = "gse"
    db_pool_min: int  = 5
    db_pool_max: int  = 20
    db_ssl:      bool = False

    @computed_field
    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @computed_field
    @property
    def sync_database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # ── Redis ──────────────────────────────────────────────────────────
    redis_url:     str  = "redis://localhost:6379/0"
    cache_ttl_s:   int  = 300
    cache_enabled: bool = True

    # ── Scraper ────────────────────────────────────────────────────────
    # Browser session override — copy from Chrome DevTools after visiting
    # https://gse.com.gh/market-data/ when Cloudflare blocks automated fetches.
    gse_nonce:      str | None = None   # wdtNonce value from browser request payload
    gse_cookies:    str | None = None   # full Cookie header string from browser
    gse_user_agent: str | None = None   # exact browser User-Agent (must match cf_clearance)

    gse_ajax_url:        str   = "https://gse.com.gh/wp-admin/admin-ajax.php"
    gse_page_url:        str   = "https://gse.com.gh/market-data/"
    gse_table_id:        str   = "39"
    scraper_batch_size:  int   = 25   # GSE DataTable default page size
    scraper_concurrency: int   = 4
    scraper_timeout_s:   int   = 30
    scraper_max_retries: int   = 5
    scraper_retry_delay_s: float = 3.0
    scraper_user_agent:  str   = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    # ── Scheduler ─────────────────────────────────────────────────────
    scheduler_hour:     int = 18
    scheduler_minute:   int = 0
    scheduler_timezone: str = "Africa/Accra"

    # ── Holidays ──────────────────────────────────────────────────────
    holiday_country_code: str = "GH"

    # ── Alerting ──────────────────────────────────────────────────────
    alert_email:       str | None = None
    smtp_host:         str | None = None
    smtp_port:         int        = 587
    smtp_user:         str | None = None
    smtp_pass:         str | None = None
    slack_webhook_url: str | None = None

    # ── Logging ───────────────────────────────────────────────────────
    log_level: str  = "INFO"
    log_json:  bool = True
    log_dir:   str  = "logs"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        return v.upper()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# Explicit type annotation so Pylance resolves all attribute accesses correctly.
# Without this, lru_cache wrapping causes Pylance to infer settings as Unknown.
settings: Settings = get_settings()