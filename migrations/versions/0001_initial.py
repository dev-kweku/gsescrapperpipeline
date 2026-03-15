"""Initial GSE schema

Revision ID: 0001_initial
Revises:
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS gse")
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    # Drop enum if it exists from a previous partial run, then recreate cleanly
    op.execute("DROP TYPE IF EXISTS gse.scrapestatus CASCADE")
    op.execute("""
        CREATE TYPE gse.scrapestatus AS ENUM
            ('pending','running','completed','failed','skipped')
    """)

    # stock_listings
    op.create_table("stock_listings",
        sa.Column("id",            sa.Integer,  primary_key=True, autoincrement=True),
        sa.Column("share_code",    sa.String(20),  nullable=False, unique=True),
        sa.Column("company_name",  sa.String(255)),
        sa.Column("sector",        sa.String(100)),
        sa.Column("is_active",     sa.Boolean,  nullable=False, server_default="true"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("last_seen_at",  sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("created_at",    sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at",    sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        schema="gse",
    )
    op.create_index("ix_stock_listings_share_code", "stock_listings", ["share_code"], schema="gse")
    op.create_index("ix_stock_listings_is_active",  "stock_listings", ["is_active"],  schema="gse")

    # daily_prices
    op.create_table("daily_prices",
        sa.Column("id",         sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("wdt_id",     sa.Integer),
        sa.Column("trade_date", sa.Date,       nullable=False),
        sa.Column("share_code", sa.String(20),
                  sa.ForeignKey("gse.stock_listings.share_code"), nullable=False),
        sa.Column("year_high",                   sa.Numeric(18,4)),
        sa.Column("year_low",                    sa.Numeric(18,4)),
        sa.Column("previous_closing_price_vwap", sa.Numeric(18,4)),
        sa.Column("opening_price",               sa.Numeric(18,4)),
        sa.Column("last_transaction_price",      sa.Numeric(18,4)),
        sa.Column("closing_price_vwap",          sa.Numeric(18,4)),
        sa.Column("price_change",                sa.Numeric(18,4)),
        sa.Column("closing_bid_price",           sa.Numeric(18,4)),
        sa.Column("closing_offer_price",         sa.Numeric(18,4)),
        sa.Column("total_shares_traded",         sa.BigInteger),
        sa.Column("total_value_traded",          sa.Numeric(20,4)),
        sa.Column("raw_data",                    postgresql.JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.UniqueConstraint("trade_date", "share_code", name="uq_daily_prices_date_code"),
        schema="gse",
    )
    op.create_index("ix_daily_prices_trade_date", "daily_prices", ["trade_date"], schema="gse")
    op.create_index("ix_daily_prices_share_code", "daily_prices", ["share_code"], schema="gse")
    op.create_index("ix_daily_prices_date_code",  "daily_prices", ["trade_date","share_code"], schema="gse")

    # scrape_jobs — use sa.Text for status to avoid SQLAlchemy auto-creating the enum
    op.create_table("scrape_jobs",
        sa.Column("id",       sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("job_id",   postgresql.UUID(as_uuid=True), nullable=False, unique=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("job_type", sa.String(50),  nullable=False, server_default="daily"),
        sa.Column("status",   sa.Text,        nullable=False, server_default="pending"),
        sa.Column("target_date",      sa.Date),
        sa.Column("total_pages",      sa.Integer, server_default="0"),
        sa.Column("total_records",    sa.Integer, server_default="0"),
        sa.Column("records_inserted", sa.Integer, server_default="0"),
        sa.Column("records_updated",  sa.Integer, server_default="0"),
        sa.Column("records_skipped",  sa.Integer, server_default="0"),
        sa.Column("error_message",    sa.Text),
        sa.Column("error_stack",      sa.Text),
        sa.Column("metadata",         postgresql.JSONB, server_default="{}"),
        sa.Column("started_at",       sa.DateTime(timezone=True)),
        sa.Column("completed_at",     sa.DateTime(timezone=True)),
        sa.Column("created_at",       sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        schema="gse",
    )
    # Add CHECK constraint to enforce valid status values without using the ENUM type
    op.execute("""
        ALTER TABLE gse.scrape_jobs
        ADD CONSTRAINT ck_scrape_jobs_status
        CHECK (status IN ('pending','running','completed','failed','skipped'))
    """)
    op.create_index("ix_scrape_jobs_job_id",      "scrape_jobs", ["job_id"],      schema="gse")
    op.create_index("ix_scrape_jobs_status",       "scrape_jobs", ["status"],      schema="gse")
    op.create_index("ix_scrape_jobs_target_date",  "scrape_jobs", ["target_date"], schema="gse")
    op.create_index("ix_scrape_jobs_created_at",   "scrape_jobs", ["created_at"],  schema="gse")

    # scrape_errors
    op.create_table("scrape_errors",
        sa.Column("id",         sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("job_id",     postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("gse.scrape_jobs.job_id", ondelete="CASCADE")),
        sa.Column("error_type", sa.String(100)),
        sa.Column("error_msg",  sa.Text),
        sa.Column("raw_row",    postgresql.JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        schema="gse",
    )
    op.create_index("ix_scrape_errors_job_id", "scrape_errors", ["job_id"], schema="gse")

    # ghana_public_holidays
    op.create_table("ghana_public_holidays",
        sa.Column("id",           sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("holiday_date", sa.Date,        nullable=False, unique=True),
        sa.Column("name",         sa.String(255),  nullable=False),
        sa.Column("local_name",   sa.String(255)),
        sa.Column("source",       sa.String(100),  server_default="holidays-lib"),
        sa.Column("created_at",   sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        schema="gse",
    )
    op.create_index("ix_ghana_holidays_date", "ghana_public_holidays", ["holiday_date"], schema="gse")

    # Views
    op.execute("""
        CREATE OR REPLACE VIEW gse.v_latest_prices AS
        SELECT DISTINCT ON (share_code)
            dp.*, sl.company_name, sl.sector
        FROM gse.daily_prices dp
        LEFT JOIN gse.stock_listings sl ON sl.share_code = dp.share_code
        ORDER BY share_code, trade_date DESC
    """)

    op.execute("""
        CREATE OR REPLACE VIEW gse.v_market_summary AS
        SELECT DISTINCT ON (dp.share_code)
            dp.share_code,
            sl.company_name,
            dp.trade_date,
            dp.closing_price_vwap,
            dp.price_change,
            CASE WHEN dp.previous_closing_price_vwap > 0
                 THEN ROUND((dp.price_change / dp.previous_closing_price_vwap) * 100, 2)
                 ELSE NULL END AS pct_change,
            dp.total_shares_traded,
            dp.total_value_traded
        FROM gse.daily_prices dp
        LEFT JOIN gse.stock_listings sl ON sl.share_code = dp.share_code
        ORDER BY dp.share_code, dp.trade_date DESC
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gse.v_market_summary")
    op.execute("DROP VIEW IF EXISTS gse.v_latest_prices")
    op.drop_table("ghana_public_holidays", schema="gse")
    op.drop_table("scrape_errors",         schema="gse")
    op.drop_table("scrape_jobs",           schema="gse")
    op.drop_table("daily_prices",          schema="gse")
    op.drop_table("stock_listings",        schema="gse")
    op.execute("DROP TYPE IF EXISTS gse.scrapestatus CASCADE")
    op.execute("DROP SCHEMA IF EXISTS gse CASCADE")