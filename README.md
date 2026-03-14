# 🇬🇭 GSE Stock Scraper — Python/FastAPI Edition (FrimpTradingData)

Production-grade async data pipeline for the **Ghana Stock Exchange**, rebuilt in Python with FastAPI, asyncpg, APScheduler, and Redis caching — optimised for the **FrimpTradingData** application.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     FrimpTradingData — GSE Data Platform                        │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐  │
│   │                      FastAPI Application                        │  │
│   │                                                                  │  │
│   │  GET /api/v1/market/prices/latest     ←── Redis Cache (5min)    │  │
│   │  GET /api/v1/market/stocks/{code}/history                        │  │
│   │  GET /api/v1/market/summary/{date}   ←── gainers/losers/volume  │  │
│   │  POST /api/v1/scraper/trigger         ←── manual trigger        │  │
│   │  GET  /health                                                     │  │
│   └────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│          ┌───────────────────┼────────────────┐                        │
│          ▼                   ▼                ▼                        │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│   │  APScheduler  │  │   Pipeline   │  │ Redis Cache  │               │
│   │  Mon-Fri 18h  │  │ Orchestrator │  │  (hiredis)   │               │
│   └──────┬───────┘  └──────┬───────┘  └──────────────┘               │
│          └────────┬─────────┘                                          │
│                   ▼                                                     │
│   ┌──────────────────────────────────────────────┐                    │
│   │              GSE Scraper (httpx/async)        │                    │
│   │  • HTTP/2 client with connection pooling      │                    │
│   │  • asyncio.gather for concurrent page fetch   │                    │
│   │  • Semaphore-controlled concurrency           │                    │
│   │  • Tenacity exponential back-off retries      │                    │
│   │  • Pre-compiled regex parsers                 │                    │
│   └──────────────────────────────────────────────┘                    │
│                   ▼                                                     │
│   ┌──────────────────────────────────────────────┐                    │
│   │           PostgreSQL (asyncpg)                │                    │
│   │  • UNNEST bulk upsert (fastest path)          │                    │
│   │  • Connection pool (min=5, max=20)            │                    │
│   │  • gse schema, indexed, with views            │                    │
│   └──────────────────────────────────────────────┘                    │
└──────────────────────────────────────────────────────────────────────┘
```

## Performance Optimisations

| Concern | Approach |
|---------|----------|
| HTTP scraping | httpx with HTTP/2 + keep-alive |
| Concurrent pages | `asyncio.gather` + semaphore (default concurrency=4) |
| Retries | Tenacity exponential back-off (3s→6s→12s→…) |
| DB writes | PostgreSQL UNNEST bulk upsert — single round-trip per 200 rows |
| DB pool | asyncpg pool, min=5, max=20, pool_pre_ping=True |
| API responses | orjson serialisation (2–3× faster than stdlib json) |
| API caching | Redis with per-endpoint TTLs (5min latest, 15min summary) |
| Parsing | Pre-compiled regex, no per-row recompilation |
| Holidays | python-holidays library (no external API needed) |

---

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 14+
- Redis 7+

### 1. Install

```bash
git clone <repo> gse-python && cd gse-python
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

### 2. Configure

```bash
cp .env.example .env
# Fill in DB_PASSWORD and other values
```

### 3. Run migrations

```bash
alembic upgrade head
```

### 4. Start the API server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Open docs at: http://localhost:8000/docs

---

## API Reference

### Market Data

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/market/prices/latest` | Latest price per stock |
| GET | `/api/v1/market/prices/date/{YYYY-MM-DD}` | All prices for a date |
| GET | `/api/v1/market/stocks/{code}/history` | Price history (+ date range params) |
| GET | `/api/v1/market/summary/{YYYY-MM-DD}` | Gainers / losers / most active |
| GET | `/api/v1/market/stocks` | All active stock listings |
| GET | `/api/v1/market/stats` | DB statistics |
| GET | `/api/v1/market/trading-days` | Trading day calendar |

### Scraper Management

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/scraper/trigger` | Trigger a scrape (see body below) |
| POST | `/api/v1/scraper/trigger/historical` | Full historical scrape (background) |
| GET  | `/api/v1/scraper/jobs` | Recent scrape job history |
| GET  | `/api/v1/scraper/scheduler` | Scheduler status + next run |
| POST | `/api/v1/scraper/scheduler/run-now` | Force immediate run |

#### Trigger body
```json
{
  "target_date": "2024-03-06",
  "mode": "manual",
  "force": false,
  "dry_run": false
}
```

### Health
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Full health (DB + scheduler) |
| GET | `/ping` | Liveness probe |

---

## Docker Compose

```bash
cp .env.example .env  # fill DB_PASSWORD

# Development (includes pgAdmin at :5050)
docker-compose --profile dev up -d

# Production (includes Nginx)
docker-compose --profile production up -d

# View logs
docker-compose logs -f app
```

---

## Scheduler

Fires **Mon–Fri at 18:00 Africa/Accra (UTC+0)** via APScheduler's `CronTrigger`. Configure in `.env`:

```ini
SCHEDULER_HOUR=18
SCHEDULER_MINUTE=0
SCHEDULER_TIMEZONE=Africa/Accra
```

Holiday detection uses the `holidays` Python library (no external API needed), supplemented by the DB cache.

---

## Testing

```bash
pytest tests/ -v --cov=app
```

---

## Database Schema

All tables live in the `gse` schema:

- `gse.daily_prices` — core price records (UNIQUE: trade_date + share_code)
- `gse.stock_listings` — master stock registry
- `gse.scrape_jobs` — audit log for every run
- `gse.scrape_errors` — per-record error log
- `gse.ghana_public_holidays` — holiday cache
- `gse.v_latest_prices` — VIEW: latest price per stock
- `gse.v_market_summary` — VIEW: summary with % change
