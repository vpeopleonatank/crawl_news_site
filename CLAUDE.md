# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a web crawler and article ingestion pipeline for Thanhnien.vn (Vietnamese news site). The system fetches article metadata from sitemaps, downloads HTML content, parses structured data, and stores everything in PostgreSQL with associated media assets (images/videos) persisted to disk.

**Tech Stack:**
- SQLAlchemy 2.0 with PostgreSQL 16
- Celery for async task processing (asset downloads)
- RabbitMQ as message broker
- httpx for HTTP client operations
- BeautifulSoup4 for HTML parsing
- Optional Playwright for HLS video manifest resolution
- Docker Compose for local development

## Database Models

Located in `models.py`:
- **Article**: Main content entity with UUIDv7 primary keys, title, content, category, tags, JSONB comments field, publish date
- **ArticleImage/ArticleVideo**: Related assets with CASCADE delete, ordered by `sequence_number`
- All models use `uuid_utils.uuid7()` for time-sortable IDs
- Unicode fully supported (Vietnamese, Chinese, emoji)

## Architecture

### Ingestion Flow
1. **Job Loader** (`crawler/jobs.py`): Reads NDJSON from `data/thanhnien_jobs.ndjson`, deduplicates against existing URLs in DB
2. **HTTP Fetcher** (`crawler/http_client.py`): Downloads HTML with retry/backoff, proxy rotation support
3. **Parser** (`crawler/parsers/thanhnien.py`): Extracts title, description, content blocks, tags, category, publish date, embedded media URLs
4. **Persistence** (`crawler/persistence.py`): Upserts article metadata via SQLAlchemy; returns article UUID
5. **Asset Pipeline** (`crawler/assets.py` + `crawler/tasks.py`): Enqueues Celery job for async media downloads, then persists file paths to DB

### Celery Architecture
- **Broker**: RabbitMQ (`amqp://crawler:crawler_password@rabbitmq:5672//`)
- **Result Backend**: PostgreSQL via SQLAlchemy DB URL
- **Tasks**: `crawler.download_assets` downloads media assets, stores to `storage/articles/{uuid}/images|videos/{seq}.ext`
- Toggle eager mode via `CRAWLER_CELERY_TASK_ALWAYS_EAGER` (default: False in Docker, True otherwise)

### Storage Layout
```
storage/
├── raw/{article_uuid}.html          # Optional raw HTML cache
├── articles/{article_uuid}/
│   ├── images/{001,002,...}.{jpg,png,webp}
│   └── videos/{001,002,...}.{mp4,ts}
└── logs/
    └── fetch_failures.ndjson
```

## Development Commands

### Environment Setup
```bash
# Copy and configure environment
cp .env.sample .env

# Start all services (PostgreSQL, RabbitMQ, pgAdmin)
docker compose up -d

# Stop and clean
docker compose down
docker compose down -v  # also removes volumes
```

### Database Operations
```bash
# Run model smoke tests
docker compose run --rm test_app python test_models.py

# Direct psql access
docker compose exec postgres psql -U crawl_user -d crawl_db

# pgAdmin web UI
# http://localhost:5050 (admin@admin.com / admin)
```

### Running Ingestion
```bash
# Standard ingestion with Celery worker
docker compose run --rm test_app \
  python -m crawler.ingest_thanhnien \
    --jobs-file data/thanhnien_jobs.ndjson \
    --storage-root /app/storage \
    --max-workers 4 \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db

# With resume (skip existing URLs)
docker compose run --rm test_app \
  python -m crawler.ingest_thanhnien \
    --jobs-file data/thanhnien_jobs.ndjson \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --resume

# Enable Playwright for HLS video manifest resolution
docker compose run --rm test_app \
  python -m crawler.ingest_thanhnien \
    --jobs-file data/thanhnien_jobs.ndjson \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --use-playwright

# With proxy rotation
docker compose run --rm test_app \
  python -m crawler.ingest_thanhnien \
    --jobs-file data/thanhnien_jobs.ndjson \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --proxy 192.168.1.100:8080:apikey \
    --proxy-change-url http://192.168.1.100:8080/rotate \
    --proxy-rotation-interval 240
```

### Celery Worker Management
```bash
# Start Celery worker
docker compose run --rm test_app \
  python -m celery -A crawler.celery_app worker --loglevel=info

# Clear pending tasks in RabbitMQ queue
docker compose exec rabbitmq rabbitmqctl purge_queue celery

# Monitor RabbitMQ via web UI
# http://localhost:15672 (crawler / crawler_password)
```

### Testing
```bash
# Run all tests (requires DATABASE_URL set)
export DATABASE_URL=postgresql://crawl_user:crawl_password@localhost:5433/crawl_db
python -m unittest discover

# Run specific test file
python -m unittest tests.test_http_client
python -m unittest tests.parsers.test_thanhnien_parser

# Run with Docker
docker compose run --rm test_app python -m unittest discover
```

## Configuration

### Environment Variables
Primary config via `.env` (see `.env.sample`):
- `DATABASE_URL`: SQLAlchemy connection string for app
- `CRAWLER_DATABASE_URL`: SQLAlchemy connection for Celery workers
- `CRAWLER_CELERY_BROKER_URL`: RabbitMQ AMQP URL
- `CRAWLER_CELERY_RESULT_BACKEND`: Result storage (`db+postgresql://...`)
- `CRAWLER_CELERY_TASK_ALWAYS_EAGER`: Set to `False` for async execution

### CLI Configuration (`crawler/config.py`)
Encapsulated in `IngestConfig` dataclass:
- Rate limiting: `max_workers`, `per_domain_delay`
- Retry: `max_attempts`, `backoff_factor`
- Timeouts: `request_timeout`, `asset_timeout`
- Proxy: scheme/host/port, rotation API, key, interval
- Storage paths, logging

Override via CLI flags (see `crawler/ingest_thanhnien.py` argparse setup)

## Key Implementation Notes

### UUIDv7 Generation
All entities use time-ordered UUIIDv7 via `uuid_utils.uuid7()` in `models.py:10`. Converter function ensures compatibility with standard `uuid.UUID`.

### Parser Determinism
`ThanhnienParser` extracts data deterministically from HTML. Unit tests in `tests/parsers/test_thanhnien_parser.py` use saved fixtures. CSS selectors target multiple patterns for resilience to site structure changes.

### Asset Sequencing
Assets maintain insertion order via `sequence_number` in DB. Parser returns ordered list; `ensure_asset_sequence()` validates continuity.

### Proxy Rotation
HTTP client supports rotating proxies via `ProxyConfig.change_ip_url`. Throttles rotation calls to `min_rotation_interval` (default 240s). API key passed via query param or config.

### Playwright Integration
Optional HLS manifest resolution when `--use-playwright` flag set. Launches headless browser to intercept video network requests, extracts m3u8 URLs, updates asset payload before download. Timeout configurable via `--playwright-timeout`.

### Idempotency
- Jobs keyed by `Article.url` (unique constraint)
- Resume mode loads existing URLs from DB and skips them
- Asset downloads use deterministic paths; re-running overwrites existing files

### Error Handling
- Fetch failures logged to `storage/logs/fetch_failures.ndjson` with sitemap URL, error type, timestamp
- Asset download failures trigger Celery task retry with exponential backoff
- Ingestion process returns exit code 1 if any jobs failed

## Service Details

### PostgreSQL (postgres:16-alpine)
- Port: 5433 (host) → 5432 (container)
- Credentials: crawl_user / crawl_password
- DB: crawl_db
- Volume: postgres_data (persistent)
- Init script: `init.sql` (if present)

### RabbitMQ (rabbitmq:3.13-management-alpine)
- AMQP port: 5672
- Management UI: 15672 (crawler / crawler_password)
- Volume: rabbitmq_data (persistent)

### pgAdmin (dpage/pgadmin4)
- Port: 5050
- Credentials: admin@admin.com / admin
- Volume: pgadmin_data (persistent)

## Common Workflows

### Adding a New Parser Field
1. Add column to `models.Article` in `models.py`
2. Run Alembic migration (if using; currently direct DDL)
3. Update `ParsedArticle` dataclass in `crawler/parsers/__init__.py`
4. Implement extraction in `ThanhnienParser.parse()` (`crawler/parsers/thanhnien.py`)
5. Update `ArticlePersistence.upsert_metadata()` to map new field (`crawler/persistence.py`)
6. Add test case to `tests/parsers/test_thanhnien_parser.py` with fixture

### Debugging Failed Fetches
1. Check `storage/logs/fetch_failures.ndjson` for error details
2. If `--raw-html-cache` was enabled, inspect `storage/raw/{uuid}.html`
3. Re-run with `--resume` to skip successful URLs
4. Use `docker compose logs -f test_app` to see live ingestion output

### Monitoring Celery Queue Depth
1. Access RabbitMQ management UI: http://localhost:15672
2. Check "celery" queue under Queues tab
3. If backlog is large, scale workers or increase `max_workers` in ingestion CLI

### Clearing Stale Queue State
```bash
# Purge all pending tasks
docker compose exec rabbitmq rabbitmqctl purge_queue celery

# Reset persistent volumes if needed
docker compose down -v
docker compose up -d
```

## Project Structure Highlights

- `models.py`: SQLAlchemy ORM models
- `crawler/`:
  - `ingest_thanhnien.py`: CLI entrypoint and orchestration loop
  - `jobs.py`: NDJSON loader, dedupe logic
  - `http_client.py`: httpx wrapper with retry/proxy
  - `parsers/thanhnien.py`: HTML extraction logic
  - `persistence.py`: DB session management, upsert operations
  - `assets.py`: Asset serialization, download manager
  - `tasks.py`: Celery task definitions
  - `celery_app.py`: Celery app factory
  - `config.py`: Configuration dataclasses
  - `playwright_support.py`: HLS manifest resolver
- `tests/`: Unit and integration tests (unittest framework)
- `data/thanhnien_jobs.ndjson`: Input job queue (one JSON object per line)
- `storage/`: Runtime storage (bind-mounted in Docker)

## External Resources

See detailed design document: `docs/thanhnien_ingestion_design.md`
