# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-site web crawler and article ingestion pipeline supporting Thanhnien.vn (Vietnamese news site). The system fetches article metadata from sitemaps, downloads HTML content, parses structured data, and stores everything in PostgreSQL with associated media assets (images/videos) persisted to disk.

**Tech Stack:**
- SQLAlchemy 2.0.43 with PostgreSQL 16
- Celery 5.3.6 for async task processing (asset downloads)
- RabbitMQ 3.13 as message broker
- httpx 0.28.1 for HTTP client operations
- BeautifulSoup4 4.14.2 for HTML parsing
- Playwright 1.55.0 for optional HLS video manifest resolution
- Docker Compose for local development
- psycopg2-binary 2.9.10 for PostgreSQL adapter
- uuid-utils 0.11.1 for UUIDv7 generation

## Database Models

Located in `models.py`:
- **Article**: Main content entity with UUIDv7 primary keys, title, content, category, tags, JSONB comments field, publish date
- **ArticleImage/ArticleVideo**: Related assets with CASCADE delete, ordered by `sequence_number`
- All models use `uuid_utils.uuid7()` for time-sortable IDs
- Unicode fully supported (Vietnamese, Chinese, emoji)

## Architecture

### Ingestion Flow
1. **Job Loader** (`crawler/jobs.py`): Reads NDJSON from site-specific jobs file or default path, deduplicates against existing URLs in DB
2. **HTTP Fetcher** (`crawler/http_client.py`): Downloads HTML with retry/backoff, proxy rotation support, custom user agent
3. **Parser** (`crawler/parsers/thanhnien.py`): Extracts title, description, content blocks, tags, category, publish date, embedded media URLs
4. **Video Resolution** (optional, `crawler/playwright_support.py`): Resolves HLS manifests via Playwright browser automation if `--use-playwright` flag set
5. **Persistence** (`crawler/persistence.py`): Upserts article metadata via SQLAlchemy; returns article UUID
6. **Asset Pipeline** (`crawler/assets.py` + `crawler/tasks.py`): Enqueues Celery job for async media downloads, then persists file paths to DB

### Celery Architecture
- **Broker**: RabbitMQ (`amqp://crawler:crawler_password@rabbitmq:5672//`)
- **Result Backend**: PostgreSQL via SQLAlchemy DB URL (`db+postgresql://...`)
- **Tasks**:
  - `download_assets_task`: Downloads media assets, stores to `storage/articles/{uuid}/images|videos/{seq}.ext`
  - `resolve_video_assets_task`: Resolves HLS manifests with Playwright before download
- Toggle eager mode via `CRAWLER_CELERY_TASK_ALWAYS_EAGER` (default: False in Docker, True otherwise)
- Tasks support retry with exponential backoff on failures

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
# List available sites
docker compose run --rm test_app python -m crawler.ingest --help

# Standard ingestion with Celery worker (multi-site support)
docker compose run --rm test_app \
  python -m crawler.ingest \
    --site thanhnien \
    --jobs-file data/thanhnien_jobs.ndjson \
    --storage-root /app/storage \
    --max-workers 4 \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db

# Legacy single-site command (ThanhNien only)
docker compose run --rm test_app \
  python -m crawler.ingest_thanhnien \
    --jobs-file data/thanhnien_jobs.ndjson \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db

# With resume (skip existing URLs)
docker compose run --rm test_app \
  python -m crawler.ingest \
    --site thanhnien \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --resume

# Enable Playwright for HLS video manifest resolution
docker compose run --rm test_app \
  python -m crawler.ingest \
    --site thanhnien \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --use-playwright

# With proxy rotation
docker compose run --rm test_app \
  python -m crawler.ingest \
    --site thanhnien \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --proxy 192.168.1.100:8080:apikey \
    --proxy-change-url http://192.168.1.100:8080/rotate \
    --proxy-rotation-interval 240

# Enable raw HTML caching for debugging
docker compose run --rm test_app \
  python -m crawler.ingest \
    --site thanhnien \
    --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
    --raw-html-cache
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
Encapsulated in config dataclasses:
- **IngestConfig**: Main configuration container
  - Jobs file path, storage root, DB URL, user agent
  - Resume mode, raw HTML caching, logging directory
  - Playwright settings (enabled, timeout)
- **RateLimitConfig**: `per_domain_delay` (0.5s), `max_workers` (4)
- **RetryConfig**: `max_attempts` (3), `backoff_factor` (1.5), `base_delay` (1.0s)
- **TimeoutConfig**: `request_timeout` (5.0s), `asset_timeout` (30.0s)
- **ProxyConfig**: scheme/host/port, rotation API URL, key, min rotation interval (240s)

Override via CLI flags (see `crawler/ingest.py` argparse setup):
- `--site`: Choose news site (thanhnien, etc.)
- `--jobs-file`: Custom NDJSON path
- `--max-workers`: Concurrent workers
- `--resume`: Skip existing URLs
- `--raw-html-cache`: Enable HTML persistence
- `--proxy`, `--proxy-scheme`, `--proxy-change-url`, `--proxy-key`, `--proxy-rotation-interval`: Proxy configuration
- `--use-playwright`, `--playwright-timeout`: Video manifest resolution

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

### Adding a New Site Parser
1. Create parser class in `crawler/parsers/newsite.py` implementing parser interface
2. Define extraction logic following `ThanhnienParser` pattern
3. Register site in `crawler/sites.py` using `SiteDefinition`
4. Create job file in `data/newsite_jobs.ndjson`
5. Add test fixtures in `tests/parsers/test_newsite_parser.py`
6. Test with `--site newsite` flag

### Adding a New Parser Field
1. Add column to `models.Article` in `models.py`
2. Run Alembic migration (if using; currently direct DDL)
3. Update `ParsedArticle` dataclass in `crawler/parsers/__init__.py`
4. Implement extraction in parser class (e.g., `ThanhnienParser.parse()` in `crawler/parsers/thanhnien.py`)
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

- `models.py`: SQLAlchemy ORM models (Article, ArticleImage, ArticleVideo)
- `test_models.py`: Model smoke tests with unicode and relationship validation
- `test_video_download.py`: Video download testing script
- `crawler/`:
  - `ingest.py`: Multi-site CLI entrypoint and orchestration loop
  - `ingest_thanhnien.py`: Compatibility wrapper that pins the site to ThanhNien
  - `sites.py`: Registry of supported news sites and parser wiring
  - `jobs.py`: NDJSON loader, dedupe logic
  - `http_client.py`: httpx wrapper with retry/proxy
  - `parsers/`:
    - `__init__.py`: Base parser interfaces (ParsedArticle, ParsedAsset dataclasses)
    - `thanhnien.py`: HTML extraction logic for Thanhnien site
  - `persistence.py`: DB session management, upsert operations
  - `assets.py`: Asset serialization, download manager
  - `tasks.py`: Celery task definitions (download_assets_task, resolve_video_assets_task)
  - `celery_app.py`: Celery app factory with RabbitMQ broker config
  - `config.py`: Configuration dataclasses (IngestConfig, ProxyConfig, RateLimitConfig, etc.)
  - `playwright_support.py`: HLS manifest resolver with browser automation
  - `sitemap_backfill.py`: Sitemap fetching utility
  - `dedupe.py`: URL deduplication utilities
- `tests/`: Unit and integration tests (unittest framework)
  - `test_http_client.py`: HTTP client tests
  - `test_asset_manager.py`: Asset management tests
  - `test_ingest_thanhnien.py`: Integration tests
  - `parsers/test_thanhnien_parser.py`: Parser unit tests with fixtures
- `data/`: Input job queues
  - `thanhnien_jobs.ndjson`: Input job queue (one JSON object per line)
- `storage/`: Runtime storage (bind-mounted in Docker)
  - `raw/`: Optional raw HTML cache
  - `articles/{uuid}/images/` and `videos/`: Downloaded assets
  - `logs/fetch_failures.ndjson`: Failed fetch tracking
- `docs/thanhnien_ingestion_design.md`: Detailed design document
- `docker-compose.yml`: Multi-service orchestration (postgres, rabbitmq, pgadmin, test_app)
- `Dockerfile`: Application container definition
- `requirements.txt`: Python dependencies
- `.env.sample`: Environment variable template

## External Resources

See detailed design document: `docs/thanhnien_ingestion_design.md`
