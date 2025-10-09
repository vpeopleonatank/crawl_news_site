# Crawler System Processing Architecture

## Overview
Multi-site news article ingestion pipeline with async media processing, supporting Vietnamese news sites (Thanhnien.vn). Built on SQLAlchemy 2.0, Celery 5.3, PostgreSQL 16, and RabbitMQ 3.13.
9-Stage Processing Pipeline:
  1. Job Loading - NDJSON parsing with deduplication
  2. HTTP Fetch - Smart retry with proxy rotation
  3. Parsing - BeautifulSoup extraction (ThanhNien.vn)
  4. Video Resolution - Optional Playwright HLS manifest resolution
  5. Metadata Persistence - SQLAlchemy upserts with UUIDv7
  6. Async Task Queue - RabbitMQ + Celery serialization
  7. Video Asset Resolution - Background Playwright processing
  8. Asset Downloads - Parallel downloads (images via httpx, HLS via ffmpeg)
  9. Asset Persistence - DB updates with file paths

Architecture Patterns:
- Sync ingestion + Async downloads - Fast metadata feedback, scalable media processing
- Idempotent operations - Resume mode, URL-based deduplication
- Error isolation - Per-job failures don't stop batch processing
- Multi-site extensibility - Plugin parser architecture

Data Flow:
NDJSON Jobs → HTTP Fetch → Parser → [Playwright] → DB Metadata →
RabbitMQ Queue → Celery Worker → Asset Download → DB Asset Paths

The document includes:
- ASCII architecture diagram
- Database schema with indexes
- Complete workflow example with code/SQL snippets
- Performance characteristics
- Error recovery strategies
- File references to specific functions

---

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION ORCHESTRATION                             │
│                           (crawler/ingest.py)                                │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         1. JOB LOADING PHASE                                 │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ NDJSONJobLoader (crawler/jobs.py)                                  │     │
│  │                                                                     │     │
│  │  • Read data/thanhnien_jobs.ndjson (one JSON per line)            │     │
│  │  • Parse sitemap URL, article URL, lastmod timestamp              │     │
│  │  • Deduplicate against existing DB URLs (if --resume)             │     │
│  │  • Skip duplicate/invalid entries                                 │     │
│  │                                                                     │     │
│  │  Input:  {"url": "https://...", "sitemap_url": "...", ...}        │     │
│  │  Output: ArticleJob(url, sitemap_url, lastmod)                    │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ Resume Mode (Optional)                                             │     │
│  │                                                                     │     │
│  │  • Query PostgreSQL: SELECT url FROM articles                     │     │
│  │  • Build set of existing URLs                                     │     │
│  │  • Filter jobs to process only new URLs                           │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         2. HTTP FETCH PHASE                                  │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ HttpFetcher (crawler/http_client.py)                               │     │
│  │                                                                     │     │
│  │  • HTTP GET with custom User-Agent                                │     │
│  │  • Follow redirects                                                │     │
│  │  • Timeout: 5.0s (configurable)                                   │     │
│  │  • Retry on 403/429/503 with proxy rotation                       │     │
│  │  • Content-Type validation (must contain "html")                  │     │
│  │                                                                     │     │
│  │  Input:  article_url                                               │     │
│  │  Output: (html_text, response)                                    │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ ProxyRotator (Optional)                                            │     │
│  │                                                                     │     │
│  │  • Detect block responses (403, 429, 503)                         │     │
│  │  • Call rotation API endpoint                                     │     │
│  │  • Throttle rotation (min 240s interval)                          │     │
│  │  • Rebuild client with new proxy connection                       │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ Error Handling                                                     │     │
│  │                                                                     │     │
│  │  • Log failures → storage/logs/fetch_failures.ndjson              │     │
│  │  • Include: url, error_type, timestamp, sitemap_url               │     │
│  │  • Continue processing remaining jobs                             │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         3. PARSING PHASE                                     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ ThanhnienParser (crawler/parsers/thanhnien.py)                     │     │
│  │                                                                     │     │
│  │  BeautifulSoup4 HTML extraction:                                  │     │
│  │                                                                     │     │
│  │  Metadata:                                                         │     │
│  │    • Title: <h1> tag (required)                                   │     │
│  │    • Description: <h2> tag                                        │     │
│  │    • Category: breadcrumb/detail-cate links                       │     │
│  │    • Tags: div.detail__tags anchors                               │     │
│  │    • Publish Date: meta[itemprop=datePublished] or               │     │
│  │                    meta[property=article:published_time] or       │     │
│  │                    time[datetime] (multiple fallbacks)            │     │
│  │                                                                     │     │
│  │  Content:                                                          │     │
│  │    • Extract <p> tags from div[data-role="content"]               │     │
│  │    • Filter out captions (skip <p> inside <figure>/<figcaption>)  │     │
│  │    • Join with double newlines                                    │     │
│  │                                                                     │     │
│  │  Media Assets (ordered by appearance):                            │     │
│  │    • Images: <figure><img src> + <figcaption>                     │     │
│  │    • Videos: <figure><video src>                                  │     │
│  │    • Streams: div[type="VideoStream"][data-vid]                   │     │
│  │    • Normalize URLs (add https://, handle //-prefixed)            │     │
│  │    • Assign sequence numbers (1, 2, 3...)                         │     │
│  │                                                                     │     │
│  │  Input:  (url, html)                                               │     │
│  │  Output: ParsedArticle(title, content, assets[], ...)            │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ Asset Sequence Validation                                          │     │
│  │                                                                     │     │
│  │  • Ensure continuous sequence (1, 2, 3... no gaps)                │     │
│  │  • Sort by sequence number                                        │     │
│  │  • Validate no duplicate sequences                                │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  4. VIDEO RESOLUTION PHASE (Optional)                        │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ Playwright Integration (crawler/playwright_support.py)             │     │
│  │                                                                     │     │
│  │  Condition: --use-playwright flag + site has resolver              │     │
│  │                                                                     │     │
│  │  Sync Mode (Celery eager):                                         │     │
│  │    • Launch headless browser (Chromium)                           │     │
│  │    • Navigate to article URL                                      │     │
│  │    • Intercept video network requests                             │     │
│  │    • Extract HLS manifest URLs (.m3u8)                            │     │
│  │    • Update asset source_url in-place                             │     │
│  │    • Timeout: 30s (configurable)                                  │     │
│  │                                                                     │     │
│  │  Async Mode (Celery worker):                                       │     │
│  │    • Defer resolution to resolve_video_assets_task                │     │
│  │    • Chain: resolve_video_assets_task | download_assets_task      │     │
│  │                                                                     │     │
│  │  Input:  article_url, video_assets[]                               │     │
│  │  Output: Updated video_assets[] with HLS URLs                     │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    5. METADATA PERSISTENCE PHASE                             │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ ArticlePersistence.upsert_metadata (crawler/persistence.py)        │     │
│  │                                                                     │     │
│  │  SQLAlchemy ORM operations:                                        │     │
│  │                                                                     │     │
│  │  1. Query existing: SELECT * FROM articles WHERE url = ?           │     │
│  │                                                                     │     │
│  │  2. If not exists:                                                 │     │
│  │       article = Article(id=uuid7(), url=url)                      │     │
│  │       session.add(article)                                         │     │
│  │       created = True                                               │     │
│  │                                                                     │     │
│  │  3. Update fields:                                                 │     │
│  │       article.title = parsed.title                                │     │
│  │       article.content = parsed.content                            │     │
│  │       article.category_id = parsed.category_id                    │     │
│  │       article.category_name = parsed.category_name                │     │
│  │       article.publish_date = parsed.publish_date                  │     │
│  │       article.tags = ",".join(parsed.tags)                        │     │
│  │       article.comments = {                                         │     │
│  │         "crawler": {                                               │     │
│  │           "status_code": 200,                                     │     │
│  │           "sitemap_url": "...",                                   │     │
│  │           "lastmod": "2024-01-15T10:30:00Z"                       │     │
│  │         }                                                          │     │
│  │       }                                                            │     │
│  │                                                                     │     │
│  │  4. session.flush() → generates article.id                         │     │
│  │  5. session.commit()                                               │     │
│  │                                                                     │     │
│  │  Input:  ParsedArticle, fetch_metadata                             │     │
│  │  Output: PersistenceResult(article_id, created)                   │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ Raw HTML Cache (Optional)                                          │     │
│  │                                                                     │     │
│  │  Condition: --raw-html-cache flag                                  │     │
│  │                                                                     │     │
│  │  • Save to: storage/raw/{article_uuid}.html                       │     │
│  │  • UTF-8 encoding                                                 │     │
│  │  • Used for debugging parser issues                               │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    6. ASYNC ASSET PIPELINE                                   │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ Task Serialization (crawler/ingest.py:232)                         │     │
│  │                                                                     │     │
│  │  Build task payload:                                               │     │
│  │  {                                                                 │     │
│  │    "article_id": "01936a7f-...",                                   │     │
│  │    "db_url": "postgresql://...",                                   │     │
│  │    "article_url": "https://thanhnien.vn/...",                     │     │
│  │    "site": "thanhnien",                                           │     │
│  │    "assets": [                                                     │     │
│  │      {                                                             │     │
│  │        "source_url": "https://example.com/img1.jpg",              │     │
│  │        "asset_type": "image",                                     │     │
│  │        "sequence": 1,                                             │     │
│  │        "caption": "Photo caption"                                 │     │
│  │      },                                                            │     │
│  │      ...                                                           │     │
│  │    ],                                                              │     │
│  │    "config": {                                                     │     │
│  │      "storage_root": "/app/storage",                              │     │
│  │      "user_agent": "...",                                         │     │
│  │      "request_timeout": 5.0,                                      │     │
│  │      "asset_timeout": 30.0,                                       │     │
│  │      "proxy": {...}  // if configured                             │     │
│  │    }                                                               │     │
│  │  }                                                                 │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│                              ┌───────────┐                                   │
│                              │ RabbitMQ  │                                   │
│                              │  Broker   │                                   │
│                              └─────┬─────┘                                   │
│                                    │                                         │
│              ┌─────────────────────┼─────────────────────┐                  │
│              │                     │                     │                  │
│              ▼                     ▼                     ▼                  │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐  │
│  │  Celery Worker 1    │ │  Celery Worker 2    │ │  Celery Worker N    │  │
│  └─────────────────────┘ └─────────────────────┘ └─────────────────────┘  │
│                                                                               │
│  Execution Modes:                                                            │
│                                                                               │
│  A) EAGER MODE (CRAWLER_CELERY_TASK_ALWAYS_EAGER=True):                     │
│     • Tasks execute synchronously in main process                           │
│     • No RabbitMQ queue involved                                            │
│     • Used for local testing without worker                                 │
│                                                                               │
│  B) ASYNC MODE (Production, Docker):                                         │
│     • Tasks enqueued to RabbitMQ                                            │
│     • Workers poll and execute in background                                │
│     • Results stored in PostgreSQL result backend                           │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                7. CELERY TASK: RESOLVE VIDEO ASSETS                          │
│                  (crawler/tasks.py:resolve_video_assets_task)                │
│                                                                               │
│  Condition: Async mode + --use-playwright + video assets present            │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ 1. Deserialize task payload                                        │     │
│  │ 2. Filter to video assets only                                     │     │
│  │ 3. Launch Playwright browser in worker                             │     │
│  │ 4. Navigate to article URL                                         │     │
│  │ 5. Intercept video network requests                                │     │
│  │ 6. Extract HLS manifest URLs                                       │     │
│  │ 7. Update asset.source_url with resolved URLs                      │     │
│  │ 8. Re-serialize and return updated payload                         │     │
│  │ 9. Chain to download_assets_task                                   │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  Error Handling:                                                             │
│    • Retry with exponential backoff on failures                             │
│    • Log warnings for resolution failures                                   │
│    • Continue with original URLs if Playwright fails                        │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                8. CELERY TASK: DOWNLOAD ASSETS                               │
│                  (crawler/tasks.py:download_assets_task)                     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ AssetManager.download_assets (crawler/assets.py)                   │     │
│  │                                                                     │     │
│  │  For each asset in sequence order:                                │     │
│  │                                                                     │     │
│  │  A) IMAGE ASSETS:                                                  │     │
│  │     1. HTTP GET with streaming (httpx.Client.stream)              │     │
│  │     2. Extract extension from URL (jpg, png, webp)                │     │
│  │     3. Save to: storage/articles/{uuid}/images/{seq:03d}.ext      │     │
│  │     4. Compute SHA-256 checksum                                   │     │
│  │     5. Track bytes_downloaded                                     │     │
│  │                                                                     │     │
│  │  B) VIDEO ASSETS (Non-HLS):                                        │     │
│  │     1. Check for Thanhnien CDN manifest (.mp4.json)               │     │
│  │     2. If manifest exists, resolve HLS URL                        │     │
│  │     3. HTTP GET with streaming                                    │     │
│  │     4. Save to: storage/articles/{uuid}/videos/{seq:03d}.ext      │     │
│  │                                                                     │     │
│  │  C) VIDEO ASSETS (HLS Manifests .m3u8):                            │     │
│  │     1. Invoke ffmpeg with HLS input:                              │     │
│  │        ffmpeg -i manifest.m3u8 -c copy -bsf:a aac_adtstoasc       │     │
│  │               -f mp4 {seq:03d}.mp4                                │     │
│  │     2. Download all segments and mux to MP4                       │     │
│  │     3. Save to: storage/articles/{uuid}/videos/{seq:03d}.mp4      │     │
│  │     4. Compute checksum on final file                             │     │
│  │                                                                     │     │
│  │  Deduplication:                                                    │     │
│  │    • Track seen source URLs per article                           │     │
│  │    • Skip duplicate URLs                                          │     │
│  │    • Skip data: URIs (inline base64)                              │     │
│  │                                                                     │     │
│  │  Error Handling:                                                   │     │
│  │    • Delete partial files on failure                              │     │
│  │    • Raise AssetDownloadError                                     │     │
│  │    • Trigger Celery retry with backoff                            │     │
│  │                                                                     │     │
│  │  Output: List[StoredAsset(source, path, checksum, bytes)]         │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                9. ASSET METADATA PERSISTENCE                                 │
│                  (crawler/persistence.py:persist_assets)                     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │ SQLAlchemy ORM operations:                                         │     │
│  │                                                                     │     │
│  │  1. Query article: SELECT * FROM articles WHERE id = ?             │     │
│  │                                                                     │     │
│  │  2. Clear existing assets (CASCADE delete):                        │     │
│  │       article.images.clear()                                       │     │
│  │       article.videos.clear()                                       │     │
│  │                                                                     │     │
│  │  3. Insert new asset records:                                      │     │
│  │                                                                     │     │
│  │     For images:                                                    │     │
│  │       article.images.append(                                       │     │
│  │         ArticleImage(                                              │     │
│  │           image_path="articles/{uuid}/images/001.jpg",            │     │
│  │           sequence_number=1                                        │     │
│  │         )                                                          │     │
│  │       )                                                            │     │
│  │                                                                     │     │
│  │     For videos:                                                    │     │
│  │       article.videos.append(                                       │     │
│  │         ArticleVideo(                                              │     │
│  │           video_path="articles/{uuid}/videos/001.mp4",            │     │
│  │           sequence_number=1                                        │     │
│  │         )                                                          │     │
│  │       )                                                            │     │
│  │                                                                     │     │
│  │  4. session.commit()                                               │     │
│  │                                                                     │     │
│  │  Path Storage:                                                     │     │
│  │    • Stored as relative paths from storage_root                   │     │
│  │    • Enables storage root relocation                              │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘

```

---

## Data Models (SQLAlchemy)

### Article Table
```sql
CREATE TABLE articles (
    id UUID PRIMARY KEY,                    -- UUIDv7 (time-sortable)
    title VARCHAR(500) NOT NULL,
    description TEXT,
    content TEXT,
    category_id VARCHAR(100),
    category_name VARCHAR(200),
    tags VARCHAR(500),                      -- comma-separated
    url VARCHAR(2000) UNIQUE NOT NULL,      -- deduplication key
    publish_date TIMESTAMP,
    comments JSONB,                         -- {crawler: {status_code, sitemap_url, ...}}
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_articles_title ON articles(title);
CREATE INDEX idx_articles_category_id ON articles(category_id);
CREATE INDEX idx_articles_category_name ON articles(category_name);
CREATE INDEX idx_articles_tags ON articles(tags);
CREATE INDEX idx_articles_publish_date ON articles(publish_date);
```

### ArticleImage Table
```sql
CREATE TABLE article_images (
    id UUID PRIMARY KEY,                    -- UUIDv7
    article_id UUID NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    image_path VARCHAR(500) NOT NULL,       -- relative to storage_root
    sequence_number INTEGER NOT NULL,       -- ordering within article
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_article_images_article_id_seq ON article_images(article_id, sequence_number);
```

### ArticleVideo Table
```sql
CREATE TABLE article_videos (
    id UUID PRIMARY KEY,                    -- UUIDv7
    article_id UUID NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    video_path VARCHAR(500) NOT NULL,       -- relative to storage_root
    sequence_number INTEGER NOT NULL,       -- ordering within article
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_article_videos_article_id_seq ON article_videos(article_id, sequence_number);
```

---

## Key Design Patterns

### 1. Idempotency
- **URL-based deduplication**: `Article.url` is unique constraint
- **Resume mode**: Skip existing URLs via DB query
- **Asset overwrites**: Re-running ingestion overwrites existing files
- **Upsert logic**: Updates existing articles instead of failing on duplicates

### 2. Asynchronous Processing
- **Separation of concerns**: Metadata ingestion (sync) vs. asset downloads (async)
- **Queue-based decoupling**: Ingestion doesn't wait for downloads
- **Worker scalability**: Multiple Celery workers process downloads in parallel
- **Retry resilience**: Exponential backoff on failures

### 3. Error Isolation
- **Job-level errors**: One failed job doesn't stop entire batch
- **Failure logging**: `fetch_failures.ndjson` tracks errors for post-mortem
- **Partial success**: Article metadata persists even if asset download fails initially
- **Graceful degradation**: Playwright failures fall back to original URLs

### 4. Data Integrity
- **UUIDv7 primary keys**: Time-ordered, globally unique, DB-friendly
- **Sequence validation**: Assets maintain insertion order via sequence numbers
- **CASCADE deletes**: Orphaned assets automatically removed
- **Transaction boundaries**: Metadata and assets commit separately

### 5. Multi-Site Extensibility
- **Site registry**: `crawler/sites.py` maps site slug to parser class
- **Parser interface**: Standardized `ParsedArticle` output
- **Per-site config**: Default jobs file, user agent, Playwright resolver

---

## Configuration Flow

```
CLI Arguments (--db-url, --max-workers, --proxy, ...)
         ↓
   argparse.Namespace
         ↓
   build_config(args, site)
         ↓
   IngestConfig dataclass
     ├─ jobs_file: Path
     ├─ storage_root: Path
     ├─ db_url: str
     ├─ user_agent: str
     ├─ resume: bool
     ├─ raw_html_cache_enabled: bool
     ├─ proxy: ProxyConfig | None
     ├─ rate_limit: RateLimitConfig
     ├─ retry: RetryConfig
     ├─ timeout: TimeoutConfig
     ├─ playwright_enabled: bool
     └─ playwright_timeout: float
```

### Environment Variables
- `DATABASE_URL`: Main app connection
- `CRAWLER_DATABASE_URL`: Celery worker connection
- `CRAWLER_CELERY_BROKER_URL`: RabbitMQ AMQP URL
- `CRAWLER_CELERY_RESULT_BACKEND`: PostgreSQL result backend
- `CRAWLER_CELERY_TASK_ALWAYS_EAGER`: Sync/async mode toggle

---

## Service Dependencies (Docker Compose)

```yaml
services:
  postgres:
    image: postgres:16-alpine
    ports: ["5433:5432"]
    volumes: [postgres_data]
    credentials: crawl_user / crawl_password

  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    ports: ["5672", "15672"]
    volumes: [rabbitmq_data]
    credentials: crawler / crawler_password

  pgadmin:
    image: dpage/pgadmin4
    ports: ["5050:80"]
    credentials: admin@admin.com / admin

  test_app:
    build: .
    depends_on: [postgres, rabbitmq]
    volumes:
      - ./storage:/app/storage  # bind-mount for assets
```

---

## Processing Workflow Example

### Input Job (NDJSON)
```json
{
  "url": "https://thanhnien.vn/kham-pha-xu-so-than-tien-post1234567.html",
  "sitemap_url": "https://thanhnien.vn/sitemap-2024-01.xml",
  "lastmod": "2024-01-15T10:30:00+07:00"
}
```

### 1. Job Loading
```python
job = ArticleJob(
    url="https://thanhnien.vn/kham-pha-xu-so-than-tien-post1234567.html",
    sitemap_url="https://thanhnien.vn/sitemap-2024-01.xml",
    lastmod="2024-01-15T10:30:00+07:00"
)
```

### 2. HTTP Fetch
```python
html, response = fetcher.fetch_html(job.url)
# response.status_code == 200
# html == "<html><head>...</head><body>...</body></html>"
```

### 3. Parsing
```python
parsed = ThanhnienParser().parse(job.url, html)
# ParsedArticle(
#   url="...",
#   title="Khám phá xứ sở thần tiên",
#   content="Đoạn văn 1\n\nĐoạn văn 2...",
#   category_id="du-lich",
#   category_name="Du lịch",
#   tags=["núi", "thiên nhiên", "phiêu lưu"],
#   publish_date=datetime(2024, 1, 15, 10, 30, tzinfo=UTC+7),
#   assets=[
#     ParsedAsset(source_url="https://cdn.../img1.jpg", asset_type=IMAGE, sequence=1, caption="Phong cảnh"),
#     ParsedAsset(source_url="https://cdn.../video.mp4.json", asset_type=VIDEO, sequence=2),
#   ]
# )
```

### 4. Video Resolution (Playwright)
```python
# If --use-playwright:
streams = resolver.resolve_streams(job.url)
# [{"hls": "https://cdn.../video/master.m3u8"}]
assets[1].source_url = "https://cdn.../video/master.m3u8"
```

### 5. Metadata Persistence
```sql
-- Upsert article
INSERT INTO articles (id, url, title, content, category_id, category_name, tags, publish_date, comments)
VALUES (
  '01936a7f-1234-7000-abcd-0123456789ab',  -- UUIDv7
  'https://thanhnien.vn/kham-pha-xu-so-than-tien-post1234567.html',
  'Khám phá xứ sở thần tiên',
  'Đoạn văn 1\n\nĐoạn văn 2...',
  'du-lich',
  'Du lịch',
  'núi,thiên nhiên,phiêu lưu',
  '2024-01-15 10:30:00+07',
  '{"crawler": {"status_code": 200, "sitemap_url": "..."}}'::jsonb
)
ON CONFLICT (url) DO UPDATE SET
  title = EXCLUDED.title,
  content = EXCLUDED.content,
  ...
RETURNING id;
```

### 6. Task Enqueue
```python
task_payload = {
  "article_id": "01936a7f-1234-7000-abcd-0123456789ab",
  "db_url": "postgresql://crawl_user:crawl_password@postgres:5432/crawl_db",
  "article_url": "https://thanhnien.vn/kham-pha-xu-so-than-tien-post1234567.html",
  "site": "thanhnien",
  "assets": [
    {"source_url": "https://cdn.../img1.jpg", "asset_type": "image", "sequence": 1, "caption": "Phong cảnh"},
    {"source_url": "https://cdn.../video/master.m3u8", "asset_type": "video", "sequence": 2, "caption": null}
  ],
  "config": {"storage_root": "/app/storage", "user_agent": "...", "asset_timeout": 30.0}
}

download_assets_task.delay(task_payload)
# → Enqueued to RabbitMQ "celery" queue
```

### 7. Asset Download (Celery Worker)
```python
# Worker picks up task from queue
stored_assets = []

# Download image
response = httpx.get("https://cdn.../img1.jpg", stream=True)
with open("/app/storage/articles/01936a7f-.../images/001.jpg", "wb") as f:
    for chunk in response.iter_bytes():
        f.write(chunk)
stored_assets.append(StoredAsset(
    path=Path("/app/storage/articles/01936a7f-.../images/001.jpg"),
    checksum="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    bytes_downloaded=152384
))

# Download HLS video
subprocess.run([
    "ffmpeg", "-i", "https://cdn.../video/master.m3u8",
    "-c", "copy", "-f", "mp4",
    "/app/storage/articles/01936a7f-.../videos/002.mp4"
])
stored_assets.append(StoredAsset(
    path=Path("/app/storage/articles/01936a7f-.../videos/002.mp4"),
    checksum="a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
    bytes_downloaded=5242880
))
```

### 8. Asset Metadata Persistence
```sql
-- Clear existing assets
DELETE FROM article_images WHERE article_id = '01936a7f-...';
DELETE FROM article_videos WHERE article_id = '01936a7f-...';

-- Insert new asset records
INSERT INTO article_images (id, article_id, image_path, sequence_number)
VALUES ('01936a7f-...', '01936a7f-...', 'articles/01936a7f-.../images/001.jpg', 1);

INSERT INTO article_videos (id, article_id, video_path, sequence_number)
VALUES ('01936a7f-...', '01936a7f-...', 'articles/01936a7f-.../videos/002.mp4', 2);

COMMIT;
```

### Final State
```
Database:
  articles: 1 row (id=01936a7f-..., title="Khám phá xứ sở thần tiên", ...)
  article_images: 1 row (image_path="articles/.../images/001.jpg", sequence=1)
  article_videos: 1 row (video_path="articles/.../videos/002.mp4", sequence=2)

Filesystem:
  storage/articles/01936a7f-.../images/001.jpg (152 KB)
  storage/articles/01936a7f-.../videos/002.mp4 (5 MB)
  storage/raw/01936a7f-....html (if --raw-html-cache)
```

---

## Performance Characteristics

### Throughput
- **Ingestion rate**: Limited by `--max-workers` (default: 4 concurrent jobs)
- **Asset downloads**: Parallelized across multiple Celery workers
- **DB writes**: Batched per article (metadata + assets in separate transactions)

### Latency
- **HTTP fetch**: ~1-5s per article (network + site response time)
- **Parsing**: ~50-200ms per article (BeautifulSoup overhead)
- **Playwright resolution**: ~2-5s per article (browser automation)
- **Asset download**: Variable (depends on file size, network)
  - Images: ~500ms-2s
  - Videos (HLS): ~5-30s for ffmpeg muxing

### Resource Usage
- **Memory**: ~100-300 MB per worker (Playwright adds ~200 MB per browser instance)
- **Disk I/O**: Streaming downloads minimize memory footprint
- **Network**: Configurable timeouts prevent hung connections

---

## Error Recovery

### Fetch Failures
1. Logged to `storage/logs/fetch_failures.ndjson`
2. Ingestion continues with next job
3. Re-run with `--resume` to retry failed URLs

### Asset Download Failures
1. Celery task retries with exponential backoff
2. Partial files deleted on error
3. Task eventually fails to dead-letter queue after max retries
4. Manual retry via Celery Flower or CLI

### Playwright Failures
1. Warning logged
2. Original asset URLs used (no resolution)
3. Ingestion continues

### Database Failures
1. Transaction rollback
2. Job marked as failed
3. Exit code 1 if any jobs failed

---

## Monitoring & Observability

### Logs
- **Application logs**: stdout (structured logging with timestamps)
- **Fetch failures**: `storage/logs/fetch_failures.ndjson`
- **Celery logs**: Worker stdout (task execution, retries, failures)

### Metrics
- **IngestionStats**: processed, succeeded, failed counts
- **JobLoader stats**: skipped_existing, skipped_invalid, skipped_duplicate
- **Asset counts**: Per article in DB queries

### Queue Monitoring
- **RabbitMQ Management UI**: http://localhost:15672
  - Queue depth
  - Message rates
  - Consumer counts

### Database Monitoring
- **pgAdmin**: http://localhost:5050
  - Table row counts
  - Index usage
  - Query performance

---

## Extensibility Points

### Adding a New Site Parser
1. Create `crawler/parsers/newsite.py` implementing `ArticleParser` interface
2. Register in `crawler/sites.py`:
   ```python
   register_site(SiteDefinition(
       slug="newsite",
       parser_factory=NewsiteParser,
       default_jobs_file=Path("data/newsite_jobs.ndjson"),
       default_user_agent="Mozilla/5.0 ...",
       playwright_resolver_factory=NewsiteVideoResolver  # optional
   ))
   ```
3. Create jobs file: `data/newsite_jobs.ndjson`
4. Run: `python -m crawler.ingest --site newsite`

### Adding New Article Fields
1. Add column to `models.Article`
2. Update `ParsedArticle` dataclass
3. Implement extraction in parser class
4. Update `ArticlePersistence.upsert_metadata()` to map new field

### Custom Asset Processing
1. Subclass `AssetManager`
2. Override `download_assets()` method
3. Inject custom manager in task handler

---

## Security Considerations

### Input Validation
- URL validation in job loader
- Content-Type checking in HTTP fetcher
- Regex-based datetime parsing to prevent injection

### Network Security
- HTTPS enforced for media URLs
- User-Agent rotation support
- Proxy authentication via API key

### Data Privacy
- Raw HTML cache disabled by default
- Configurable storage paths
- Database credentials via environment variables

### Resource Limits
- Request timeouts prevent DoS
- File size limits implicitly via timeout
- Worker concurrency caps resource usage

---

## File References

- **Main Orchestrator**: `crawler/ingest.py` (entry point, CLI parsing, pipeline coordination)
- **Job Loading**: `crawler/jobs.py:20` (NDJSONJobLoader), `crawler/jobs.py:65` (load_existing_urls)
- **HTTP Client**: `crawler/http_client.py:94` (HttpFetcher.fetch_html)
- **Parser**: `crawler/parsers/thanhnien.py:13` (ThanhnienParser.parse)
- **Persistence**: `crawler/persistence.py:35` (upsert_metadata), `crawler/persistence.py:74` (persist_assets)
- **Asset Manager**: `crawler/assets.py:35` (AssetManager.download_assets)
- **Celery Tasks**: `crawler/tasks.py:70` (resolve_video_assets_task), `crawler/tasks.py:132` (download_assets_task)
- **Models**: `models.py:18` (Article), `models.py:52` (ArticleImage), `models.py:73` (ArticleVideo)
- **Configuration**: `crawler/config.py` (IngestConfig, ProxyConfig, TimeoutConfig, etc.)
- **Playwright**: `crawler/playwright_support.py` (HLS manifest resolution)

---

## Summary

The crawler system implements a robust, multi-stage pipeline for ingesting news articles from Vietnamese news sites. Key architectural decisions include:

1. **Sync metadata ingestion** → Fast feedback on parsing errors
2. **Async asset downloads** → Decoupled, scalable media processing
3. **Idempotent operations** → Safe re-runs with `--resume`
4. **Playwright integration** → HLS video manifest resolution
5. **Multi-site extensibility** → Pluggable parser architecture
6. **Comprehensive error handling** → Isolated failures, detailed logging

This design balances throughput, reliability, and maintainability for production news crawling at scale.
