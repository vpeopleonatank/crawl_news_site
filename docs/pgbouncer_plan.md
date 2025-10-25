# PgBouncer Integration Plan

The repo currently connects directly to Postgres from every component (`docker-compose.yml:14-110`, `.env.sample:1-26`, `crawler/celery_app.py:44-78`, `crawler/ingest.py:891-945`, `crawler/tasks.py:143-276`, `test_models.py:9-201`, plus numerous docs and helper scripts). PgBouncer needs to sit between these callers and Postgres while preserving admin access. The plan below expands the original outline with file-level changes and sequencing notes collected from the codebase audit.

## 1. Container Topology & Config Assets
- Create a dedicated `pgbouncer/` directory with a custom Docker build:
  - `pgbouncer/Dockerfile` based on Alpine, installing PgBouncer plus `postgresql15-client` for health checks.
  - `pgbouncer/docker-entrypoint.sh` that renders `pgbouncer.ini`/`userlist.txt` from environment variables at runtime and launches PgBouncer.
- Keep configuration values (pool size, auth credentials) sourced from `.env` to avoid committing secrets.
- Add a health check that uses `pg_isready` against port `6432` so Compose can gate dependent services.

## 2. Docker Compose Wiring (`docker-compose.yml`)
- Insert a new `pgbouncer` service after `postgres`:
  - Depends on `postgres` (healthy).
  - Mount config files from `./pgbouncer` (read-only) and optionally a persistent volume for command socket statistics.
  - Expose container port `6432` to the host via `${HOST_PGBOUNCER_PORT:-6432}:6432`.
  - Pass through environment values from `.env` (`PGBOUNCER_PORT=6432`, `POSTGRES_*`, and tuning knobs like `PGBOUNCER_DEFAULT_POOL_SIZE`).
- Update `test_app` and `flower` services to depend on `pgbouncer` (healthy) and point their default `DATABASE_URL`, `CRAWLER_DATABASE_URL`, and `CRAWLER_CELERY_RESULT_BACKEND` to `postgresql://crawl_user:crawl_password@pgbouncer:6432/crawl_db`. Keep optional overrides so power users can bypass the pool.
- Leave the Postgres service’s host port mapping (`HOST_POSTGRES_PORT:-5433`) untouched for direct maintenance connections.

## 3. Environment Templates (`.env.sample` and docs)
- Introduce new variables:
  - `PGBOUNCER_HOST=pgbouncer`
  - `PGBOUNCER_PORT=6432` and `HOST_PGBOUNCER_PORT=6432`
  - Pool tuning knobs (`PGBOUNCER_DEFAULT_POOL_SIZE`, `PGBOUNCER_MAX_CLIENT_CONN`, `PGBOUNCER_RESERVE_POOL_SIZE`, etc.).
  - Pooled DSNs: `DATABASE_URL=postgresql://crawl_user:crawl_password@pgbouncer:6432/crawl_db`, `CRAWLER_DATABASE_URL=...`, `CRAWLER_CELERY_RESULT_BACKEND=db+postgresql://crawl_user:crawl_password@pgbouncer:6432/crawl_db`.
- Add helper envs for direct access (`DATABASE_URL_DIRECT`, `CRAWLER_DATABASE_URL_DIRECT`) so scripts can opt out when running migrations or bulk operations.
- Update comments to describe when to use each DSN and how PgBouncer interacts with Celery’s SQLAlchemy engine options.

## 4. Application & Worker Defaults
- `crawler/celery_app.py`: verify pooled URLs are the default when env vars are omitted. Document that `CRAWLER_DB_POOL_SIZE` should remain low because PgBouncer handles multiplexing.
- `crawler/tasks.py` and `crawler/ingest.py`: keep `create_engine` calls unchanged but annotate expectations around pooled DSNs and direct override behaviour.
- CLI docs and helper scripts (`site_categories/*.txt`, `docs/crawler_architecture.md`, `docs/thanhnien_ingestion_design.md`, `CLAUDE.md`, etc.) must replace `postgres:5432` DSNs with `pgbouncer:6432` for standard runs.
- `test_models.py`: switch default fallback DSN to `postgresql://crawl_user:crawl_password@localhost:6432/crawl_db` so local smoke tests exercise the pool. Add an opt-in flag or environment read (e.g., respect `DATABASE_URL_DIRECT`) for bypass scenarios.

## 5. Supporting Assets & Tooling
- Add a helper script (e.g., `scripts/pgbouncer_status.sh`) to run `SHOW POOLS` via `psql` for troubleshooting.
- For Docker-based workflows, decide whether migrations continue to run through the pool. If direct access is preferred, document running `docker compose run --rm test_app python -m crawler.ingest ... --db-url ${DATABASE_URL_DIRECT}`.
- Extend storage ops / runbooks (`docs/storage_operations.md` or new `docs/pgbouncer.md`) with troubleshooting steps: checking container logs, verifying PgBouncer statistics, and explaining pool sizing guidance for Celery concurrency.

## 6. Documentation Refresh
- Update `Readme.md` sections describing the stack (mention PgBouncer, new port mappings, default connection instructions for pgAdmin pointing to `pgbouncer:6432`).
- Ensure `CLAUDE.md` and other onboarding docs mirror the new defaults and highlight how to connect directly to Postgres when needed.
- Include a short migration note summarizing how existing `.env` files must be regenerated or patched when upgrading.

## 7. Validation & Rollout Checklist
- `docker compose up --build` to confirm PgBouncer starts, health checks pass, and `test_app` connects via the pool.
- Run representative ingestion commands (`crawler.ingest` for Thanhnien and Znews) to ensure Celery job payloads still succeed and asset downloads persist.
- Execute `python -m unittest discover` with `DATABASE_URL` pointing at PgBouncer and again with the direct DSN to confirm both paths operate.
- Use `scripts/pgbouncer_status.sh` (or `docker compose exec pgbouncer psql -h localhost -p 6432 -U crawl_user pgbouncer -c "SHOW POOLS"`) to observe active connections under load.
- Document any tuning adjustments required after live testing (e.g., increasing pool size for heavy ingest runs) before publishing the change.
