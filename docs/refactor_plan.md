# Refactoring Roadmap

This roadmap captures the current recommendations for improving maintainability and contributor experience across the ingestion pipeline.

## Pain Points
- `crawler/ingest.py:42` combines CLI setup, configuration, orchestration, Celery scheduling, and storage checks in a single 800+ line module, hampering focused changes and testing.
- `crawler/ingest.py:603` and `crawler/ingest.py:680` embed deferred-video and failed-download retry logic directly in the runtime loop, rather than delegating to dedicated services.
- `crawler/jobs.py:108` houses multiple job-loader implementations, HTTP helpers, and sitemap parsing utilities together, leading to duplicated pagination logic across Thanhnien, Nld, Kenh14, and PLO.
- `crawler/assets.py:40` mixes download orchestration, URL normalization, HLS handling, checksum management, and Celery payload serialization, blurring module responsibilities.
- `crawler/persistence.py:42` acts as a monolithic persistence layer handling metadata, assets, pending videos, and failure tracking without smaller repositories or seams for testing.
- `crawler/storage.py:40` couples environment parsing, Telegram notifications, disk-usage monitoring, and CLI subcommands in a single ~500 line module, making targeted storage changes risky.
- Contributor tooling lacks shared standards (`CONTRIBUTING.md`, formatter/linter configuration), slowing onboarding and code review.

## Refactor Phases

### Phase 1: Ingestion Pipeline Layering
- Split `crawler/ingest.py` into:
  - `crawler/cli.py` (argument parsing + logging setup).
  - `crawler/pipeline/runner.py` (orchestrates fetch → parse → persist).
  - `crawler/pipeline/services/` for storage monitoring, retry processors, and queue management.
- Move `_process_job`, `_process_pending_video_assets`, and `_process_failed_media_downloads` into dedicated service classes with unit tests so the `process_pending_videos` and `process_failed_downloads` CLIs no longer reach into ingestion internals.

### Phase 2: Job Loader Modularisation
- Create `crawler/job_loaders/` package.
- Define abstract base classes for sitemap, timeline, and API loaders sharing pagination, dedupe, and resume behavior.
- Migrate Thanhnien, Znews, Kenh14, Nld, and PLO loaders to subclasses, consolidating duplicated logic.

### Phase 3: Asset Pipeline Extraction
- Split `crawler/assets.py` into:
  - `download_manager.py` (stateful downloader interface).
  - `url_normalizer.py` (embed/HLS normalization utilities).
  - `payload_codec.py` (serialize/deserialize Celery payloads).
- Introduce `AssetDownloader` interface so Celery tasks and synchronous ingestion reuse the same contract.
- Extract `crawler/storage.py` into focused modules (`config_loader.py`, `monitor.py`, `cli.py`) to isolate environment parsing, pause/notify logic, and command-line utilities.

### Phase 4: Persistence Re-architecture
- Replace `ArticlePersistence` with smaller repositories:
  - `ArticleRepository` (article metadata upsert + comments).
  - `MediaRepository` (image/video syncing + pending asset transitions).
  - `FailureRepository` (failed media and retry bookkeeping).
- Create translator helpers/Data Transfer Objects to keep SQLAlchemy models isolated from parser dataclasses.

### Phase 5: Tooling & Contributor Experience
- Add `CONTRIBUTING.md` covering coding standards, module boundaries, and test expectations.
- Adopt shared tooling (`ruff`, `black`, `mypy`) and expose a `scripts/dev_check.sh` pre-flight.
- Provide blueprint docs for adding a new site parser and extending asset policies, plus template fixtures under `tests/fixtures/`.
- Introduce ADR notes in `docs/architecture/` describing the layered design to guide future work.

## Immediate Next Steps
1. Align the team on phase sequencing (suggest starting with ingestion layering).
2. Prototype the pipeline/service split on a branch, validating API boundaries before migrating other modules.
3. Draft contributor documentation in parallel so structure changes launch alongside clear guidance.
4. Capture desired storage responsibilities and break down the `crawler/storage.py` split before wiring ingestion to the new monitor module.
