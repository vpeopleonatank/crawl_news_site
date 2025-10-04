"""Command-line entrypoint for Thanhnien article ingestion."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .assets import AssetDownloadError, AssetManager
from .config import IngestConfig
from .http_client import HttpFetchError, HttpFetcher
from .jobs import NDJSONJobLoader, load_existing_urls
from .parsers import ParsingError
from .parsers.thanhnien import ThanhnienParser
from .persistence import ArticlePersistence, ArticlePersistenceError

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class IngestionStats:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest Thanhnien articles into PostgreSQL")
    parser.add_argument("--jobs-file", type=Path, default=IngestConfig().jobs_file, help="Path to NDJSON jobs file")
    parser.add_argument("--db-url", type=str, required=True, help="SQLAlchemy database URL")
    parser.add_argument("--storage-root", type=Path, default=IngestConfig().storage_root, help="Base directory to store assets")
    parser.add_argument("--max-workers", type=int, default=4, help="Number of concurrent workers")
    parser.add_argument("--resume", action="store_true", help="Skip jobs already processed")
    parser.add_argument("--raw-html-cache", action="store_true", help="Persist raw HTML payloads for debugging")
    return parser


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def build_config(args: argparse.Namespace) -> IngestConfig:
    config = IngestConfig(
        jobs_file=args.jobs_file,
        storage_root=args.storage_root,
        db_url=args.db_url,
        resume=args.resume,
        raw_html_cache_enabled=args.raw_html_cache,
    )
    config.rate_limit.max_workers = args.max_workers
    config.ensure_directories()
    return config


def persist_raw_html(config: IngestConfig, article_id: str, html: str) -> None:
    raw_path = config.raw_html_path(article_id)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(html, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    config = build_config(args)

    if not config.db_url:
        parser.error("--db-url is required")

    engine = create_engine(config.db_url)
    SessionLocal = sessionmaker(bind=engine)

    existing_urls: set[str] = set()
    if config.resume:
        with SessionLocal() as session:
            existing_urls = load_existing_urls(session)
        LOGGER.info("Loaded %d existing article URLs for resume mode", len(existing_urls))

    job_loader = NDJSONJobLoader(
        jobs_file=config.jobs_file,
        existing_urls=existing_urls,
        resume=config.resume,
    )

    persistence = ArticlePersistence(session_factory=SessionLocal, storage_root=config.storage_root)
    parser_impl = ThanhnienParser()
    stats = IngestionStats()

    with HttpFetcher(config) as fetcher, AssetManager(config) as asset_manager:
        for job in job_loader:
            stats.processed += 1
            LOGGER.info("Processing article %s", job.url)
            try:
                html, response = fetcher.fetch_html(job.url)
                parsed = parser_impl.parse(job.url, html)
                fetch_metadata = {
                    "status_code": response.status_code,
                    "sitemap_url": job.sitemap_url,
                    "lastmod": job.lastmod,
                }
                result = persistence.upsert_metadata(parsed, fetch_metadata)
                article_id = result.article_id

                if config.raw_html_cache_enabled:
                    persist_raw_html(config, article_id, html)

                if parsed.assets:
                    stored_assets = asset_manager.download_assets(article_id, parsed.assets)
                    persistence.persist_assets(article_id, stored_assets)

                stats.succeeded += 1
            except (HttpFetchError, ParsingError, AssetDownloadError, ArticlePersistenceError) as exc:
                stats.failed += 1
                LOGGER.error("Failed to process %s: %s", job.url, exc)
            except Exception as exc:  # pragma: no cover - unexpected failure
                stats.failed += 1
                LOGGER.exception("Unhandled error for %s", job.url)

    LOGGER.info(
        "Processed %d jobs: %d succeeded, %d failed, %d skipped by loader",
        stats.processed,
        stats.succeeded,
        stats.failed,
        job_loader.stats.skipped_existing + job_loader.stats.skipped_invalid + job_loader.stats.skipped_duplicate,
    )

    return 0 if stats.failed == 0 else 1


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
