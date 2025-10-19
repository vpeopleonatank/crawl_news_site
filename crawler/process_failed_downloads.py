"""CLI utility to re-enqueue media downloads that exhausted Celery retries."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base

from .config import IngestConfig, TimeoutConfig
from .ingest import (
    _process_failed_media_downloads,
    build_config,
    configure_logging,
)
from .sites import get_site_definition, list_sites
from .tasks import download_assets_task

LOGGER = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    available_sites = list_sites()
    if not available_sites:
        raise RuntimeError("No sites registered for ingestion")

    parser = argparse.ArgumentParser(
        description="Retry media downloads that previously failed after exhausting retries",
    )
    parser.set_defaults(
        jobs_file=None,
        resume=False,
        raw_html_cache=False,
        sitemap_max_documents=None,
        sitemap_max_urls_per_document=None,
        max_workers=1,
    )
    parser.add_argument(
        "--site",
        choices=available_sites,
        required=True,
        help="Slug of the news site whose failed media downloads should be retried",
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="SQLAlchemy database URL",
    )
    parser.add_argument(
        "--storage-root",
        type=Path,
        default=IngestConfig().storage_root,
        help="Base directory for asset storage (must match ingestion runs)",
    )
    parser.add_argument(
        "--use-playwright",
        action="store_true",
        help="Resolve video manifests via Playwright before download",
    )
    parser.add_argument(
        "--playwright-timeout",
        type=float,
        default=IngestConfig().playwright_timeout,
        help="Seconds to wait for Playwright video manifest responses",
    )
    parser.add_argument(
        "--hls-download-timeout",
        type=float,
        default=TimeoutConfig().hls_download_timeout,
        help="Maximum seconds to allow ffmpeg when downloading HLS video streams.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        site = get_site_definition(args.site)
    except KeyError as exc:
        parser.error(str(exc))

    try:
        config = build_config(args, site)
    except ValueError as exc:
        parser.error(str(exc))

    if not config.db_url:
        parser.error("--db-url is required")

    engine = create_engine(config.db_url)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    celery_app = download_assets_task.app
    task_always_eager = bool(getattr(celery_app.conf, "task_always_eager", False))
    use_celery_playwright = (
        config.playwright_enabled
        and site.playwright_resolver_factory is not None
        and not task_always_eager
    )

    LOGGER.info(
        "Retrying failed media downloads for site %s",
        site.slug,
    )
    _process_failed_media_downloads(
        config,
        site,
        SessionLocal,
        use_celery_playwright=use_celery_playwright,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
