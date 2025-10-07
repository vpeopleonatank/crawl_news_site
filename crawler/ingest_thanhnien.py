"""Command-line entrypoint for Thanhnien article ingestion."""

from __future__ import annotations

import argparse
import json
import logging
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .assets import assets_to_payload
from .config import IngestConfig, ProxyConfig
from .http_client import HttpFetchError, HttpFetcher
from .jobs import NDJSONJobLoader, load_existing_urls
from .parsers import AssetType, ParsedAsset, ParsingError
from .parsers.thanhnien import ThanhnienParser
from .persistence import ArticlePersistence, ArticlePersistenceError
from .playwright_support import (
    PlaywrightVideoResolverError,
    ThanhnienVideoResolver,
)
from .tasks import download_assets_task
from models import Base

LOGGER = logging.getLogger(__name__)
_FETCH_FAILURE_LOG = "fetch_failures.ndjson"


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
    parser.add_argument("--proxy", type=str, help="Proxy endpoint in ip:port[:key] format")
    parser.add_argument("--proxy-scheme", type=str, default="http", help="Proxy scheme (default: http)")
    parser.add_argument("--proxy-change-url", type=str, help="API endpoint to trigger proxy IP rotation")
    parser.add_argument("--proxy-key", type=str, help="Override proxy rotation key if not embedded in --proxy")
    parser.add_argument(
        "--proxy-rotation-interval",
        type=float,
        default=240.0,
        help="Minimum seconds between proxy rotation requests",
    )
    parser.add_argument(
        "--use-playwright",
        action="store_true",
        help="Resolve video manifests with Playwright before downloading",
    )
    parser.add_argument(
        "--playwright-timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for Playwright video manifest responses",
    )
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
    proxy_value = getattr(args, "proxy", None)
    proxy_change_url = getattr(args, "proxy_change_url", None)
    proxy_key = getattr(args, "proxy_key", None)
    proxy_scheme = getattr(args, "proxy_scheme", "http")
    proxy_interval = getattr(args, "proxy_rotation_interval", 240.0)

    proxy_config: ProxyConfig | None = None
    if proxy_value:
        try:
            proxy_config = ProxyConfig.from_endpoint(
                proxy_value,
                scheme=proxy_scheme,
                change_ip_url=proxy_change_url,
                min_rotation_interval=proxy_interval,
                api_key=proxy_key,
            )
        except ValueError as exc:
            raise ValueError(f"Invalid proxy configuration: {exc}") from exc
    elif proxy_change_url or proxy_key:
        proxy_config = ProxyConfig(
            scheme=proxy_scheme,
            api_key=proxy_key,
            change_ip_url=proxy_change_url,
            min_rotation_interval=proxy_interval,
        )

    config.proxy = proxy_config
    config.rate_limit.max_workers = args.max_workers
    config.ensure_directories()
    config.playwright_enabled = getattr(args, "use_playwright", False)
    config.playwright_timeout = getattr(args, "playwright_timeout", config.playwright_timeout)
    return config


def persist_raw_html(config: IngestConfig, article_id: str, html: str) -> None:
    raw_path = config.raw_html_path(article_id)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(html, encoding="utf-8")


def _record_fetch_failure(config: IngestConfig, job: ArticleJob, exc: Exception) -> None:
    payload = {
        "url": job.url,
        "sitemap_url": job.sitemap_url,
        "lastmod": job.lastmod,
        "error": str(exc),
        "error_type": type(exc).__name__,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    log_path = config.log_dir / _FETCH_FAILURE_LOG
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError as file_error:  # pragma: no cover - filesystem failure path
        LOGGER.warning(
            "Failed to record fetch failure for %s: %s", job.url, file_error
        )


def _update_video_assets_with_playwright(
    resolver: ThanhnienVideoResolver | None,
    article_url: str,
    assets: list[ParsedAsset],
) -> None:
    if resolver is None:
        return

    video_assets = [asset for asset in assets if asset.asset_type == AssetType.VIDEO]
    if not video_assets:
        return

    try:
        streams = resolver.resolve_streams(article_url)
    except PlaywrightVideoResolverError as exc:
        LOGGER.warning("Playwright failed to resolve video streams for %s: %s", article_url, exc)
        return

    if not streams:
        LOGGER.debug("No video manifests detected for %s via Playwright", article_url)
        return

    for asset, stream in zip(video_assets, streams):
        hls_url = stream.get("hls") or stream.get("mhls")
        if hls_url:
            LOGGER.debug("Resolved HLS manifest %s for %s", hls_url, article_url)
            asset.source_url = hls_url


def _build_task_payload(config: IngestConfig, article_id: str, assets: list[ParsedAsset]) -> dict:
    payload = {
        "article_id": article_id,
        "db_url": config.db_url,
        "assets": assets_to_payload(assets),
        "config": {
            "storage_root": str(config.storage_root),
            "user_agent": config.user_agent,
            "request_timeout": config.timeout.request_timeout,
            "asset_timeout": config.timeout.asset_timeout,
        },
    }

    if config.proxy:
        payload["config"]["proxy"] = {
            "scheme": config.proxy.scheme,
            "host": config.proxy.host,
            "port": config.proxy.port,
            "api_key": config.proxy.api_key,
            "change_ip_url": config.proxy.change_ip_url,
            "min_rotation_interval": config.proxy.min_rotation_interval,
        }

    return payload


def _enqueue_asset_downloads(config: IngestConfig, article_id: str, assets: list[ParsedAsset]) -> None:
    if not assets:
        return

    if not config.db_url:
        raise ValueError("Database URL is required to enqueue asset downloads")

    task_payload = _build_task_payload(config, article_id, assets)
    download_assets_task.delay(task_payload)
    LOGGER.info("Queued %d assets for article %s", len(assets), article_id)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        config = build_config(args)
    except ValueError as exc:
        parser.error(str(exc))

    if not config.db_url:
        parser.error("--db-url is required")

    engine = create_engine(config.db_url)
    Base.metadata.create_all(engine)  # ensure required tables exist before queries
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

    with ExitStack() as stack:
        fetcher = stack.enter_context(HttpFetcher(config))
        video_resolver: ThanhnienVideoResolver | None = None
        if config.playwright_enabled:
            try:
                video_resolver = stack.enter_context(
                    ThanhnienVideoResolver(timeout=config.playwright_timeout)
                )
            except PlaywrightVideoResolverError as exc:
                LOGGER.error("Unable to initialize Playwright resolver: %s", exc)
                video_resolver = None

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

                if config.playwright_enabled and video_resolver:
                    _update_video_assets_with_playwright(video_resolver, job.url, parsed.assets)

                _enqueue_asset_downloads(config, article_id, parsed.assets)

                stats.succeeded += 1
            except (HttpFetchError, ParsingError, ArticlePersistenceError) as exc:
                stats.failed += 1
                LOGGER.error("Failed to process %s: %s", job.url, exc)
                if isinstance(exc, HttpFetchError):
                    _record_fetch_failure(config, job, exc)
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
