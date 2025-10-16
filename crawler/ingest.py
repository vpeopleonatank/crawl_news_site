"""Command-line entrypoint for multi-site article ingestion."""

from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .assets import assets_to_payload
from .config import IngestConfig, ProxyConfig, TimeoutConfig
from .http_client import HttpFetchError, HttpFetcher
from .jobs import ArticleJob, NDJSONJobLoader, load_existing_urls
from .parsers import AssetType, ParsedAsset, ParsingError
from .persistence import ArticlePersistence, ArticlePersistenceError
from .playwright_support import PlaywrightVideoResolverError
from .sites import SiteDefinition, get_site_definition, list_sites
from .storage import StorageMonitor, load_storage_settings
from .tasks import download_assets_task, resolve_video_assets_task
from models import Base

LOGGER = logging.getLogger(__name__)
_FETCH_FAILURE_LOG = "fetch_failures.ndjson"


@dataclass(slots=True)
class IngestionStats:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0


def build_arg_parser() -> argparse.ArgumentParser:
    available_sites = list_sites()
    if not available_sites:
        raise RuntimeError("No sites registered for ingestion")

    parser = argparse.ArgumentParser(description="Ingest articles into PostgreSQL")
    parser.add_argument(
        "--site",
        choices=available_sites,
        default=available_sites[0],
        help="Slug of the news site to ingest",
    )
    parser.add_argument(
        "--jobs-file",
        type=Path,
        default=None,
        help="Path to NDJSON jobs file (defaults to site-specific path)",
    )
    parser.add_argument("--db-url", type=str, required=True, help="SQLAlchemy database URL")
    parser.add_argument(
        "--storage-root",
        type=Path,
        default=IngestConfig().storage_root,
        help="Base directory to store assets",
    )
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
    parser.add_argument(
        "--hls-download-timeout",
        type=float,
        default=TimeoutConfig().hls_download_timeout,
        help="Maximum seconds to allow ffmpeg when downloading HLS video streams (default: 900).",
    )
    parser.add_argument(
        "--sitemap-max-documents",
        type=int,
        default=None,
        help="Maximum number of sitemap documents to process (0 or negative disables the limit; defaults to site configuration).",
    )
    parser.add_argument(
        "--sitemap-max-urls-per-document",
        type=int,
        default=None,
        help="Maximum URLs to read from each sitemap document (0 or negative disables the limit; defaults to site configuration).",
    )
    parser.add_argument(
        "--znews-use-categories",
        action="store_true",
        help="Fetch Znews article URLs via category pagination instead of sitemaps.",
    )
    parser.add_argument(
        "--znews-categories",
        type=str,
        default=None,
        help="Comma-separated list of Znews category slugs to ingest when using category pagination.",
    )
    parser.add_argument(
        "--znews-all-categories",
        action="store_true",
        help="Crawl all known Znews categories (requires category pagination mode).",
    )
    parser.add_argument(
        "--znews-max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to fetch per Znews category (0 or negative disables the limit; default is 50).",
    )
    parser.add_argument(
        "--thanhnien-categories",
        type=str,
        default=None,
        help="Comma-separated list of Thanhnien category slugs to ingest (defaults to curated subset when omitted).",
    )
    parser.add_argument(
        "--thanhnien-all-categories",
        action="store_true",
        help="Crawl all known Thanhnien categories (overrides curated defaults).",
    )
    parser.add_argument(
        "--thanhnien-max-pages",
        type=int,
        default=None,
        help="Maximum number of timeline pages to fetch per Thanhnien category (0 or negative disables the limit; default is 10).",
    )
    parser.add_argument(
        "--thanhnien-max-empty-pages",
        type=int,
        default=None,
        help="Maximum consecutive Thanhnien timeline pages allowed without emitting new articles before stopping (0 or negative disables the guard; default is 2).",
    )
    parser.add_argument(
        "--nld-categories",
        type=str,
        default=None,
        help="Comma-separated list of Nld category slugs to ingest (defaults to curated subset when omitted).",
    )
    parser.add_argument(
        "--nld-all-categories",
        action="store_true",
        help="Crawl all known Nld categories (overrides curated defaults).",
    )
    parser.add_argument(
        "--nld-max-pages",
        type=int,
        default=None,
        help="Maximum number of timeline pages to fetch per Nld category (0 or negative disables the limit; default is unlimited).",
    )
    parser.add_argument(
        "--nld-max-empty-pages",
        type=int,
        default=None,
        help="Maximum consecutive Nld timeline pages without new URLs before stopping (0 or negative disables the guard; default is 1).",
    )
    parser.add_argument(
        "--kenh14-categories",
        type=str,
        default=None,
        help="Comma-separated list of Kenh14 category slugs to ingest (defaults to curated subset when omitted).",
    )
    parser.add_argument(
        "--kenh14-all-categories",
        action="store_true",
        help="Crawl all known Kenh14 categories (overrides curated defaults).",
    )
    parser.add_argument(
        "--kenh14-max-pages",
        type=int,
        default=None,
        help="Maximum number of timeline pages to fetch per Kenh14 category (0 or negative disables the limit; default is 600).",
    )
    parser.add_argument(
        "--kenh14-max-empty-pages",
        type=int,
        default=None,
        help="Maximum consecutive Kenh14 timeline pages without new URLs before stopping (0 or negative disables the guard; default is 3).",
    )
    parser.add_argument(
        "--plo-categories",
        type=str,
        default=None,
        help="Comma-separated list of PLO category slugs to ingest (defaults to curated subset when omitted).",
    )
    parser.add_argument(
        "--plo-all-categories",
        action="store_true",
        help="Crawl all known PLO categories (overrides curated defaults).",
    )
    parser.add_argument(
        "--plo-max-pages",
        type=int,
        default=None,
        help="Maximum number of API pages to fetch per PLO category (0 or negative disables the limit; default is 200).",
    )
    parser.add_argument(
        "--plo-max-empty-pages",
        type=int,
        default=None,
        help="Maximum consecutive PLO API pages without new URLs before stopping (0 or negative disables the guard; default is 3).",
    )
    return parser


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def _parse_proxy_config(args: argparse.Namespace) -> ProxyConfig | None:
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

    return proxy_config


def _parse_category_slugs(raw_value: str | None) -> tuple[str, ...]:
    if not raw_value:
        return ()

    selected: list[str] = []
    for part in raw_value.split(","):
        slug = part.strip().lower()
        if not slug or slug in selected:
            continue
        selected.append(slug)
    return tuple(selected)


def _parse_thanhnien_categories(raw_value: str | None) -> tuple[str, ...]:
    return _parse_category_slugs(raw_value)


def _parse_kenh14_categories(raw_value: str | None) -> tuple[str, ...]:
    return _parse_category_slugs(raw_value)


def _derive_storage_root(base_root: Path, site_slug: str) -> Path:
    """Ensure per-site isolation by appending the site slug when needed."""

    if base_root.name.lower() == site_slug.lower():
        return base_root
    return base_root / site_slug


def build_config(args: argparse.Namespace, site: SiteDefinition) -> IngestConfig:
    jobs_file = args.jobs_file or site.default_jobs_file
    storage_settings = load_storage_settings(args.storage_root)
    storage_volume_path = storage_settings.active_path
    storage_root = _derive_storage_root(storage_volume_path, site.slug)
    config = IngestConfig(
        jobs_file=jobs_file,
        storage_root=storage_root,
        db_url=args.db_url,
        resume=args.resume,
        raw_html_cache_enabled=args.raw_html_cache,
        user_agent=site.default_user_agent,
        log_dir=storage_root / "logs",
    )

    config.storage_volume_name = storage_settings.active_volume
    config.storage_volume_path = storage_volume_path
    config.storage_volumes = storage_settings.volumes
    config.storage_warn_threshold = storage_settings.warn_threshold
    config.storage_pause_file = storage_settings.pause_file

    config.proxy = _parse_proxy_config(args)
    config.rate_limit.max_workers = args.max_workers
    config.ensure_directories()
    config.playwright_enabled = getattr(args, "use_playwright", False)
    config.playwright_timeout = getattr(args, "playwright_timeout", config.playwright_timeout)
    hls_timeout = getattr(args, "hls_download_timeout", config.timeout.hls_download_timeout)
    if hls_timeout and hls_timeout > 0:
        config.timeout.hls_download_timeout = float(hls_timeout)
    config.sitemap_max_documents = _apply_sitemap_limit(
        config.sitemap_max_documents, getattr(args, "sitemap_max_documents", None)
    )
    config.sitemap_max_urls_per_document = _apply_sitemap_limit(
        config.sitemap_max_urls_per_document, getattr(args, "sitemap_max_urls_per_document", None)
    )
    config.jobs_file_provided = args.jobs_file is not None

    if site.slug == "thanhnien":
        config.thanhnien.selected_slugs = _parse_thanhnien_categories(getattr(args, "thanhnien_categories", None))
        config.thanhnien.crawl_all = bool(getattr(args, "thanhnien_all_categories", False))
        config.thanhnien.max_pages = _apply_sitemap_limit(
            config.thanhnien.max_pages, getattr(args, "thanhnien_max_pages", None)
        )
        config.thanhnien.max_empty_pages = _apply_sitemap_limit(
            config.thanhnien.max_empty_pages, getattr(args, "thanhnien_max_empty_pages", None)
        )
    elif site.slug == "znews":
        selected_slugs = _parse_category_slugs(getattr(args, "znews_categories", None))
        config.znews.selected_slugs = selected_slugs
        config.znews.crawl_all = bool(getattr(args, "znews_all_categories", False))
        config.znews.max_pages = _apply_sitemap_limit(
            config.znews.max_pages, getattr(args, "znews_max_pages", None)
        )

        use_categories_flag = bool(getattr(args, "znews_use_categories", False))
        config.znews.use_categories = bool(selected_slugs or config.znews.crawl_all or use_categories_flag)
    elif site.slug == "nld":
        config.nld.selected_slugs = _parse_category_slugs(getattr(args, "nld_categories", None))
        config.nld.crawl_all = bool(getattr(args, "nld_all_categories", False))
        config.nld.max_pages = _apply_sitemap_limit(
            config.nld.max_pages, getattr(args, "nld_max_pages", None)
        )
        config.nld.max_empty_pages = _apply_sitemap_limit(
            config.nld.max_empty_pages, getattr(args, "nld_max_empty_pages", None)
        )
    elif site.slug == "kenh14":
        config.kenh14.selected_slugs = _parse_kenh14_categories(getattr(args, "kenh14_categories", None))
        config.kenh14.crawl_all = bool(getattr(args, "kenh14_all_categories", False))
        config.kenh14.max_pages = _apply_sitemap_limit(
            config.kenh14.max_pages, getattr(args, "kenh14_max_pages", None)
        )
        config.kenh14.max_empty_pages = _apply_sitemap_limit(
            config.kenh14.max_empty_pages, getattr(args, "kenh14_max_empty_pages", None)
        )
    elif site.slug == "plo":
        config.plo.selected_slugs = _parse_category_slugs(getattr(args, "plo_categories", None))
        config.plo.crawl_all = bool(getattr(args, "plo_all_categories", False))
        config.plo.max_pages = _apply_sitemap_limit(config.plo.max_pages, getattr(args, "plo_max_pages", None))
        config.plo.max_empty_pages = _apply_sitemap_limit(
            config.plo.max_empty_pages, getattr(args, "plo_max_empty_pages", None)
        )
    return config


def _apply_sitemap_limit(default_value: int | None, arg_value: int | None) -> int | None:
    if arg_value is None:
        return default_value
    if arg_value <= 0:
        return None
    return arg_value


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
        LOGGER.warning("Failed to record fetch failure for %s: %s", job.url, file_error)


def _build_task_payload(
    config: IngestConfig,
    site: SiteDefinition,
    article_id: str,
    article_url: str,
    assets: list[ParsedAsset],
    *,
    include_playwright: bool,
) -> dict:
    for asset in assets:
        if not asset.referrer:
            asset.referrer = article_url

    payload = {
        "article_id": article_id,
        "db_url": config.db_url,
        "article_url": article_url,
        "site": site.slug,
        "assets": assets_to_payload(assets),
        "config": {
            "storage_root": str(config.storage_root),
            "storage_volume": config.storage_volume_name,
            "storage_volume_root": str(config.storage_volume_path),
            "storage_warn_threshold": config.storage_warn_threshold,
            "storage_pause_file": str(config.storage_pause_file)
            if config.storage_pause_file
            else None,
            "user_agent": config.user_agent,
            "request_timeout": config.timeout.request_timeout,
            "asset_timeout": config.timeout.asset_timeout,
            "hls_download_timeout": config.timeout.hls_download_timeout,
        },
    }

    if config.proxy:
        payload["config"]["proxy"] = {
            "scheme": config.proxy.scheme,
            "host": config.proxy.host,
            "port": config.proxy.port,
            "username": config.proxy.username,
            "password": config.proxy.password,
            "api_key": config.proxy.api_key,
            "change_ip_url": config.proxy.change_ip_url,
            "min_rotation_interval": config.proxy.min_rotation_interval,
        }

    if include_playwright:
        payload["playwright"] = {
            "timeout": config.playwright_timeout,
        }

    return payload


def _update_video_assets_with_playwright(resolver, article_url: str, assets: list[ParsedAsset]) -> None:
    try:
        streams = resolver.resolve_streams(article_url)
    except PlaywrightVideoResolverError as exc:
        LOGGER.warning("Playwright resolver failed for %s: %s", article_url, exc)
        return

    if not streams:
        return

    video_assets = [asset for asset in assets if asset.asset_type == AssetType.VIDEO]
    existing_sequences = {asset.sequence for asset in assets}
    existing_urls = {asset.source_url for asset in video_assets}

    def _select_stream_url(stream: dict) -> str | None:
        if not isinstance(stream, dict):
            return None
        return (
            stream.get("hls")
            or stream.get("mhls")
            or stream.get("url")
            or stream.get("mp4")
        )

    for asset, stream in zip(video_assets, streams):
        url = _select_stream_url(stream)
        if not url:
            continue
        if url != asset.source_url:
            asset.source_url = url
        existing_urls.add(url)

    remaining_streams = []
    if len(streams) > len(video_assets):
        remaining_streams = streams[len(video_assets) :]

    if not video_assets and streams:
        remaining_streams = streams

    if not remaining_streams:
        return

    next_sequence = max(existing_sequences, default=0) + 1
    for stream in remaining_streams:
        url = _select_stream_url(stream)
        if not url or url in existing_urls:
            continue
        assets.append(
            ParsedAsset(
                source_url=url,
                asset_type=AssetType.VIDEO,
                sequence=next_sequence,
            )
        )
        existing_urls.add(url)
        existing_sequences.add(next_sequence)
        next_sequence += 1


def _enqueue_asset_downloads(
    config: IngestConfig,
    site: SiteDefinition,
    article_id: str,
    article_url: str,
    assets: list[ParsedAsset],
    *,
    use_celery_playwright: bool,
) -> None:
    if not assets:
        return

    if not config.db_url:
        raise ValueError("Database URL is required to enqueue asset downloads")

    task_payload = _build_task_payload(
        config,
        site,
        article_id,
        article_url,
        assets,
        include_playwright=use_celery_playwright,
    )

    soft_limit: int | None = None
    hard_limit: int | None = None
    hls_timeout = getattr(config.timeout, "hls_download_timeout", 0)
    if hls_timeout and hls_timeout > 0:
        try:
            base_timeout = int(float(hls_timeout))
        except (TypeError, ValueError):
            base_timeout = 0
        if base_timeout > 0:
            soft_limit = base_timeout + 60
            hard_limit = soft_limit + 60

    signature_kwargs: dict[str, int] = {}
    if soft_limit:
        signature_kwargs["soft_time_limit"] = soft_limit
    if hard_limit:
        signature_kwargs["time_limit"] = hard_limit

    if use_celery_playwright:
        download_sig = download_assets_task.s()
        if signature_kwargs:
            download_sig = download_sig.set(**signature_kwargs)
        pipeline = resolve_video_assets_task.s(task_payload) | download_sig
        pipeline.delay()
    else:
        apply_kwargs: dict[str, int] = {}
        if soft_limit:
            apply_kwargs["soft_time_limit"] = soft_limit
        if hard_limit:
            apply_kwargs["time_limit"] = hard_limit
        download_assets_task.apply_async((task_payload,), **apply_kwargs)
    LOGGER.info("Queued %d assets for article %s", len(assets), article_id)


def _process_job(
    job: ArticleJob,
    *,
    config: IngestConfig,
    site: SiteDefinition,
    persistence: ArticlePersistence,
    use_celery_playwright: bool,
) -> bool:
    LOGGER.info("Processing article %s for site %s", job.url, site.slug)
    parser_impl = site.build_parser()

    with ExitStack() as stack:
        fetcher = stack.enter_context(HttpFetcher(config))
        resolver = None
        if (
            config.playwright_enabled
            and site.playwright_resolver_factory is not None
            and not use_celery_playwright
        ):
            try:
                resolver = stack.enter_context(site.build_playwright_resolver(config.playwright_timeout))
            except PlaywrightVideoResolverError as exc:
                LOGGER.warning(
                    "Playwright resolver initialisation failed for site %s; continuing without it: %s",
                    site.slug,
                    exc,
                )
                resolver = None

        try:
            html, response = fetcher.fetch_html(job.url)
            parsed = parser_impl.parse(job.url, html)
            for asset in parsed.assets:
                if not asset.referrer:
                    asset.referrer = job.url
            fetch_metadata = {
                "status_code": response.status_code,
                "sitemap_url": job.sitemap_url,
                "lastmod": job.lastmod,
            }
            if resolver:
                _update_video_assets_with_playwright(resolver, job.url, parsed.assets)

            result = persistence.upsert_metadata(parsed, site.slug, fetch_metadata)
            article_id = result.article_id

            if config.raw_html_cache_enabled:
                persist_raw_html(config, article_id, html)

            _enqueue_asset_downloads(
                config,
                site,
                article_id,
                job.url,
                parsed.assets,
                use_celery_playwright=use_celery_playwright,
            )

            return True
        except (HttpFetchError, ParsingError, ArticlePersistenceError) as exc:
            LOGGER.error("Failed to process %s: %s", job.url, exc)
            if isinstance(exc, HttpFetchError):
                _record_fetch_failure(config, job, exc)
            return False
        except PlaywrightVideoResolverError as exc:
            LOGGER.error("Playwright resolver error for %s: %s", job.url, exc)
            return False
        except Exception:
            LOGGER.exception("Unhandled error for %s", job.url)
            return False


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
    Base.metadata.create_all(engine)  # ensure required tables exist before queries
    SessionLocal = sessionmaker(bind=engine)

    existing_urls: set[str] = set()
    if config.resume:
        with SessionLocal() as session:
            existing_urls = load_existing_urls(session, site.slug)
        LOGGER.info(
            "Loaded %d existing article URLs for resume mode (site=%s)",
            len(existing_urls),
            site.slug,
        )

    celery_app = download_assets_task.app
    task_always_eager = bool(getattr(celery_app.conf, "task_always_eager", False))
    use_celery_playwright = (
        config.playwright_enabled and site.playwright_resolver_factory is not None and not task_always_eager
    )

    if site.job_loader_factory is not None:
        job_loader = site.job_loader_factory(config, existing_urls)
    else:
        job_loader = NDJSONJobLoader(
            jobs_file=config.jobs_file,
            existing_urls=existing_urls,
            resume=config.resume,
        )

    monitor = StorageMonitor(
        volume_path=config.storage_volume_path,
        pause_file=config.storage_pause_file or (config.storage_volume_path / ".pause_ingest"),
        warn_threshold=config.storage_warn_threshold,
    )

    if monitor.check_and_maybe_pause():
        LOGGER.warning("Storage pause sentinel present; stop ingestion before processing jobs.")
        return 0

    persistence = ArticlePersistence(
        session_factory=SessionLocal,
        storage_root=config.storage_root,
        storage_volume_name=config.storage_volume_name,
        storage_volume_path=config.storage_volume_path,
    )
    stats = IngestionStats()

    max_workers = max(1, config.rate_limit.max_workers)
    future_to_job: dict[Future[bool], ArticleJob] = {}

    def _drain_completed(*, block_until_empty: bool) -> None:
        while future_to_job:
            pending = tuple(future_to_job.keys())
            if not pending:
                return
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            if not done:
                return
            for finished in done:
                job = future_to_job.pop(finished, None)
                if job is None:
                    continue
                try:
                    succeeded = finished.result()
                except Exception:  # pragma: no cover - defensive guard
                    LOGGER.exception("Worker raised unexpectedly for %s", job.url)
                    stats.failed += 1
                    continue

                if succeeded:
                    stats.succeeded += 1
                else:
                    stats.failed += 1
            if not block_until_empty:
                break

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for job in job_loader:
            if monitor.check_and_maybe_pause():
                LOGGER.warning("Storage threshold reached; pausing ingestion before scheduling additional jobs")
                break
            stats.processed += 1
            future = executor.submit(
                _process_job,
                job,
                config=config,
                site=site,
                persistence=persistence,
                use_celery_playwright=use_celery_playwright,
            )
            future_to_job[future] = job
            if len(future_to_job) >= max_workers:
                _drain_completed(block_until_empty=False)

        _drain_completed(block_until_empty=True)

    LOGGER.info(
        "Processed %d jobs for site %s: %d succeeded, %d failed, %d skipped by loader",
        stats.processed,
        site.slug,
        stats.succeeded,
        stats.failed,
        job_loader.stats.skipped_existing + job_loader.stats.skipped_invalid + job_loader.stats.skipped_duplicate,
    )

    return 0 if stats.failed == 0 else 1


__all__ = [
    "build_arg_parser",
    "build_config",
    "configure_logging",
    "main",
    "persist_raw_html",
    "_build_task_payload",
    "_enqueue_asset_downloads",
    "_record_fetch_failure",
    "_update_video_assets_with_playwright",
]


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
