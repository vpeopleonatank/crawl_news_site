"""Celery tasks for asynchronous asset downloading."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from celery import Task
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .assets import (
    AssetDownloadError,
    AssetManager,
    assets_from_payload,
    assets_to_payload,
)
from .celery_app import celery_app
from .config import IngestConfig, ProxyConfig, TimeoutConfig
from .persistence import ArticlePersistence, ArticlePersistenceError
from .parsers import AssetType
from .playwright_support import PlaywrightVideoResolverError
from .sites import get_site_definition


LOGGER = logging.getLogger(__name__)


def _build_config(config_payload: Mapping[str, Any]) -> IngestConfig:
    storage_root = Path(config_payload["storage_root"])
    timeout = TimeoutConfig(
        request_timeout=float(config_payload.get("request_timeout", TimeoutConfig().request_timeout)),
        asset_timeout=float(config_payload.get("asset_timeout", TimeoutConfig().asset_timeout)),
    )

    config = IngestConfig(
        storage_root=storage_root,
        user_agent=str(config_payload.get("user_agent", IngestConfig().user_agent)),
        timeout=timeout,
    )

    proxy_payload = config_payload.get("proxy")
    if isinstance(proxy_payload, Mapping) and proxy_payload:
        config.proxy = ProxyConfig(
            scheme=str(proxy_payload.get("scheme", "http")),
            host=proxy_payload.get("host"),
            port=proxy_payload.get("port"),
            username=proxy_payload.get("username"),
            password=proxy_payload.get("password"),
            api_key=proxy_payload.get("api_key"),
            change_ip_url=proxy_payload.get("change_ip_url"),
            min_rotation_interval=float(
                proxy_payload.get(
                    "min_rotation_interval",
                    ProxyConfig().min_rotation_interval,
                )
            ),
        )

    config.ensure_directories()
    return config


@lru_cache(maxsize=8)
def _session_factory(db_url: str):
    engine = create_engine(db_url)
    return sessionmaker(bind=engine)


@celery_app.task(name="crawler.resolve_video_assets", bind=True, autoretry_for=(Exception,), retry_backoff=True)
def resolve_video_assets_task(self: Task, job: Mapping[str, Any]) -> Mapping[str, Any]:
    article_id = str(job["article_id"])
    article_url = job.get("article_url")
    assets_payload = job.get("assets") or []

    if not article_url or not assets_payload:
        LOGGER.debug("Resolver skipped for article %s due to missing URL/assets", article_id)
        return job

    assets = assets_from_payload(assets_payload)
    video_assets = [asset for asset in assets if asset.asset_type == AssetType.VIDEO]
    if not video_assets:
        LOGGER.debug("No video assets for article %s; skipping Playwright resolution", article_id)
        return job

    timeout = IngestConfig().playwright_timeout
    playwright_cfg = job.get("playwright") or {}
    try:
        timeout = float(playwright_cfg.get("timeout", timeout))
    except (TypeError, ValueError):
        LOGGER.debug("Invalid Playwright timeout %r; falling back to default %s", playwright_cfg.get("timeout"), timeout)

    site_slug = str(job.get("site") or "thanhnien")
    try:
        site = get_site_definition(site_slug)
    except KeyError:
        LOGGER.warning("Skipping Playwright resolution for unknown site %s (article %s)", site_slug, article_id)
        return job

    if not site.playwright_resolver_factory:
        LOGGER.debug("Site %s has no Playwright resolver; skipping article %s", site.slug, article_id)
        return job

    try:
        with site.build_playwright_resolver(timeout) as resolver:
            streams = resolver.resolve_streams(article_url)
    except PlaywrightVideoResolverError as exc:
        LOGGER.warning("Playwright failed to resolve streams for article %s: %s", article_id, exc)
        return job

    if not streams:
        LOGGER.debug("Playwright returned no streams for article %s", article_id)
        return job

    updated = False
    for asset, stream in zip(video_assets, streams):
        hls_url = stream.get("hls") or stream.get("mhls")
        if hls_url and hls_url != asset.source_url:
            asset.source_url = hls_url
            updated = True

    if updated:
        LOGGER.info("Updated video assets for article %s via Playwright", article_id)
        job = dict(job)
        job["assets"] = assets_to_payload(assets)
    else:
        LOGGER.debug("No video asset changes for article %s after Playwright resolution", article_id)

    return job


@celery_app.task(name="crawler.download_assets", bind=True, autoretry_for=(Exception,), retry_backoff=True)
def download_assets_task(self: Task, job: Mapping[str, Any]) -> dict[str, Any]:
    article_id = str(job["article_id"])
    assets_payload = job.get("assets", [])
    if not assets_payload:
        LOGGER.info("No assets to download for article %s", article_id)
        return {"status": "skipped", "reason": "no_assets"}

    try:
        config = _build_config(job["config"])
        db_url = str(job["db_url"])
        session_factory = _session_factory(db_url)
        assets = assets_from_payload(assets_payload)

        with AssetManager(config) as manager:
            stored_assets = manager.download_assets(article_id, assets)

        persistence = ArticlePersistence(session_factory=session_factory, storage_root=config.storage_root)
        persistence.persist_assets(article_id, stored_assets)

        LOGGER.info(
            "Downloaded %d assets for article %s", len(stored_assets), article_id
        )
        return {"status": "ok", "assets": len(stored_assets)}
    except (AssetDownloadError, ArticlePersistenceError) as exc:
        LOGGER.exception("Download task failed for article %s", article_id)
        raise self.retry(exc=exc)
    except Exception as exc:  # pragma: no cover - unexpected failure
        LOGGER.exception("Unexpected error in download task for article %s", article_id)
        raise exc


__all__ = ["resolve_video_assets_task", "download_assets_task"]
