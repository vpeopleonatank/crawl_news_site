"""Asset download and storage utilities."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx

from .config import IngestConfig
from .parsers import AssetType, ParsedAsset, ensure_asset_sequence


class AssetDownloadError(RuntimeError):
    """Raised when an asset cannot be downloaded or persisted."""


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StoredAsset:
    source: ParsedAsset
    path: Path
    checksum: str
    bytes_downloaded: int


class AssetManager:
    """Download and persist media assets referenced by an article."""

    def __init__(self, config: IngestConfig, client: httpx.Client | None = None) -> None:
        self._config = config
        if client is None:
            self._client = httpx.Client(
                timeout=config.timeout.asset_timeout,
                headers={"User-Agent": config.user_agent},
                follow_redirects=True,
            )
            self._owns_client = True
        else:
            self._client = client
            self._owns_client = False

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "AssetManager":  # pragma: no cover - convenience wrapper
        return self

    def __exit__(self, *_exc_info) -> None:  # pragma: no cover - convenience wrapper
        self.close()

    def download_assets(self, article_id: str, assets: Iterable[ParsedAsset]) -> list[StoredAsset]:
        storage_root = self._config.article_asset_root(article_id)
        image_root = storage_root / "images"
        video_root = storage_root / "videos"
        image_root.mkdir(parents=True, exist_ok=True)
        video_root.mkdir(parents=True, exist_ok=True)

        stored: list[StoredAsset] = []
        seen_sources: set[str] = set()
        for asset in ensure_asset_sequence(assets):
            if asset.source_url.startswith("data:"):
                LOGGER.debug("Skipping inline data URI for article %s", article_id)
                continue

            if asset.source_url in seen_sources:
                LOGGER.debug("Skipping duplicate asset URL %s for article %s", asset.source_url, article_id)
                continue

            seen_sources.add(asset.source_url)
            if asset.asset_type == AssetType.IMAGE:
                target_dir = image_root
                extension = self._extension_from_url(asset.source_url, default="jpg")
            else:
                target_dir = video_root
                extension = self._extension_from_url(asset.source_url, default="mp4")

            filename = f"{asset.sequence:03d}.{extension}"
            target_path = target_dir / filename
            checksum, bytes_written = self._stream_to_file(asset.source_url, target_path)
            stored.append(
                StoredAsset(
                    source=asset,
                    path=target_path,
                    checksum=checksum,
                    bytes_downloaded=bytes_written,
                )
            )
        return stored

    def _stream_to_file(self, url: str, target: Path) -> tuple[str, int]:
        hasher = hashlib.sha256()
        bytes_written = 0
        try:
            with self._client.stream("GET", url) as response:
                response.raise_for_status()
                with target.open("wb") as handle:
                    for chunk in response.iter_bytes():
                        if not chunk:
                            continue
                        handle.write(chunk)
                        hasher.update(chunk)
                        bytes_written += len(chunk)
        except httpx.HTTPError as exc:  # pragma: no cover - network failure path
            if target.exists():
                target.unlink(missing_ok=True)
            raise AssetDownloadError(str(exc)) from exc
        if bytes_written == 0:
            target.unlink(missing_ok=True)
            raise AssetDownloadError(f"Empty response body for {url}")

        return hasher.hexdigest(), bytes_written

    @staticmethod
    def _extension_from_url(url: str, default: str) -> str:
        suffix = url.split("?")[0].split("#")[0].rsplit(".", 1)
        if len(suffix) == 2 and suffix[1]:
            return suffix[1].lower()
        return default
