"""Asset download and storage utilities."""

from __future__ import annotations

import hashlib
import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from urllib.parse import urlsplit

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

            resolved_url = asset.source_url
            if asset.asset_type == AssetType.VIDEO:
                resolved_url = self._resolve_video_source(resolved_url)
                asset.source_url = resolved_url

            seen_sources.add(resolved_url)
            if asset.asset_type == AssetType.IMAGE:
                target_dir = image_root
                extension = self._extension_from_url(resolved_url, default="jpg")
                filename = f"{asset.sequence:03d}.{extension}"
                target_path = target_dir / filename
                checksum, bytes_written = self._stream_to_file(resolved_url, target_path)
            else:
                target_dir = video_root
                if self._is_hls_manifest(resolved_url):
                    filename = f"{asset.sequence:03d}.mp4"
                    target_path = target_dir / filename
                    checksum, bytes_written = self._download_hls(resolved_url, target_path)
                else:
                    extension = self._extension_from_url(resolved_url, default="mp4")
                    filename = f"{asset.sequence:03d}.{extension}"
                    target_path = target_dir / filename
                    checksum, bytes_written = self._stream_to_file(resolved_url, target_path)
            stored.append(
                StoredAsset(
                    source=asset,
                    path=target_path,
                    checksum=checksum,
                    bytes_downloaded=bytes_written,
                )
            )
        return stored

    @staticmethod
    def _is_hls_manifest(url: str) -> bool:
        path = urlsplit(url).path
        return path.endswith(".m3u8")

    def _resolve_video_source(self, url: str) -> str:
        if self._is_hls_manifest(url):
            return url

        parsed = urlsplit(url)
        if "thanhnien.mediacdn.vn" not in parsed.netloc:
            return url

        if not parsed.path.endswith(".mp4"):
            return url

        manifest_url = url + ".json"
        try:
            response = self._client.get(manifest_url, timeout=self._config.timeout.asset_timeout)
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network failure path
            LOGGER.debug("Failed to fetch manifest %s: %s", manifest_url, exc)
            return url

        try:
            payload = response.json()
        except ValueError:
            LOGGER.debug("Manifest %s returned non-JSON payload", manifest_url)
            return url

        if isinstance(payload, dict):
            hls_url = payload.get("hls") or payload.get("mhls")
            if hls_url:
                LOGGER.debug("Resolved Thanhnien HLS %s from manifest %s", hls_url, manifest_url)
                return hls_url

        return url

    def _download_hls(self, manifest_url: str, target: Path) -> tuple[str, int]:
        ffmpeg_path = shutil.which("ffmpeg")
        if not ffmpeg_path:
            raise AssetDownloadError("ffmpeg is required to download HLS streams")

        temporary_target = target.with_name(target.name + ".tmp")
        LOGGER.debug("Downloading HLS stream %s to %s", manifest_url, target)
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            manifest_url,
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            "-f",
            "mp4",
            str(temporary_target),
        ]

        try:
            subprocess.run(command, check=True, capture_output=True)
        except subprocess.CalledProcessError as exc:
            if temporary_target.exists():
                temporary_target.unlink(missing_ok=True)
            stderr_output = exc.stderr.decode(errors="ignore") if exc.stderr else ""
            message = stderr_output.strip() or str(exc)
            raise AssetDownloadError(f"ffmpeg failed to process {manifest_url}: {message}") from exc

        temporary_target.replace(target)
        return self._hash_file(target)

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

    @staticmethod
    def _hash_file(path: Path) -> tuple[str, int]:
        hasher = hashlib.sha256()
        bytes_written = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                if not chunk:
                    break
                hasher.update(chunk)
                bytes_written += len(chunk)

        if bytes_written == 0:
            raise AssetDownloadError(f"Downloaded file {path} is empty")

        return hasher.hexdigest(), bytes_written


def asset_to_payload(asset: ParsedAsset) -> dict[str, str | int | None]:
    """Serialize a parsed asset into a queue-friendly payload."""

    return {
        "source_url": asset.source_url,
        "asset_type": asset.asset_type.value,
        "sequence": asset.sequence,
        "caption": asset.caption,
    }


def assets_to_payload(assets: Sequence[ParsedAsset]) -> list[dict[str, str | int | None]]:
    """Serialize a sequence of parsed assets."""

    return [asset_to_payload(asset) for asset in ensure_asset_sequence(assets)]


def asset_from_payload(payload: Mapping[str, object]) -> ParsedAsset:
    """Reconstruct a ParsedAsset instance from serialized payload."""

    return ParsedAsset(
        source_url=str(payload["source_url"]),
        asset_type=AssetType(payload["asset_type"]),
        sequence=int(payload["sequence"]),
        caption=payload.get("caption") or None,
    )


def assets_from_payload(payloads: Sequence[Mapping[str, object]]) -> list[ParsedAsset]:
    """Reconstruct a sorted ParsedAsset list from serialized payloads."""

    reconstructed = [asset_from_payload(item) for item in payloads]
    return ensure_asset_sequence(reconstructed)
