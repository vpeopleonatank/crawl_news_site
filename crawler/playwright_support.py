"""Playwright helpers for resolving ThanhNien video streams."""

from __future__ import annotations

import logging
from typing import Dict, List

LOGGER = logging.getLogger(__name__)


class PlaywrightVideoResolverError(RuntimeError):
    """Raised when Playwright cannot initialise or resolve manifests."""


class ThanhnienVideoResolver:
    """Use Playwright to capture HLS manifests loaded by ThanhNien video players."""

    def __init__(self, *, headless: bool = True, timeout: float = 30.0) -> None:
        self._headless = headless
        self._timeout_ms = int(timeout * 1000)
        # Keep a short settle window after DOM load so we observe manifest requests without stalling the crawl.
        self._settle_timeout_ms = min(self._timeout_ms, 5000)
        self._playwright_cm = None
        self._playwright = None
        self._browser = None
        self._context = None
        self._timeout_error_cls = None

    def __enter__(self) -> "ThanhnienVideoResolver":
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise PlaywrightVideoResolverError(
                "Playwright is not installed. Install it with `pip install playwright` and run `playwright install`."
            ) from exc

        self._timeout_error_cls = PlaywrightTimeoutError
        self._playwright_cm = sync_playwright()
        self._playwright = self._playwright_cm.__enter__()
        self._browser = self._playwright.chromium.launch(headless=self._headless)
        self._context = self._browser.new_context()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._context is not None:
            self._context.close()
        if self._browser is not None:
            self._browser.close()
        if self._playwright_cm is not None:
            self._playwright_cm.__exit__(exc_type, exc, tb)

        self._context = None
        self._browser = None
        self._playwright = None
        self._playwright_cm = None
        self._timeout_error_cls = None
        return False

    def resolve_streams(self, article_url: str) -> List[Dict[str, str]]:
        if self._context is None:
            raise PlaywrightVideoResolverError("Resolver must be used as a context manager")

        page = self._context.new_page()
        responses: List[Dict[str, str]] = []
        seen_urls: set[str] = set()

        def handle_response(response) -> None:
            target_url = response.url
            if not target_url.endswith(".mp4.json"):
                return
            if target_url in seen_urls:
                return
            try:
                payload = response.json()
            except Exception as exc:  # pragma: no cover - defensive path
                LOGGER.debug("Failed to parse video manifest %s: %s", target_url, exc)
                return
            seen_urls.add(target_url)
            manifest: Dict[str, str] = {"json_url": target_url}
            if isinstance(payload, dict):
                manifest.update(payload)
            responses.append(manifest)

        page.on("response", handle_response)
        try:
            page.goto(article_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            page.wait_for_timeout(self._settle_timeout_ms)
        except self._timeout_error_cls as exc:
            raise PlaywrightVideoResolverError(f"Timed out while loading {article_url}") from exc
        finally:
            try:
                page.off("response", handle_response)
            except Exception:  # pragma: no cover - API compatibility guard
                pass
            page.close()

        if not responses:
            LOGGER.debug("No video manifests observed for %s", article_url)

        return responses
