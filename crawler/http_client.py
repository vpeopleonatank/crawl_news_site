"""HTTP utilities for fetching article content."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import httpx

from .config import IngestConfig, ProxyConfig

LOGGER = logging.getLogger(__name__)

_BLOCK_STATUS_CODES = {
    httpx.codes.FORBIDDEN,
    httpx.codes.TOO_MANY_REQUESTS,
    httpx.codes.SERVICE_UNAVAILABLE,
}


class HttpFetchError(RuntimeError):
    """Raised when an HTTP request fails irrecoverably."""


class ProxyRotator:
    """Encapsulates proxy IP rotation via an external API."""

    def __init__(
        self,
        proxy_config: ProxyConfig,
        *,
        time_source: Callable[[], float] | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self._config = proxy_config
        self._time_source = time_source or time.monotonic
        self._client = client or httpx.Client(timeout=10.0)
        self._owns_client = client is None
        self._lock = threading.Lock()
        self._last_rotation_at: float | None = None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def should_rotate_response(self, response: httpx.Response) -> bool:
        return response.status_code in _BLOCK_STATUS_CODES

    def rotate(self) -> bool:
        change_url = self._config.change_ip_url
        if not change_url:
            return False

        now = self._time_source()
        with self._lock:
            if (
                self._last_rotation_at is not None
                and now - self._last_rotation_at < self._config.min_rotation_interval
            ):
                LOGGER.debug(
                    "Skipping proxy rotation; only %.2fs elapsed",
                    now - self._last_rotation_at,
                )
                return False

            params = {}
            if self._config.api_key:
                params["key"] = self._config.api_key

            try:
                response = self._client.get(change_url, params=params)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                LOGGER.warning("Proxy rotation request failed: %s", exc)
                return False

            payload: object
            try:
                payload = response.json()
            except ValueError:
                payload = None

            if isinstance(payload, dict) and payload.get("status") == "error":
                LOGGER.warning("Proxy rotation endpoint reported error: %s", payload)
                return False

            self._last_rotation_at = now
            LOGGER.info("Proxy rotation requested successfully")
            return True


class HttpFetcher:
    """Lightweight HTTP client with sane defaults for crawling."""

    def __init__(
        self,
        config: IngestConfig,
        *,
        client: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        rotator: ProxyRotator | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._client = client or self._build_client()
        self._owns_client = client is None
        self._rotator = rotator
        if self._rotator is None and config.proxy:
            self._rotator = ProxyRotator(config.proxy)

    def _build_client(self) -> httpx.Client:
        timeout = self._config.timeout.request_timeout
        headers = {"User-Agent": self._config.user_agent}
        kwargs: dict[str, object] = {
            "timeout": timeout,
            "headers": headers,
            "follow_redirects": True,
        }
        proxy_url: str | None = None
        if self._config.proxy:
            proxy_url = self._config.proxy.httpx_proxy()
        if proxy_url:
            kwargs["proxies"] = proxy_url
        if self._transport:
            kwargs["transport"] = self._transport
        return httpx.Client(**kwargs)

    def _reset_client(self) -> None:
        if not self._owns_client:
            return
        self._client.close()
        self._client = self._build_client()

    def fetch_html(self, url: str) -> tuple[str, httpx.Response]:
        attempts_remaining = 2
        while attempts_remaining:
            attempts_remaining -= 1
            try:
                response = self._client.get(url)
            except httpx.HTTPError as exc:  # pragma: no cover - network failure path
                raise HttpFetchError(str(exc)) from exc

            if response.status_code == httpx.codes.OK:
                content_type = response.headers.get("content-type", "")
                if "html" not in content_type:
                    raise HttpFetchError(f"Unsupported content type '{content_type}' for {url}")
                return response.text, response

            handled_block = False
            if self._rotator and self._rotator.should_rotate_response(response):
                if self._rotator.rotate():
                    handled_block = True
                    self._reset_client()

            if handled_block and attempts_remaining:
                continue

            raise HttpFetchError(f"Unexpected status {response.status_code} for {url}")

        raise HttpFetchError("Exhausted retries while fetching HTML")

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
        if self._rotator:
            self._rotator.close()

    def __enter__(self) -> "HttpFetcher":  # pragma: no cover - convenience wrapper
        return self

    def __exit__(self, *_exc_info) -> None:  # pragma: no cover - convenience wrapper
        self.close()
