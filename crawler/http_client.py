"""HTTP utilities for fetching article content."""

from __future__ import annotations

import httpx

from .config import IngestConfig


class HttpFetchError(RuntimeError):
    """Raised when an HTTP request fails irrecoverably."""


class HttpFetcher:
    """Lightweight HTTP client with sane defaults for crawling."""

    def __init__(self, config: IngestConfig) -> None:
        self._config = config
        self._client = httpx.Client(
            timeout=config.timeout.request_timeout,
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
        )

    def fetch_html(self, url: str) -> tuple[str, httpx.Response]:
        try:
            response = self._client.get(url)
        except httpx.HTTPError as exc:  # pragma: no cover - network failure path
            raise HttpFetchError(str(exc)) from exc

        if response.status_code != httpx.codes.OK:
            raise HttpFetchError(f"Unexpected status {response.status_code} for {url}")

        content_type = response.headers.get("content-type", "")
        if "html" not in content_type:
            raise HttpFetchError(f"Unsupported content type '{content_type}' for {url}")

        return response.text, response

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpFetcher":  # pragma: no cover - convenience wrapper
        return self

    def __exit__(self, *_exc_info) -> None:  # pragma: no cover - convenience wrapper
        self.close()
