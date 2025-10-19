"""Job management utilities for article ingestion."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Protocol, Sequence
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import IngestConfig, ProxyConfig
from models import Article

LOGGER = logging.getLogger(__name__)
_SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
}


@dataclass(slots=True)
class ArticleJob:
    url: str
    lastmod: str | None
    sitemap_url: str | None
    image_url: str | None
    category_slug: str | None = None


@dataclass(slots=True)
class JobLoaderStats:
    total: int = 0
    emitted: int = 0
    skipped_existing: int = 0
    skipped_invalid: int = 0
    skipped_duplicate: int = 0


_THANHNIEN_BASE_URL = "https://thanhnien.vn"
_THANHNIEN_ARTICLE_PATTERN = re.compile(r"^https?://(?:[^./]+\.)?thanhnien\.vn/[^?#]+-185\d+\.htm$")
_KENH14_BASE_URL = "https://kenh14.vn"
_KENH14_ARTICLE_PATTERN = re.compile(
    r"^https?://(?:[^./]+\.)?kenh14\.vn/[^?#]+-\d{6,}\.chn$",
    re.IGNORECASE,
)
_NLD_BASE_URL = "https://nld.com.vn"
_NLD_ARTICLE_PATTERN = re.compile(
    r"^https?://(?:[^./]+\.)?nld\.com\.vn/[^?#]+-\d{6,}\.htm$",
    re.IGNORECASE,
)
_PLO_BASE_URL = "https://plo.vn"
_PLO_API_BASE = "https://api.plo.vn"
_PLO_ARTICLE_PATTERN = re.compile(r"^https?://(?:[^./]+\.)?plo\.vn/[^?#]+-post\d+\.html$", re.IGNORECASE)


@dataclass(slots=True)
class ThanhnienCategoryDefinition:
    slug: str
    name: str
    category_id: int
    landing_url: str

    def normalized_landing_url(self) -> str:
        return _normalize_thanhnien_url(self.landing_url)

    def timeline_url(self, page: int) -> str:
        return f"{_THANHNIEN_BASE_URL}/timelinelist/{self.category_id}/{page}.htm"


@dataclass(slots=True)
class Kenh14CategoryDefinition:
    slug: str
    name: str
    timeline_id: int
    landing_url: str

    def normalized_landing_url(self) -> str:
        return _normalize_kenh14_url(self.landing_url)

    def timeline_url(self, page: int) -> str:
        return f"{_KENH14_BASE_URL}/timeline/laytinmoitronglist-{self.timeline_id}/page-{page}.chn"


@dataclass(slots=True)
class NldCategoryDefinition:
    slug: str
    name: str
    category_id: int
    landing_url: str

    def normalized_landing_url(self) -> str:
        return _normalize_nld_url(self.landing_url)

    def timeline_url(self, page: int) -> str:
        return f"{_NLD_BASE_URL}/timelinelist/{self.category_id}/{page}.htm"


class NldCategoryLoader:
    """Iterate Nld category landing pages and timeline endpoints."""

    def __init__(
        self,
        categories: Sequence[NldCategoryDefinition],
        *,
        existing_urls: set[str] | None = None,
        resume: bool = False,
        user_agent: str | None = None,
        max_pages: int | None = None,
        max_empty_pages: int | None = 1,
        request_timeout: float = 5.0,
        include_landing_page: bool = True,
        duplicate_fingerprint_size: int = 5,
        stop_on_duplicate: bool = True,
        proxy: ProxyConfig | None = None,
        max_fetch_attempts: int = 3,
        fetch_retry_backoff: float = 1.0,
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._max_empty_pages = max_empty_pages
        self._request_timeout = request_timeout
        self._include_landing_page = include_landing_page
        self._duplicate_fingerprint_size = max(1, duplicate_fingerprint_size)
        self._stop_on_duplicate = stop_on_duplicate
        self._proxy = proxy
        self._max_fetch_attempts = max(1, int(max_fetch_attempts))
        self._fetch_retry_backoff = max(0.0, float(fetch_retry_backoff))

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        proxy_url = self._proxy.httpx_proxy() if self._proxy else None
        client_kwargs: dict[str, object] = {
            "headers": headers,
            "timeout": self._request_timeout,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        with httpx.Client(**client_kwargs) as client:
            for category in self._categories:
                yield from self._iterate_category(client, category)

    def _iterate_category(self, client: httpx.Client, category: NldCategoryDefinition) -> Iterator[ArticleJob]:
        emitted_before = self.stats.emitted
        skipped_existing_before = self.stats.skipped_existing
        skipped_duplicate_before = self.stats.skipped_duplicate

        if self._include_landing_page:
            landing_html = self._fetch_html(client, category.normalized_landing_url())
            if landing_html:
                yield from self._emit_jobs_from_html(landing_html, category_slug=category.slug)

        page = 1
        consecutive_empty_pages = 0
        previous_fingerprint: list[str] | None = None

        while True:
            if self._max_pages is not None and page > self._max_pages:
                break

            timeline_url = category.timeline_url(page)
            html = self._fetch_html(client, timeline_url)
            if not html:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
                page += 1
                continue

            urls = self._extract_article_urls(html)
            if not urls:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
                page += 1
                continue

            consecutive_empty_pages = 0
            fingerprint = urls[: self._duplicate_fingerprint_size]
            if (
                self._stop_on_duplicate
                and previous_fingerprint is not None
                and fingerprint
                and fingerprint == previous_fingerprint
            ):
                LOGGER.info(
                    "Nld category '%s': detected duplicate pagination at %s; stopping.",
                    category.slug,
                    timeline_url,
                )
                break
            if fingerprint:
                previous_fingerprint = fingerprint

            for job in self._emit_jobs_from_urls(urls):
                yield job

            page += 1

        LOGGER.info(
            "Nld category '%s': emitted=%d skipped_existing=%d skipped_duplicate=%d",
            category.slug,
            self.stats.emitted - emitted_before,
            self.stats.skipped_existing - skipped_existing_before,
            self.stats.skipped_duplicate - skipped_duplicate_before,
        )

    def _fetch_html(self, client: httpx.Client, url: str) -> str:
        for attempt in range(self._max_fetch_attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "Nld category request timeout (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPStatusError as exc:
                LOGGER.warning("Nld category request failed (%s): %s", url, exc)
                if (
                    exc.response is not None
                    and 500 <= exc.response.status_code < 600
                    and self._should_retry(attempt)
                ):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPError as exc:
                LOGGER.warning(
                    "Nld category request error (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            return response.text.strip()
        return ""

    def _should_retry(self, attempt: int) -> bool:
        return attempt + 1 < self._max_fetch_attempts

    def _sleep_before_retry(self, attempt: int) -> None:
        if self._fetch_retry_backoff <= 0:
            return
        delay = self._fetch_retry_backoff * (2 ** attempt)
        time.sleep(delay)

    def _emit_jobs_from_html(self, html: str) -> Iterator[ArticleJob]:
        for url in self._extract_article_urls(html):
            self.stats.total += 1

            if url in self._seen_urls:
                self.stats.skipped_duplicate += 1
                continue
            self._seen_urls.add(url)

            if self._resume and url in self._existing_urls:
                self.stats.skipped_existing += 1
                continue

            job = ArticleJob(url=url, lastmod=None, sitemap_url=None, image_url=None)
            self.stats.emitted += 1
            yield job

    def _emit_jobs_from_urls(self, urls: Sequence[str]) -> Iterator[ArticleJob]:
        for url in urls:
            self.stats.total += 1

            if url in self._seen_urls:
                self.stats.skipped_duplicate += 1
                continue
            self._seen_urls.add(url)

            if self._resume and url in self._existing_urls:
                self.stats.skipped_existing += 1
                continue

            job = ArticleJob(url=url, lastmod=None, sitemap_url=None, image_url=None)
            self.stats.emitted += 1
            yield job

    def _extract_article_urls(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        seen_local: set[str] = set()

        candidate_attributes = (
            "data-io-canonical-url",
            "data-link",
            "data-url",
            "data-href",
            "data-src",
            "href",
        )

        for anchor in soup.find_all("a"):
            normalized: str | None = None
            for attribute in candidate_attributes:
                if attribute not in anchor.attrs:
                    continue
                normalized = _normalize_nld_article_href(anchor.get(attribute))
                if normalized:
                    break

            if not normalized:
                continue
            if normalized in seen_local:
                continue

            seen_local.add(normalized)
            urls.append(normalized)

        return urls

@dataclass(slots=True)
class PloCategoryDefinition:
    slug: str
    name: str
    zone_id: int
    landing_url: str

    def normalized_landing_url(self) -> str:
        return _normalize_plo_url(self.landing_url)

    def api_url(self, page: int) -> str:
        return f"{_PLO_API_BASE}/api/morenews-zone-{self.zone_id}-{page}.html?phrase="


def _normalize_thanhnien_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        return _THANHNIEN_BASE_URL
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_THANHNIEN_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_THANHNIEN_BASE_URL}/", cleaned)
    return cleaned


def _normalize_article_href(raw_href: str | None) -> str | None:
    if not raw_href:
        return None
    cleaned = raw_href.strip()
    if not cleaned or cleaned.lower().startswith(("javascript:", "mailto:")):
        return None
    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_THANHNIEN_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_THANHNIEN_BASE_URL}/", cleaned)
    if not _THANHNIEN_ARTICLE_PATTERN.match(cleaned):
        return None
    return cleaned


def _normalize_plo_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        return _PLO_BASE_URL
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_PLO_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_PLO_BASE_URL}/", cleaned)
    return cleaned


def _normalize_plo_article_href(raw_href: str | None) -> str | None:
    if not raw_href:
        return None

    cleaned = raw_href.strip()
    if not cleaned or cleaned.lower().startswith(("javascript:", "mailto:")):
        return None

    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    if not cleaned:
        return None

    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_PLO_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_PLO_BASE_URL}/", cleaned)

    if not _PLO_ARTICLE_PATTERN.match(cleaned):
        return None
    return cleaned


def _normalize_kenh14_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        return _KENH14_BASE_URL
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_KENH14_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_KENH14_BASE_URL}/", cleaned)
    return cleaned


def _normalize_kenh14_article_href(raw_href: str | None) -> str | None:
    if not raw_href:
        return None

    cleaned = raw_href.strip()
    if not cleaned or cleaned.lower().startswith(("javascript:", "mailto:")):
        return None

    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    if not cleaned:
        return None

    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_KENH14_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_KENH14_BASE_URL}/", cleaned)

    if not _KENH14_ARTICLE_PATTERN.match(cleaned):
        return None
    return cleaned


def _normalize_nld_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        return _NLD_BASE_URL
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_NLD_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_NLD_BASE_URL}/", cleaned)
    return cleaned


def _normalize_nld_article_href(raw_href: str | None) -> str | None:
    if not raw_href:
        return None

    cleaned = raw_href.strip()
    if not cleaned or cleaned.lower().startswith(("javascript:", "mailto:")):
        return None

    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    if not cleaned:
        return None

    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_NLD_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_NLD_BASE_URL}/", cleaned)

    if not _NLD_ARTICLE_PATTERN.match(cleaned):
        return None
    return cleaned


class JobLoader(Protocol):
    """Protocol for iterables that yield article jobs."""

    stats: JobLoaderStats

    def __iter__(self) -> Iterator[ArticleJob]:
        ...


class NDJSONJobLoader:
    """Read article jobs from an NDJSON file with basic dedupe logic."""

    def __init__(
        self,
        jobs_file: Path,
        existing_urls: set[str] | None = None,
        resume: bool = False,
    ) -> None:
        self._jobs_file = jobs_file
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        if not self._jobs_file.exists():
            raise FileNotFoundError(f"Jobs file '{self._jobs_file}' does not exist")

        with self._jobs_file.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, 1):
                self.stats.total += 1
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    self.stats.skipped_invalid += 1
                    LOGGER.warning("Invalid JSON on line %d", line_number)
                    continue

                url = payload.get("url")
                if not isinstance(url, str) or not url:
                    self.stats.skipped_invalid += 1
                    LOGGER.warning("Missing 'url' on line %d", line_number)
                    continue

                if url in self._seen_urls:
                    self.stats.skipped_duplicate += 1
                    continue
                self._seen_urls.add(url)

                if self._resume and url in self._existing_urls:
                    self.stats.skipped_existing += 1
                    continue

                job = ArticleJob(
                    url=url,
                    lastmod=payload.get("lastmod"),
                    sitemap_url=payload.get("sitemap_url"),
                    image_url=payload.get("image_url"),
                )
                self.stats.emitted += 1
                yield job


class SitemapJobLoader:
    """Load article jobs from a remote sitemap index.

    ``max_sitemaps`` or ``max_urls_per_sitemap`` can be set to ``None`` to disable the limit.
    """

    def __init__(
        self,
        sitemap_url: str,
        existing_urls: set[str] | None = None,
        resume: bool = False,
        *,
        user_agent: str | None = None,
        allowed_patterns: tuple[str, ...] | None = None,
        max_sitemaps: int | None = None,
        max_urls_per_sitemap: int | None = None,
        request_timeout: float = 10.0,
        proxy: ProxyConfig | None = None,
    ) -> None:
        self._sitemap_url = sitemap_url
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._allowed_patterns = allowed_patterns
        self._max_sitemaps = max_sitemaps
        self._max_urls_per_sitemap = max_urls_per_sitemap
        self._request_timeout = request_timeout
        self._proxy = proxy

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()
        self._processed_sitemaps = 0

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()
        self._processed_sitemaps = 0

        headers = {}
        if self._user_agent:
            headers["User-Agent"] = self._user_agent

        try:
            proxy_url = self._proxy.httpx_proxy() if self._proxy else None
            client_kwargs: dict[str, object] = {
                "headers": headers or None,
                "timeout": self._request_timeout,
            }
            if proxy_url:
                client_kwargs["proxy"] = proxy_url
            with httpx.Client(**client_kwargs) as client:
                yield from self._walk_sitemap(client, self._sitemap_url)
        except httpx.HTTPError as exc:
            LOGGER.error("Failed to fetch sitemap %s: %s", self._sitemap_url, exc)

    def _walk_sitemap(self, client: httpx.Client, sitemap_url: str, depth: int = 0) -> Iterator[ArticleJob]:
        if self._max_sitemaps is not None and self._processed_sitemaps >= self._max_sitemaps:
            return

        root = self._fetch_xml(client, sitemap_url)
        if root is None:
            return

        tag = self._strip_tag(root.tag)
        if tag == "sitemapindex":
            for child_url in self._extract_child_sitemaps(root):
                if self._allowed_patterns and not any(pattern in child_url for pattern in self._allowed_patterns):
                    continue
                # Re-check limit before descending into a child sitemap
                if self._max_sitemaps is not None and self._processed_sitemaps >= self._max_sitemaps:
                    break
                yield from self._walk_sitemap(client, child_url, depth + 1)
        elif tag == "urlset":
            if self._max_sitemaps is not None and self._processed_sitemaps >= self._max_sitemaps:
                return
            self._processed_sitemaps += 1
            yield from self._iterate_urls(root, sitemap_url)

    def _fetch_xml(self, client: httpx.Client, url: str) -> ET.Element | None:
        try:
            response = client.get(url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            LOGGER.warning("Failed to load sitemap URL %s: %s", url, exc)
            return None

        try:
            return ET.fromstring(response.content)
        except ET.ParseError as exc:
            LOGGER.warning("Invalid XML received from %s: %s", url, exc)
            return None

    def _extract_child_sitemaps(self, root: ET.Element) -> Iterator[str]:
        for sitemap in root.findall("sm:sitemap", _SITEMAP_NS):
            loc = sitemap.findtext("sm:loc", default="", namespaces=_SITEMAP_NS).strip()
            if loc:
                yield loc

    def _iterate_urls(self, root: ET.Element, sitemap_url: str) -> Iterator[ArticleJob]:
        count = 0
        for url_node in root.findall("sm:url", _SITEMAP_NS):
            loc_text = url_node.findtext("sm:loc", default="", namespaces=_SITEMAP_NS).strip()
            if not loc_text:
                self.stats.skipped_invalid += 1
                continue

            if self._max_urls_per_sitemap is not None and count >= self._max_urls_per_sitemap:
                break
            count += 1
            self.stats.total += 1

            if self._resume and loc_text in self._existing_urls:
                self.stats.skipped_existing += 1
                continue

            if loc_text in self._seen_urls:
                self.stats.skipped_duplicate += 1
                continue
            self._seen_urls.add(loc_text)

            lastmod_text = url_node.findtext("sm:lastmod", default=None, namespaces=_SITEMAP_NS)
            image_url = None
            image_node = url_node.find("image:image", _SITEMAP_NS)
            if image_node is not None:
                image_loc = image_node.findtext("image:loc", default="", namespaces=_SITEMAP_NS).strip()
                if image_loc:
                    image_url = image_loc

            job = ArticleJob(
                url=loc_text,
                lastmod=lastmod_text.strip() if isinstance(lastmod_text, str) else lastmod_text,
                sitemap_url=sitemap_url,
                image_url=image_url,
            )
            self.stats.emitted += 1
            yield job

    @staticmethod
    def _strip_tag(tag: str) -> str:
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag


class ThanhnienCategoryLoader:
    """Iterate Thanhnien category landing pages and timeline endpoints."""

    def __init__(
        self,
        categories: Sequence[ThanhnienCategoryDefinition],
        *,
        existing_urls: set[str] | None = None,
        resume: bool = False,
        user_agent: str | None = None,
        max_pages: int | None = 10,
        max_empty_pages: int | None = 2,
        request_timeout: float = 5.0,
        include_landing_page: bool = True,
        proxy: ProxyConfig | None = None,
        max_fetch_attempts: int = 3,
        fetch_retry_backoff: float = 1.0,
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._max_empty_pages = max_empty_pages
        self._request_timeout = request_timeout
        self._include_landing_page = include_landing_page
        self._proxy = proxy
        self._max_fetch_attempts = max(1, int(max_fetch_attempts))
        self._fetch_retry_backoff = max(0.0, float(fetch_retry_backoff))

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        proxy_url = self._proxy.httpx_proxy() if self._proxy else None
        client_kwargs: dict[str, object] = {
            "headers": headers,
            "timeout": self._request_timeout,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        with httpx.Client(**client_kwargs) as client:
            for category in self._categories:
                yield from self._iterate_category(client, category)

    def _iterate_category(self, client: httpx.Client, category: ThanhnienCategoryDefinition) -> Iterator[ArticleJob]:
        emitted_before = self.stats.emitted
        skipped_existing_before = self.stats.skipped_existing
        skipped_duplicate_before = self.stats.skipped_duplicate

        if self._include_landing_page:
            landing_html = self._fetch_html(client, category.normalized_landing_url())
            if landing_html:
                yield from self._emit_jobs_from_html(landing_html, category_slug=category.slug)

        page = 1
        consecutive_empty_pages = 0
        while True:
            if self._max_pages is not None and page > self._max_pages:
                break

            timeline_url = category.timeline_url(page)
            html = self._fetch_html(client, timeline_url)
            if not html:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
                page += 1
                continue

            emitted_on_page = False
            for job in self._emit_jobs_from_html(html, category_slug=category.slug):
                emitted_on_page = True
                yield job

            if not emitted_on_page:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
            else:
                consecutive_empty_pages = 0

            page += 1

        LOGGER.info(
            "Thanhnien category '%s': emitted=%d skipped_existing=%d skipped_duplicate=%d",
            category.slug,
            self.stats.emitted - emitted_before,
            self.stats.skipped_existing - skipped_existing_before,
            self.stats.skipped_duplicate - skipped_duplicate_before,
        )

    def _fetch_html(self, client: httpx.Client, url: str) -> str:
        for attempt in range(self._max_fetch_attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "Thanhnien category request timeout (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPStatusError as exc:
                LOGGER.warning("Thanhnien category request failed (%s): %s", url, exc)
                if (
                    exc.response is not None
                    and 500 <= exc.response.status_code < 600
                    and self._should_retry(attempt)
                ):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPError as exc:
                LOGGER.warning(
                    "Thanhnien category request error (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            return response.text.strip()
        return ""

    def _should_retry(self, attempt: int) -> bool:
        return attempt + 1 < self._max_fetch_attempts

    def _sleep_before_retry(self, attempt: int) -> None:
        if self._fetch_retry_backoff <= 0:
            return
        delay = self._fetch_retry_backoff * (2 ** attempt)
        time.sleep(delay)

    def _emit_jobs_from_html(self, html: str, *, category_slug: str | None = None) -> Iterator[ArticleJob]:
        for url in self._extract_article_urls(html):
            self.stats.total += 1

            if url in self._seen_urls:
                self.stats.skipped_duplicate += 1
                continue
            self._seen_urls.add(url)

            if self._resume and url in self._existing_urls:
                self.stats.skipped_existing += 1
                continue

            job = ArticleJob(url=url, lastmod=None, sitemap_url=None, image_url=None, category_slug=category_slug)
            self.stats.emitted += 1
            yield job

    def _extract_article_urls(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        seen_local: set[str] = set()

        for anchor in soup.find_all("a"):
            primary_href = anchor.get("data-io-canonical-url") or anchor.get("href")
            normalized = _normalize_article_href(primary_href)
            if not normalized and primary_href:
                backup_href = anchor.get("href") if primary_href != anchor.get("href") else None
                normalized = _normalize_article_href(backup_href)
            if not normalized:
                continue
            if normalized in seen_local:
                continue
            seen_local.add(normalized)
            urls.append(normalized)

        return urls


class Kenh14CategoryLoader:
    """Iterate Kenh14 category landing pages and timeline endpoints."""

    def __init__(
        self,
        categories: Sequence[Kenh14CategoryDefinition],
        *,
        existing_urls: set[str] | None = None,
        resume: bool = False,
        user_agent: str | None = None,
        max_pages: int | None = 600,
        max_empty_pages: int | None = 3,
        request_timeout: float = 5.0,
        include_landing_page: bool = True,
        proxy: ProxyConfig | None = None,
        max_fetch_attempts: int = 3,
        fetch_retry_backoff: float = 1.0,
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._max_empty_pages = max_empty_pages
        self._request_timeout = request_timeout
        self._include_landing_page = include_landing_page
        self._proxy = proxy
        self._max_fetch_attempts = max(1, int(max_fetch_attempts))
        self._fetch_retry_backoff = max(0.0, float(fetch_retry_backoff))

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        proxy_url = self._proxy.httpx_proxy() if self._proxy else None
        client_kwargs: dict[str, object] = {
            "headers": headers,
            "timeout": self._request_timeout,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        with httpx.Client(**client_kwargs) as client:
            for category in self._categories:
                yield from self._iterate_category(client, category)

    def _iterate_category(self, client: httpx.Client, category: Kenh14CategoryDefinition) -> Iterator[ArticleJob]:
        emitted_before = self.stats.emitted
        skipped_existing_before = self.stats.skipped_existing
        skipped_duplicate_before = self.stats.skipped_duplicate

        if self._include_landing_page:
            landing_html = self._fetch_payload(client, category.normalized_landing_url())
            if landing_html:
                yield from self._emit_jobs_from_html(landing_html)

        page = 1
        consecutive_empty_pages = 0
        while True:
            if self._max_pages is not None and page > self._max_pages:
                break

            timeline_url = category.timeline_url(page)
            html = self._fetch_payload(client, timeline_url)
            if not html:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
                page += 1
                continue

            emitted_on_page = False
            for job in self._emit_jobs_from_html(html):
                emitted_on_page = True
                yield job

            if not emitted_on_page:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
            else:
                consecutive_empty_pages = 0

            page += 1

        LOGGER.info(
            "Kenh14 category '%s': emitted=%d skipped_existing=%d skipped_duplicate=%d",
            category.slug,
            self.stats.emitted - emitted_before,
            self.stats.skipped_existing - skipped_existing_before,
            self.stats.skipped_duplicate - skipped_duplicate_before,
        )

    def _fetch_payload(self, client: httpx.Client, url: str) -> str:
        for attempt in range(self._max_fetch_attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "Kenh14 category request timeout (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPStatusError as exc:
                LOGGER.warning("Kenh14 category request failed (%s): %s", url, exc)
                if (
                    exc.response is not None
                    and 500 <= exc.response.status_code < 600
                    and self._should_retry(attempt)
                ):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPError as exc:
                LOGGER.warning(
                    "Kenh14 category request error (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""

            text = response.text.strip()
            if not text:
                return ""

            if text.startswith("{") or text.startswith("["):
                try:
                    payload = response.json()
                except ValueError:
                    return text
                html_fragment = self._extract_html_from_payload(payload)
                if html_fragment:
                    return html_fragment
                return ""
            return text
        return ""

    def _should_retry(self, attempt: int) -> bool:
        return attempt + 1 < self._max_fetch_attempts

    def _sleep_before_retry(self, attempt: int) -> None:
        if self._fetch_retry_backoff <= 0:
            return
        delay = self._fetch_retry_backoff * (2 ** attempt)
        time.sleep(delay)

    def _extract_html_from_payload(self, payload: object) -> str:
        fragments: list[str] = []
        stack: list[object] = [payload]
        while stack:
            current = stack.pop()
            if isinstance(current, str):
                cleaned = current.strip()
                if cleaned:
                    fragments.append(cleaned)
                continue
            if isinstance(current, dict):
                for key in ("html", "data", "content", "Content", "items", "list", "body", "Body"):
                    if key in current:
                        stack.append(current[key])
                for value in current.values():
                    if isinstance(value, (dict, list, tuple, set, str)):
                        stack.append(value)
                continue
            if isinstance(current, (list, tuple, set)):
                stack.extend(current)
        return "".join(fragments)

    def _emit_jobs_from_html(self, html: str) -> Iterator[ArticleJob]:
        for url in self._extract_article_urls(html):
            self.stats.total += 1

            if url in self._seen_urls:
                self.stats.skipped_duplicate += 1
                continue
            self._seen_urls.add(url)

            if self._resume and url in self._existing_urls:
                self.stats.skipped_existing += 1
                continue

            job = ArticleJob(url=url, lastmod=None, sitemap_url=None, image_url=None)
            self.stats.emitted += 1
            yield job

    def _extract_article_urls(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        seen_local: set[str] = set()

        candidate_attributes = (
            "data-io-canonical-url",
            "data-link",
            "data-url",
            "data-href",
            "data-src",
            "href",
        )

        for anchor in soup.find_all("a"):
            normalized: str | None = None
            for attribute in candidate_attributes:
                if attribute not in anchor.attrs:
                    continue
                normalized = _normalize_kenh14_article_href(anchor.get(attribute))
                if normalized:
                    break

            if not normalized:
                continue
            if normalized in seen_local:
                continue

            seen_local.add(normalized)
            urls.append(normalized)

        return urls


class PloCategoryLoader:
    """Iterate PLO category API endpoints and emit article URLs."""

    def __init__(
        self,
        categories: Sequence[PloCategoryDefinition],
        *,
        existing_urls: set[str] | None = None,
        resume: bool = False,
        user_agent: str | None = None,
        max_pages: int | None = None,
        max_empty_pages: int | None = 2,
        request_timeout: float = 5.0,
        include_landing_page: bool = False,
        proxy: ProxyConfig | None = None,
        max_fetch_attempts: int = 3,
        fetch_retry_backoff: float = 1.0,
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._max_empty_pages = max_empty_pages
        self._request_timeout = request_timeout
        self._include_landing_page = include_landing_page
        self._proxy = proxy
        self._max_fetch_attempts = max(1, int(max_fetch_attempts))
        self._fetch_retry_backoff = max(0.0, float(fetch_retry_backoff))

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        proxy_url = self._proxy.httpx_proxy() if self._proxy else None
        client_kwargs: dict[str, object] = {
            "headers": headers,
            "timeout": self._request_timeout,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url

        with httpx.Client(**client_kwargs) as client:
            for category in self._categories:
                yield from self._iterate_category(client, category)

    def _iterate_category(self, client: httpx.Client, category: PloCategoryDefinition) -> Iterator[ArticleJob]:
        emitted_before = self.stats.emitted
        skipped_existing_before = self.stats.skipped_existing
        skipped_duplicate_before = self.stats.skipped_duplicate
        skipped_invalid_before = self.stats.skipped_invalid

        if self._include_landing_page:
            landing_html = self._fetch_landing_html(client, category.normalized_landing_url())
            if landing_html:
                yield from self._emit_jobs_from_html(landing_html)

        page = 1
        consecutive_empty_pages = 0
        while True:
            if self._max_pages is not None and page > self._max_pages:
                break

            api_url = category.api_url(page)
            contents = self._fetch_api_contents(client, api_url)
            if contents is None:
                break

            emitted_on_page = False
            for job in self._emit_jobs_from_contents(contents):
                emitted_on_page = True
                yield job

            if not emitted_on_page:
                consecutive_empty_pages += 1
                if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
                    break
            else:
                consecutive_empty_pages = 0

            page += 1

        LOGGER.info(
            "PLO category '%s': emitted=%d skipped_existing=%d skipped_duplicate=%d skipped_invalid=%d",
            category.slug,
            self.stats.emitted - emitted_before,
            self.stats.skipped_existing - skipped_existing_before,
            self.stats.skipped_duplicate - skipped_duplicate_before,
            self.stats.skipped_invalid - skipped_invalid_before,
        )

    def _fetch_landing_html(self, client: httpx.Client, url: str) -> str:
        for attempt in range(self._max_fetch_attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "PLO landing request timeout (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPStatusError as exc:
                LOGGER.warning("PLO landing request failed (%s): %s", url, exc)
                if (
                    exc.response is not None
                    and 500 <= exc.response.status_code < 600
                    and self._should_retry(attempt)
                ):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPError as exc:
                LOGGER.warning(
                    "PLO landing request error (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            return response.text.strip()
        return ""

    def _fetch_api_contents(self, client: httpx.Client, url: str) -> list[dict] | None:
        for attempt in range(self._max_fetch_attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "PLO API request timeout (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return None
            except httpx.HTTPStatusError as exc:
                LOGGER.warning("PLO API request failed (%s): %s", url, exc)
                if (
                    exc.response is not None
                    and 500 <= exc.response.status_code < 600
                    and self._should_retry(attempt)
                ):
                    self._sleep_before_retry(attempt)
                    continue
                return None
            except httpx.HTTPError as exc:
                LOGGER.warning(
                    "PLO API request error (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return None
            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                LOGGER.warning("Invalid PLO API payload (%s): %s", url, exc)
                return None
            data_section = payload.get("data")
            if not isinstance(data_section, dict):
                LOGGER.warning("Unexpected PLO API structure (%s): missing data section", url)
                return []
            contents = data_section.get("contents")
            if contents is None:
                return []
            if not isinstance(contents, list):
                LOGGER.warning("Unexpected PLO API contents format (%s)", url)
                return []
            return contents
        return None
    def _should_retry(self, attempt: int) -> bool:
        return attempt + 1 < self._max_fetch_attempts

    def _sleep_before_retry(self, attempt: int) -> None:
        if self._fetch_retry_backoff <= 0:
            return
        delay = self._fetch_retry_backoff * (2 ** attempt)
        time.sleep(delay)

    def _emit_jobs_from_html(self, html: str) -> Iterator[ArticleJob]:
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            normalized = _normalize_plo_article_href(anchor.get("href"))
            if not normalized:
                continue
            job = self._maybe_emit_job(normalized, None, None)
            if job:
                yield job

    def _emit_jobs_from_contents(self, contents: list[dict]) -> Iterator[ArticleJob]:
        for entry in contents:
            self.stats.total += 1

            if not isinstance(entry, dict):
                self.stats.skipped_invalid += 1
                continue

            raw_url = entry.get("url") or entry.get("redirect_link")
            normalized = _normalize_plo_article_href(raw_url)
            if not normalized:
                self.stats.skipped_invalid += 1
                continue

            lastmod = self._format_timestamp(entry.get("update_time") or entry.get("date"))
            avatar = entry.get("avatar_url")

            job = self._maybe_emit_job(normalized, lastmod, avatar)
            if job:
                yield job

    def _maybe_emit_job(self, url: str, lastmod: str | None, image_url: str | None) -> ArticleJob | None:
        if url in self._seen_urls:
            self.stats.skipped_duplicate += 1
            return None
        self._seen_urls.add(url)

        if self._resume and url in self._existing_urls:
            self.stats.skipped_existing += 1
            return None

        job = ArticleJob(url=url, lastmod=lastmod, sitemap_url=None, image_url=image_url)
        self.stats.emitted += 1
        return job

    def _format_timestamp(self, value: object) -> str | None:
        if value is None:
            return None
        try:
            epoch = int(value)
        except (TypeError, ValueError):
            return None
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.isoformat()


_ZNEWS_BASE_URL = "https://znews.vn"
_ZNEWS_ARTICLE_PATTERN = re.compile(r"^https?://(?:[^./]+\.)?znews\.vn/[^?#]+-(?:post|news|video)\d+\.html$", re.IGNORECASE)


def _normalize_znews_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        return _ZNEWS_BASE_URL
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_ZNEWS_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_ZNEWS_BASE_URL}/", cleaned)
    return cleaned


def _normalize_znews_article_href(raw_href: str | None) -> str | None:
    if not raw_href:
        return None

    cleaned = raw_href.strip()
    if not cleaned or cleaned.lower().startswith(("javascript:", "mailto:")):
        return None

    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    if not cleaned:
        return None

    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    elif cleaned.startswith("/"):
        cleaned = urljoin(_ZNEWS_BASE_URL, cleaned)
    elif not cleaned.startswith("http"):
        cleaned = urljoin(f"{_ZNEWS_BASE_URL}/", cleaned)

    if not _ZNEWS_ARTICLE_PATTERN.match(cleaned):
        return None
    return cleaned


@dataclass(slots=True)
class ZnewsCategoryDefinition:
    slug: str
    name: str
    landing_url: str

    def normalized_landing_url(self) -> str:
        return _normalize_znews_url(self.landing_url)

    def page_url(self, page: int) -> str:
        landing = self.normalized_landing_url()
        if page <= 1:
            return landing

        if landing.endswith(".html"):
            base = landing[: -len(".html")]
        else:
            base = landing.rstrip("/")
        return f"{base}/trang{page}.html"


class ZnewsCategoryLoader:
    """Iterate Znews category landing pages and paginate to collect article URLs."""

    def __init__(
        self,
        categories: Sequence[ZnewsCategoryDefinition],
        *,
        existing_urls: set[str] | None = None,
        resume: bool = False,
        user_agent: str | None = None,
        max_pages: int | None = 50,
        request_timeout: float = 5.0,
        duplicate_fingerprint_size: int = 3,
        stop_on_duplicate: bool = True,
        proxy: ProxyConfig | None = None,
        max_fetch_attempts: int = 3,
        fetch_retry_backoff: float = 1.0,
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._request_timeout = request_timeout
        self._duplicate_fingerprint_size = max(1, duplicate_fingerprint_size)
        self._stop_on_duplicate = stop_on_duplicate
        self._proxy = proxy
        self._max_fetch_attempts = max(1, int(max_fetch_attempts))
        self._fetch_retry_backoff = max(0.0, float(fetch_retry_backoff))

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        proxy_url = self._proxy.httpx_proxy() if self._proxy else None
        client_kwargs: dict[str, object] = {
            "headers": headers,
            "timeout": self._request_timeout,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        with httpx.Client(**client_kwargs) as client:
            for category in self._categories:
                yield from self._iterate_category(client, category)

    def _iterate_category(self, client: httpx.Client, category: ZnewsCategoryDefinition) -> Iterator[ArticleJob]:
        previous_fingerprint: list[str] | None = None
        emitted_before = self.stats.emitted
        skipped_existing_before = self.stats.skipped_existing
        skipped_duplicate_before = self.stats.skipped_duplicate

        page = 1
        consecutive_empty_pages = 0
        while True:
            if self._max_pages is not None and page > self._max_pages:
                break

            page_url = category.page_url(page)
            html = self._fetch_html(client, page_url)
            if not html:
                break

            urls = self._extract_article_urls(html)
            if not urls:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    break
                page += 1
                continue

            consecutive_empty_pages = 0
            fingerprint = urls[: self._duplicate_fingerprint_size]
            if (
                self._stop_on_duplicate
                and previous_fingerprint is not None
                and fingerprint
                and fingerprint == previous_fingerprint
            ):
                LOGGER.info(
                    "Znews category '%s': detected duplicate pagination at %s; stopping.",
                    category.slug,
                    page_url,
                )
                break
            if fingerprint:
                previous_fingerprint = fingerprint

            for job in self._emit_jobs_from_urls(urls):
                yield job

            page += 1

        LOGGER.info(
            "Znews category '%s': emitted=%d skipped_existing=%d skipped_duplicate=%d",
            category.slug,
            self.stats.emitted - emitted_before,
            self.stats.skipped_existing - skipped_existing_before,
            self.stats.skipped_duplicate - skipped_duplicate_before,
        )

    def _fetch_html(self, client: httpx.Client, url: str) -> str:
        for attempt in range(self._max_fetch_attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "Znews category request timeout (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPStatusError as exc:
                LOGGER.warning("Znews category request failed (%s): %s", url, exc)
                if (
                    exc.response is not None
                    and 500 <= exc.response.status_code < 600
                    and self._should_retry(attempt)
                ):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            except httpx.HTTPError as exc:
                LOGGER.warning(
                    "Znews category request error (%s) attempt %d/%d: %s",
                    url,
                    attempt + 1,
                    self._max_fetch_attempts,
                    exc,
                )
                if self._should_retry(attempt):
                    self._sleep_before_retry(attempt)
                    continue
                return ""
            return response.text.strip()
        return ""

    def _should_retry(self, attempt: int) -> bool:
        return attempt + 1 < self._max_fetch_attempts

    def _sleep_before_retry(self, attempt: int) -> None:
        if self._fetch_retry_backoff <= 0:
            return
        delay = self._fetch_retry_backoff * (2 ** attempt)
        time.sleep(delay)

    def _emit_jobs_from_urls(self, urls: Sequence[str]) -> Iterator[ArticleJob]:
        for url in urls:
            self.stats.total += 1

            if url in self._seen_urls:
                self.stats.skipped_duplicate += 1
                continue
            self._seen_urls.add(url)

            if self._resume and url in self._existing_urls:
                self.stats.skipped_existing += 1
                continue

            job = ArticleJob(url=url, lastmod=None, sitemap_url=None, image_url=None)
            self.stats.emitted += 1
            yield job

    def _extract_article_urls(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        seen_local: set[str] = set()

        for anchor in soup.find_all("a"):
            primary_href = anchor.get("data-utm-src") or anchor.get("data-utm-source") or anchor.get("href")
            normalized = _normalize_znews_article_href(primary_href)
            if not normalized and primary_href:
                backup_href = anchor.get("href") if primary_href != anchor.get("href") else None
                normalized = _normalize_znews_article_href(backup_href)
            if not normalized:
                continue
            if normalized in seen_local:
                continue
            seen_local.add(normalized)
            urls.append(normalized)

        return urls


_DEFAULT_KENH14_CATEGORY_SLUGS: tuple[str, ...] = ("phap-luat",)
_DEFAULT_KENH14_CATEGORIES: tuple[Kenh14CategoryDefinition, ...] = (
    Kenh14CategoryDefinition(
        slug="phap-luat",
        name="Pháp luật",
        timeline_id=215195,
        landing_url="https://kenh14.vn/xa-hoi/phap-luat.chn",
    ),
)


def _load_kenh14_category_catalog(catalog_path: Path) -> dict[str, Kenh14CategoryDefinition]:
    try:
        raw_payload = catalog_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Kenh14 category catalog not found: {catalog_path}") from exc

    try:
        records = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Kenh14 category catalog: {exc}") from exc

    catalog: dict[str, Kenh14CategoryDefinition] = {}
    for entry in records:
        slug = entry.get("slug")
        name = entry.get("name") or ""
        timeline_id = entry.get("timeline_id")
        landing_url = entry.get("landing_url") or ""

        if not isinstance(slug, str) or not slug:
            raise ValueError("Category catalog entries must include a non-empty 'slug'")
        if not isinstance(timeline_id, int):
            try:
                timeline_id = int(timeline_id)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid timeline_id for slug '{slug}': {entry.get('timeline_id')}") from exc

        definition = Kenh14CategoryDefinition(
            slug=slug.strip().lower(),
            name=name.strip() or slug,
            timeline_id=timeline_id,
            landing_url=_normalize_kenh14_url(landing_url),
        )
        catalog[definition.slug] = definition

    if not catalog:
        raise ValueError("Kenh14 category catalog is empty")
    return catalog


def _select_kenh14_categories(
    config: IngestConfig, catalog: dict[str, Kenh14CategoryDefinition]
) -> list[Kenh14CategoryDefinition]:
    if config.kenh14.crawl_all:
        selected_slugs = list(catalog.keys())
    elif config.kenh14.selected_slugs:
        selected_slugs = list(config.kenh14.selected_slugs)
    else:
        selected_slugs = [slug for slug in _DEFAULT_KENH14_CATEGORY_SLUGS if slug in catalog]

    if not selected_slugs:
        raise ValueError(
            "No Kenh14 categories selected. Provide --kenh14-categories or update the category catalog."
        )

    missing = [slug for slug in selected_slugs if slug not in catalog]
    if missing:
        raise ValueError(f"Unknown Kenh14 categories requested: {', '.join(missing)}")

    return [catalog[slug] for slug in selected_slugs]


def build_kenh14_job_loader(config: IngestConfig, existing_urls: set[str]) -> JobLoader:
    if config.jobs_file_provided:
        LOGGER.info("Kenh14 jobs file supplied; using NDJSONJobLoader at %s", config.jobs_file)
        return NDJSONJobLoader(
            jobs_file=config.jobs_file,
            existing_urls=existing_urls,
            resume=config.resume,
        )

    catalog: dict[str, Kenh14CategoryDefinition] = {
        category.slug: category for category in _DEFAULT_KENH14_CATEGORIES
    }

    catalog_path = Path("data/kenh14_categories.json")
    if catalog_path.exists():
        try:
            catalog = _load_kenh14_category_catalog(catalog_path)
            LOGGER.info("Loaded Kenh14 category catalog from %s", catalog_path)
        except ValueError as exc:
            LOGGER.warning("Ignoring invalid Kenh14 category catalog at %s: %s", catalog_path, exc)

    try:
        categories = _select_kenh14_categories(config, catalog)
    except ValueError as exc:
        raise ValueError(f"Failed to select Kenh14 categories: {exc}") from exc

    LOGGER.info(
        "Initialized Kenh14CategoryLoader with categories: %s",
        ", ".join(category.slug for category in categories),
    )

    return Kenh14CategoryLoader(
        categories=categories,
        existing_urls=existing_urls,
        resume=config.resume,
        user_agent=config.user_agent,
        max_pages=config.kenh14.max_pages,
        max_empty_pages=config.kenh14.max_empty_pages,
        request_timeout=config.timeout.request_timeout,
        proxy=config.proxy,
    )


_DEFAULT_NLD_CATEGORY_SLUGS: tuple[str, ...] = ("phap-luat", "chinh-tri")
_DEFAULT_NLD_CATEGORIES: tuple[NldCategoryDefinition, ...] = (
    NldCategoryDefinition(
        slug="phap-luat",
        name="Pháp luật",
        category_id=1961019,
        landing_url="https://nld.com.vn/phap-luat.htm",
    ),
    NldCategoryDefinition(
        slug="chinh-tri",
        name="Chính trị",
        category_id=1961206,
        landing_url="https://nld.com.vn/thoi-su/chinh-tri.htm",
    ),
)
_DEFAULT_PLO_CATEGORY_SLUGS: tuple[str, ...] = ("phap-luat", "chinh-tri")
_DEFAULT_PLO_CATEGORIES: tuple[PloCategoryDefinition, ...] = (
    PloCategoryDefinition(
        slug="phap-luat",
        name="Pháp luật",
        zone_id=114,
        landing_url="https://plo.vn/phap-luat/",
    ),
    PloCategoryDefinition(
        slug="chinh-tri",
        name="Chính trị",
        zone_id=2,
        landing_url="https://plo.vn/thoi-su/chinh-tri/",
    ),
)


def _load_nld_category_catalog(catalog_path: Path) -> dict[str, NldCategoryDefinition]:
    try:
        raw_payload = catalog_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Nld category catalog not found: {catalog_path}") from exc

    try:
        records = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Nld category catalog: {exc}") from exc

    catalog: dict[str, NldCategoryDefinition] = {}
    for entry in records:
        slug = entry.get("slug")
        name = entry.get("name") or ""
        category_id = entry.get("category_id")
        landing_url = entry.get("landing_url") or ""

        if not isinstance(slug, str) or not slug:
            raise ValueError("Category catalog entries must include a non-empty 'slug'")
        if not isinstance(category_id, int):
            try:
                category_id = int(category_id)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid category_id for slug '{slug}': {entry.get('category_id')}") from exc

        definition = NldCategoryDefinition(
            slug=slug.strip().lower(),
            name=name.strip() or slug,
            category_id=category_id,
            landing_url=_normalize_nld_url(landing_url),
        )
        catalog[definition.slug] = definition

    if not catalog:
        raise ValueError("Nld category catalog is empty")
    return catalog


def _select_nld_categories(
    config: IngestConfig, catalog: dict[str, NldCategoryDefinition]
) -> list[NldCategoryDefinition]:
    if config.nld.crawl_all:
        selected_slugs = list(catalog.keys())
    elif config.nld.selected_slugs:
        selected_slugs = list(config.nld.selected_slugs)
    else:
        selected_slugs = [slug for slug in _DEFAULT_NLD_CATEGORY_SLUGS if slug in catalog]

    if not selected_slugs:
        raise ValueError(
            "No Nld categories selected. Provide --nld-categories or update the category catalog."
        )

    missing = [slug for slug in selected_slugs if slug not in catalog]
    if missing:
        raise ValueError(f"Unknown Nld categories requested: {', '.join(missing)}")

    return [catalog[slug] for slug in selected_slugs]


def build_nld_job_loader(config: IngestConfig, existing_urls: set[str]) -> JobLoader:
    if config.jobs_file_provided:
        LOGGER.info("Nld jobs file supplied; using NDJSONJobLoader at %s", config.jobs_file)
        return NDJSONJobLoader(
            jobs_file=config.jobs_file,
            existing_urls=existing_urls,
            resume=config.resume,
        )

    catalog: dict[str, NldCategoryDefinition] = {category.slug: category for category in _DEFAULT_NLD_CATEGORIES}

    catalog_path = Path("data/nld_categories.json")
    if catalog_path.exists():
        try:
            catalog = _load_nld_category_catalog(catalog_path)
            LOGGER.info("Loaded Nld category catalog from %s", catalog_path)
        except ValueError as exc:
            LOGGER.warning("Ignoring invalid Nld category catalog at %s: %s", catalog_path, exc)

    try:
        categories = _select_nld_categories(config, catalog)
    except ValueError as exc:
        raise ValueError(f"Failed to select Nld categories: {exc}") from exc

    LOGGER.info(
        "Initialized NldCategoryLoader with categories: %s",
        ", ".join(category.slug for category in categories),
    )

    return NldCategoryLoader(
        categories=categories,
        existing_urls=existing_urls,
        resume=config.resume,
        user_agent=config.user_agent,
        max_pages=config.nld.max_pages,
        max_empty_pages=config.nld.max_empty_pages,
        request_timeout=config.timeout.request_timeout,
        proxy=config.proxy,
    )


def _load_plo_category_catalog(catalog_path: Path) -> dict[str, PloCategoryDefinition]:
    try:
        raw_payload = catalog_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"PLO category catalog not found: {catalog_path}") from exc

    try:
        records = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid PLO category catalog: {exc}") from exc

    catalog: dict[str, PloCategoryDefinition] = {}
    for entry in records:
        slug = entry.get("slug")
        name = entry.get("name") or ""
        zone_id = entry.get("zone_id")
        landing_url = entry.get("landing_url") or ""

        if not isinstance(slug, str) or not slug:
            raise ValueError("Category catalog entries must include a non-empty 'slug'")
        if not isinstance(zone_id, int):
            try:
                zone_id = int(zone_id)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid zone_id for slug '{slug}': {entry.get('zone_id')}") from exc

        definition = PloCategoryDefinition(
            slug=slug.strip().lower(),
            name=name.strip() or slug,
            zone_id=zone_id,
            landing_url=_normalize_plo_url(landing_url),
        )
        catalog[definition.slug] = definition

    if not catalog:
        raise ValueError("PLO category catalog is empty")
    return catalog


def _select_plo_categories(
    config: IngestConfig, catalog: dict[str, PloCategoryDefinition]
) -> list[PloCategoryDefinition]:
    if config.plo.crawl_all:
        selected_slugs = list(catalog.keys())
    elif config.plo.selected_slugs:
        selected_slugs = list(config.plo.selected_slugs)
    else:
        selected_slugs = [slug for slug in _DEFAULT_PLO_CATEGORY_SLUGS if slug in catalog]

    if not selected_slugs:
        raise ValueError(
            "No PLO categories selected. Provide --plo-categories or update the PLO category catalog."
        )

    missing = [slug for slug in selected_slugs if slug not in catalog]
    if missing:
        raise ValueError(f"Unknown PLO categories requested: {', '.join(missing)}")

    return [catalog[slug] for slug in selected_slugs]


def build_plo_job_loader(config: IngestConfig, existing_urls: set[str]) -> JobLoader:
    if config.jobs_file_provided:
        LOGGER.info("PLO jobs file supplied; using NDJSONJobLoader at %s", config.jobs_file)
        return NDJSONJobLoader(
            jobs_file=config.jobs_file,
            existing_urls=existing_urls,
            resume=config.resume,
        )

    catalog: dict[str, PloCategoryDefinition] = {category.slug: category for category in _DEFAULT_PLO_CATEGORIES}

    catalog_path = Path("data/plo_categories.json")
    if catalog_path.exists():
        try:
            catalog = _load_plo_category_catalog(catalog_path)
            LOGGER.info("Loaded PLO category catalog from %s", catalog_path)
        except ValueError as exc:
            LOGGER.warning("Ignoring invalid PLO category catalog at %s: %s", catalog_path, exc)

    try:
        categories = _select_plo_categories(config, catalog)
    except ValueError as exc:
        raise ValueError(f"Failed to select PLO categories: {exc}") from exc

    LOGGER.info(
        "Initialized PloCategoryLoader with categories: %s",
        ", ".join(category.slug for category in categories),
    )

    return PloCategoryLoader(
        categories=categories,
        existing_urls=existing_urls,
        resume=config.resume,
        user_agent=config.user_agent,
        max_pages=config.plo.max_pages,
        max_empty_pages=config.plo.max_empty_pages,
        request_timeout=config.timeout.request_timeout,
        include_landing_page=False,
        proxy=config.proxy,
    )


_DEFAULT_THANHNIEN_CATEGORY_SLUGS: tuple[str, ...] = ("chinh-tri", "thoi-su-phap-luat")
_DEFAULT_THANHNIEN_CATEGORIES: tuple[ThanhnienCategoryDefinition, ...] = (
    ThanhnienCategoryDefinition(
        slug="chinh-tri",
        name="Chính trị",
        category_id=185227,
        landing_url="https://thanhnien.vn/chinh-tri.htm",
    ),
    ThanhnienCategoryDefinition(
        slug="thoi-su-phap-luat",
        name="Thời sự - Pháp luật",
        category_id=1855,
        landing_url="https://thanhnien.vn/thoi-su/phap-luat.htm",
    ),
)


def _load_thanhnien_category_catalog(catalog_path: Path) -> dict[str, ThanhnienCategoryDefinition]:
    try:
        raw_payload = catalog_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Thanhnien category catalog not found: {catalog_path}") from exc

    try:
        records = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Thanhnien category catalog: {exc}") from exc

    catalog: dict[str, ThanhnienCategoryDefinition] = {}
    for entry in records:
        slug = entry.get("slug")
        name = entry.get("name") or ""
        category_id = entry.get("category_id")
        landing_url = entry.get("landing_url") or ""

        if not isinstance(slug, str) or not slug:
            raise ValueError("Category catalog entries must include a non-empty 'slug'")
        if not isinstance(category_id, int):
            try:
                category_id = int(category_id)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid category_id for slug '{slug}': {entry.get('category_id')}") from exc

        definition = ThanhnienCategoryDefinition(
            slug=slug.strip().lower(),
            name=name.strip() or slug,
            category_id=category_id,
            landing_url=_normalize_thanhnien_url(landing_url),
        )
        catalog[definition.slug] = definition

    if not catalog:
        raise ValueError("Thanhnien category catalog is empty")
    return catalog


def _select_thanhnien_categories(
    config: IngestConfig, catalog: dict[str, ThanhnienCategoryDefinition]
) -> list[ThanhnienCategoryDefinition]:
    if config.thanhnien.crawl_all:
        selected_slugs = list(catalog.keys())
    elif config.thanhnien.selected_slugs:
        selected_slugs = list(config.thanhnien.selected_slugs)
    else:
        selected_slugs = [slug for slug in _DEFAULT_THANHNIEN_CATEGORY_SLUGS if slug in catalog]

    if not selected_slugs:
        raise ValueError(
            "No Thanhnien categories selected. Provide --thanhnien-categories or update the category catalog."
        )

    missing = [slug for slug in selected_slugs if slug not in catalog]
    if missing:
        raise ValueError(f"Unknown Thanhnien categories requested: {', '.join(missing)}")

    return [catalog[slug] for slug in selected_slugs]


def build_thanhnien_job_loader(config: IngestConfig, existing_urls: set[str]) -> JobLoader:
    if config.jobs_file_provided:
        LOGGER.info("Thanhnien jobs file supplied; using NDJSONJobLoader at %s", config.jobs_file)
        return NDJSONJobLoader(
            jobs_file=config.jobs_file,
            existing_urls=existing_urls,
            resume=config.resume,
        )

    catalog: dict[str, ThanhnienCategoryDefinition] = {
        category.slug: category for category in _DEFAULT_THANHNIEN_CATEGORIES
    }

    catalog_path = Path("data/thanhnien_categories.json")
    if catalog_path.exists():
        try:
            catalog = _load_thanhnien_category_catalog(catalog_path)
            LOGGER.info("Loaded Thanhnien category catalog from %s", catalog_path)
        except ValueError as exc:
            LOGGER.warning("Ignoring invalid Thanhnien category catalog at %s: %s", catalog_path, exc)

    try:
        categories = _select_thanhnien_categories(config, catalog)
    except ValueError as exc:
        raise ValueError(f"Failed to select Thanhnien categories: {exc}") from exc

    LOGGER.info(
        "Initialized ThanhnienCategoryLoader with categories: %s",
        ", ".join(category.slug for category in categories),
    )

    return ThanhnienCategoryLoader(
        categories=categories,
        existing_urls=existing_urls,
        resume=config.resume,
        user_agent=config.user_agent,
        max_pages=config.thanhnien.max_pages,
        max_empty_pages=config.thanhnien.max_empty_pages,
        request_timeout=config.timeout.request_timeout,
        proxy=config.proxy,
    )


_DEFAULT_ZNEWS_CATEGORY_SLUGS: tuple[str, ...] = (
    "thoi-su",
    "the-gioi",
    "kinh-doanh",
    "cong-nghe",
    "the-thao",
    "giai-tri",
    "doi-song",
    "phap-luat",
    "suc-khoe",
    "du-lich",
    "giao-duc",
    "oto-xe-may",
    "xuat-ban",
    "that-gia",
)
_DEFAULT_ZNEWS_CATEGORIES: tuple[ZnewsCategoryDefinition, ...] = (
    ZnewsCategoryDefinition(
        slug="thoi-su",
        name="Thời sự",
        landing_url=_normalize_znews_url("https://znews.vn/thoi-su.html"),
    ),
    ZnewsCategoryDefinition(
        slug="the-gioi",
        name="Thế giới",
        landing_url=_normalize_znews_url("https://znews.vn/the-gioi.html"),
    ),
    ZnewsCategoryDefinition(
        slug="kinh-doanh",
        name="Kinh doanh",
        landing_url=_normalize_znews_url("https://znews.vn/kinh-doanh.html"),
    ),
    ZnewsCategoryDefinition(
        slug="cong-nghe",
        name="Công nghệ",
        landing_url=_normalize_znews_url("https://znews.vn/cong-nghe.html"),
    ),
    ZnewsCategoryDefinition(
        slug="the-thao",
        name="Thể thao",
        landing_url=_normalize_znews_url("https://znews.vn/the-thao.html"),
    ),
    ZnewsCategoryDefinition(
        slug="giai-tri",
        name="Giải trí",
        landing_url=_normalize_znews_url("https://znews.vn/giai-tri.html"),
    ),
    ZnewsCategoryDefinition(
        slug="doi-song",
        name="Đời sống",
        landing_url=_normalize_znews_url("https://znews.vn/doi-song.html"),
    ),
    ZnewsCategoryDefinition(
        slug="phap-luat",
        name="Pháp luật",
        landing_url=_normalize_znews_url("https://lifestyle.znews.vn/phap-luat.html"),
    ),
    ZnewsCategoryDefinition(
        slug="suc-khoe",
        name="Sức khỏe",
        landing_url=_normalize_znews_url("https://znews.vn/suc-khoe.html"),
    ),
    ZnewsCategoryDefinition(
        slug="du-lich",
        name="Du lịch",
        landing_url=_normalize_znews_url("https://znews.vn/du-lich.html"),
    ),
    ZnewsCategoryDefinition(
        slug="giao-duc",
        name="Giáo dục",
        landing_url=_normalize_znews_url("https://znews.vn/giao-duc.html"),
    ),
    ZnewsCategoryDefinition(
        slug="oto-xe-may",
        name="Xe",
        landing_url=_normalize_znews_url("https://znews.vn/oto-xe-may.html"),
    ),
    ZnewsCategoryDefinition(
        slug="xuat-ban",
        name="Xuất bản",
        landing_url=_normalize_znews_url("https://znews.vn/xuat-ban.html"),
    ),
    ZnewsCategoryDefinition(
        slug="that-gia",
        name="Thật giả",
        landing_url=_normalize_znews_url("https://znews.vn/that-gia.html"),
    ),
)


def _load_znews_category_catalog(catalog_path: Path) -> dict[str, ZnewsCategoryDefinition]:
    try:
        raw_payload = catalog_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Znews category catalog not found: {catalog_path}") from exc

    try:
        records = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Znews category catalog: {exc}") from exc

    catalog: dict[str, ZnewsCategoryDefinition] = {}
    for entry in records:
        slug = entry.get("slug")
        name = entry.get("name") or ""
        landing_url = entry.get("landing_url") or ""

        if not isinstance(slug, str) or not slug:
            raise ValueError("Category catalog entries must include a non-empty 'slug'")

        definition = ZnewsCategoryDefinition(
            slug=slug.strip().lower(),
            name=name.strip() or slug,
            landing_url=_normalize_znews_url(landing_url),
        )
        catalog[definition.slug] = definition

    if not catalog:
        raise ValueError("Znews category catalog is empty")
    return catalog


def _select_znews_categories(
    config: IngestConfig, catalog: dict[str, ZnewsCategoryDefinition]
) -> list[ZnewsCategoryDefinition]:
    if config.znews.crawl_all:
        selected_slugs = list(catalog.keys())
    elif config.znews.selected_slugs:
        selected_slugs = list(config.znews.selected_slugs)
    else:
        selected_slugs = [slug for slug in _DEFAULT_ZNEWS_CATEGORY_SLUGS if slug in catalog]

    if not selected_slugs:
        raise ValueError("No Znews categories selected. Provide --znews-categories or update the category catalog.")

    missing = [slug for slug in selected_slugs if slug not in catalog]
    if missing:
        raise ValueError(f"Unknown Znews categories requested: {', '.join(missing)}")

    return [catalog[slug] for slug in selected_slugs]


def build_znews_job_loader(config: IngestConfig, existing_urls: set[str]) -> JobLoader:
    if config.jobs_file_provided:
        LOGGER.info("Znews jobs file supplied; using NDJSONJobLoader at %s", config.jobs_file)
        return NDJSONJobLoader(
            jobs_file=config.jobs_file,
            existing_urls=existing_urls,
            resume=config.resume,
        )

    if not config.znews.use_categories:
        LOGGER.info("Using Znews sitemap loader (category pagination disabled)")
        return SitemapJobLoader(
            sitemap_url="https://znews.vn/sitemap/sitemap.xml",
            existing_urls=existing_urls,
            resume=config.resume,
            user_agent=config.user_agent,
            allowed_patterns=("sitemap-article", "sitemap-news"),
            max_sitemaps=config.sitemap_max_documents,
            max_urls_per_sitemap=config.sitemap_max_urls_per_document,
            request_timeout=config.timeout.request_timeout,
            proxy=config.proxy,
        )

    catalog: dict[str, ZnewsCategoryDefinition] = {
        category.slug: category for category in _DEFAULT_ZNEWS_CATEGORIES
    }

    catalog_path = Path("data/znews_categories.json")
    if catalog_path.exists():
        try:
            catalog = _load_znews_category_catalog(catalog_path)
            LOGGER.info("Loaded Znews category catalog from %s", catalog_path)
        except ValueError as exc:
            LOGGER.warning("Ignoring invalid Znews category catalog at %s: %s", catalog_path, exc)

    try:
        categories = _select_znews_categories(config, catalog)
    except ValueError as exc:
        raise ValueError(f"Failed to select Znews categories: {exc}") from exc

    LOGGER.info(
        "Initialized ZnewsCategoryLoader with categories: %s",
        ", ".join(category.slug for category in categories),
    )

    return ZnewsCategoryLoader(
        categories=categories,
        existing_urls=existing_urls,
        resume=config.resume,
        user_agent=config.user_agent,
        max_pages=config.znews.max_pages,
        request_timeout=config.timeout.request_timeout,
        proxy=config.proxy,
    )


def load_existing_urls(session: Session, site_slug: str | None = None) -> set[str]:
    """Return a set of article URLs already stored in the database.

    When ``site_slug`` is provided, only URLs for that site are returned.
    """

    statement = select(Article.url)
    if site_slug:
        statement = statement.where(Article.site_slug == site_slug)

    result = session.execute(statement)
    return {row[0] for row in result if row[0] is not None}
