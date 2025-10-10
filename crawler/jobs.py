"""Job management utilities for article ingestion."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol, Sequence
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import IngestConfig
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


@dataclass(slots=True)
class JobLoaderStats:
    total: int = 0
    emitted: int = 0
    skipped_existing: int = 0
    skipped_invalid: int = 0
    skipped_duplicate: int = 0


_THANHNIEN_BASE_URL = "https://thanhnien.vn"
_THANHNIEN_ARTICLE_PATTERN = re.compile(r"^https?://(?:[^./]+\.)?thanhnien\.vn/[^?#]+-185\d+\.htm$")


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
    ) -> None:
        self._sitemap_url = sitemap_url
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._allowed_patterns = allowed_patterns
        self._max_sitemaps = max_sitemaps
        self._max_urls_per_sitemap = max_urls_per_sitemap
        self._request_timeout = request_timeout

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
            with httpx.Client(headers=headers or None, timeout=self._request_timeout) as client:
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
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._max_empty_pages = max_empty_pages
        self._request_timeout = request_timeout
        self._include_landing_page = include_landing_page

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        with httpx.Client(headers=headers, timeout=self._request_timeout) as client:
            for category in self._categories:
                yield from self._iterate_category(client, category)

    def _iterate_category(self, client: httpx.Client, category: ThanhnienCategoryDefinition) -> Iterator[ArticleJob]:
        emitted_before = self.stats.emitted
        skipped_existing_before = self.stats.skipped_existing
        skipped_duplicate_before = self.stats.skipped_duplicate

        if self._include_landing_page:
            landing_html = self._fetch_html(client, category.normalized_landing_url())
            if landing_html:
                yield from self._emit_jobs_from_html(landing_html)

        page = 1
        consecutive_empty_pages = 0
        while True:
            if self._max_pages is not None and page > self._max_pages:
                break

            timeline_url = category.timeline_url(page)
            html = self._fetch_html(client, timeline_url)
            if not html:
                break

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
            "Thanhnien category '%s': emitted=%d skipped_existing=%d skipped_duplicate=%d",
            category.slug,
            self.stats.emitted - emitted_before,
            self.stats.skipped_existing - skipped_existing_before,
            self.stats.skipped_duplicate - skipped_duplicate_before,
        )

    def _fetch_html(self, client: httpx.Client, url: str) -> str:
        try:
            response = client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            LOGGER.warning("Thanhnien category request failed (%s): %s", url, exc)
            return ""
        except httpx.HTTPError as exc:
            LOGGER.warning("Thanhnien category request error (%s): %s", url, exc)
            return ""
        return response.text.strip()

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
    ) -> None:
        self._categories = list(categories)
        self._existing_urls = existing_urls or set()
        self._resume = resume
        self._user_agent = user_agent
        self._max_pages = max_pages
        self._request_timeout = request_timeout
        self._duplicate_fingerprint_size = max(1, duplicate_fingerprint_size)
        self._stop_on_duplicate = stop_on_duplicate

        self.stats = JobLoaderStats()
        self._seen_urls: set[str] = set()

    def __iter__(self) -> Iterator[ArticleJob]:
        self.stats = JobLoaderStats()
        self._seen_urls.clear()

        headers = {"User-Agent": self._user_agent} if self._user_agent else None
        with httpx.Client(headers=headers, timeout=self._request_timeout) as client:
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
        try:
            response = client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            LOGGER.warning("Znews category request failed (%s): %s", url, exc)
            return ""
        except httpx.HTTPError as exc:
            LOGGER.warning("Znews category request error (%s): %s", url, exc)
            return ""
        return response.text.strip()

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
