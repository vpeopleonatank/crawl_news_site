"""Job management utilities for article ingestion."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol
from xml.etree import ElementTree as ET

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

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

def load_existing_urls(session: Session, site_slug: str | None = None) -> set[str]:
    """Return a set of article URLs already stored in the database.

    When ``site_slug`` is provided, only URLs for that site are returned.
    """

    statement = select(Article.url)
    if site_slug:
        statement = statement.where(Article.site_slug == site_slug)

    result = session.execute(statement)
    return {row[0] for row in result if row[0] is not None}
