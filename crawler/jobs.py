"""Job management utilities for article ingestion."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Article

LOGGER = logging.getLogger(__name__)


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


def load_existing_urls(session: Session) -> set[str]:
    """Return a set of article URLs already stored in the database."""
    result = session.execute(select(Article.url))
    return {row[0] for row in result if row[0] is not None}
