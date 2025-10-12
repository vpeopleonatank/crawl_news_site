"""Parser interfaces and data models for article ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable


class AssetType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"


@dataclass(slots=True)
class ParsedAsset:
    source_url: str
    asset_type: AssetType
    sequence: int
    caption: str | None = None
    referrer: str | None = None


@dataclass(slots=True)
class ParsedArticle:
    url: str
    title: str
    description: str | None
    content: str
    category_id: str | None
    category_name: str | None
    publish_date: datetime | None
    tags: list[str]
    comments: dict | None
    assets: list[ParsedAsset]


class ParsingError(RuntimeError):
    """Raised when an article cannot be parsed into structured data."""


class ArticleParser:
    """Base interface for site-specific article parsers."""

    def parse(self, url: str, html: str) -> ParsedArticle:  # pragma: no cover - interface only
        raise NotImplementedError


def ensure_asset_sequence(assets: Iterable[ParsedAsset]) -> list[ParsedAsset]:
    """Returns assets sorted by their declared sequence number."""
    return sorted(assets, key=lambda asset: asset.sequence)
