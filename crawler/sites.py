"""Site registry and utilities for multi-site ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict

from .parsers import ArticleParser
from .parsers.thanhnien import ThanhnienParser
from .playwright_support import ThanhnienVideoResolver


@dataclass(slots=True)
class SiteDefinition:
    """Configuration for a supported news site."""

    slug: str
    parser_factory: Callable[[], ArticleParser]
    default_jobs_file: Path
    default_user_agent: str
    playwright_resolver_factory: Callable[[float], object] | None = None

    def build_parser(self) -> ArticleParser:
        """Instantiate the parser associated with this site."""

        return self.parser_factory()

    def build_playwright_resolver(self, timeout: float):
        """Instantiate the site's Playwright resolver, if configured."""

        if self.playwright_resolver_factory is None:
            return None
        return self.playwright_resolver_factory(timeout)


_SITE_REGISTRY: Dict[str, SiteDefinition] = {
    "thanhnien": SiteDefinition(
        slug="thanhnien",
        parser_factory=ThanhnienParser,
        default_jobs_file=Path("data/thanhnien_jobs.ndjson"),
        default_user_agent="thanhnien-ingestor/1.0",
        playwright_resolver_factory=lambda timeout: ThanhnienVideoResolver(timeout=timeout),
    ),
}


def get_site_definition(site_slug: str) -> SiteDefinition:
    """Return the registered site definition for the given slug."""

    try:
        return _SITE_REGISTRY[site_slug]
    except KeyError as exc:
        raise KeyError(f"Unknown site '{site_slug}'") from exc


def list_sites() -> list[str]:
    """Return a sorted list of supported site slugs."""

    return sorted(_SITE_REGISTRY)
