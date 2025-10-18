"""HTML parser for Nld.com.vn article pages."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Tag

from . import (
    ArticleParser,
    AssetType,
    ParsedArticle,
    ParsedAsset,
    ParsingError,
    ensure_asset_sequence,
)

_NLD_BASE_URL = "https://nld.com.vn"
_VIETNAM_TZ = timezone(timedelta(hours=7))


class NldParser(ArticleParser):
    """Parse Nld.com.vn article HTML into structured data."""

    _CONTENT_SELECTORS = (
        "div.content_detail",
        "div.content-detail",
        "div.content-body",
        "div.maincontent",
        "div.video__d-focus",
        "div.box-video-content",
        "div.detail-content[data-role='content']",
        "div[data-role='content'][itemprop='articleBody']",
        "div.detail-content",
        "div.afcbc-body",
        "[itemprop='articleBody']",
        "article.article-body",
        "article#content_detail",
    )
    _DATETIME_PATTERN = re.compile(
        r"(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})(?:\s*[,-]?\s*(?P<hour>\d{1,2}):(?P<minute>\d{2}))?"
    )

    def parse(self, url: str, html: str) -> ParsedArticle:
        soup = BeautifulSoup(html, "html.parser")

        title = self._extract_title(soup)
        if not title:
            raise ParsingError("Article title not found")

        description = self._extract_description(soup)
        content_container = self._find_content_container(soup)
        if content_container is None:
            raise ParsingError("Article body not found")

        content = self._extract_content_text(content_container)
        if not content.strip():
            if description:
                content = description
            else:
                content = title
        category_id, category_name = self._extract_category(soup)
        publish_date = self._extract_publish_date(soup)
        tags = self._extract_tags(soup)
        assets = ensure_asset_sequence(self._extract_assets(content_container))

        return ParsedArticle(
            url=url,
            title=title,
            description=description,
            content=content,
            category_id=category_id,
            category_name=category_name,
            publish_date=publish_date,
            tags=tags,
            comments=None,
            assets=assets,
        )

    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        title_tag = soup.select_one(
            "h1.title-detail, h1.title, h1.article-title, h1.detail-title, h1"
        )
        if title_tag and title_tag.get_text(strip=True):
            return title_tag.get_text(strip=True)

        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            return og_title["content"].strip()
        return None

    def _extract_description(self, soup: BeautifulSoup) -> str | None:
        description_tag = soup.select_one(
            "p.sapo, p.sapo-detail, h2.sapo, h2.article-sapo, h2.detail-sapo, div.detail-sapo, div.sapo, p.lead"
        )
        if description_tag and description_tag.get_text(strip=True):
            return description_tag.get_text(strip=True)

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            return meta_desc["content"].strip()
        return None

    def _find_content_container(self, soup: BeautifulSoup) -> Tag | None:
        for selector in self._CONTENT_SELECTORS:
            container = soup.select_one(selector)
            if container:
                return container
        fallback = soup.find(attrs={"itemprop": "articleBody"})
        if fallback:
            return fallback
        fallback = soup.select_one("[data-role='content']")
        if fallback:
            return fallback
        fallback = soup.select_one("div.video__d-focus")
        if fallback:
            return fallback
        return soup.find("article")

    def _extract_content_text(self, container: Tag) -> str:
        paragraphs: list[str] = []
        for candidate in container.find_all(["p", "blockquote"]):
            if candidate.find_parent(["figure", "figcaption"]):
                continue
            if candidate.find_parent(class_=re.compile(r"detail-(author|info|social)", re.IGNORECASE)):
                continue
            text = candidate.get_text(" ", strip=True)
            if not text:
                continue
            cleaned = re.sub(r"\s+", " ", text).strip()
            if cleaned:
                paragraphs.append(cleaned)
        return "\n\n".join(paragraphs)

    def _extract_category(self, soup: BeautifulSoup) -> tuple[str | None, str | None]:
        breadcrumb_link = soup.select_one(
            "ul.breadcrumb a[href], nav.breadcrumb a[href], div.breadcrum a[href], div.breadcrumb a[href], div.detail-cate a[href], a[data-role='cate-name']"
        )
        if breadcrumb_link and breadcrumb_link.get_text(strip=True):
            name = breadcrumb_link.get_text(strip=True)
            slug = self._slug_from_href(breadcrumb_link.get("href"))
            return slug or self._slugify(name), name

        meta_section = soup.find("meta", attrs={"property": "article:section"})
        if meta_section and meta_section.get("content"):
            name = meta_section["content"].strip()
            return self._slugify(name), name

        return None, None

    def _extract_publish_date(self, soup: BeautifulSoup) -> datetime | None:
        meta_tag = soup.find("meta", attrs={"property": "article:published_time"})
        if meta_tag and meta_tag.get("content"):
            parsed = self._parse_iso_datetime(meta_tag["content"])
            if parsed:
                return parsed

        time_tag = soup.find("time")
        if time_tag:
            datetime_attr = time_tag.get("datetime")
            if datetime_attr:
                parsed = self._parse_iso_datetime(datetime_attr)
                if parsed:
                    return parsed
            text = time_tag.get_text(strip=True)
            parsed = self._parse_text_datetime(text)
            if parsed:
                return parsed

        time_span = soup.select_one("span.time, div.time")
        if time_span and time_span.get_text(strip=True):
            parsed = self._parse_text_datetime(time_span.get_text(strip=True))
            if parsed:
                return parsed

        return None

    def _parse_iso_datetime(self, raw_value: str) -> datetime | None:
        cleaned = raw_value.strip().replace("Z", "+00:00")
        if not cleaned:
            return None
        try:
            dt = datetime.fromisoformat(cleaned)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=_VIETNAM_TZ)
        return dt

    def _parse_text_datetime(self, raw_value: str | None) -> datetime | None:
        if not raw_value:
            return None
        match = self._DATETIME_PATTERN.search(raw_value.replace("\xa0", " "))
        if not match:
            return None
        try:
            day = int(match.group("day"))
            month = int(match.group("month"))
            year = int(match.group("year"))
            hour = int(match.group("hour")) if match.group("hour") else 0
            minute = int(match.group("minute")) if match.group("minute") else 0
        except (TypeError, ValueError):
            return None

        try:
            return datetime(year, month, day, hour, minute, tzinfo=_VIETNAM_TZ)
        except ValueError:
            return None

    def _extract_tags(self, soup: BeautifulSoup) -> list[str]:
        tags: list[str] = []
        seen: set[str] = set()

        for selector in (
            "meta[property='article:tag']",
            "meta[name='news_keywords']",
            "meta[name='keywords']",
        ):
            for meta_tag in soup.select(selector):
                content = meta_tag.get("content")
                if not content:
                    continue
                for piece in content.split(","):
                    candidate = self._normalize_tag(piece)
                    if not candidate:
                        continue
                    key = candidate.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    tags.append(candidate)

        tag_container = soup.select_one("div.tags, div.tag-box, ul.tags-list, section.tags")
        if tag_container:
            for anchor in tag_container.find_all("a"):
                candidate = self._normalize_tag(anchor.get_text(strip=True))
                if not candidate:
                    continue
                key = candidate.lower()
                if key in seen:
                    continue
                seen.add(key)
                tags.append(candidate)

        return tags

    def _normalize_tag(self, value: str | None) -> str | None:
        if not value:
            return None
        cleaned = value.strip()
        return cleaned or None

    def _extract_assets(self, container: Tag) -> Iterable[ParsedAsset]:
        assets: list[ParsedAsset] = []
        sequence = 1
        seen: set[str] = set()

        for figure in container.find_all("figure"):
            image_candidate = figure.find("img")
            if not image_candidate:
                continue
            src = self._extract_image_source(image_candidate)
            if not src:
                continue
            absolute = self._to_absolute_url(src)
            if not absolute or absolute in seen:
                continue
            caption_tag = figure.find("figcaption")
            caption = caption_tag.get_text(strip=True) if caption_tag else None
            assets.append(
                ParsedAsset(
                    source_url=absolute,
                    asset_type=AssetType.IMAGE,
                    sequence=sequence,
                    caption=caption or None,
                )
            )
            seen.add(absolute)
            sequence += 1

        for image_tag in container.find_all("img"):
            if image_tag.find_parent("figure"):
                continue
            src = self._extract_image_source(image_tag)
            if not src:
                continue
            absolute = self._to_absolute_url(src)
            if not absolute or absolute in seen:
                continue
            caption = None
            figure_parent = image_tag.find_parent("div", class_=re.compile("caption", re.IGNORECASE))
            if figure_parent and figure_parent.get_text(strip=True):
                caption = figure_parent.get_text(strip=True)
            assets.append(
                ParsedAsset(
                    source_url=absolute,
                    asset_type=AssetType.IMAGE,
                    sequence=sequence,
                    caption=caption,
                )
            )
            seen.add(absolute)
            sequence += 1

        for video in container.find_all("video"):
            source = self._extract_video_source(video)
            if not source:
                continue
            absolute = self._to_absolute_url(source)
            if not absolute or absolute in seen:
                continue
            assets.append(
                ParsedAsset(
                    source_url=absolute,
                    asset_type=AssetType.VIDEO,
                    sequence=sequence,
                )
            )
            seen.add(absolute)
            sequence += 1

        for iframe in container.find_all("iframe"):
            src = iframe.get("data-src") or iframe.get("src")
            if not src:
                continue
            absolute = self._to_absolute_url(src)
            if not absolute or absolute in seen:
                continue
            assets.append(
                ParsedAsset(
                    source_url=absolute,
                    asset_type=AssetType.VIDEO,
                    sequence=sequence,
                )
            )
            seen.add(absolute)
            sequence += 1

        for embed in container.find_all(attrs={"data-vid": True}):
            src = embed.get("data-vid")
            if not src:
                continue
            absolute = self._to_absolute_url(src)
            if not absolute or absolute in seen:
                continue
            assets.append(
                ParsedAsset(
                    source_url=absolute,
                    asset_type=AssetType.VIDEO,
                    sequence=sequence,
                )
            )
            seen.add(absolute)
            sequence += 1

        return assets

    def _extract_image_source(self, tag: Tag) -> str | None:
        candidates = [
            tag.get("data-src"),
            tag.get("data-original"),
            tag.get("data-echo"),
            tag.get("data-lazy-src"),
            tag.get("data-srcset"),
            tag.get("src"),
        ]
        for candidate in candidates:
            if not candidate:
                continue
            value = candidate.strip()
            if not value:
                continue
            if "," in value and " " in value:
                first = value.split(",")[0].strip()
                if " " in first:
                    first = first.split(" ")[0]
                if first:
                    return first
            return value
        return None

    def _extract_video_source(self, tag: Tag) -> str | None:
        source = tag.get("src")
        if source:
            return source.strip()
        source_child = tag.find("source")
        if source_child and source_child.get("src"):
            return source_child["src"].strip()
        data_src = tag.get("data-src")
        if data_src:
            return data_src.strip()
        return None

    def _to_absolute_url(self, url: str) -> str:
        cleaned = (url or "").strip()
        if not cleaned:
            return cleaned
        if cleaned.startswith("//"):
            return f"https:{cleaned}"
        if cleaned.startswith("http"):
            return cleaned
        if "/" in cleaned and re.match(r"^[a-z0-9.-]+\.[a-z]{2,}/", cleaned, re.IGNORECASE):
            return f"https://{cleaned.lstrip('/')}"
        return urljoin(f"{_NLD_BASE_URL}/", cleaned.lstrip("/"))

    def _slug_from_href(self, href: str | None) -> str | None:
        if not href:
            return None
        cleaned = href.strip()
        if not cleaned:
            return None
        if cleaned.startswith("//"):
            cleaned = f"https:{cleaned}"
        elif cleaned.startswith("/"):
            cleaned = urljoin(_NLD_BASE_URL, cleaned)
        elif not cleaned.startswith("http"):
            cleaned = urljoin(f"{_NLD_BASE_URL}/", cleaned)

        try:
            path = urlsplit(cleaned).path
        except ValueError:
            return None

        segments = [segment for segment in path.split("/") if segment]
        for segment in reversed(segments):
            base = segment.split(".", 1)[0]
            if base and base not in {"home", "tin-tuc"}:
                return base.lower()
        return None

    def _slugify(self, text: str | None) -> str | None:
        if not text:
            return None
        normalized = text.strip().lower()
        normalized = re.sub(r"[^\w\s-]", "", normalized)
        normalized = re.sub(r"\s+", "-", normalized)
        return normalized or None
