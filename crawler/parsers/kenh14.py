"""HTML parser for Kenh14 article pages."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from . import (
    ArticleParser,
    AssetType,
    ParsedArticle,
    ParsedAsset,
    ParsingError,
    ensure_asset_sequence,
)

_KENH14_BASE_URL = "https://kenh14.vn"
_VIETNAM_TZ = timezone(timedelta(hours=7))


class Kenh14Parser(ArticleParser):
    """Parse Kenh14.vn article HTML into structured data."""

    _DATETIME_TEXT_PATTERN = re.compile(
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*,?\s*(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})"
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
        title_tag = soup.select_one("h1.kbwc-title, h1.kbwcb-title, h1.article-title, h1")
        if title_tag and title_tag.get_text(strip=True):
            return title_tag.get_text(strip=True)

        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            return og_title["content"].strip()
        return None

    def _extract_description(self, soup: BeautifulSoup) -> str | None:
        description_tag = soup.select_one("h2.knc-sapo, h2.kbwcb-sapo, h2.article-sapo, h2.article-summary")
        if description_tag and description_tag.get_text(strip=True):
            return description_tag.get_text(strip=True)

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            return meta_desc["content"].strip()
        return None

    def _find_content_container(self, soup: BeautifulSoup) -> Tag | None:
        selectors = [
            "div.knc-content",
            "div.kbwcb-content",
            "div.kbwc-body",
            "div.detail-content",
            "article#k14-detail-page",
            "article.article-content",
        ]
        for selector in selectors:
            container = soup.select_one(selector)
            if container:
                return container
        return soup.find("article")

    def _extract_content_text(self, container: Tag) -> str:
        paragraphs: list[str] = []
        for paragraph in container.find_all("p"):
            if paragraph.find_parent(["figure", "figcaption"]):
                continue
            text = paragraph.get_text(" ", strip=True)
            if not text:
                continue
            cleaned = re.sub(r"\s+", " ", text).strip()
            if cleaned:
                paragraphs.append(cleaned)
        return "\n\n".join(paragraphs)

    def _extract_category(self, soup: BeautifulSoup) -> tuple[str | None, str | None]:
        category_link = soup.select_one(
            "div.kbwc-meta a.kbwc__cate, div.kbwc-meta a.kbwc-cate, nav.bread-crumb a, "
            "ul.kbwc-breadcrumb a, ul.breadcrumb a, li.breadcrumb-item a"
        )
        if not category_link or not category_link.get_text(strip=True):
            meta_section = soup.find("meta", attrs={"property": "article:section"})
            if meta_section and meta_section.get("content"):
                name = meta_section["content"].strip()
                return self._slugify(name), name
            return None, None

        name = category_link.get_text(strip=True)
        if not name:
            return None, None

        href = category_link.get("href") or ""
        slug = self._slug_from_href(href) or self._slugify(name)
        return slug, name

    def _extract_publish_date(self, soup: BeautifulSoup) -> datetime | None:
        meta_date = soup.find("meta", attrs={"property": "article:published_time"})
        if meta_date and meta_date.get("content"):
            parsed = self._parse_iso_datetime(meta_date["content"])
            if parsed:
                return parsed

        meta_itemprop = soup.find("meta", attrs={"itemprop": "datePublished"})
        if meta_itemprop and meta_itemprop.get("content"):
            parsed = self._parse_iso_datetime(meta_itemprop["content"])
            if parsed:
                return parsed

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
            except (TypeError, json.JSONDecodeError):
                continue
            date_value = self._extract_date_from_ldjson(data)
            if isinstance(date_value, str):
                parsed = self._parse_iso_datetime(date_value)
                if parsed:
                    return parsed

        time_tag = soup.select_one("span.kbwcm-time, span.kbwc-meta-time, div.kbwc-meta time")
        if time_tag:
            raw_date = time_tag.get("datetime") or time_tag.get_text(strip=True)
            if raw_date:
                parsed = self._parse_datetime_text(raw_date)
                if parsed:
                    return parsed

        return None

    def _extract_date_from_ldjson(self, data: object) -> str | None:
        if isinstance(data, dict):
            for key in ("datePublished", "datecreated", "dateCreated"):
                value = data.get(key)
                if isinstance(value, str):
                    return value
            if "@graph" in data:
                return self._extract_date_from_ldjson(data["@graph"])
        elif isinstance(data, list):
            for item in data:
                value = self._extract_date_from_ldjson(item)
                if value:
                    return value
        return None

    def _parse_iso_datetime(self, raw_value: str) -> datetime | None:
        cleaned = raw_value.strip().replace("Z", "+00:00")
        if not cleaned:
            return None
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            return None

    def _parse_datetime_text(self, raw_value: str) -> datetime | None:
        match = self._DATETIME_TEXT_PATTERN.search(raw_value.replace("\xa0", " ").strip())
        if not match:
            return None
        try:
            hour = int(match.group("hour"))
            minute = int(match.group("minute"))
            day = int(match.group("day"))
            month = int(match.group("month"))
            year = int(match.group("year"))
        except (TypeError, ValueError):
            return None

        try:
            return datetime(year, month, day, hour, minute, tzinfo=_VIETNAM_TZ)
        except ValueError:
            return None

    def _extract_tags(self, soup: BeautifulSoup) -> list[str]:
        tags: list[str] = []
        for anchor in soup.select(
            "div.kbwc-tags a, div.kbw__tags a, ul.kbw__tags a, ul.tag-list a, div.tag a"
        ):
            text = anchor.get_text(strip=True)
            if text and text not in tags:
                tags.append(text)
        return tags

    def _extract_assets(self, container: Tag) -> Iterable[ParsedAsset]:
        sequence = 1
        for block in container.find_all(["figure", "div"], recursive=True):
            if block.name == "figure":
                created = self._extract_figure_asset(block, sequence)
                if created:
                    yield created
                    sequence += 1
            elif block.name == "div":
                created = self._extract_div_asset(block, sequence)
                if created:
                    yield created
                    sequence += 1

    def _extract_figure_asset(self, block: Tag, sequence: int) -> ParsedAsset | None:
        img = block.find("img")
        if not img:
            return None

        source_url = self._extract_image_source(img)
        if not source_url:
            return None

        caption_tag = block.find("figcaption")
        caption = caption_tag.get_text(strip=True) if caption_tag and caption_tag.get_text(strip=True) else None

        return ParsedAsset(
            source_url=source_url,
            asset_type=AssetType.IMAGE,
            sequence=sequence,
            caption=caption,
        )

    def _extract_div_asset(self, block: Tag, sequence: int) -> ParsedAsset | None:
        video = block.find("video")
        if video:
            source_url = self._extract_video_source(video)
            if source_url:
                return ParsedAsset(
                    source_url=source_url,
                    asset_type=AssetType.VIDEO,
                    sequence=sequence,
                )

        iframe = block.find("iframe")
        if iframe:
            source_url = self._normalize_media_url(iframe.get("src"))
            if source_url:
                return ParsedAsset(
                    source_url=source_url,
                    asset_type=AssetType.VIDEO,
                    sequence=sequence,
                )
        return None

    def _extract_image_source(self, tag: Tag) -> str | None:
        candidate_attrs = (
            "data-original",
            "data-src",
            "data-lazy-src",
            "data-srcset",
            "data-io-src",
            "srcset",
            "src",
        )
        for attr in candidate_attrs:
            if attr not in tag.attrs:
                continue
            raw_value = tag.get(attr) or ""
            if attr in {"srcset", "data-srcset"}:
                raw_value = raw_value.split(",")[0].split(" ")[0]
            source = self._normalize_media_url(raw_value)
            if source:
                return source
        return None

    def _extract_video_source(self, tag: Tag) -> str | None:
        if tag.get("src"):
            source = self._normalize_media_url(tag["src"])
            if source:
                return source
        for source_tag in tag.find_all("source"):
            if source_tag.get("src"):
                source = self._normalize_media_url(source_tag["src"])
                if source:
                    return source
        return None

    def _normalize_media_url(self, raw_url: str | None) -> str | None:
        if not raw_url:
            return None
        cleaned = raw_url.strip()
        if not cleaned:
            return None
        if cleaned.startswith("//"):
            cleaned = f"https:{cleaned}"
        elif cleaned.startswith("/"):
            cleaned = urljoin(f"{_KENH14_BASE_URL}/", cleaned.lstrip("/"))
        elif not cleaned.startswith("http"):
            return None
        return cleaned

    def _slug_from_href(self, href: str) -> str | None:
        if not href:
            return None
        cleaned = href.strip().strip("/")
        if not cleaned:
            return None
        slug = cleaned.split("/")[-1]
        slug = slug.split(".")[0]
        slug = slug.replace(".chn", "").strip()
        return self._slugify(slug) if slug else None

    def _slugify(self, value: str) -> str:
        cleaned = re.sub(r"\s+", "-", value.strip().lower())
        return cleaned or value.strip().lower()
