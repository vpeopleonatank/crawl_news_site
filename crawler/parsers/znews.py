"""HTML parser for Znews article and video pages."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
from typing import Tuple

from bs4 import BeautifulSoup, Tag

from . import (
    ArticleParser,
    AssetType,
    ParsedArticle,
    ParsedAsset,
    ParsingError,
    ensure_asset_sequence,
)


class ZnewsParser(ArticleParser):
    """Parse Znews.vn article HTML into structured data."""

    _DATE_TEXT_PATTERN = re.compile(
        r"(?:[^,]+,\s*)?"  # optional weekday prefix
        r"(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})\s+"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})"
        r"(?:\s*\((?:GMT|UTC)\s*(?P<offset_sign>[+-]?)(?P<offset_hours>\d{1,2})\))?",
        re.IGNORECASE,
    )

    def parse(self, url: str, html: str) -> ParsedArticle:
        soup = BeautifulSoup(html, "html.parser")

        title = self._extract_title(soup)
        if not title:
            raise ParsingError("Article title not found")

        description = self._extract_description(soup)
        category_id, category_name = self._extract_category(soup)

        publish_date = self._extract_publish_date(soup)

        article_body = soup.select_one("div.the-article-body")
        if article_body is not None:
            paragraphs = self._gather_paragraphs(article_body)
            content = "\n\n".join(paragraphs)
            asset_container = article_body
        else:
            content = description or ""
            video_feature = soup.select_one("#video-featured .video-player")
            if video_feature is None:
                video_feature = soup.select_one(".video-player")
            asset_container = video_feature

        structured_assets = self._gather_assets(asset_container)

        return ParsedArticle(
            url=url,
            title=title,
            description=description,
            content=content,
            category_id=category_id,
            category_name=category_name,
            publish_date=publish_date,
            tags=self._extract_tags(soup),
            comments=None,
            assets=structured_assets,
        )

    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        title_tag = soup.select_one("h1.the-article-title")
        if title_tag and title_tag.get_text(strip=True):
            return title_tag.get_text(strip=True)

        video_title = soup.select_one(".video-title h1, .video-info h1")
        if video_title and video_title.get_text(strip=True):
            return video_title.get_text(strip=True)

        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            return og_title["content"].strip()

        return None

    def _extract_description(self, soup: BeautifulSoup) -> str | None:
        summary = soup.select_one("p.the-article-summary, p.video-summary")
        if summary and summary.get_text(strip=True):
            return summary.get_text(strip=True)

        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description and meta_description.get("content"):
            return meta_description["content"].strip()

        return None

    def _extract_category(self, soup: BeautifulSoup) -> Tuple[str | None, str | None]:
        category_link = soup.select_one("p.the-article-category a, p.video-cate a")
        if not category_link:
            return None, None

        name = category_link.get_text(strip=True) or None
        if not name:
            return None, None

        href = category_link.get("href") or ""
        category_id = self._slug_from_href(href) or name.lower().replace(" ", "-")
        return category_id, name

    def _extract_publish_date(self, soup: BeautifulSoup) -> datetime | None:
        meta_date = soup.find("meta", attrs={"property": "article:published_time"})
        if meta_date and meta_date.get("content"):
            parsed = self._parse_iso_datetime(meta_date["content"])
            if parsed:
                return parsed

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
            except (TypeError, json.JSONDecodeError):
                continue
            date_val = self._extract_date_from_ldjson(data)
            if date_val:
                parsed = self._parse_iso_datetime(date_val)
                if parsed:
                    return parsed

        meta_list = soup.select_one("ul.the-article-meta li, .video-info .publish")
        if meta_list:
            text_value = meta_list.get_text(strip=True)
            parsed = self._parse_datetime_text(text_value)
            if parsed:
                return parsed

        return None

    def _extract_date_from_ldjson(self, data: object) -> str | None:
        if isinstance(data, dict):
            for key in ("datePublished", "datecreated", "dateCreated"):
                if key in data and isinstance(data[key], str):
                    return data[key]
            if "@graph" in data:
                return self._extract_date_from_ldjson(data["@graph"])
        elif isinstance(data, list):
            for item in data:
                value = self._extract_date_from_ldjson(item)
                if value:
                    return value
        return None

    def _parse_iso_datetime(self, raw_value: str) -> datetime | None:
        value = raw_value.strip()
        if not value:
            return None
        value = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _parse_datetime_text(self, raw_value: str) -> datetime | None:
        match = self._DATE_TEXT_PATTERN.search(raw_value.replace("\xa0", " ").strip())
        if not match:
            return None

        try:
            day = int(match.group("day"))
            month = int(match.group("month"))
            year = int(match.group("year"))
            hour = int(match.group("hour"))
            minute = int(match.group("minute"))
        except (TypeError, ValueError):
            return None

        dt = datetime(year, month, day, hour, minute)

        offset_hours_raw = match.group("offset_hours")
        if offset_hours_raw:
            try:
                hours_value = int(offset_hours_raw)
            except ValueError:
                hours_value = 0
            sign = -1 if (match.group("offset_sign") or "+").strip() == "-" else 1
            delta = timedelta(hours=hours_value * sign)
            tz_info = timezone(delta)
            dt = dt.replace(tzinfo=tz_info)

        return dt

    def _gather_paragraphs(self, container: Tag) -> list[str]:
        paragraphs: list[str] = []
        for element in container.find_all("p"):
            if self._is_caption(element) or not element.get_text(strip=True):
                continue
            text = element.get_text(" ", strip=True).replace("\xa0", " ").strip()
            if text:
                text = re.sub(r"\s+", " ", text)
            if text:
                paragraphs.append(text)
        return paragraphs

    def _gather_assets(self, container: Tag | None) -> list[ParsedAsset]:
        if container is None:
            return []

        assets: list[ParsedAsset] = []
        seen_sources: set[str] = set()
        sequence = 1

        for element in container.descendants:
            if not isinstance(element, Tag):
                continue

            if element.name == "img":
                source_url = self._normalize_media_url(element.get("data-src") or element.get("src"))
                if not source_url or source_url in seen_sources:
                    continue
                caption = self._extract_image_caption(element)
                assets.append(
                    ParsedAsset(
                        source_url=source_url,
                        asset_type=AssetType.IMAGE,
                        sequence=sequence,
                        caption=caption,
                    )
                )
                seen_sources.add(source_url)
                sequence += 1

            elif element.name == "video":
                source_url = (
                    element.get("src")
                    or element.get("data-hls")
                    or element.get("data-src")
                )
                normalized_url = self._normalize_media_url(source_url)
                if not normalized_url or normalized_url in seen_sources:
                    continue
                assets.append(
                    ParsedAsset(
                        source_url=normalized_url,
                        asset_type=AssetType.VIDEO,
                        sequence=sequence,
                    )
                )
                seen_sources.add(normalized_url)
                sequence += 1

        return ensure_asset_sequence(assets)

    def _extract_image_caption(self, element: Tag) -> str | None:
        figure = element.find_parent("figure")
        if figure:
            caption = figure.find("figcaption")
            if caption and caption.get_text(strip=True):
                return caption.get_text(strip=True)

        table = element.find_parent("table", class_="picture")
        if table:
            caption_cell = table.find(class_=re.compile("caption", re.IGNORECASE))
            if caption_cell and caption_cell.get_text(strip=True):
                return caption_cell.get_text(strip=True)

        title = element.get("title")
        if title:
            stripped = title.strip()
            if stripped:
                return stripped

        return None

    def _extract_tags(self, soup: BeautifulSoup) -> list[str]:
        tags = []
        for anchor in soup.select(".the-article-tags a"):
            text = anchor.get_text(strip=True)
            if text:
                tags.append(text)
        return tags

    def _slug_from_href(self, href: str) -> str | None:
        if not href:
            return None
        # Extract last path component without extension
        parts = href.strip("/").split("/")
        if not parts:
            return None
        last = parts[-1]
        if "." in last:
            last = last.split(".")[0]
        return last or None

    def _is_caption(self, element: Tag) -> bool:
        for ancestor in element.parents:
            classes = ancestor.get("class") or []
            if any("caption" in cls.lower() for cls in classes):
                return True
        classes = element.get("class") or []
        return any("caption" in cls.lower() for cls in classes)

    def _normalize_media_url(self, raw_url: str | None) -> str | None:
        if raw_url is None:
            return None
        url = raw_url.strip()
        if not url or url.startswith("data:"):
            return None
        if url.startswith("//"):
            return f"https:{url}"
        if url.startswith("http://") or url.startswith("https://"):
            return url
        if url.startswith("/"):
            return f"https://znews.vn{url}"
        return f"https://{url}"
