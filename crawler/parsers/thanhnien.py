"""HTML parser for Thanhnien article pages."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re

from bs4 import BeautifulSoup, Tag

from . import ArticleParser, ParsedArticle, ParsedAsset, AssetType, ParsingError, ensure_asset_sequence


class ThanhnienParser(ArticleParser):
    """Parse ThanhNien.vn article HTML into structured data."""

    _DATETIME_TEXT_PATTERN = re.compile(
        r"(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})\s+"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})"
        r"(?:\s+(?:GMT|UTC)(?P<offset>[+-]\d{1,2})(?::?(?P<offset_minute>\d{2}))?)?",
        re.IGNORECASE,
    )

    def parse(self, url: str, html: str) -> ParsedArticle:
        soup = BeautifulSoup(html, "html.parser")

        title_tag = soup.find("h1")
        if title_tag is None or not title_tag.text.strip():
            raise ParsingError("Article title not found")

        title = title_tag.text.strip()

        description_tag = soup.find("h2")
        description = description_tag.text.strip() if description_tag and description_tag.text else None

        content_container = soup.select_one(
            'div[data-role="content"], div.detail__content, div.detail-content'
        )
        if content_container is None:
            raise ParsingError("Article body not found")

        paragraphs = []
        for element in content_container.find_all("p"):
            if element.find_parent(["figure", "figcaption"]):
                continue
            text = element.get_text(strip=True)
            if text:
                paragraphs.append(text)
        content = "\n\n".join(paragraphs)

        category_name = None
        category_id = None
        category_link = soup.select_one(
            "ul.breadcrumb a:last-child, div.detail-cate a[data-role='cate-name'], div.detail-cate a"
        )
        if category_link:
            category_name = category_link.get_text(strip=True) or None
            if category_name:
                category_id = category_name.lower().replace(" ", "-")

        publish_date = None
        meta_date = soup.find("meta", attrs={"itemprop": "datePublished"})
        if meta_date and meta_date.get("content"):
            publish_date = self._parse_iso_datetime(meta_date["content"])

        if publish_date is None:
            meta_date = soup.find("meta", attrs={"property": "article:published_time"})
            if meta_date and meta_date.get("content"):
                publish_date = self._parse_iso_datetime(meta_date["content"])

        if publish_date is None:
            time_node = soup.select_one("div.detail__meta time, div.detail-time [data-role='publishdate']")
            if time_node:
                raw_datetime = time_node.get("datetime") or time_node.get_text(strip=True)
                if raw_datetime:
                    publish_date = self._parse_datetime_text(raw_datetime)

        tags = self._extract_tags(soup)

        assets = []
        sequence = 1
        if content_container is not None:
            for block in content_container.find_all(["figure", "div"], recursive=True):
                if block.name == "figure":
                    sequence = self._extract_figure_asset(block, assets, sequence)
                elif block.name == "div":
                    sequence = self._extract_stream_asset(block, assets, sequence)

        structured_assets = ensure_asset_sequence(assets)

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
            assets=structured_assets,
        )

    def _parse_iso_datetime(self, raw_value: str) -> datetime | None:
        try:
            return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _parse_datetime_text(self, raw_value: str) -> datetime | None:
        raw_value = raw_value.strip()

        parsed_iso = self._parse_iso_datetime(raw_value)
        if parsed_iso is not None:
            return parsed_iso

        match = self._DATETIME_TEXT_PATTERN.search(raw_value)
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

        offset_str = match.group("offset")
        if offset_str:
            minutes_part = match.group("offset_minute")
            try:
                hours_value = abs(int(offset_str))
                minutes_value = int(minutes_part) if minutes_part else 0
            except ValueError:
                return dt

            sign = -1 if offset_str.strip().startswith("-") else 1
            delta = timedelta(hours=hours_value, minutes=minutes_value)
            delta *= sign
            tzinfo = timezone(delta)
            dt = dt.replace(tzinfo=tzinfo)

        return dt

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

        tag_section = soup.select_one("div.detail__tags, div[data-role='tags']")
        if tag_section:
            for anchor in tag_section.find_all("a"):
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

    def _extract_figure_asset(self, block: Tag, assets: list[ParsedAsset], sequence: int) -> int:
        img = block.find("img")
        if img and img.get("src"):
            caption = None
            caption_tag = block.find("figcaption")
            if caption_tag:
                caption = caption_tag.get_text(strip=True) or None
            source_url = self._normalize_media_url(img["src"])
            if source_url:
                assets.append(
                    ParsedAsset(
                        source_url=source_url,
                        asset_type=AssetType.IMAGE,
                        sequence=sequence,
                        caption=caption,
                    )
                )
                return sequence + 1

        video = block.find("video")
        if video and video.get("src"):
            source_url = self._normalize_media_url(video["src"])
            if source_url and not source_url.startswith("blob:"):
                assets.append(
                    ParsedAsset(
                        source_url=source_url,
                        asset_type=AssetType.VIDEO,
                        sequence=sequence,
                    )
                )
                return sequence + 1

        return sequence

    def _extract_stream_asset(self, block: Tag, assets: list[ParsedAsset], sequence: int) -> int:
        if block.attrs.get("type") != "VideoStream":
            return sequence

        data_vid = block.attrs.get("data-vid") or block.attrs.get("data-src")
        source_url = self._normalize_media_url(data_vid) if data_vid else None

        if not source_url:
            return sequence

        assets.append(
            ParsedAsset(
                source_url=source_url,
                asset_type=AssetType.VIDEO,
                sequence=sequence,
                caption=self._extract_stream_caption(block),
            )
        )
        return sequence + 1

    def _extract_stream_caption(self, block: Tag) -> str | None:
        caption_container = block.find(class_="VideoCMS_Caption")
        if caption_container:
            text = caption_container.get_text(strip=True)
            return text or None
        return None

    @staticmethod
    def _normalize_media_url(raw_url: str | None) -> str | None:
        if raw_url is None:
            return None
        url = raw_url.strip()
        if not url or url.startswith("blob:"):
            return None
        if url.startswith("//"):
            return f"https:{url}"
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"https://{url.lstrip('/')}"
