"""HTML parser for PLO (Pháp Luật TP.HCM) article pages."""

from __future__ import annotations

import re
import unicodedata
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

_PLO_BASE_URL = "https://plo.vn"
_VIETNAM_TZ = timezone(timedelta(hours=7))


class PloParser(ArticleParser):
    """Parse plo.vn article HTML into structured data."""

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
        title_tag = soup.select_one("h1.article__title, h1.article-title, h1.detail__title, h1")
        if title_tag and title_tag.get_text(strip=True):
            return title_tag.get_text(strip=True)

        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            return og_title["content"].strip()
        return None

    def _extract_description(self, soup: BeautifulSoup) -> str | None:
        sapo = soup.select_one(".article__sapo, .article__lead, .detail__sapo")
        if sapo and sapo.get_text(strip=True):
            return self._clean_text(sapo.get_text(" ", strip=True))

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            return meta_desc["content"].strip()
        return None

    def _find_content_container(self, soup: BeautifulSoup) -> Tag | None:
        selectors = [
            "div.article__body",
            "div.article__content",
            "article.detail__body",
            "div.detail__content",
        ]
        for selector in selectors:
            container = soup.select_one(selector)
            if container:
                return container
        return soup.find("article")

    def _extract_content_text(self, container: Tag) -> str:
        paragraphs: list[str] = []
        for element in container.find_all(["p", "li"]):
            if self._is_excluded_node(element):
                continue
            text = element.get_text(" ", strip=True)
            cleaned = self._clean_text(text)
            if cleaned:
                paragraphs.append(cleaned)
        return "\n\n".join(paragraphs)

    def _is_excluded_node(self, node: Tag) -> bool:
        if node.find_parent(["figure", "figcaption"]):
            return True
        parent = node
        while parent:
            classes = parent.get("class") or []
            if any(self._looks_ad_class(cls) for cls in classes):
                return True
            parent = parent.parent if isinstance(parent.parent, Tag) else None
        return False

    def _looks_ad_class(self, class_name: str) -> bool:
        lowered = class_name.lower()
        return any(token in lowered for token in ("ads", "banner", "related", "zce-sapo"))

    def _extract_category(self, soup: BeautifulSoup) -> tuple[str | None, str | None]:
        meta_section = soup.find("meta", attrs={"property": "article:section"})
        if meta_section and meta_section.get("content"):
            parts = [part.strip() for part in meta_section["content"].split(",") if part.strip()]
            if parts:
                name = parts[-1]
                return self._slugify(name), name

        breadcrumb_links = soup.select(".breadcrumb a, .article__breadcrumb a")
        if breadcrumb_links:
            name = breadcrumb_links[-1].get_text(strip=True)
            if name:
                href = breadcrumb_links[-1].get("href") or ""
                slug = self._slug_from_href(href) or self._slugify(name)
                return slug, name
        return None, None

    def _extract_publish_date(self, soup: BeautifulSoup) -> datetime | None:
        time_tag = soup.find("time", attrs={"class": "article__time"})
        if time_tag and time_tag.get("datetime"):
            parsed = self._parse_datetime(time_tag["datetime"])
            if parsed:
                return parsed

        meta_date = soup.find("meta", attrs={"property": "article:published_time"})
        if meta_date and meta_date.get("content"):
            parsed = self._parse_datetime(meta_date["content"])
            if parsed:
                return parsed

        meta_itemprop = soup.find("meta", attrs={"itemprop": "datePublished"})
        if meta_itemprop and meta_itemprop.get("content"):
            parsed = self._parse_datetime(meta_itemprop["content"])
            if parsed:
                return parsed

        return None

    def _parse_datetime(self, raw_value: str) -> datetime | None:
        cleaned = raw_value.strip().replace("Z", "+00:00")
        if not cleaned:
            return None
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            pass
        try:
            timestamp = int(cleaned)
        except ValueError:
            return None
        return datetime.fromtimestamp(timestamp, tz=_VIETNAM_TZ)

    def _extract_tags(self, soup: BeautifulSoup) -> list[str]:
        tags: list[str] = []
        seen: set[str] = set()
        for selector in (
            "meta[property='article:tag']",
            "meta[name='news_keywords']",
            "meta[name='keywords']",
        ):
            for meta in soup.select(selector):
                content = meta.get("content")
                if not content:
                    continue
                for piece in content.split(","):
                    cleaned = self._clean_text(piece)
                    if not cleaned:
                        continue
                    key = cleaned.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    tags.append(cleaned)

        for anchor in soup.select(".article__tags a, .article__topic a"):
            name = self._clean_text(anchor.get_text(" ", strip=True))
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            tags.append(name)
        return tags

    def _extract_assets(self, container: Tag) -> list[ParsedAsset]:
        assets: list[ParsedAsset] = []
        sequence = 1

        for element in container.find_all(["figure", "img", "video", "iframe"]):
            if element.name == "figure":
                media = element.find("video")
                if media:
                    source = self._resolve_video_src(media)
                    if not source:
                        continue
                    caption = self._extract_caption_from_figure(element)
                    assets.append(
                        ParsedAsset(source_url=source, asset_type=AssetType.VIDEO, sequence=sequence, caption=caption)
                    )
                    sequence += 1
                    continue

                img = element.find("img")
                if img:
                    source = self._resolve_image_src(img)
                    if not source or self._should_ignore_image(source):
                        continue
                    caption = self._extract_caption_from_figure(element)
                    assets.append(
                        ParsedAsset(source_url=source, asset_type=AssetType.IMAGE, sequence=sequence, caption=caption)
                    )
                    sequence += 1
                continue

            if element.name == "img":
                if element.find_parent("figure"):
                    continue
                source = self._resolve_image_src(element)
                if not source or self._should_ignore_image(source):
                    continue
                caption = self._find_image_caption(element)
                assets.append(
                    ParsedAsset(source_url=source, asset_type=AssetType.IMAGE, sequence=sequence, caption=caption)
                )
                sequence += 1
                continue

            if element.name == "video":
                if element.find_parent("figure"):
                    continue
                source = self._resolve_video_src(element)
                if not source:
                    continue
                caption = self._find_video_caption(element)
                assets.append(
                    ParsedAsset(source_url=source, asset_type=AssetType.VIDEO, sequence=sequence, caption=caption)
                )
                sequence += 1
                continue

            if element.name == "iframe":
                source = element.get("src") or element.get("data-src")
                if not source:
                    continue
                normalized = self._normalize_media_url(source)
                if not normalized or not self._is_video_iframe(normalized):
                    continue
                caption = self._find_video_caption(element)
                assets.append(
                    ParsedAsset(source_url=normalized, asset_type=AssetType.VIDEO, sequence=sequence, caption=caption)
                )
                sequence += 1

        return assets

    def _resolve_image_src(self, img: Tag) -> str | None:
        candidates: Iterable[str | None] = (
            img.get("data-src"),
            self._extract_from_srcset(img.get("data-srcset")),
            self._extract_from_srcset(img.get("srcset")),
            img.get("src"),
        )
        for candidate in candidates:
            if not candidate:
                continue
            normalized = self._normalize_media_url(candidate)
            if normalized:
                return normalized
        return None

    def _extract_from_srcset(self, srcset: str | None) -> str | None:
        if not srcset:
            return None
        parts = [segment.strip() for segment in srcset.split(",") if segment.strip()]
        if not parts:
            return None
        first = parts[0].split(" ")[0].strip()
        return first or None

    def _resolve_video_src(self, video: Tag) -> str | None:
        source_tag = video.find("source")
        if source_tag and source_tag.get("src"):
            return self._normalize_media_url(source_tag["src"])

        if video.get("src"):
            return self._normalize_media_url(video["src"])

        data_src = video.get("data-src") or video.get("data-video-src")
        if data_src:
            return self._normalize_media_url(data_src)
        return None

    def _extract_caption_from_figure(self, figure: Tag) -> str | None:
        caption = figure.find("figcaption") or figure.find(class_="caption")
        if not caption:
            return None
        return self._clean_text(caption.get_text(" ", strip=True))

    def _find_image_caption(self, img: Tag) -> str | None:
        title = img.get("title")
        if title:
            cleaned = self._clean_text(title)
            if cleaned:
                return cleaned
        return None

    def _find_video_caption(self, element: Tag) -> str | None:
        title = element.get("title") or element.get("aria-label")
        if title:
            cleaned = self._clean_text(title)
            if cleaned:
                return cleaned
        return None

    def _should_ignore_image(self, url: str) -> bool:
        lowered = url.lower()
        if lowered.startswith("data:"):
            return True
        if lowered.endswith(".svg"):
            return True
        if "google-news" in lowered:
            return True
        if "icon" in lowered and lowered.endswith(".png"):
            return True
        return False

    def _normalize_media_url(self, url: str) -> str | None:
        cleaned = url.strip()
        if not cleaned:
            return None

        if cleaned.startswith("//"):
            cleaned = f"https:{cleaned}"
        elif cleaned.startswith("/"):
            cleaned = urljoin(_PLO_BASE_URL, cleaned)
        elif not cleaned.startswith("http"):
            cleaned = urljoin(f"{_PLO_BASE_URL}/", cleaned)
        return cleaned

    def _is_video_iframe(self, url: str) -> bool:
        lowered = url.lower()
        return any(token in lowered for token in ("youtube.com", "player.vimeo.com", "video"))

    def _slug_from_href(self, href: str) -> str | None:
        if not href:
            return None
        cleaned = href.strip().strip("/")
        if not cleaned:
            return None
        slug = cleaned.split("/")[-1]
        slug = slug.split(".")[0]
        return self._slugify(slug) if slug else None

    def _slugify(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        lowered = stripped.strip().lower()
        cleaned = re.sub(r"[^a-z0-9]+", "-", lowered)
        cleaned = cleaned.strip("-")
        return cleaned or lowered

    def _clean_text(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()
