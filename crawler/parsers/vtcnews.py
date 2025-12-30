"""HTML parser for VTCNews.vn articles."""

from __future__ import annotations

from datetime import datetime
import json
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


class VtcnewsParser(ArticleParser):
    """Parse VTCNews.vn article HTML into structured data."""

    _CONTENT_SELECTORS = (
        "div[itemprop='articleBody']",
        "article",
        "div.detail__content",
        "div.detail-content",
        "div.article-content",
        "div#article-content",
        "div#content-detail",
        "div.entry-content",
    )

    def parse(self, url: str, html: str) -> ParsedArticle:
        soup = BeautifulSoup(html, "html.parser")

        ld_article = self._extract_ldjson_article(soup)

        title = self._extract_title(soup, ld_article)
        if not title:
            raise ParsingError("Article title not found")

        description = self._extract_description(soup, ld_article)
        category_id, category_name = self._extract_category(soup, ld_article)
        publish_date = self._extract_publish_date(soup, ld_article)

        content_container = self._extract_content_container(soup)
        content = self._extract_content_text(content_container) or self._extract_article_body(ld_article) or ""

        assets = self._extract_assets(url, content_container, ld_article)

        return ParsedArticle(
            url=url,
            title=title,
            description=description,
            content=content,
            category_id=category_id,
            category_name=category_name,
            publish_date=publish_date,
            tags=self._extract_tags(soup, ld_article),
            comments=None,
            assets=assets,
        )

    def _extract_ldjson_article(self, soup: BeautifulSoup) -> dict | None:
        for payload in self._iter_ldjson_payloads(soup):
            candidate = self._find_newsarticle(payload)
            if candidate is not None:
                return candidate
        return None

    def _iter_ldjson_payloads(self, soup: BeautifulSoup) -> Iterable[object]:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue

    def _find_newsarticle(self, payload: object) -> dict | None:
        if isinstance(payload, dict):
            if self._is_newsarticle(payload):
                return payload
            graph = payload.get("@graph")
            if graph is not None:
                return self._find_newsarticle(graph)
            return None

        if isinstance(payload, list):
            for item in payload:
                found = self._find_newsarticle(item)
                if found is not None:
                    return found
        return None

    @staticmethod
    def _is_newsarticle(payload: dict) -> bool:
        raw_type = payload.get("@type")
        if isinstance(raw_type, str):
            return raw_type.lower() in {"newsarticle", "article"}
        if isinstance(raw_type, list):
            normalized = {str(entry).lower() for entry in raw_type if entry}
            return bool(normalized.intersection({"newsarticle", "article"}))
        return False

    def _extract_title(self, soup: BeautifulSoup, ld_article: dict | None) -> str | None:
        title_tag = soup.select_one("h1")
        if title_tag and title_tag.get_text(strip=True):
            return title_tag.get_text(strip=True)

        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            return og_title["content"].strip()

        if ld_article:
            headline = ld_article.get("headline")
            if isinstance(headline, str) and headline.strip():
                return headline.strip()

        return None

    def _extract_description(self, soup: BeautifulSoup, ld_article: dict | None) -> str | None:
        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description and meta_description.get("content"):
            return meta_description["content"].strip()

        og_description = soup.find("meta", attrs={"property": "og:description"})
        if og_description and og_description.get("content"):
            return og_description["content"].strip()

        if ld_article:
            description = ld_article.get("description")
            if isinstance(description, str) and description.strip():
                return description.strip()

        return None

    def _extract_category(self, soup: BeautifulSoup, ld_article: dict | None) -> tuple[str | None, str | None]:
        meta_section = soup.find("meta", attrs={"property": "article:section"})
        if meta_section and meta_section.get("content"):
            name = meta_section["content"].strip()
            if name:
                return self._slugify(name), name

        if ld_article:
            section = ld_article.get("articleSection")
            if isinstance(section, str) and section.strip():
                name = section.strip()
                return self._slugify(name), name

        breadcrumb = soup.select_one('nav[aria-label="breadcrumb"] a, .breadcrumb a')
        if breadcrumb and breadcrumb.get_text(strip=True):
            name = breadcrumb.get_text(strip=True)
            return self._slugify(name), name

        return None, None

    def _extract_publish_date(self, soup: BeautifulSoup, ld_article: dict | None) -> datetime | None:
        meta_date = soup.find("meta", attrs={"property": "article:published_time"})
        if meta_date and meta_date.get("content"):
            parsed = self._parse_iso_datetime(meta_date["content"])
            if parsed:
                return parsed

        if ld_article:
            date_value = ld_article.get("datePublished") or ld_article.get("dateCreated")
            if isinstance(date_value, str):
                parsed = self._parse_iso_datetime(date_value)
                if parsed:
                    return parsed

        return None

    def _extract_content_container(self, soup: BeautifulSoup) -> Tag | None:
        for selector in self._CONTENT_SELECTORS:
            node = soup.select_one(selector)
            if node is None:
                continue
            if node.get_text(strip=True):
                return node
        return None

    def _extract_content_text(self, container: Tag | None) -> str | None:
        if container is None:
            return None

        paragraphs: list[str] = []
        for element in container.find_all("p"):
            text = element.get_text(" ", strip=True).replace("\xa0", " ").strip()
            if not text:
                continue
            paragraphs.append(text)

        if not paragraphs:
            return None
        return "\n\n".join(paragraphs)

    @staticmethod
    def _extract_article_body(ld_article: dict | None) -> str | None:
        if not ld_article:
            return None
        body = ld_article.get("articleBody")
        if isinstance(body, str) and body.strip():
            return body.strip()
        return None

    def _extract_assets(self, url: str, container: Tag | None, ld_article: dict | None) -> list[ParsedAsset]:
        assets: list[ParsedAsset] = []
        seen: set[str] = set()

        for image_url in self._extract_ldjson_images(url, ld_article):
            if image_url in seen:
                continue
            seen.add(image_url)
            assets.append(
                ParsedAsset(
                    source_url=image_url,
                    asset_type=AssetType.IMAGE,
                    sequence=len(assets) + 1,
                    caption=None,
                    referrer=url,
                )
            )

        if container is not None:
            for img in container.find_all("img"):
                src = img.get("data-src") or img.get("data-original") or img.get("src") or ""
                normalized = self._normalize_media_url(url, src)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                caption = self._closest_caption(img)
                assets.append(
                    ParsedAsset(
                        source_url=normalized,
                        asset_type=AssetType.IMAGE,
                        sequence=len(assets) + 1,
                        caption=caption,
                        referrer=url,
                    )
                )

            for video in container.find_all("video"):
                source_url = self._normalize_media_url(url, video.get("src") or "")
                if not source_url:
                    source_tag = video.find("source")
                    if source_tag is not None:
                        source_url = self._normalize_media_url(url, source_tag.get("src") or "")
                if not source_url or source_url in seen:
                    continue
                seen.add(source_url)
                assets.append(
                    ParsedAsset(
                        source_url=source_url,
                        asset_type=AssetType.VIDEO,
                        sequence=len(assets) + 1,
                        caption=None,
                        referrer=url,
                    )
                )

        return ensure_asset_sequence(assets)

    @staticmethod
    def _extract_ldjson_images(url: str, ld_article: dict | None) -> list[str]:
        if not ld_article:
            return []

        image_value = ld_article.get("image")
        candidates: list[str] = []
        if isinstance(image_value, str):
            candidates = [image_value]
        elif isinstance(image_value, list):
            candidates = [entry for entry in image_value if isinstance(entry, str)]
        elif isinstance(image_value, dict):
            loc = image_value.get("url")
            if isinstance(loc, str):
                candidates = [loc]

        images: list[str] = []
        for raw in candidates:
            cleaned = raw.strip()
            if not cleaned:
                continue
            images.append(urljoin(url, cleaned))
        return images

    @staticmethod
    def _normalize_media_url(base_url: str, raw_url: str | None) -> str | None:
        if not raw_url:
            return None
        cleaned = raw_url.strip()
        if not cleaned:
            return None
        if cleaned.startswith("data:"):
            return None
        return urljoin(base_url, cleaned)

    @staticmethod
    def _closest_caption(img: Tag) -> str | None:
        for parent in img.parents:
            if not isinstance(parent, Tag):
                continue
            if parent.name == "figure":
                figcaption = parent.find("figcaption")
                if figcaption and figcaption.get_text(strip=True):
                    return figcaption.get_text(" ", strip=True)

            caption = parent.find(
                ["span", "div"],
                class_=lambda c: isinstance(c, str) and "caption" in c,
            )
            if caption and caption.get_text(strip=True):
                return caption.get_text(" ", strip=True)
            if parent.name in {"figure", "div"}:
                continue
        return None

    def _extract_tags(self, soup: BeautifulSoup, ld_article: dict | None) -> list[str]:
        tags: list[str] = []
        if ld_article:
            keywords = ld_article.get("keywords")
            if isinstance(keywords, str):
                tags.extend(self._split_keywords(keywords))
            elif isinstance(keywords, list):
                for entry in keywords:
                    if isinstance(entry, str):
                        tags.extend(self._split_keywords(entry))

        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        if meta_keywords and meta_keywords.get("content"):
            tags.extend(self._split_keywords(meta_keywords["content"]))

        for link in soup.select(".tags a, .tag a, a[href*='/tag/']"):
            text = link.get_text(strip=True)
            if text:
                tags.append(text)

        deduped: list[str] = []
        seen: set[str] = set()
        for tag in tags:
            cleaned = tag.strip()
            if not cleaned or cleaned.lower() in seen:
                continue
            seen.add(cleaned.lower())
            deduped.append(cleaned)
        return deduped

    @staticmethod
    def _split_keywords(raw_value: str) -> list[str]:
        return [part.strip() for part in raw_value.split(",") if part.strip()]

    @staticmethod
    def _parse_iso_datetime(raw_value: str) -> datetime | None:
        value = raw_value.strip()
        if not value:
            return None
        value = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _slugify(value: str) -> str:
        return "-".join(value.strip().lower().split())
