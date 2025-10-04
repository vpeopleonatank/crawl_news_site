"""HTML parser for Thanhnien article pages."""

from __future__ import annotations

from datetime import datetime

from bs4 import BeautifulSoup

from . import ArticleParser, ParsedArticle, ParsedAsset, AssetType, ParsingError, ensure_asset_sequence


class ThanhnienParser(ArticleParser):
    """Parse ThanhNien.vn article HTML into structured data."""

    def parse(self, url: str, html: str) -> ParsedArticle:
        soup = BeautifulSoup(html, "html.parser")

        title_tag = soup.find("h1")
        if title_tag is None or not title_tag.text.strip():
            raise ParsingError("Article title not found")

        title = title_tag.text.strip()

        description_tag = soup.find("h2")
        description = description_tag.text.strip() if description_tag and description_tag.text else None

        content_container = soup.find("div", class_="detail__content")
        if content_container is None:
            raise ParsingError("Article body not found")
        paragraphs = [p.get_text(strip=True) for p in content_container.find_all("p") if p.get_text(strip=True)]
        content = "\n\n".join(paragraphs)

        category_name = None
        category_id = None
        breadcrumb = soup.find("ul", class_="breadcrumb")
        if breadcrumb:
            category_links = breadcrumb.find_all("a")
            if category_links:
                category_name = category_links[-1].get_text(strip=True) or None
                if category_name:
                    category_id = category_name.lower().replace(" ", "-")

        publish_date = None
        date_tag = soup.find("div", class_="detail__meta")
        if date_tag and date_tag.time:
            raw_datetime = date_tag.time.get("datetime") or date_tag.time.get_text(strip=True)
            if raw_datetime:
                try:
                    publish_date = datetime.fromisoformat(raw_datetime.replace("Z", "+00:00"))
                except ValueError:
                    publish_date = None

        tags = []
        tag_section = soup.find("div", class_="detail__tags")
        if tag_section:
            tags = [tag.get_text(strip=True) for tag in tag_section.find_all("a") if tag.get_text(strip=True)]

        assets = []
        media_blocks = content_container.find_all("figure") if content_container else []
        sequence = 1
        for block in media_blocks:
            img = block.find("img")
            if img and img.get("src"):
                caption = None
                caption_tag = block.find("figcaption")
                if caption_tag:
                    caption = caption_tag.get_text(strip=True) or None
                assets.append(
                    ParsedAsset(
                        source_url=img["src"],
                        asset_type=AssetType.IMAGE,
                        sequence=sequence,
                        caption=caption,
                    )
                )
                sequence += 1
                continue

            video = block.find("video")
            if video and video.get("src"):
                assets.append(
                    ParsedAsset(
                        source_url=video["src"],
                        asset_type=AssetType.VIDEO,
                        sequence=sequence,
                    )
                )
                sequence += 1

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
