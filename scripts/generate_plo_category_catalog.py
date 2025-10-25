#!/usr/bin/env python3
"""
Generate the PLO category catalog with zone identifiers.

The script fetches the category sitemap, visits each landing page to extract the
`data-zone` identifier from the "Xem thêm" button, and writes the catalog to
`data/plo_categories.json`.
"""

from __future__ import annotations

import json
import logging
import re
import time
from itertools import count
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

SITEMAP_URL = "https://plo.vn/sitemaps/categories.xml"
OUTPUT_PATH = Path("data/plo_categories.json")
REQUEST_TIMEOUT = 10.0
USER_AGENT = "plo-category-catalog/1.0 (+https://plo.vn)"
LOAD_MORE_SELECTOR = "button.control__loadmore[data-zone]"
HEADING_SELECTORS: tuple[str, ...] = (
    "h1.category__title",
    "h1.section__title",
    "h1.detail__title",
    "header .title h1",
    "h1",
)

LOGGER = logging.getLogger("plo-category-catalog")


def fetch_category_urls(client: httpx.Client) -> list[str]:
    LOGGER.info("Fetching sitemap: %s", SITEMAP_URL)
    response = client.get(SITEMAP_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    try:
        document = ET.fromstring(response.text)
    except ET.ParseError as exc:
        raise RuntimeError(f"Failed to parse sitemap XML: {exc}") from exc

    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls: list[str] = []

    for url_element in document.findall("sm:url", namespace):
        loc_element = url_element.find("sm:loc", namespace)
        if loc_element is None or loc_element.text is None:
            continue
        url = loc_element.text.strip()
        if not url:
            continue
        if not url.endswith("/"):
            url = f"{url}/"
        if url.rstrip("/") == "https://plo.vn":
            continue
        urls.append(url)

    LOGGER.info("Discovered %d category URLs from sitemap", len(urls))
    return urls


def build_slug(url: str, *, existing: set[str]) -> str:
    parsed = urlparse(url)
    parts = [segment for segment in parsed.path.split("/") if segment]
    if not parts:
        slug = "home"
    else:
        slug = "-".join(parts)

    if slug not in existing:
        return slug

    for suffix in count(2):
        candidate = f"{slug}-{suffix}"
        if candidate not in existing:
            LOGGER.warning("Slug collision for %s; using %s", slug, candidate)
            return candidate


def extract_zone_metadata(client: httpx.Client, url: str) -> tuple[int, str]:
    LOGGER.debug("Fetching category page: %s", url)
    response = client.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    button = soup.select_one(LOAD_MORE_SELECTOR)
    zone_raw = (button.get("data-zone") or "").strip() if button else ""

    if not zone_raw.isdigit():
        script_match = re.search(r"adsZone\s*:\s*(\d+)", response.text)
        if not script_match:
            raise RuntimeError("Load-more button with data-zone not found")
        zone_raw = script_match.group(1)

    zone_id = int(zone_raw)

    heading_text: str | None = None
    for selector in HEADING_SELECTORS:
        heading = soup.select_one(selector)
        if heading and heading.get_text(strip=True):
            heading_text = heading.get_text(strip=True)
            break

    if not heading_text:
        title_tag = soup.find("title")
        if title_tag and title_tag.get_text(strip=True):
            heading_text = title_tag.get_text(strip=True).split("|", 1)[0].strip()

    if not heading_text:
        heading_text = build_slug(url, existing=set()).replace("-", " ").title()

    return zone_id, heading_text


def iterate_categories(client: httpx.Client, urls: Iterable[str]) -> list[dict[str, object]]:
    unique_urls = sorted({url.rstrip("/") + "/" for url in urls})
    records: list[dict[str, object]] = []
    known_slugs: set[str] = set()

    for index, url in enumerate(unique_urls, start=1):
        slug = build_slug(url, existing=known_slugs)
        LOGGER.info("[%d/%d] Processing %s -> %s", index, len(unique_urls), url, slug)

        for attempt in range(3):
            try:
                zone_id, name = extract_zone_metadata(client, url)
                break
            except httpx.HTTPError as exc:
                LOGGER.warning("HTTP error for %s (attempt %d/3): %s", url, attempt + 1, exc)
                time.sleep(1.5 * (attempt + 1))
            except RuntimeError as exc:
                LOGGER.warning("Failed to extract zone for %s: %s", url, exc)
                raise
        else:
            raise RuntimeError(f"Exceeded retry attempts for {url}")

        known_slugs.add(slug)
        records.append(
            {
                "slug": slug,
                "name": name,
                "zone_id": zone_id,
                "landing_url": url,
            }
        )

        time.sleep(0.2)

    return sorted(records, key=lambda record: record["slug"])


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(headers=headers, follow_redirects=True) as client:
        urls = fetch_category_urls(client)
        records = iterate_categories(client, urls)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("Wrote %d categories to %s", len(records), OUTPUT_PATH)


if __name__ == "__main__":
    main()
