from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Deque, Iterable, Iterator, Optional, TextIO
from urllib.error import URLError
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from http.client import HTTPException, IncompleteRead
import socket

if __package__ in (None, ""):
    # Allow running the module as a script by ensuring the project root is importable.
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from crawler.dedupe import ArticleRecord, SQLiteDedupeStore
else:
    from .dedupe import ArticleRecord, SQLiteDedupeStore


LOGGER = logging.getLogger(__name__)

SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
IMAGE_NS = "{http://www.google.com/schemas/sitemap-image/1.1}"


@dataclass(frozen=True)
class CrawlJob:
    url: str
    lastmod: Optional[str]
    sitemap_url: str
    image_url: Optional[str]


def _strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_lastmod(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    # Normalise ISO strings and retain timezone information if provided.
    try:
        normalised = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        from email.utils import parsedate_to_datetime

        try:
            normalised = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            LOGGER.debug("Failed to parse lastmod '%s'", text)
            return None
    return normalised.isoformat()


class SitemapCrawler:
    def __init__(
        self,
        root_url: str,
        storage: SQLiteDedupeStore,
        user_agent: str = "sitemap-backfill/1.0",
        max_workers: int = 1,
        request_timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff: float = 1.5,
        retry_base_delay: float = 1.0,
        error_stream: Optional[TextIO] = None,
    ) -> None:
        self.root_url = root_url
        self.storage = storage
        self.user_agent = user_agent
        self._root_netloc = urlparse(root_url).netloc
        self._max_workers = max(1, max_workers)
        self._request_timeout = max(1.0, request_timeout)
        self._max_retries = max(1, max_retries)
        self._retry_backoff = max(1.0, retry_backoff)
        self._retry_base_delay = max(0.1, retry_base_delay)
        self._error_stream = error_stream
        self._error_lock = threading.Lock() if error_stream is not None else None

    def crawl(self) -> Iterator[CrawlJob]:
        queue = self._load_sitemap_queue()
        LOGGER.info("Processing %d sitemap buckets", len(queue))
        if self._max_workers == 1:
            while queue:
                sitemap_url = queue.popleft()
                try:
                    data = self._fetch_sitemap(sitemap_url)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error("Failed to process %s: %s", sitemap_url, exc)
                    continue
                try:
                    yield from self._iter_and_emit(sitemap_url, data)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error("Failed to parse %s: %s", sitemap_url, exc)
                    self._record_error(sitemap_url, exc)
            return

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {executor.submit(self._fetch_sitemap, url): url for url in list(queue)}
            for future in as_completed(futures):
                sitemap_url = futures[future]
                try:
                    data = future.result()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error("Failed to process %s: %s", sitemap_url, exc)
                    continue
                try:
                    yield from self._iter_and_emit(sitemap_url, data)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error("Failed to parse %s: %s", sitemap_url, exc)
                    self._record_error(sitemap_url, exc)

    def _load_sitemap_queue(self) -> Deque[str]:
        data = self._fetch_xml(self.root_url)
        queue: Deque[str] = deque()
        for child in self._iter_sitemap_index(data):
            queue.append(child)
        return queue

    def _fetch_sitemap(self, sitemap_url: str) -> bytes:
        LOGGER.info("Crawling sitemap %s", sitemap_url)
        return self._fetch_xml(sitemap_url)

    def _iter_and_emit(self, sitemap_url: str, data: bytes) -> Iterator[CrawlJob]:
        for job in self._iter_sitemap_entries(data, sitemap_url):
            if self._emit(job):
                yield job

    def _emit(self, job: CrawlJob) -> bool:
        record = ArticleRecord(
            url=job.url,
            lastmod=job.lastmod,
            sitemap_url=job.sitemap_url,
            image_url=job.image_url,
        )
        return self.storage.upsert(record)

    def _record_error(self, sitemap_url: str, exc: Exception) -> None:
        if self._error_stream is None:
            return
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "sitemap_url": sitemap_url,
            "error": f"{type(exc).__name__}: {exc}",
        }
        line = json.dumps(payload, ensure_ascii=False)
        if self._error_lock is not None:
            with self._error_lock:
                self._error_stream.write(line + "\n")
                self._error_stream.flush()
        else:
            self._error_stream.write(line + "\n")
            self._error_stream.flush()

    def _iter_sitemap_index(self, data: bytes) -> Iterator[str]:
        tree = ET.fromstring(data)
        if _strip_namespace(tree.tag) != "sitemapindex":
            raise ValueError("Root sitemap is not a sitemapindex")
        for sitemap in tree.findall(f"{SITEMAP_NS}sitemap"):
            loc = sitemap.find(f"{SITEMAP_NS}loc")
            if loc is None or not loc.text:
                continue
            url = loc.text.strip()
            yield url

    def _iter_sitemap_entries(self, data: bytes, sitemap_url: str) -> Iterator[CrawlJob]:
        context = ET.iterparse(BytesIO(data), events=("end",))
        for event, elem in context:
            if event != "end" or _strip_namespace(elem.tag) != "url":
                continue

            raw_url: Optional[str] = None
            lastmod_raw: Optional[str] = None
            image_url: Optional[str] = None

            for child in elem:
                child_tag = _strip_namespace(child.tag)
                if child_tag == "loc" and child.text:
                    raw_url = child.text.strip()
                elif child_tag == "lastmod" and child.text:
                    lastmod_raw = child.text.strip()
                elif child_tag == "image":
                    for image_child in child:
                        if _strip_namespace(image_child.tag) == "loc" and image_child.text:
                            image_url = image_child.text.strip()
                            break

            if not raw_url:
                elem.clear()
                continue

            absolute_url = urljoin(sitemap_url, raw_url)
            normalised_url = self._normalise_url(absolute_url)

            resolved_image_url = None
            if image_url:
                resolved_image_url = self._normalise_url(urljoin(normalised_url, image_url))

            lastmod = _parse_lastmod(lastmod_raw)

            # Preserve chronological ordering within the sitemap by yielding immediately.
            yield CrawlJob(
                url=normalised_url,
                lastmod=lastmod,
                sitemap_url=sitemap_url,
                image_url=resolved_image_url,
            )
            elem.clear()

    def _normalise_url(self, url: str) -> str:
        parsed = urlparse(url.strip())
        scheme = "https"
        netloc = parsed.netloc or self._root_netloc
        path = parsed.path or "/"
        normalized = urlunparse((scheme, netloc.lower(), path, "", "", ""))
        return normalized

    def _fetch_xml(self, url: str) -> bytes:
        parsed = urlparse(url)
        if parsed.scheme in {"http", "https"}:
            req = Request(url, headers={"User-Agent": self.user_agent})
            for attempt in range(1, self._max_retries + 1):
                try:
                    with urlopen(req, timeout=self._request_timeout) as response:
                        data = response.read()
                        encoding = response.headers.get("Content-Encoding", "").lower()
                        if encoding == "gzip":
                            import gzip

                            data = gzip.decompress(data)
                        elif encoding == "deflate":
                            import zlib

                            data = zlib.decompress(data)
                        return data
                except (  # noqa: RUF100 - consolidated network failure cases
                    URLError,
                    socket.timeout,
                    TimeoutError,
                    ConnectionResetError,
                    socket.error,
                    IncompleteRead,
                    HTTPException,
                    OSError,
                    ValueError,
                ) as exc:
                    if attempt >= self._max_retries:
                        self._record_error(url, exc)
                        raise
                    delay = self._retry_base_delay * (self._retry_backoff ** (attempt - 1))
                    LOGGER.warning(
                        "Attempt %d/%d failed for %s: %s; retrying in %.1fs",
                        attempt,
                        self._max_retries,
                        url,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
        if parsed.scheme == "file":
            return Path(parsed.path).read_bytes()
        if parsed.scheme:
            raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")
        return Path(url).read_bytes()


def crawl_sitemaps(
    root_url: str,
    storage_path: Path,
    output_path: Path,
    limit: Optional[int] = None,
    workers: int = 1,
    timeout: float = 30.0,
    retries: int = 3,
    retry_wait: float = 1.0,
    retry_backoff: float = 1.5,
    error_output: Optional[Path] = None,
) -> int:
    storage = SQLiteDedupeStore(storage_path)
    error_stream: Optional[TextIO] = None
    try:
        if error_output is not None:
            error_output.parent.mkdir(parents=True, exist_ok=True)
            error_stream = error_output.open("a", encoding="utf-8")
        crawler = SitemapCrawler(
            root_url=root_url,
            storage=storage,
            max_workers=workers,
            request_timeout=timeout,
            max_retries=retries,
            retry_base_delay=retry_wait,
            retry_backoff=retry_backoff,
            error_stream=error_stream,
        )
        emitted = 0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if output_path.exists() else "w"
        with output_path.open(mode, encoding="utf-8") as stream:
            for job in crawler.crawl():
                stream.write(json.dumps(job.__dict__, ensure_ascii=False) + "\n")
                emitted += 1
                if limit is not None and emitted >= limit:
                    break
        LOGGER.info("Emitted %d crawl jobs", emitted)
        return emitted
    finally:
        if error_stream is not None:
            error_stream.close()


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill ThanhNien.vn via sitemaps")
    parser.add_argument("root_url", help="Root sitemap index URL, e.g., https://thanhnien.vn/sitemap.xml")
    parser.add_argument(
        "--state-db",
        dest="state_db",
        default=".cache/thanhnien_sitemap.db",
        type=Path,
        help="Path to the SQLite database used for dedupe",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="sitemap_jobs.ndjson",
        type=Path,
        help="File where crawl jobs will be appended",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        help="Optional maximum number of jobs to emit",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=1,
        help="Number of concurrent sitemap fetchers (default: 1)",
    )
    parser.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--retries",
        dest="retries",
        type=int,
        default=3,
        help="Number of retry attempts for failed sitemap downloads (default: 3)",
    )
    parser.add_argument(
        "--retry-wait",
        dest="retry_wait",
        type=float,
        default=1.0,
        help="Initial delay before retrying a failed request in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--retry-backoff",
        dest="retry_backoff",
        type=float,
        default=1.5,
        help="Multiplicative backoff factor applied to the retry wait (default: 1.5)",
    )
    parser.add_argument(
        "--error-output",
        dest="error_output",
        type=Path,
        help="Path to append JSON records for sitemap download failures",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        help="Python logging level (default: INFO)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    emitted = crawl_sitemaps(
        root_url=args.root_url,
        storage_path=args.state_db,
        output_path=args.output,
        limit=args.limit,
        workers=args.workers,
        timeout=args.timeout,
        retries=args.retries,
        retry_wait=args.retry_wait,
        retry_backoff=args.retry_backoff,
        error_output=args.error_output,
    )
    return 0 if emitted >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
