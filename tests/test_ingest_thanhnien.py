import argparse
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx

from crawler.config import IngestConfig, ProxyConfig
from crawler.http_client import HttpFetchError
from crawler.ingest_thanhnien import (
    _build_task_payload,
    _record_fetch_failure,
    _update_video_assets_with_playwright,
    build_config as build_thanhnien_config,
)
from crawler.jobs import ArticleJob, NDJSONJobLoader, ThanhnienCategoryLoader, build_thanhnien_job_loader
from crawler.parsers import AssetType, ParsedAsset
from crawler.playwright_support import PlaywrightVideoResolverError
from crawler.sites import get_site_definition


class DummyResolver:
    def __init__(self, streams, should_raise=False):
        self._streams = streams
        self._should_raise = should_raise
        self.calls = 0

    def resolve_streams(self, article_url: str):
        self.calls += 1
        if self._should_raise:
            raise PlaywrightVideoResolverError("resolver failure")
        return self._streams


class UpdateVideoAssetsPlaywrightTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.article_url = (
            "https://thanhnien.vn/quyen-linh-dan-con-gai-xinh-nhu-hoa-hau-di-cung-to-nghe-san-khau-"
            "185251003181545979.htm"
        )
        self.assets = [
            ParsedAsset(
                source_url="https://thanhnien.mediacdn.vn/video/sample.mp4",
                asset_type=AssetType.VIDEO,
                sequence=1,
            )
        ]

    def test_updates_video_asset_with_hls_manifest(self) -> None:
        expected_hls = (
            "https://thanhnien.mediacdn.vn/.hls/325084952045817856/2025/10/3/"
            "1-1759489185419194083592.mp4.master.m3u8?v=f-f3f2838c-1"
        )
        resolver = DummyResolver([
            {
                "json_url": "https://thanhnien.mediacdn.vn/325084952045817856/2025/10/3/"
                "1-1759489185419194083592.mp4.json",
                "hls": expected_hls,
                "mhls": "https://thanhnien.mediacdn.vn/.hls/mobile.m3u8",
            }
        ])

        _update_video_assets_with_playwright(resolver, self.article_url, self.assets)

        self.assertEqual(resolver.calls, 1)
        self.assertEqual(self.assets[0].source_url, expected_hls)

    def test_leaves_asset_unchanged_on_failure(self) -> None:
        original_url = self.assets[0].source_url
        resolver = DummyResolver([], should_raise=True)

        _update_video_assets_with_playwright(resolver, self.article_url, self.assets)

        self.assertEqual(self.assets[0].source_url, original_url)


class RecordFetchFailureTestCase(unittest.TestCase):
    def test_writes_failure_entry_to_log(self) -> None:
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            config = IngestConfig(
                storage_root=tmp_path / "storage",
                log_dir=tmp_path / "logs",
            )
            config.ensure_directories()
            job = ArticleJob(
                url="https://example.com/article",
                lastmod="2025-10-03T00:00:00+07:00",
                sitemap_url="https://example.com/sitemap.xml",
                image_url=None,
            )
            error = HttpFetchError("Exhausted retries while fetching HTML")

            _record_fetch_failure(config, job, error)

            log_file = config.log_dir / "fetch_failures.ndjson"
            self.assertTrue(log_file.exists())
            lines = log_file.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload["url"], job.url)
            self.assertEqual(payload["error"], str(error))
            self.assertEqual(payload["error_type"], "HttpFetchError")


class ThanhnienConfigTestCase(unittest.TestCase):
    def test_build_config_defaults_site_settings(self) -> None:
        site = get_site_definition("thanhnien")
        with TemporaryDirectory() as tmpdir:
            storage_root = Path(tmpdir) / "storage"
            args = argparse.Namespace(
                site="thanhnien",
                jobs_file=None,
                storage_root=storage_root,
                db_url="postgresql://user:pass@localhost/db",
                resume=False,
                raw_html_cache=False,
                proxy=None,
                proxy_change_url=None,
                proxy_key=None,
                proxy_scheme="http",
                proxy_rotation_interval=240.0,
                max_workers=4,
                use_playwright=False,
                playwright_timeout=30.0,
                sitemap_max_documents=None,
                sitemap_max_urls_per_document=None,
                thanhnien_categories=None,
                thanhnien_all_categories=False,
                thanhnien_max_pages=None,
                thanhnien_max_empty_pages=None,
            )

            config = build_thanhnien_config(args)

            self.assertEqual(config.jobs_file, site.default_jobs_file)
            self.assertEqual(config.user_agent, site.default_user_agent)
            self.assertFalse(config.jobs_file_provided)
            self.assertEqual(config.thanhnien.selected_slugs, ())
            self.assertFalse(config.thanhnien.crawl_all)
            self.assertEqual(config.thanhnien.max_pages, 10)
            self.assertEqual(config.thanhnien.max_empty_pages, 2)

    def test_build_config_parses_category_flags(self) -> None:
        site = get_site_definition("thanhnien")
        with TemporaryDirectory() as tmpdir:
            storage_root = Path(tmpdir) / "storage"
            args = argparse.Namespace(
                site="thanhnien",
                jobs_file=None,
                storage_root=storage_root,
                db_url="postgresql://user:pass@localhost/db",
                resume=False,
                raw_html_cache=False,
                proxy=None,
                proxy_change_url=None,
                proxy_key=None,
                proxy_scheme="http",
                proxy_rotation_interval=240.0,
                max_workers=4,
                use_playwright=False,
                playwright_timeout=30.0,
                sitemap_max_documents=None,
                sitemap_max_urls_per_document=None,
                thanhnien_categories="chinh-tri, THOI-SU-PHAP-LUAT",
                thanhnien_all_categories=False,
                thanhnien_max_pages=3,
                thanhnien_max_empty_pages=1,
            )

            config = build_thanhnien_config(args)

            self.assertEqual(config.jobs_file, site.default_jobs_file)
            self.assertEqual(config.thanhnien.selected_slugs, ("chinh-tri", "thoi-su-phap-luat"))
            self.assertFalse(config.thanhnien.crawl_all)
            self.assertEqual(config.thanhnien.max_pages, 3)
            self.assertEqual(config.thanhnien.max_empty_pages, 1)


class ThanhnienJobLoaderFactoryTestCase(unittest.TestCase):
    def test_returns_category_loader_when_catalog_available(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = IngestConfig(
                jobs_file=Path("data/thanhnien_jobs.ndjson"),
                storage_root=Path(tmpdir) / "storage",
                db_url="postgresql://user:pass@localhost/db",
            )
            config.jobs_file_provided = False
            config.thanhnien.selected_slugs = ("chinh-tri",)
            config.thanhnien.max_pages = 2

            loader = build_thanhnien_job_loader(config, set())

            self.assertIsInstance(loader, ThanhnienCategoryLoader)
            self.assertEqual([category.slug for category in loader._categories], ["chinh-tri"])

    def test_returns_ndjson_loader_when_jobs_file_override_present(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = IngestConfig(
                jobs_file=Path(tmpdir) / "custom.ndjson",
                storage_root=Path(tmpdir) / "storage",
                db_url="postgresql://user:pass@localhost/db",
            )
            config.jobs_file_provided = True

            loader = build_thanhnien_job_loader(config, set())

            self.assertIsInstance(loader, NDJSONJobLoader)

    def test_category_loader_passes_proxy_to_httpx_client(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = IngestConfig(
                jobs_file=Path("data/thanhnien_jobs.ndjson"),
                storage_root=Path(tmpdir) / "storage",
                db_url="postgresql://user:pass@localhost/db",
            )
            config.jobs_file_provided = False
            config.thanhnien.selected_slugs = ("chinh-tri",)
            config.proxy = ProxyConfig.from_endpoint("127.0.0.1:8181")

            loader = build_thanhnien_job_loader(config, set())
            self.assertIsInstance(loader, ThanhnienCategoryLoader)

            with patch("crawler.jobs.httpx.Client") as client_cls:
                client_instance = client_cls.return_value.__enter__.return_value
                client_instance.get.side_effect = httpx.HTTPError("boom")
                list(loader)

            kwargs = client_cls.call_args.kwargs
            self.assertEqual(kwargs.get("proxy"), config.proxy.httpx_proxy())


class BuildTaskPayloadTestCase(unittest.TestCase):
    def test_payload_includes_site_slug(self) -> None:
        site = get_site_definition("thanhnien")
        config = IngestConfig(
            jobs_file=site.default_jobs_file,
            storage_root=Path("storage"),
            db_url="postgresql://user:pass@localhost/db",
            user_agent=site.default_user_agent,
        )
        assets = [
            ParsedAsset(source_url="https://example.com/image.jpg", asset_type=AssetType.IMAGE, sequence=1),
        ]

        payload = _build_task_payload(
            config,
            site,
            "article-123",
            "https://example.com/article",
            assets,
            include_playwright=False,
        )

        self.assertEqual(payload["site"], site.slug)
        self.assertEqual(payload["assets"][0]["referrer"], "https://example.com/article")
        self.assertEqual(payload["config"]["storage_warn_threshold"], config.storage_warn_threshold)
        self.assertIsNone(payload["config"]["storage_pause_file"])


if __name__ == "__main__":
    unittest.main()
