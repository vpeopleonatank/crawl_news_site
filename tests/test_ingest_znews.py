import argparse
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from crawler.config import IngestConfig
from crawler.ingest import build_config as build_generic_config
from crawler.jobs import (
    NDJSONJobLoader,
    SitemapJobLoader,
    ZnewsCategoryLoader,
    build_znews_job_loader,
)
from crawler.sites import get_site_definition


def build_znews_config(args: argparse.Namespace) -> IngestConfig:
    site = get_site_definition("znews")
    setattr(args, "site", "znews")
    return build_generic_config(args, site)


class ZnewsConfigTestCase(unittest.TestCase):
    def test_build_config_defaults_site_settings(self) -> None:
        site = get_site_definition("znews")
        with TemporaryDirectory() as tmpdir:
            storage_root = Path(tmpdir) / "storage"
            args = argparse.Namespace(
                site="znews",
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
                znews_use_categories=False,
                znews_categories=None,
                znews_all_categories=False,
                znews_max_pages=None,
                thanhnien_categories=None,
                thanhnien_all_categories=False,
                thanhnien_max_pages=None,
                thanhnien_max_empty_pages=None,
            )

            config = build_znews_config(args)

            self.assertEqual(config.jobs_file, site.default_jobs_file)
            self.assertEqual(config.user_agent, site.default_user_agent)
            self.assertFalse(config.jobs_file_provided)
            self.assertFalse(config.znews.use_categories)
            self.assertEqual(config.znews.selected_slugs, ())
            self.assertFalse(config.znews.crawl_all)
            self.assertEqual(config.znews.max_pages, 50)

    def test_build_config_enables_category_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            storage_root = Path(tmpdir) / "storage"
            args = argparse.Namespace(
                site="znews",
                jobs_file=None,
                storage_root=storage_root,
                db_url="postgresql://user:pass@localhost/db",
                resume=True,
                raw_html_cache=False,
                proxy=None,
                proxy_change_url=None,
                proxy_key=None,
                proxy_scheme="http",
                proxy_rotation_interval=240.0,
                max_workers=2,
                use_playwright=True,
                playwright_timeout=45.0,
                sitemap_max_documents=3,
                sitemap_max_urls_per_document=150,
                znews_use_categories=True,
                znews_categories="phap-luat,  doi-song ",
                znews_all_categories=False,
                znews_max_pages=25,
                thanhnien_categories=None,
                thanhnien_all_categories=False,
                thanhnien_max_pages=None,
                thanhnien_max_empty_pages=None,
            )

            config = build_znews_config(args)

            self.assertTrue(config.znews.use_categories)
            self.assertEqual(config.znews.selected_slugs, ("phap-luat", "doi-song"))
            self.assertFalse(config.znews.crawl_all)
            self.assertEqual(config.znews.max_pages, 25)
            self.assertEqual(config.sitemap_max_documents, 3)
            self.assertEqual(config.sitemap_max_urls_per_document, 150)


class ZnewsJobLoaderFactoryTestCase(unittest.TestCase):
    def test_returns_sitemap_loader_when_categories_disabled(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = IngestConfig(
                jobs_file=Path("data/znews_jobs.ndjson"),
                storage_root=Path(tmpdir) / "storage",
                db_url="postgresql://user:pass@localhost/db",
            )
            config.jobs_file_provided = False
            config.znews.use_categories = False

            loader = build_znews_job_loader(config, set())

            self.assertIsInstance(loader, SitemapJobLoader)

    def test_returns_category_loader_when_enabled(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = IngestConfig(
                jobs_file=Path("data/znews_jobs.ndjson"),
                storage_root=Path(tmpdir) / "storage",
                db_url="postgresql://user:pass@localhost/db",
            )
            config.jobs_file_provided = False
            config.znews.use_categories = True
            config.znews.selected_slugs = ("phap-luat",)
            config.znews.max_pages = 5

            loader = build_znews_job_loader(config, set())

            self.assertIsInstance(loader, ZnewsCategoryLoader)
            self.assertEqual([category.slug for category in loader._categories], ["phap-luat"])
            self.assertEqual(loader._max_pages, 5)

    def test_returns_ndjson_loader_when_jobs_file_override_present(self) -> None:
        with TemporaryDirectory() as tmpdir:
            jobs_file = Path(tmpdir) / "custom.ndjson"
            jobs_file.write_text('{"url": "https://example.com"}\n', encoding="utf-8")

            config = IngestConfig(
                jobs_file=jobs_file,
                storage_root=Path(tmpdir) / "storage",
                db_url="postgresql://user:pass@localhost/db",
            )
            config.jobs_file_provided = True
            config.znews.use_categories = True

            loader = build_znews_job_loader(config, set())

            self.assertIsInstance(loader, NDJSONJobLoader)


if __name__ == "__main__":  # pragma: no cover - test runner entrypoint
    unittest.main()
