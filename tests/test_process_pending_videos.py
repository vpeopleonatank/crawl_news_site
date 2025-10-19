from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from crawler.config import IngestConfig
from crawler import process_pending_videos


class ProcessPendingVideosCLITests(unittest.TestCase):
    @patch("crawler.process_pending_videos._process_pending_video_assets")
    @patch("crawler.process_pending_videos.sessionmaker")
    @patch("crawler.process_pending_videos.create_engine")
    @patch("crawler.process_pending_videos.Base")
    @patch("crawler.process_pending_videos.build_config")
    @patch("crawler.process_pending_videos.get_site_definition")
    def test_main_enqueues_pending_assets(
        self,
        get_site_definition_mock: MagicMock,
        build_config_mock: MagicMock,
        base_mock: MagicMock,
        create_engine_mock: MagicMock,
        sessionmaker_mock: MagicMock,
        process_pending_mock: MagicMock,
    ) -> None:
        site = SimpleNamespace(slug="thanhnien", playwright_resolver_factory=None)
        get_site_definition_mock.return_value = site

        config = IngestConfig()
        config.db_url = "sqlite://"
        config.video.enabled_categories = ("sports",)
        build_config_mock.return_value = config

        session_factory = MagicMock()
        sessionmaker_mock.return_value = session_factory

        download_task = MagicMock()
        download_task.app.conf.task_always_eager = False

        with patch.object(process_pending_videos, "download_assets_task", download_task):
            exit_code = process_pending_videos.main(
                ["--site", "thanhnien", "--db-url", "sqlite://"]
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(config.video.process_pending)
        create_engine_mock.assert_called_once_with("sqlite://")
        base_mock.metadata.create_all.assert_called_once()
        sessionmaker_mock.assert_called_once()
        process_pending_mock.assert_called_once_with(
            config,
            site,
            session_factory,
            use_celery_playwright=False,
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
