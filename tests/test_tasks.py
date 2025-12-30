import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from celery.exceptions import WorkerShutdown

from crawler.parsers import AssetType
from crawler.tasks import download_assets_task


def _build_job_config(root: Path) -> dict:
    storage_root = root / "storage"
    pause_file = storage_root / ".pause_ingest"
    return {
        "storage_root": str(storage_root),
        "storage_volume": "default",
        "storage_volume_root": str(storage_root),
        "storage_warn_threshold": 0.1,
        "storage_pause_file": str(pause_file),
        "user_agent": "test-agent",
        "request_timeout": 5.0,
        "asset_timeout": 5.0,
        "hls_download_timeout": 5.0,
    }


def _build_job_payload(root: Path) -> dict:
    return {
        "article_id": "article-123",
        "db_url": "sqlite://",
        "article_url": "https://example.com/article",
        "assets": [
            {
                "source_url": "https://example.com/image.jpg",
                "asset_type": AssetType.IMAGE.value,
                "sequence": 1,
                "caption": None,
                "referrer": "https://example.com/article",
            }
        ],
        "config": _build_job_config(root),
    }


class DownloadAssetsTaskTestCase(unittest.TestCase):
    @patch("crawler.tasks.StorageMonitor")
    @patch("crawler.tasks._session_factory")
    @patch("crawler.tasks.ArticlePersistence")
    @patch("crawler.tasks.AssetManager")
    def test_worker_shutdown_on_pre_download_check(
        self,
        asset_manager_cls: MagicMock,
        persistence_cls: MagicMock,
        session_factory: MagicMock,
        monitor_cls: MagicMock,
    ) -> None:
        monitor_instance = monitor_cls.return_value
        monitor_instance.check_and_maybe_pause.return_value = True

        with TemporaryDirectory() as tmpdir:
            job = _build_job_payload(Path(tmpdir))
            with self.assertRaises(WorkerShutdown):
                download_assets_task.run(job)

        asset_manager_cls.assert_not_called()
        persistence_cls.assert_not_called()
        session_factory.assert_not_called()

    @patch("crawler.tasks.StorageMonitor")
    @patch("crawler.tasks._session_factory")
    @patch("crawler.tasks.ArticlePersistence")
    @patch("crawler.tasks.AssetManager")
    def test_worker_shutdown_after_download_when_storage_crosses_threshold(
        self,
        asset_manager_cls: MagicMock,
        persistence_cls: MagicMock,
        session_factory: MagicMock,
        monitor_cls: MagicMock,
    ) -> None:
        monitor_pre = MagicMock()
        monitor_post = MagicMock()
        monitor_pre.check_and_maybe_pause.return_value = False
        monitor_post.check_and_maybe_pause.return_value = True
        monitor_pre.volume_path = Path("/tmp/pre")
        monitor_post.volume_path = Path("/tmp/post")
        monitor_cls.side_effect = [monitor_pre, monitor_post]

        manager = MagicMock()
        manager.download_assets.return_value = []
        asset_manager_cls.return_value.__enter__.return_value = manager

        persistence_instance = persistence_cls.return_value

        with TemporaryDirectory() as tmpdir:
            job = _build_job_payload(Path(tmpdir))
            with self.assertRaises(WorkerShutdown):
                download_assets_task.run(job)

        asset_manager_cls.assert_called_once()
        manager.download_assets.assert_called_once()
        persistence_instance.persist_assets.assert_called_once()
        session_factory.assert_called_once_with("sqlite://")
        self.assertEqual(monitor_cls.call_count, 2)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
