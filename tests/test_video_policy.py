from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from crawler.config import IngestConfig
from crawler.ingest import _process_failed_media_downloads, _process_pending_video_assets
from crawler.assets import StoredAsset
from crawler.parsers import AssetType, ParsedAsset
from crawler.persistence import ArticlePersistence


class _SessionContext:
    def __init__(self, session: MagicMock) -> None:
        self._session = session

    def __enter__(self) -> MagicMock:
        return self._session

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class VideoPolicyTests(TestCase):
    def test_save_deferred_video_assets_updates_existing_record(self) -> None:
        article_id = uuid.uuid4()
        existing = SimpleNamespace(
            sequence_number=1,
            source_url="https://example.com/old.mp4",
            referrer="https://example.com/old",
            site_slug="thanhnien",
            article_url="https://example.com/article-old",
            category_id="old",
            category_name="Old",
            category_key="old",
            ingest_category_slug="old",
            deferred_reason="stale",
            deferred_at=None,
            enqueued_at=uuid.uuid4(),
        )

        session = MagicMock()
        query = MagicMock()
        query.filter.return_value = [existing]
        session.query.return_value = query
        session_factory = MagicMock(return_value=_SessionContext(session))

        persistence = ArticlePersistence(
            session_factory=session_factory,
            storage_root=Path("/tmp/storage"),
        )

        asset = ParsedAsset(
            source_url="https://example.com/new.mp4",
            asset_type=AssetType.VIDEO,
            sequence=1,
            referrer=None,
        )

        persistence.save_deferred_video_assets(
            article_id=str(article_id),
            site_slug="thanhnien",
            article_url="https://example.com/article",
            category_id="sports",
            category_name="Sports",
            ingest_category_slug="sports",
            deferred_assets=[asset],
        )

        self.assertEqual(existing.source_url, "https://example.com/new.mp4")
        self.assertEqual(existing.referrer, "https://example.com/article")
        self.assertEqual(existing.category_key, "sports")
        self.assertEqual(existing.ingest_category_slug, "sports")
        self.assertIsNone(existing.enqueued_at)
        session.add.assert_not_called()
        session.commit.assert_called_once()

    def test_process_pending_video_assets_enqueues_and_marks_records(self) -> None:
        pending_id = uuid.uuid4()
        article_id = uuid.uuid4()
        pending_record = SimpleNamespace(
            id=pending_id,
            article_id=article_id,
            article_url="https://example.com/article",
            sequence_number=1,
            source_url="https://example.com/video.mp4",
            referrer=None,
            category_key=None,
            ingest_category_slug="sports",
        )

        fetch_session = MagicMock()
        fetch_query = MagicMock()
        fetch_query.filter.return_value = fetch_query
        fetch_query.order_by.return_value = fetch_query
        fetch_query.all.return_value = [pending_record]
        fetch_session.query.return_value = fetch_query

        update_session = MagicMock()
        update_query = MagicMock()
        update_filter = MagicMock()
        update_query.filter.return_value = update_filter
        update_session.query.return_value = update_query

        session_factory = MagicMock(side_effect=[_SessionContext(fetch_session), _SessionContext(update_session)])

        config = IngestConfig()
        config.video.enabled_categories = ("sports",)

        site = SimpleNamespace(slug="thanhnien", playwright_resolver_factory=None)

        with patch("crawler.ingest._enqueue_asset_downloads") as enqueue_mock:
            _process_pending_video_assets(
                config=config,
                site=site,
                session_factory=session_factory,
                use_celery_playwright=False,
            )

        enqueue_mock.assert_called_once()
        args, kwargs = enqueue_mock.call_args
        self.assertEqual(args[2], str(article_id))
        assets = args[4]
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].asset_type, AssetType.VIDEO)
        update_filter.update.assert_called_once()
        update_session.commit.assert_called_once()

    def test_video_category_allowed_uses_ingest_slug_first(self) -> None:
        config = IngestConfig()
        config.video.enabled_categories = ("sports",)

        self.assertTrue(config.video.category_allowed("sports", "news", "features"))
        self.assertTrue(config.video.category_allowed("Sports", None, None))
        self.assertFalse(config.video.category_allowed("business", "economy", None))

    def test_persist_assets_keeps_pending_for_images_only(self) -> None:
        article_id = uuid.uuid4()
        article = SimpleNamespace(
            id=article_id,
            images=["existing"],
            videos=[],
        )

        article_query = MagicMock()
        article_filter = MagicMock()
        article_filter.one.return_value = article
        article_query.filter.return_value = article_filter

        session = MagicMock()
        session.query.return_value = article_query

        session_factory = MagicMock(return_value=_SessionContext(session))

        storage_root = Path("/tmp/storage")
        persistence = ArticlePersistence(
            session_factory=session_factory,
            storage_root=storage_root,
        )

        stored_assets = [
            StoredAsset(
                source=ParsedAsset(
                    source_url="https://example.com/img.jpg",
                    asset_type=AssetType.IMAGE,
                    sequence=1,
                ),
                path=storage_root / "articles" / str(article_id) / "images" / "001.jpg",
                checksum="abc",
                bytes_downloaded=123,
            )
        ]

        persistence.persist_assets(str(article_id), stored_assets)

        self.assertEqual(len(article.images), 1)
        self.assertEqual(len(article.videos), 0)
        self.assertEqual(session.query.call_count, 2)
        session.commit.assert_called_once()

    def test_record_failed_media_downloads_creates_and_updates(self) -> None:
        article_id = uuid.uuid4()
        session = MagicMock()
        query = MagicMock()
        query.filter.return_value = []
        session.query.return_value = query
        session_factory = MagicMock(return_value=_SessionContext(session))

        persistence = ArticlePersistence(
            session_factory=session_factory,
            storage_root=Path("/tmp/storage"),
        )

        asset = ParsedAsset(
            source_url="https://example.com/image.jpg",
            asset_type=AssetType.IMAGE,
            sequence=1,
        )

        persistence.record_failed_media_downloads(
            article_id=str(article_id),
            site_slug="thanhnien",
            article_url="https://example.com/article",
            assets=[asset],
            failure_reason="download failed",
            error_type="AssetDownloadError",
        )

        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        self.assertEqual(added.media_type, AssetType.IMAGE.value)
        self.assertEqual(added.failure_count, 1)
        session.commit.assert_called_once()

        # Simulate update path
        existing = SimpleNamespace(
            media_type=AssetType.IMAGE.value,
            sequence_number=1,
            source_url="https://example.com/image.jpg",
            referrer="https://example.com/article",
            failure_reason="stale",
            failure_count=1,
            first_failed_at=None,
            last_failed_at=None,
            status="queued",
            resolved_at=None,
            article_url="https://example.com/article",
        )
        session.reset_mock()
        query2 = MagicMock()
        query2.filter.return_value = [existing]
        session.query.return_value = query2

        persistence.record_failed_media_downloads(
            article_id=str(article_id),
            site_slug="thanhnien",
            article_url="https://example.com/article",
            assets=[asset],
            failure_reason="download failed again",
            error_type="AssetDownloadError",
        )

        session.add.assert_not_called()
        self.assertEqual(existing.failure_count, 2)
        self.assertEqual(existing.status, "pending")
        session.commit.assert_called_once()

    def test_process_failed_media_downloads_enqueues_and_marks_records(self) -> None:
        failed_id = uuid.uuid4()
        article_id = uuid.uuid4()
        failed_record = SimpleNamespace(
            id=failed_id,
            article_id=article_id,
            site_slug="thanhnien",
            article_url="https://example.com/article",
            media_type=AssetType.IMAGE.value,
            sequence_number=1,
            source_url="https://example.com/img.jpg",
            referrer=None,
        )

        fetch_session = MagicMock()
        fetch_query = MagicMock()
        fetch_query.filter.return_value = fetch_query
        fetch_query.order_by.return_value = fetch_query
        fetch_query.all.return_value = [failed_record]
        fetch_session.query.return_value = fetch_query

        update_session = MagicMock()
        update_query = MagicMock()
        update_filter = MagicMock()
        update_query.filter.return_value = update_filter
        update_session.query.return_value = update_query

        session_factory = MagicMock(side_effect=[_SessionContext(fetch_session), _SessionContext(update_session)])

        config = IngestConfig()
        site = SimpleNamespace(slug="thanhnien", playwright_resolver_factory=None)

        with patch("crawler.ingest._enqueue_asset_downloads") as enqueue_mock:
            _process_failed_media_downloads(
                config=config,
                site=site,
                session_factory=session_factory,
                use_celery_playwright=False,
            )

        enqueue_mock.assert_called_once()
        args, _ = enqueue_mock.call_args
        self.assertEqual(args[2], str(article_id))
        assets = args[4]
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].asset_type, AssetType.IMAGE)
        update_filter.update.assert_called_once()
        update_session.commit.assert_called_once()

    def test_persist_assets_only_removes_pending_sequences_for_downloaded_videos(self) -> None:
        article_id = uuid.uuid4()
        article = SimpleNamespace(
            id=article_id,
            images=["existing-image"],
            videos=[],
        )

        article_query = MagicMock()
        article_filter = MagicMock()
        article_filter.one.return_value = article
        article_query.filter.return_value = article_filter

        pending_query = MagicMock()
        pending_filter = MagicMock()
        pending_query.filter.return_value = pending_filter

        failed_query = MagicMock()
        failed_filter = MagicMock()
        failed_query.filter.return_value = failed_filter

        session = MagicMock()
        session.query.side_effect = [article_query, pending_query, failed_query]

        session_factory = MagicMock(return_value=_SessionContext(session))

        storage_root = Path("/tmp/storage")
        persistence = ArticlePersistence(
            session_factory=session_factory,
            storage_root=storage_root,
        )

        stored_assets = [
            StoredAsset(
                source=ParsedAsset(
                    source_url="https://example.com/video.m3u8",
                    asset_type=AssetType.VIDEO,
                    sequence=5,
                ),
                path=storage_root / "articles" / str(article_id) / "videos" / "005.mp4",
                checksum="def",
                bytes_downloaded=2048,
            )
        ]

        persistence.persist_assets(str(article_id), stored_assets)

        self.assertEqual(article.images, ["existing-image"])
        self.assertEqual(len(article.videos), 1)
        pending_query.filter.assert_called_once()
        pending_filter.delete.assert_called_once_with(synchronize_session=False)
        failed_filter.update.assert_called_once()
