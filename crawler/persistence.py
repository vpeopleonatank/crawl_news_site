"""Database persistence helpers for Thanhnien article ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from models import (
    Article,
    ArticleImage,
    ArticleVideo,
    FailedMediaDownload,
    PendingVideoAsset,
    generate_uuid7,
)

from .parsers import AssetType, ParsedArticle, ParsedAsset
from .assets import StoredAsset


class ArticlePersistenceError(RuntimeError):
    """Raised when persisting an article fails."""


@dataclass(slots=True)
class PersistenceResult:
    article_id: str
    created: bool


class ArticlePersistence:
    """Handles metadata and asset upserts for parsed articles."""

    def __init__(
        self,
        session_factory,
        storage_root: Path,
        *,
        storage_volume_name: Optional[str] = None,
        storage_volume_path: Optional[Path] = None,
    ) -> None:
        self._session_factory = session_factory
        self._storage_root = storage_root
        self._storage_volume_name = storage_volume_name or None
        self._storage_volume_path = storage_volume_path or storage_root

    def upsert_metadata(
        self,
        parsed: ParsedArticle,
        site_slug: str,
        *,
        fetch_metadata: dict | None = None,
        ingest_category_slug: str | None = None,
    ) -> PersistenceResult:
        try:
            with self._session_factory() as session:
                article, created = self._upsert_metadata(
                    session,
                    parsed,
                    site_slug,
                    fetch_metadata,
                    ingest_category_slug,
                )
                article_id = str(article.id)
                session.commit()
                return PersistenceResult(article_id=article_id, created=created)
        except Exception as exc:  # pragma: no cover - failure path
            raise ArticlePersistenceError(str(exc)) from exc

    def _upsert_metadata(
        self,
        session: Session,
        parsed: ParsedArticle,
        site_slug: str,
        fetch_metadata: dict | None,
        ingest_category_slug: str | None,
    ) -> tuple[Article, bool]:
        if not site_slug:
            raise ValueError("site_slug is required when persisting articles")

        article = session.query(Article).filter(Article.url == parsed.url).one_or_none()
        created = False
        if article is None:
            article = Article(id=generate_uuid7(), url=parsed.url, site_slug=site_slug)
            session.add(article)
            created = True
        else:
            article.site_slug = site_slug

        article.title = parsed.title
        article.description = parsed.description
        article.content = parsed.content
        article.category_id = parsed.category_id
        article.category_name = parsed.category_name
        article.publish_date = parsed.publish_date
        article.tags = ",".join(parsed.tags) if parsed.tags else None
        if ingest_category_slug is not None:
            article.ingest_category_slug = ingest_category_slug

        combined_comments = parsed.comments.copy() if parsed.comments else {}
        if fetch_metadata:
            combined_comments["crawler"] = fetch_metadata
        article.comments = combined_comments or None

        session.flush()  # ensures article.id is populated
        return article, created

    def persist_assets(self, article_id: str, stored_assets: Iterable[StoredAsset]) -> None:
        try:
            with self._session_factory() as session:
                article_uuid = UUID(article_id)
                article = session.query(Article).filter(Article.id == article_uuid).one()
                new_images: list[ArticleImage] = []
                new_videos: list[ArticleVideo] = []
                downloaded_video_sequences: set[int] = set()
                downloaded_image_sequences: set[int] = set()

                for stored in stored_assets:
                    stored_ref = self._format_asset_reference(stored.path)
                    if stored.source.asset_type == AssetType.IMAGE:
                        new_images.append(
                            ArticleImage(
                                image_path=stored_ref,
                                sequence_number=stored.source.sequence,
                            )
                        )
                        downloaded_image_sequences.add(stored.source.sequence)
                    else:
                        new_videos.append(
                            ArticleVideo(
                                video_path=stored_ref,
                                sequence_number=stored.source.sequence,
                            )
                        )
                        downloaded_video_sequences.add(stored.source.sequence)

                if new_images:
                    article.images.clear()
                    article.images.extend(new_images)

                if new_videos:
                    article.videos.clear()
                    article.videos.extend(new_videos)

                timestamp = datetime.utcnow()

                if downloaded_video_sequences:
                    session.query(PendingVideoAsset).filter(
                        PendingVideoAsset.article_id == article_uuid,
                        PendingVideoAsset.sequence_number.in_(list(downloaded_video_sequences)),
                    ).delete(synchronize_session=False)
                    session.query(FailedMediaDownload).filter(
                        FailedMediaDownload.article_id == article_uuid,
                        FailedMediaDownload.media_type == AssetType.VIDEO.value,
                        FailedMediaDownload.sequence_number.in_(list(downloaded_video_sequences)),
                        FailedMediaDownload.resolved_at.is_(None),
                    ).update(
                        {
                            "status": "resolved",
                            "resolved_at": timestamp,
                        },
                        synchronize_session=False,
                    )

                if downloaded_image_sequences:
                    session.query(FailedMediaDownload).filter(
                        FailedMediaDownload.article_id == article_uuid,
                        FailedMediaDownload.media_type == AssetType.IMAGE.value,
                        FailedMediaDownload.sequence_number.in_(list(downloaded_image_sequences)),
                        FailedMediaDownload.resolved_at.is_(None),
                    ).update(
                        {
                            "status": "resolved",
                            "resolved_at": timestamp,
                        },
                        synchronize_session=False,
                    )
                session.commit()
        except Exception as exc:  # pragma: no cover - failure path
            raise ArticlePersistenceError(str(exc)) from exc

    def _format_asset_reference(self, asset_path: Path) -> str:
        if self._storage_volume_path:
            try:
                relative = asset_path.relative_to(self._storage_volume_path)
            except ValueError:
                pass
            else:
                relative_posix = relative.as_posix()
                if self._storage_volume_name:
                    return f"{self._storage_volume_name}:{relative_posix}"
                return relative_posix

        relative = asset_path.relative_to(self._storage_root)
        return relative.as_posix()

    @staticmethod
    def _category_key(
        category_id: str | None,
        category_name: str | None,
        ingest_category_slug: str | None,
    ) -> str | None:
        for candidate in (category_id, category_name, ingest_category_slug):
            if not candidate:
                continue
            cleaned = candidate.strip().lower()
            if cleaned:
                return cleaned
        return None

    def save_deferred_video_assets(
        self,
        article_id: str,
        site_slug: str,
        article_url: str,
        category_id: str | None,
        category_name: str | None,
        ingest_category_slug: str | None,
        deferred_assets: Iterable[ParsedAsset],
        reason: str = "category_not_enabled",
    ) -> None:
        assets = list(deferred_assets)
        if not assets:
            return

        try:
            with self._session_factory() as session:
                article_uuid = UUID(article_id)
                existing = {
                    pending.sequence_number: pending
                    for pending in session.query(PendingVideoAsset)
                    .filter(PendingVideoAsset.article_id == article_uuid)
                }
                category_key = self._category_key(category_id, category_name, ingest_category_slug)
                timestamp = datetime.utcnow()

                for asset in assets:
                    referrer = asset.referrer or article_url
                    record = existing.get(asset.sequence)
                    if record:
                        record.source_url = asset.source_url
                        record.referrer = referrer
                        record.site_slug = site_slug
                        record.article_url = article_url
                        record.category_id = category_id
                        record.category_name = category_name
                        record.category_key = category_key
                        record.ingest_category_slug = ingest_category_slug
                        record.deferred_reason = reason
                        record.deferred_at = timestamp
                        record.enqueued_at = None
                    else:
                        session.add(
                            PendingVideoAsset(
                                article_id=article_uuid,
                                site_slug=site_slug,
                                article_url=article_url,
                                category_id=category_id,
                                category_name=category_name,
                                category_key=category_key,
                                ingest_category_slug=ingest_category_slug,
                                sequence_number=asset.sequence,
                                source_url=asset.source_url,
                                referrer=referrer,
                                deferred_reason=reason,
                                deferred_at=timestamp,
                            )
                        )

                session.commit()
        except Exception as exc:  # pragma: no cover - failure path
            raise ArticlePersistenceError(str(exc)) from exc

    def record_failed_media_downloads(
        self,
        article_id: str,
        site_slug: str,
        article_url: str,
        assets: Iterable[ParsedAsset],
        *,
        failure_reason: str,
        error_type: Optional[str] = None,
    ) -> None:
        assets_list = list(assets)
        if not assets_list:
            return

        try:
            with self._session_factory() as session:
                article_uuid = UUID(article_id)
                existing_records = {
                    (record.media_type, record.sequence_number): record
                    for record in session.query(FailedMediaDownload)
                    .filter(FailedMediaDownload.article_id == article_uuid)
                }

                timestamp = datetime.utcnow()
                for asset in assets_list:
                    media_type = asset.asset_type.value
                    key = (media_type, asset.sequence)
                    referrer = asset.referrer or article_url
                    record = existing_records.get(key)
                    if record:
                        record.source_url = asset.source_url
                        record.referrer = referrer
                        record.failure_reason = failure_reason
                        record.article_url = article_url
                        record.failure_count = (record.failure_count or 0) + 1
                        record.last_error = failure_reason
                        record.last_error_type = error_type
                        if record.first_failed_at is None:
                            record.first_failed_at = timestamp
                        record.last_failed_at = timestamp
                        record.status = "pending"
                        record.resolved_at = None
                    else:
                        session.add(
                            FailedMediaDownload(
                                article_id=article_uuid,
                                site_slug=site_slug,
                                article_url=article_url,
                                media_type=media_type,
                                sequence_number=asset.sequence,
                                source_url=asset.source_url,
                                referrer=referrer,
                                failure_reason=failure_reason,
                                failure_count=1,
                                last_error=failure_reason,
                                last_error_type=error_type,
                                first_failed_at=timestamp,
                                last_failed_at=timestamp,
                                status="pending",
                            )
                        )

                session.commit()
        except Exception as exc:  # pragma: no cover - failure path
            raise ArticlePersistenceError(str(exc)) from exc
