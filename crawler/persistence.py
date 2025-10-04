"""Database persistence helpers for Thanhnien article ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from uuid import UUID

from sqlalchemy.orm import Session

from models import Article, ArticleImage, ArticleVideo, generate_uuid7

from .parsers import AssetType, ParsedArticle
from .assets import StoredAsset


class ArticlePersistenceError(RuntimeError):
    """Raised when persisting an article fails."""


@dataclass(slots=True)
class PersistenceResult:
    article_id: str
    created: bool


class ArticlePersistence:
    """Handles metadata and asset upserts for parsed articles."""

    def __init__(self, session_factory, storage_root: Path) -> None:
        self._session_factory = session_factory
        self._storage_root = storage_root

    def upsert_metadata(self, parsed: ParsedArticle, fetch_metadata: dict | None = None) -> PersistenceResult:
        try:
            with self._session_factory() as session:
                article, created = self._upsert_metadata(session, parsed, fetch_metadata)
                article_id = str(article.id)
                session.commit()
                return PersistenceResult(article_id=article_id, created=created)
        except Exception as exc:  # pragma: no cover - failure path
            raise ArticlePersistenceError(str(exc)) from exc

    def _upsert_metadata(
        self,
        session: Session,
        parsed: ParsedArticle,
        fetch_metadata: dict | None,
    ) -> tuple[Article, bool]:
        article = session.query(Article).filter(Article.url == parsed.url).one_or_none()
        created = False
        if article is None:
            article = Article(id=generate_uuid7(), url=parsed.url)
            session.add(article)
            created = True

        article.title = parsed.title
        article.description = parsed.description
        article.content = parsed.content
        article.category_id = parsed.category_id
        article.category_name = parsed.category_name
        article.publish_date = parsed.publish_date
        article.tags = ",".join(parsed.tags) if parsed.tags else None

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
                article.images.clear()
                article.videos.clear()

                for stored in stored_assets:
                    relative_path = stored.path.relative_to(self._storage_root)
                    if stored.source.asset_type == AssetType.IMAGE:
                        article.images.append(
                            ArticleImage(
                                image_path=str(relative_path),
                                sequence_number=stored.source.sequence,
                            )
                        )
                    else:
                        article.videos.append(
                            ArticleVideo(
                                video_path=str(relative_path),
                                sequence_number=stored.source.sequence,
                            )
                        )
                session.commit()
        except Exception as exc:  # pragma: no cover - failure path
            raise ArticlePersistenceError(str(exc)) from exc
