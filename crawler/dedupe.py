from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:  # pragma: no cover - optional dependency
    import sqlite3
except ModuleNotFoundError:  # pragma: no cover - fallback when SQLite is missing
    sqlite3 = None  # type: ignore[assignment]


@dataclass(frozen=True)
class ArticleRecord:
    url: str
    lastmod: Optional[str]
    sitemap_url: str
    image_url: Optional[str]


class _JSONDedupeBackend:
    """Fallback dedupe store when SQLite extensions are unavailable."""

    def __init__(self, path: Path) -> None:
        self._path = path.with_suffix(".json")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load()

    def _load(self) -> dict[str, dict[str, Optional[str]]]:
        if not self._path.exists():
            return {}
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _flush(self) -> None:
        tmp_path = self._path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(self._state, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self._path)

    def upsert(self, record: ArticleRecord, url_hash: str) -> bool:
        payload = {
            "url": record.url,
            "lastmod": record.lastmod,
            "sitemap_url": record.sitemap_url,
            "image_url": record.image_url,
        }
        existing = self._state.get(url_hash)
        if existing is None:
            self._state[url_hash] = payload
            self._flush()
            return True

        should_emit = False
        if record.lastmod and record.lastmod != existing.get("lastmod"):
            should_emit = True
        if record.image_url and record.image_url != existing.get("image_url"):
            should_emit = True

        if should_emit:
            self._state[url_hash] = payload
            self._flush()

        return should_emit


class SQLiteDedupeStore:
    """Persists seen article URLs to avoid re-enqueueing duplicates."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        if sqlite3 is None:
            self._backend = _JSONDedupeBackend(self._path)
        else:
            self._backend = None
            self._init_db()

    @staticmethod
    def sha256(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    # SQLite-backed implementation -------------------------------------------------
    def _init_db(self) -> None:
        if sqlite3 is None:
            return
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    url_hash TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    lastmod TEXT,
                    sitemap_url TEXT NOT NULL,
                    image_url TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def _connect(self):  # type: ignore[override]
        if sqlite3 is None:
            raise RuntimeError("SQLite backend requested but sqlite3 module is unavailable")
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return _SQLiteConnectionWrapper(conn)

    # Public API -------------------------------------------------------------------
    def upsert(self, record: ArticleRecord) -> bool:
        url_hash = self.sha256(record.url)

        if sqlite3 is None:
            return self._backend.upsert(record, url_hash)

        with self._connect() as conn:
            row = conn.execute(
                "SELECT lastmod, image_url FROM articles WHERE url_hash = ?",
                (url_hash,),
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO articles (url_hash, url, lastmod, sitemap_url, image_url)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (url_hash, record.url, record.lastmod, record.sitemap_url, record.image_url),
                )
                return True

            existing_lastmod, existing_image = row
            if record.lastmod and record.lastmod != existing_lastmod:
                conn.execute(
                    """
                    UPDATE articles
                    SET lastmod = ?, sitemap_url = ?, image_url = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE url_hash = ?
                    """,
                    (record.lastmod, record.sitemap_url, record.image_url, url_hash),
                )
                return True

            if record.image_url and record.image_url != existing_image:
                conn.execute(
                    "UPDATE articles SET image_url = ?, sitemap_url = ?, updated_at = CURRENT_TIMESTAMP WHERE url_hash = ?",
                    (record.image_url, record.sitemap_url, url_hash),
                )

            return False


class _SQLiteConnectionWrapper:
    """Context manager wrapper so we can reuse the same interface for commit."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        if exc is None:
            self._conn.commit()
        else:  # pragma: no cover - failure path
            self._conn.rollback()
        self._conn.close()
