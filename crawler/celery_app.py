"""Celery application setup for the crawler task queue."""

from __future__ import annotations

import os
from typing import Optional

from celery import Celery


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _sqla_broker_from_db(db_url: Optional[str]) -> Optional[str]:
    if not db_url:
        return None
    return db_url if db_url.startswith("sqla+") else f"sqla+{db_url}"


def _db_backend_from_db(db_url: Optional[str]) -> Optional[str]:
    if not db_url:
        return None
    return db_url if db_url.startswith("db+") else f"db+{db_url}"


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return max(0, parsed)


def create_celery_app() -> Celery:
    """Instantiate the Celery app with environment driven configuration."""

    db_url = os.getenv("CRAWLER_DATABASE_URL")
    broker_url = os.getenv("CRAWLER_CELERY_BROKER_URL")
    backend_url = os.getenv("CRAWLER_CELERY_RESULT_BACKEND")

    if broker_url is None:
        broker_url = _sqla_broker_from_db(db_url)
    if backend_url is None:
        backend_url = _db_backend_from_db(db_url)

    if broker_url is None:
        broker_url = "memory://"
    if backend_url is None:
        backend_url = "cache+memory://"

    engine_options = {
        # Keep connection usage conservative; PgBouncer multiplexes worker connections.
        "pool_size": _env_int("CRAWLER_DB_POOL_SIZE", 2),
        "max_overflow": _env_int("CRAWLER_DB_MAX_OVERFLOW", 0),
        "pool_recycle": _env_int("CRAWLER_DB_POOL_RECYCLE", 1800),
        "pool_pre_ping": True,
    }

    app = Celery("crawler", broker=broker_url, backend=backend_url, include=["crawler.tasks"])
    conf_updates = {
        "task_serializer": "json",
        "accept_content": ["json"],
        "result_serializer": "json",
        "task_always_eager": _env_bool("CRAWLER_CELERY_TASK_ALWAYS_EAGER", True),
        "task_acks_late": True,
        "task_reject_on_worker_lost": True,
        "worker_prefetch_multiplier": 1,
        "result_persistent": True,
        "broker_connection_retry_on_startup": True,
    }
    if backend_url.startswith("db+"):
        conf_updates["database_engine_options"] = engine_options
        conf_updates["database_short_lived_sessions"] = True
    app.conf.update(**conf_updates)
    return app


celery_app = create_celery_app()


__all__ = ["celery_app", "create_celery_app"]
