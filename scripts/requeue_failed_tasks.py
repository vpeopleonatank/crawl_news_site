"""Resubmit failed Celery tasks stored in the SQLAlchemy result backend."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Iterable

from celery.backends.database import DatabaseBackend
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session

# Ensure the repository root is importable when executing from the scripts/ directory.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crawler.celery_app import create_celery_app


LOGGER = logging.getLogger(__name__)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Requeue failed Celery tasks recorded in the SQLAlchemy result backend. "
            "Uses the crawler Celery configuration and resubmits one task per record."
        )
    )
    parser.add_argument(
        "--backend-url",
        help=(
            "SQLAlchemy URL for the Celery result backend. Defaults to the value from "
            "CRAWLER_CELERY_RESULT_BACKEND or CRAWLER_DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--state",
        dest="states",
        action="append",
        default=None,
        help=(
            "Task state(s) to requeue. May be supplied multiple times. Default is FAILURE."
        ),
    )
    parser.add_argument(
        "--task-name",
        help="Only requeue tasks matching this full Celery task name.",
    )
    parser.add_argument(
        "--queue",
        help=(
            "Override target queue when resubmitting. Defaults to the queue stored "
            "with each task record."
        ),
    )
    parser.add_argument(
        "--since",
        help=(
            "Only consider tasks whose completion timestamp is greater than or equal to "
            "the supplied ISO-8601 datetime."
        ),
    )
    parser.add_argument(
        "--until",
        help="Only consider tasks completed before the supplied ISO-8601 datetime.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of tasks to process. Defaults to 250.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List candidate tasks without resubmitting them.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging output.",
    )
    return parser.parse_args(argv)


def resolve_backend_url(cli_backend_url: str | None) -> str:
    if cli_backend_url:
        return cli_backend_url

    env_backend = os.getenv("CRAWLER_CELERY_RESULT_BACKEND")
    if env_backend:
        return env_backend

    env_db = os.getenv("CRAWLER_DATABASE_URL")
    if env_db:
        return f"db+{env_db}" if not env_db.startswith("db+") else env_db

    raise SystemExit(
        "No backend URL provided. Supply --backend-url or configure "
        "CRAWLER_CELERY_RESULT_BACKEND/CRAWLER_DATABASE_URL in the environment."
    )


def parse_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SystemExit(f"Invalid datetime '{value}': {exc}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def ensure_backend(app, backend_url: str) -> DatabaseBackend:
    try:
        return DatabaseBackend(app=app, url=backend_url)
    except NoSuchModuleError as exc:
        if backend_url.startswith("db+"):
            stripped = backend_url[3:]
            LOGGER.debug(
                "Falling back to SQLAlchemy URL without 'db+' prefix: %s", stripped
            )
            return DatabaseBackend(app=app, url=stripped)
        raise SystemExit(
            f"Failed to load SQLAlchemy backend for URL {backend_url!r}: {exc}"
        ) from exc
    except Exception as exc:  # pragma: no cover - defensive logging only
        raise SystemExit(
            f"Failed to initialise Celery database backend: {exc}"  # noqa: TRY003
        ) from exc


def decode_payload(backend: DatabaseBackend, payload: Any) -> Iterable[Any]:
    if payload in (None, b"", ""):
        return ()
    try:
        return backend.decode(payload)
    except Exception as exc:  # pragma: no cover - defensive logging only
        LOGGER.warning("Could not decode payload %r: %s", payload, exc)
        return ()


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    states = {state.upper() for state in (args.states or ["FAILURE"])}
    since = parse_datetime(args.since)
    until = parse_datetime(args.until)

    celery_app = create_celery_app()
    backend_url = resolve_backend_url(args.backend_url)
    backend = ensure_backend(celery_app, backend_url)

    if not isinstance(backend, DatabaseBackend):  # pragma: no cover - safety check
        raise SystemExit("Configured Celery backend is not DatabaseBackend; aborting.")

    session: Session = backend.ResultSession()
    TaskModel = backend.task_cls

    try:
        query = session.query(TaskModel).filter(TaskModel.status.in_(states))
        if args.task_name:
            query = query.filter(TaskModel.name == args.task_name)
        if since is not None:
            query = query.filter(TaskModel.date_done >= since)
        if until is not None:
            query = query.filter(TaskModel.date_done <= until)

        query = query.order_by(TaskModel.date_done.asc())
        if args.limit and args.limit > 0:
            query = query.limit(args.limit)

        records = list(query)
        if not records:
            LOGGER.info("No tasks matched the selection criteria.")
            return 0

        requeued = 0
        for record in records:
            task_name = record.name or "<unknown>"
            if task_name not in celery_app.tasks:
                LOGGER.warning(
                    "Skipping task %s (%s): task not registered in current app.",
                    record.task_id,
                    task_name,
                )
                continue

            decoded_args = decode_payload(backend, record.args)
            if isinstance(decoded_args, (list, tuple)):
                args_payload = tuple(decoded_args)
            elif decoded_args:
                args_payload = (decoded_args,)
            else:
                args_payload = ()

            decoded_kwargs = decode_payload(backend, record.kwargs)
            if isinstance(decoded_kwargs, Mapping):
                kwargs_payload = dict(decoded_kwargs)
            elif decoded_kwargs:
                LOGGER.warning(
                    "Task %s (%s) kwargs are not a mapping; discarding decoded value %r",
                    record.task_id,
                    task_name,
                    decoded_kwargs,
                )
                kwargs_payload = {}
            else:
                kwargs_payload = {}

            task_queue = args.queue or record.queue

            if args.dry_run:
                LOGGER.info(
                    "[DRY-RUN] Would requeue task %s (%s) with args=%s kwargs=%s queue=%s",
                    record.task_id,
                    task_name,
                    args_payload,
                    kwargs_payload,
                    task_queue,
                )
                continue

            LOGGER.info(
                "Requeueing task %s (%s) queue=%s",
                record.task_id,
                task_name,
                task_queue,
            )
            try:
                celery_app.send_task(
                    task_name,
                    args=args_payload,
                    kwargs=kwargs_payload,
                    queue=task_queue,
                )
            except Exception as exc:  # pragma: no cover - network/queue failure
                LOGGER.error(
                    "Failed to requeue task %s (%s): %s",
                    record.task_id,
                    task_name,
                    exc,
                )
                continue

            requeued += 1

        LOGGER.info("Requeued %d task(s).", requeued)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
