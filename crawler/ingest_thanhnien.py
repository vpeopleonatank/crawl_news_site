"""Backward-compatible CLI wrapper for ThanhNien ingestion."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from .config import IngestConfig
from .ingest import (
    build_arg_parser as _build_generic_parser,
    build_config as _build_generic_config,
    configure_logging,
    main as _generic_main,
    persist_raw_html,
    _build_task_payload,
    _enqueue_asset_downloads,
    _record_fetch_failure,
    _update_video_assets_with_playwright,
)
from .sites import get_site_definition


def build_arg_parser() -> argparse.ArgumentParser:
    parser = _build_generic_parser()
    parser.set_defaults(site="thanhnien")
    return parser


def build_config(args: argparse.Namespace) -> IngestConfig:
    site = get_site_definition("thanhnien")
    # Ensure downstream logic sees the correct site even if caller constructed args manually.
    setattr(args, "site", "thanhnien")
    return _build_generic_config(args, site)


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    args_list = list(argv)
    if "--site" not in args_list:
        args_list = ["--site", "thanhnien", *args_list]
    return _generic_main(args_list)


__all__ = [
    "build_arg_parser",
    "build_config",
    "configure_logging",
    "main",
    "persist_raw_html",
    "_build_task_payload",
    "_enqueue_asset_downloads",
    "_record_fetch_failure",
    "_update_video_assets_with_playwright",
]


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
