"""Verification module for crawled content analysis."""

from __future__ import annotations

from typing import Any


__all__ = ["AdImageDetector", "AdCheckResult", "SuspiciousImage"]


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name not in __all__:
        raise AttributeError(name)

    try:
        from .ad_detector import AdImageDetector, AdCheckResult, SuspiciousImage
    except ModuleNotFoundError as exc:
        if exc.name == "PIL":
            raise ImportError(
                "Optional dependency 'Pillow' is required for crawler.verification.AdImageDetector "
                "(install with `pip install pillow`)."
            ) from exc
        raise

    return {"AdImageDetector": AdImageDetector, "AdCheckResult": AdCheckResult, "SuspiciousImage": SuspiciousImage}[
        name
    ]
