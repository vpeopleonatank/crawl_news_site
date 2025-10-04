"""Configuration utilities for the Thanhnien ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

DEFAULT_JOBS_FILE = Path("data/thanhnien_jobs.ndjson")
DEFAULT_STORAGE_ROOT = Path("storage")
DEFAULT_LOG_DIR = DEFAULT_STORAGE_ROOT / "logs"

DEFAULT_USER_AGENT = "thanhnien-ingestor/1.0"


@dataclass(slots=True)
class RateLimitConfig:
    per_domain_delay: float = 0.5
    max_workers: int = 4


@dataclass(slots=True)
class RetryConfig:
    max_attempts: int = 3
    backoff_factor: float = 1.5
    base_delay: float = 1.0


@dataclass(slots=True)
class TimeoutConfig:
    request_timeout: float = 5.0
    asset_timeout: float = 30.0


@dataclass(slots=True)
class IngestConfig:
    jobs_file: Path = DEFAULT_JOBS_FILE
    storage_root: Path = DEFAULT_STORAGE_ROOT
    db_url: Optional[str] = None
    user_agent: str = DEFAULT_USER_AGENT
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout: TimeoutConfig = field(default_factory=TimeoutConfig)
    resume: bool = False
    raw_html_cache_enabled: bool = False
    log_dir: Path = DEFAULT_LOG_DIR

    def ensure_directories(self) -> None:
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def raw_html_path(self, article_id: str) -> Path:
        return self.storage_root / "raw" / f"{article_id}.html"

    def article_asset_root(self, article_id: str) -> Path:
        return self.storage_root / "articles" / article_id
