"""Configuration utilities shared by all ingestion pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import quote

DEFAULT_JOBS_FILE = Path("data/jobs.ndjson")
DEFAULT_STORAGE_ROOT = Path("storage")
DEFAULT_LOG_DIR = DEFAULT_STORAGE_ROOT / "logs"

DEFAULT_USER_AGENT = "article-ingestor/1.0"


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
    hls_download_timeout: float = 900.0


@dataclass(slots=True)
class StorageNotificationConfig:
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    telegram_thread_id: Optional[int] = None

    def has_telegram(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)


@dataclass(slots=True)
class ProxyConfig:
    """Configuration for outbound proxy usage and IP rotation."""

    scheme: str = "http"
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    change_ip_url: Optional[str] = None
    min_rotation_interval: float = 240.0

    @property
    def address(self) -> Optional[str]:
        if self.host is None or self.port is None:
            return None
        return f"{self.host}:{self.port}"

    def httpx_proxy(self) -> Optional[str]:
        address = self.address
        if not address:
            return None
        credentials = ""
        if self.username:
            user = quote(self.username, safe="")
            if self.password:
                pwd = quote(self.password, safe="")
                credentials = f"{user}:{pwd}@"
            else:
                credentials = f"{user}@"
        return f"{self.scheme}://{credentials}{address}"

    @classmethod
    def from_endpoint(
        cls,
        endpoint: str,
        *,
        scheme: str = "http",
        change_ip_url: Optional[str] = None,
        min_rotation_interval: float = 240.0,
        api_key: Optional[str] = None,
    ) -> "ProxyConfig":
        cleaned = endpoint.strip()
        if not cleaned:
            raise ValueError("Proxy endpoint must not be empty")

        parts = cleaned.split(":")
        if len(parts) < 2:
            raise ValueError("Proxy endpoint must be in 'host:port[:key]' format")

        host = parts[0].strip()
        if not host:
            raise ValueError("Proxy host must not be empty")

        port_str = parts[1].strip()
        if not port_str:
            raise ValueError("Proxy port must not be empty")

        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError("Proxy port must be an integer") from exc

        username: Optional[str] = None
        password: Optional[str] = None
        key: Optional[str] = None
        extras = parts[2:]
        if extras:
            cleaned_extras = [segment.strip() for segment in extras]
            if len(cleaned_extras) == 1:
                key = cleaned_extras[0] or None
            elif len(cleaned_extras) >= 2:
                username = cleaned_extras[0] or None
                password = cleaned_extras[1] or None
                remaining = [segment for segment in cleaned_extras[2:] if segment]
                if remaining:
                    key = ":".join(remaining)
        if api_key is not None:
            key = api_key

        return cls(
            scheme=scheme,
            host=host,
            port=port,
            username=username,
            password=password,
            api_key=key if key else None,
            change_ip_url=change_ip_url,
            min_rotation_interval=min_rotation_interval,
        )


@dataclass(slots=True)
class ThanhnienCategoryConfig:
    """Category selection controls for Thanhnien ingestion."""

    selected_slugs: tuple[str, ...] = ()
    crawl_all: bool = False
    max_pages: int | None = 10
    max_empty_pages: int | None = 2


@dataclass(slots=True)
class ZnewsCategoryConfig:
    """Category selection controls for Znews ingestion."""

    use_categories: bool = False
    selected_slugs: tuple[str, ...] = ()
    crawl_all: bool = False
    max_pages: int | None = 50


@dataclass(slots=True)
class NldCategoryConfig:
    """Category selection controls for Nld ingestion."""

    selected_slugs: tuple[str, ...] = ()
    crawl_all: bool = False
    max_pages: int | None = None
    max_empty_pages: int | None = 1


@dataclass(slots=True)
class Kenh14CategoryConfig:
    """Category selection controls for Kenh14 ingestion."""

    selected_slugs: tuple[str, ...] = ()
    crawl_all: bool = False
    max_pages: int | None = 600
    max_empty_pages: int | None = 3


@dataclass(slots=True)
class PloCategoryConfig:
    """Category selection controls for PLO ingestion."""

    selected_slugs: tuple[str, ...] = ()
    crawl_all: bool = False
    max_pages: int | None = 200
    max_empty_pages: int | None = 3


@dataclass(slots=True)
class VovCategoryConfig:
    """Category selection controls for VOV ingestion."""

    selected_slugs: tuple[str, ...] = ()
    crawl_all: bool = False
    max_pages: int | None = 200
    max_empty_pages: int | None = 2


@dataclass(slots=True)
class VideoDownloadConfig:
    """Controls for per-category video asset downloads."""

    enabled_categories: tuple[str, ...] = ()
    process_pending: bool = False

    @staticmethod
    def _normalize(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip().lower()
        return cleaned or None

    def categories_key_set(self) -> set[str]:
        return {
            key
            for key in (self._normalize(category) for category in self.enabled_categories)
            if key
        }

    def category_allowed(self, *identifiers: str | None) -> bool:
        allowed = self.categories_key_set()
        if not allowed:
            return True
        for candidate in identifiers:
            key = self._normalize(candidate)
            if key and key in allowed:
                return True
        return False


@dataclass(slots=True)
class IngestConfig:
    jobs_file: Path = DEFAULT_JOBS_FILE
    storage_root: Path = DEFAULT_STORAGE_ROOT
    storage_volume_name: str = "default"
    storage_volume_path: Path = DEFAULT_STORAGE_ROOT
    storage_volumes: Dict[str, Path] = field(default_factory=dict)
    storage_warn_threshold: float = 0.9
    storage_pause_file: Optional[Path] = None
    storage_notifications: StorageNotificationConfig = field(default_factory=StorageNotificationConfig)
    db_url: Optional[str] = None
    user_agent: str = DEFAULT_USER_AGENT
    sitemap_max_documents: int | None = 5
    sitemap_max_urls_per_document: int | None = 200
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout: TimeoutConfig = field(default_factory=TimeoutConfig)
    resume: bool = False
    raw_html_cache_enabled: bool = False
    log_dir: Path = DEFAULT_LOG_DIR
    proxy: Optional[ProxyConfig] = None
    playwright_enabled: bool = False
    playwright_timeout: float = 30.0
    jobs_file_provided: bool = False
    thanhnien: ThanhnienCategoryConfig = field(default_factory=ThanhnienCategoryConfig)
    znews: ZnewsCategoryConfig = field(default_factory=ZnewsCategoryConfig)
    nld: NldCategoryConfig = field(default_factory=NldCategoryConfig)
    kenh14: Kenh14CategoryConfig = field(default_factory=Kenh14CategoryConfig)
    plo: PloCategoryConfig = field(default_factory=PloCategoryConfig)
    vov: VovCategoryConfig = field(default_factory=VovCategoryConfig)
    video: VideoDownloadConfig = field(default_factory=VideoDownloadConfig)

    def ensure_directories(self) -> None:
        self.storage_volume_path.mkdir(parents=True, exist_ok=True)
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        if self.storage_pause_file:
            self.storage_pause_file.parent.mkdir(parents=True, exist_ok=True)

    def raw_html_path(self, article_id: str) -> Path:
        return self.storage_root / "raw" / f"{article_id}.html"

    def article_asset_root(self, article_id: str) -> Path:
        return self.storage_root / "articles" / article_id

    def format_asset_reference(self, asset_path: Path) -> str:
        """Return a persistent reference for an asset path including volume metadata."""

        volume_path = self.storage_volume_path
        try:
            relative = asset_path.relative_to(volume_path)
        except ValueError:
            relative = asset_path.relative_to(self.storage_root)
            return relative.as_posix()
        relative_posix = relative.as_posix()
        if self.storage_volume_name:
            return f"{self.storage_volume_name}:{relative_posix}"
        return relative_posix
