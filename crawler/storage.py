"""Storage configuration helpers, monitoring, and CLI utilities."""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Protocol

import httpx

from .config import StorageNotificationConfig

LOGGER = logging.getLogger(__name__)

_VOLUMES_ENV = "STORAGE_VOLUMES"
_ACTIVE_ENV = "STORAGE_ACTIVE_VOLUME"
_WARN_ENV = "STORAGE_WARN_THRESHOLD"
_PAUSE_ENV = "STORAGE_PAUSE_FILE"
_TELEGRAM_TOKEN_ENV = "STORAGE_NOTIFY_TELEGRAM_BOT_TOKEN"
_TELEGRAM_CHAT_ENV = "STORAGE_NOTIFY_TELEGRAM_CHAT_ID"
_TELEGRAM_THREAD_ENV = "STORAGE_NOTIFY_TELEGRAM_THREAD_ID"

_DEFAULT_VOLUME_NAME = "default"
_DEFAULT_WARN_THRESHOLD = 0.9
_TELEGRAM_TOKEN_RE = re.compile(r"/bot(?P<token>[^/\s]+)/")


def _normalise_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    try:
        return path.resolve()
    except FileNotFoundError:
        return path


def _parse_volume_entry(entry: str) -> tuple[str, Path]:
    cleaned = entry.strip()
    if not cleaned:
        raise ValueError("Empty volume entry")

    delimiter = "=" if "=" in cleaned else ":"
    if delimiter not in cleaned:
        raise ValueError(f"Volume entry must be NAME:PATH (got {entry!r})")

    name, path_value = cleaned.split(delimiter, 1)
    volume_name = name.strip()
    if not volume_name:
        raise ValueError(f"Volume name must not be empty in entry {entry!r}")
    if not path_value.strip():
        raise ValueError(f"Volume path must not be empty in entry {entry!r}")
    return volume_name, _normalise_path(path_value.strip())


def _parse_volumes(raw_value: str) -> Dict[str, Path]:
    if not raw_value:
        return {}

    volumes: Dict[str, Path] = {}
    separators = [";", ","]
    for delimiter in separators:
        if delimiter in raw_value:
            tokens = [segment.strip() for segment in raw_value.split(delimiter)]
            break
    else:
        tokens = [raw_value.strip()]

    for token in tokens:
        if not token:
            continue
        name, path = _parse_volume_entry(token)
        volumes[name] = path
    return volumes


def _coerce_threshold(raw_value: str | None) -> float:
    if raw_value is None or not raw_value.strip():
        return _DEFAULT_WARN_THRESHOLD

    cleaned = raw_value.strip()
    try:
        value = float(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid storage warn threshold {cleaned!r}") from exc

    if value > 1:
        value = value / 100.0
    value = max(0.0, min(value, 0.999))
    return value


def _load_notification_settings() -> StorageNotificationConfig:
    token_raw = os.getenv(_TELEGRAM_TOKEN_ENV)
    chat_raw = os.getenv(_TELEGRAM_CHAT_ENV)
    thread_raw = os.getenv(_TELEGRAM_THREAD_ENV)

    settings = StorageNotificationConfig()
    if token_raw and token_raw.strip():
        settings.telegram_bot_token = token_raw.strip()
    if chat_raw and chat_raw.strip():
        settings.telegram_chat_id = chat_raw.strip()
    if thread_raw and thread_raw.strip():
        cleaned = thread_raw.strip()
        try:
            settings.telegram_thread_id = int(cleaned)
        except ValueError as exc:
            raise ValueError(f"Invalid Telegram thread ID {cleaned!r}") from exc
    return settings


def _mask_telegram_token(text: str) -> str:
    return _TELEGRAM_TOKEN_RE.sub("/bot<redacted>/", text)


class _HttpxTelegramFilter(logging.Filter):
    """Redact Telegram bot tokens from httpx request logs."""

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - exercised indirectly
        try:
            message = record.getMessage()
        except Exception:
            return True
        masked = _mask_telegram_token(message)
        if masked != message:
            record.msg = masked
            record.args = ()
        return True


def _ensure_httpx_filter() -> None:
    logger = logging.getLogger("httpx")
    if any(isinstance(f, _HttpxTelegramFilter) for f in logger.filters):
        return
    logger.addFilter(_HttpxTelegramFilter())


_ensure_httpx_filter()


@dataclass(slots=True)
class StorageSettings:
    """Resolved storage configuration."""

    volumes: Dict[str, Path]
    active_volume: str
    warn_threshold: float
    pause_file: Path
    notifications: StorageNotificationConfig

    @property
    def active_path(self) -> Path:
        return self.volumes[self.active_volume]


class StorageThresholdNotifier(Protocol):
    """Observer notified when storage approaches capacity."""

    def notify_threshold(
        self,
        *,
        volume_path: Path,
        usage_fraction: float,
        threshold_fraction: float,
        pause_file: Path,
    ) -> None:
        ...


@dataclass(slots=True)
class TelegramNotifier:
    """Send storage capacity alerts to Telegram."""

    bot_token: str
    chat_id: str
    thread_id: Optional[int] = None
    timeout: float = 10.0

    def notify_threshold(
        self,
        *,
        volume_path: Path,
        usage_fraction: float,
        threshold_fraction: float,
        pause_file: Path,
    ) -> None:
        usage_percent = round(usage_fraction * 100, 2)
        threshold_percent = round(threshold_fraction * 100, 2)
        message_lines = [
            "Storage usage threshold reached.",
            f"Volume: {volume_path}",
            f"Usage: {usage_percent:.2f}% (threshold {threshold_percent:.2f}%)",
            f"Pause sentinel: {pause_file}",
        ]
        message_text = "\n".join(message_lines).strip()
        if not message_text:
            LOGGER.warning("Skipping Telegram notification because message text is empty (volume=%s)", volume_path)
            return
        payload = {
            "chat_id": self.chat_id,
            "text": message_text,
            "disable_notification": False,
            "disable_web_page_preview": True,
        }
        if self.thread_id is not None:
            payload["message_thread_id"] = self.thread_id

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            LOGGER.debug("Sending Telegram storage alert: chat=%s thread=%s", self.chat_id, self.thread_id)
            response = httpx.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            masked_error = _mask_telegram_token(str(exc))
            LOGGER.warning("Failed to send Telegram notification: %s", masked_error)


def build_storage_notifier(settings: StorageNotificationConfig | None) -> StorageThresholdNotifier | None:
    if not settings:
        return None
    if settings.telegram_bot_token and settings.telegram_chat_id:
        return TelegramNotifier(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
            thread_id=settings.telegram_thread_id,
        )
    return None


def load_storage_settings(requested_root: Path) -> StorageSettings:
    """Load storage settings from environment variables."""

    volumes_env = os.getenv(_VOLUMES_ENV, "")
    volumes = _parse_volumes(volumes_env)

    if not volumes:
        volumes = {_DEFAULT_VOLUME_NAME: requested_root}

    active = os.getenv(_ACTIVE_ENV, "").strip() or next(iter(volumes.keys()))
    if active not in volumes:
        valid = ", ".join(sorted(volumes))
        raise ValueError(f"Active storage volume {active!r} is not defined. Available volumes: {valid}")

    warn_threshold = _coerce_threshold(os.getenv(_WARN_ENV))
    pause_file_raw = os.getenv(_PAUSE_ENV)
    if pause_file_raw and pause_file_raw.strip():
        pause_file = _normalise_path(pause_file_raw.strip())
    else:
        pause_file = volumes[active] / ".pause_ingest"

    return StorageSettings(
        volumes=volumes,
        active_volume=active,
        warn_threshold=warn_threshold,
        pause_file=pause_file,
        notifications=_load_notification_settings(),
    )


class StorageMonitor:
    """Monitor active storage volume usage and surface pause signals."""

    def __init__(
        self,
        volume_path: Path,
        pause_file: Path,
        warn_threshold: float,
        notifier: StorageThresholdNotifier | None = None,
    ) -> None:
        self._volume_path = volume_path
        self._pause_file = pause_file
        self._warn_threshold = warn_threshold
        if notifier is None:
            notifier = build_storage_notifier(_load_notification_settings())
        self._notifier = notifier

    @classmethod
    def from_settings(cls, settings: StorageSettings) -> "StorageMonitor":
        notifier = build_storage_notifier(settings.notifications)
        return cls(settings.active_path, settings.pause_file, settings.warn_threshold, notifier=notifier)

    @property
    def pause_file(self) -> Path:
        return self._pause_file

    @property
    def warn_threshold(self) -> float:
        return self._warn_threshold

    @property
    def volume_path(self) -> Path:
        return self._volume_path

    def is_paused(self) -> bool:
        return self._pause_file.exists()

    def usage_fraction(self) -> float:
        usage = shutil.disk_usage(self._volume_path)
        if usage.total <= 0:
            return 0.0
        used = usage.total - usage.free
        return used / usage.total

    def mark_paused(self) -> None:
        self._pause_file.parent.mkdir(parents=True, exist_ok=True)
        self._pause_file.touch(exist_ok=True)

    def clear_pause(self) -> None:
        if self._pause_file.exists():
            self._pause_file.unlink()

    def check_and_maybe_pause(self) -> bool:
        """Return True when ingestion should remain paused."""

        if self.is_paused():
            return True

        fraction = self.usage_fraction()
        if fraction >= self._warn_threshold:
            percent = round(fraction * 100, 2)
            LOGGER.warning(
                "Storage volume %s at %s%% capacity (threshold %.0f%%); creating pause sentinel %s",
                self._volume_path,
                percent,
                self._warn_threshold * 100,
                self._pause_file,
            )
            if self._notifier is not None:
                try:
                    self._notifier.notify_threshold(
                        volume_path=self._volume_path,
                        usage_fraction=fraction,
                        threshold_fraction=self._warn_threshold,
                        pause_file=self._pause_file,
                    )
                except Exception:
                    LOGGER.exception("Storage notifier raised unexpectedly")
            self.mark_paused()
            return True
        return False


def _load_env_file(env_path: Path) -> Dict[str, str]:
    if not env_path.exists():
        return {}

    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def _write_env_file(env_path: Path, values: Mapping[str, str]) -> None:
    existing_lines = []
    seen: Dict[str, str] = {}
    if env_path.exists():
        existing_lines = env_path.read_text(encoding="utf-8").splitlines()
        for raw_line in existing_lines:
            if "=" not in raw_line:
                continue
            key, value = raw_line.split("=", 1)
            seen[key.strip()] = value

    updated_lines: list[str] = []
    handled_keys: set[str] = set()
    for line in existing_lines:
        if "=" not in line:
            updated_lines.append(line)
            continue
        key, _ = line.split("=", 1)
        trimmed_key = key.strip()
        if trimmed_key in values:
            updated_lines.append(f"{trimmed_key}={values[trimmed_key]}")
            handled_keys.add(trimmed_key)
        else:
            updated_lines.append(line)

    for key, value in values.items():
        if key in handled_keys:
            continue
        updated_lines.append(f"{key}={value}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")


def set_active_volume(env_path: Path, target_volume: str) -> None:
    """Update STORAGE_ACTIVE_VOLUME in the provided .env file."""

    env = _load_env_file(env_path)
    volumes = _parse_volumes(env.get(_VOLUMES_ENV, ""))
    if volumes and target_volume not in volumes:
        valid = ", ".join(sorted(volumes))
        raise ValueError(f"Unknown volume {target_volume!r}; valid volumes: {valid}")

    _write_env_file(env_path, {_ACTIVE_ENV: target_volume})
    LOGGER.info("Updated %s to use active volume %s", env_path, target_volume)


def mark_paused(env_path: Path, pause_file: Path | None = None) -> Path:
    """Create the pause sentinel file."""

    if pause_file is None:
        env = _load_env_file(env_path)
        pause_file = _normalise_path(env.get(_PAUSE_ENV, ".pause_ingest"))
    pause_file.parent.mkdir(parents=True, exist_ok=True)
    pause_file.touch(exist_ok=True)
    LOGGER.info("Created pause sentinel at %s", pause_file)
    return pause_file


def clear_pause(env_path: Path, pause_file: Path | None = None) -> None:
    """Clear the pause sentinel file."""

    if pause_file is None:
        env = _load_env_file(env_path)
        pause_file = _normalise_path(env.get(_PAUSE_ENV, ".pause_ingest"))
    if pause_file.exists():
        pause_file.unlink()
        LOGGER.info("Cleared pause sentinel at %s", pause_file)


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Storage management utilities")
    sub = parser.add_subparsers(dest="command", required=True)

    set_active = sub.add_parser("set-active", help="Switch STORAGE_ACTIVE_VOLUME in the .env file")
    set_active.add_argument("volume", type=str, help="Volume name to activate")
    set_active.add_argument("--env", type=Path, default=Path(".env"), help="Path to .env (default: ./.env)")

    sub.add_parser("usage", help="Print usage information for the active volume").add_argument(
        "--env", type=Path, default=Path(".env"), help="Path to .env (default: ./.env)"
    )

    mark = sub.add_parser("pause", help="Create the pause sentinel file")
    mark.add_argument("--env", type=Path, default=Path(".env"), help="Path to .env (default: ./.env)")
    mark.add_argument("--file", type=Path, help="Override pause sentinel path")

    clear = sub.add_parser("resume", help="Remove the pause sentinel file")
    clear.add_argument("--env", type=Path, default=Path(".env"), help="Path to .env (default: ./.env)")
    clear.add_argument("--file", type=Path, help="Override pause sentinel path")

    return parser


def _cli_usage(env_path: Path) -> None:
    env = _load_env_file(env_path)
    volumes_value = env.get(_VOLUMES_ENV, "")
    volumes = _parse_volumes(volumes_value)
    active = env.get(_ACTIVE_ENV) or (next(iter(volumes)) if volumes else _DEFAULT_VOLUME_NAME)
    warn_threshold = _coerce_threshold(env.get(_WARN_ENV))

    if not volumes:
        volumes = {active: _normalise_path("storage")}

    if active not in volumes:
        raise SystemExit(f"Active volume {active} not defined in {_VOLUMES_ENV}")

    monitor = StorageMonitor(volumes[active], _normalise_path(env.get(_PAUSE_ENV, ".pause_ingest")), warn_threshold)
    fraction = monitor.usage_fraction()
    percent = round(fraction * 100, 2)
    status = "PAUSED" if monitor.is_paused() else "ACTIVE"
    print(f"Volume {active}: {percent}% used ({status}) at {monitor.volume_path}")


def main(argv: Iterable[str] | None = None) -> None:
    parser = _build_cli()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.command == "set-active":
        set_active_volume(args.env, args.volume)
    elif args.command == "usage":
        _cli_usage(args.env)
    elif args.command == "pause":
        mark_paused(args.env, args.file)
    elif args.command == "resume":
        clear_pause(args.env, args.file)
    else:  # pragma: no cover - defensive guard
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
