import io
import logging
import os
import unittest
from collections import namedtuple
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from crawler.storage import (
    StorageMonitor,
    clear_pause,
    load_storage_settings,
    set_active_volume,
)


class StorageSettingsTestCase(unittest.TestCase):
    def test_defaults_use_requested_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "storage"
            settings = load_storage_settings(base)
            self.assertEqual(settings.active_volume, "default")
            self.assertEqual(settings.active_path, base.resolve())
            self.assertAlmostEqual(settings.warn_threshold, 0.9)
            self.assertEqual(settings.pause_file, base.resolve() / ".pause_ingest")

    def test_parses_env_volumes_and_threshold(self) -> None:
        with TemporaryDirectory() as tmpdir, patch.dict(
            os.environ,
            {
                "STORAGE_VOLUMES": "primary:/mnt/storage01;hdd02:/mnt/storage02",
                "STORAGE_ACTIVE_VOLUME": "hdd02",
                "STORAGE_WARN_THRESHOLD": "95",
                "STORAGE_PAUSE_FILE": f"{tmpdir}/pause.flag",
                "STORAGE_NOTIFY_TELEGRAM_BOT_TOKEN": "token-123",
                "STORAGE_NOTIFY_TELEGRAM_CHAT_ID": "987654321",
                "STORAGE_NOTIFY_TELEGRAM_THREAD_ID": "42",
            },
            clear=True,
        ):
            settings = load_storage_settings(Path("/unused"))
            self.assertEqual(settings.active_volume, "hdd02")
            self.assertEqual(settings.active_path, Path("/mnt/storage02").resolve())
            self.assertAlmostEqual(settings.warn_threshold, 0.95)
            self.assertEqual(settings.pause_file, Path(tmpdir, "pause.flag").resolve())
            self.assertEqual(settings.notifications.telegram_bot_token, "token-123")
            self.assertEqual(settings.notifications.telegram_chat_id, "987654321")
            self.assertEqual(settings.notifications.telegram_thread_id, 42)

    def test_invalid_telegram_thread_id_raises(self) -> None:
        with TemporaryDirectory() as tmpdir, patch.dict(
            os.environ,
            {
                "STORAGE_NOTIFY_TELEGRAM_BOT_TOKEN": "token",
                "STORAGE_NOTIFY_TELEGRAM_CHAT_ID": "chat",
                "STORAGE_NOTIFY_TELEGRAM_THREAD_ID": "not-a-number",
            },
            clear=True,
        ):
            with self.assertRaises(ValueError):
                load_storage_settings(Path(tmpdir))


class StorageMonitorTestCase(unittest.TestCase):
    def test_creates_pause_file_when_threshold_exceeded(self) -> None:
        with TemporaryDirectory() as tmpdir:
            volume_path = Path(tmpdir) / "volume"
            volume_path.mkdir()
            pause_file = Path(tmpdir) / "pause.flag"
            monitor = StorageMonitor(volume_path, pause_file, warn_threshold=0.8)

            usage_tuple = namedtuple("usage", ("total", "used", "free"))
            with patch("crawler.storage.shutil.disk_usage") as mock_usage:
                mock_usage.return_value = usage_tuple(total=100, used=10, free=10)
                should_pause = monitor.check_and_maybe_pause()

            self.assertTrue(should_pause)
            self.assertTrue(pause_file.exists())

            monitor.clear_pause()
            self.assertFalse(pause_file.exists())

    def test_notifier_invoked_when_threshold_exceeded(self) -> None:
        class DummyNotifier:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            def notify_threshold(
                self,
                *,
                volume_path: Path,
                usage_fraction: float,
                threshold_fraction: float,
                pause_file: Path,
            ) -> None:
                self.calls.append(
                    {
                        "volume_path": volume_path,
                        "usage_fraction": usage_fraction,
                        "threshold_fraction": threshold_fraction,
                        "pause_file": pause_file,
                    }
                )

        with TemporaryDirectory() as tmpdir:
            volume_path = Path(tmpdir) / "volume"
            volume_path.mkdir()
            pause_file = Path(tmpdir) / "pause.flag"
            notifier = DummyNotifier()
            monitor = StorageMonitor(volume_path, pause_file, warn_threshold=0.8, notifier=notifier)

            usage_tuple = namedtuple("usage", ("total", "used", "free"))
            with patch("crawler.storage.shutil.disk_usage") as mock_usage:
                mock_usage.return_value = usage_tuple(total=100, used=0, free=10)
                monitor.check_and_maybe_pause()

            self.assertTrue(pause_file.exists())
            self.assertEqual(len(notifier.calls), 1)
            call_kwargs = notifier.calls[0]
            self.assertGreaterEqual(call_kwargs["usage_fraction"], 0.8)
            self.assertEqual(call_kwargs["pause_file"], pause_file)

    def test_httpx_logger_redacts_token(self) -> None:
        logger = logging.getLogger("httpx")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        previous_level = logger.level
        logger.setLevel(logging.INFO)
        try:
            logger.info(
                'HTTP Request: POST https://api.telegram.org/bot123456:ABCdef/sendMessage "HTTP/1.1 200 OK"'
            )
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous_level)
        output = stream.getvalue()
        self.assertNotIn("123456:ABCdef", output)
        self.assertIn("bot<redacted>", output)


class StorageCLIHelpersTestCase(unittest.TestCase):
    def test_set_active_volume_updates_env_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "STORAGE_VOLUMES=primary:/mnt/a;hdd:/mnt/b\nSTORAGE_ACTIVE_VOLUME=primary\n",
                encoding="utf-8",
            )
            set_active_volume(env_path, "hdd")

            contents = env_path.read_text(encoding="utf-8")
            self.assertIn("STORAGE_ACTIVE_VOLUME=hdd", contents)

    def test_set_active_volume_raises_for_unknown_volume(self) -> None:
        with TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("STORAGE_VOLUMES=primary:/mnt/a\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                set_active_volume(env_path, "missing")

    def test_clear_pause_removes_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            pause_file = Path(tmpdir) / "pause.flag"
            pause_file.write_text("paused", encoding="utf-8")
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(f"STORAGE_PAUSE_FILE={pause_file}\n", encoding="utf-8")

            clear_pause(env_path)
            self.assertFalse(pause_file.exists())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
