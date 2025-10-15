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
            },
            clear=True,
        ):
            settings = load_storage_settings(Path("/unused"))
            self.assertEqual(settings.active_volume, "hdd02")
            self.assertEqual(settings.active_path, Path("/mnt/storage02").resolve())
            self.assertAlmostEqual(settings.warn_threshold, 0.95)
            self.assertEqual(settings.pause_file, Path(tmpdir, "pause.flag").resolve())


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
