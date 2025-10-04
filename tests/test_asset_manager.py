import hashlib
import tempfile
import unittest
from pathlib import Path

import httpx

from crawler.assets import AssetManager, AssetDownloadError
from crawler.config import IngestConfig
from crawler.parsers import AssetType, ParsedAsset


class AssetManagerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        storage_root = Path(self._tmpdir.name)
        self.config = IngestConfig(storage_root=storage_root)

        asset_payloads = {
            "https://cdn.example.com/image.jpg": b"image-bytes",
            "https://cdn.example.com/video.mp4": b"video-bytes",
            "https://cdn.example.com/empty.bin": b"",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url not in asset_payloads:
                return httpx.Response(404)
            return httpx.Response(200, content=asset_payloads[url])

        self.transport = httpx.MockTransport(handler)
        self.client = httpx.Client(transport=self.transport)
        self.manager = AssetManager(self.config, client=self.client)
        self.asset_payloads = asset_payloads

    def tearDown(self) -> None:
        # AssetManager only closes clients it owns.
        self.client.close()
        self._tmpdir.cleanup()

    def test_download_assets_streams_and_hashes(self) -> None:
        assets = [
            ParsedAsset(
                source_url="https://cdn.example.com/image.jpg",
                asset_type=AssetType.IMAGE,
                sequence=2,
                caption="Example image",
            ),
            ParsedAsset(
                source_url="data:image/png;base64,AAA",
                asset_type=AssetType.IMAGE,
                sequence=3,
            ),
            ParsedAsset(
                source_url="https://cdn.example.com/image.jpg",
                asset_type=AssetType.IMAGE,
                sequence=1,
            ),
            ParsedAsset(
                source_url="https://cdn.example.com/video.mp4",
                asset_type=AssetType.VIDEO,
                sequence=4,
            ),
        ]

        stored_assets = self.manager.download_assets("article-123", assets)

        self.assertEqual(len(stored_assets), 2)

        image_asset, video_asset = stored_assets

        self.assertTrue(image_asset.path.exists())
        expected_image_path = (
            self.config.storage_root
            / "articles"
            / "article-123"
            / "images"
            / "001.jpg"
        )
        self.assertEqual(image_asset.path, expected_image_path)
        self.assertEqual(
            image_asset.checksum,
            hashlib.sha256(self.asset_payloads["https://cdn.example.com/image.jpg"]).hexdigest(),
        )
        self.assertEqual(
            image_asset.bytes_downloaded,
            len(self.asset_payloads["https://cdn.example.com/image.jpg"]),
        )

        self.assertTrue(video_asset.path.exists())
        expected_video_path = (
            self.config.storage_root
            / "articles"
            / "article-123"
            / "videos"
            / "004.mp4"
        )
        self.assertEqual(video_asset.path, expected_video_path)
        self.assertEqual(
            video_asset.checksum,
            hashlib.sha256(self.asset_payloads["https://cdn.example.com/video.mp4"]).hexdigest(),
        )
        self.assertEqual(
            video_asset.bytes_downloaded,
            len(self.asset_payloads["https://cdn.example.com/video.mp4"]),
        )

    def test_empty_body_raises_error(self) -> None:
        empty_assets = [
            ParsedAsset(
                source_url="https://cdn.example.com/empty.bin",
                asset_type=AssetType.IMAGE,
                sequence=1,
            )
        ]

        with self.assertRaises(AssetDownloadError):
            self.manager.download_assets("article-abc", empty_assets)


if __name__ == "__main__":
    unittest.main()
