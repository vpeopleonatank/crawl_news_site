import unittest

from crawler.ingest_thanhnien import _update_video_assets_with_playwright
from crawler.parsers import AssetType, ParsedAsset
from crawler.playwright_support import PlaywrightVideoResolverError


class DummyResolver:
    def __init__(self, streams, should_raise=False):
        self._streams = streams
        self._should_raise = should_raise
        self.calls = 0

    def resolve_streams(self, article_url: str):
        self.calls += 1
        if self._should_raise:
            raise PlaywrightVideoResolverError("resolver failure")
        return self._streams


class UpdateVideoAssetsPlaywrightTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.article_url = (
            "https://thanhnien.vn/quyen-linh-dan-con-gai-xinh-nhu-hoa-hau-di-cung-to-nghe-san-khau-"
            "185251003181545979.htm"
        )
        self.assets = [
            ParsedAsset(
                source_url="https://thanhnien.mediacdn.vn/video/sample.mp4",
                asset_type=AssetType.VIDEO,
                sequence=1,
            )
        ]

    def test_updates_video_asset_with_hls_manifest(self) -> None:
        expected_hls = (
            "https://thanhnien.mediacdn.vn/.hls/325084952045817856/2025/10/3/"
            "1-1759489185419194083592.mp4.master.m3u8?v=f-f3f2838c-1"
        )
        resolver = DummyResolver([
            {
                "json_url": "https://thanhnien.mediacdn.vn/325084952045817856/2025/10/3/"
                "1-1759489185419194083592.mp4.json",
                "hls": expected_hls,
                "mhls": "https://thanhnien.mediacdn.vn/.hls/mobile.m3u8",
            }
        ])

        _update_video_assets_with_playwright(resolver, self.article_url, self.assets)

        self.assertEqual(resolver.calls, 1)
        self.assertEqual(self.assets[0].source_url, expected_hls)

    def test_leaves_asset_unchanged_on_failure(self) -> None:
        original_url = self.assets[0].source_url
        resolver = DummyResolver([], should_raise=True)

        _update_video_assets_with_playwright(resolver, self.article_url, self.assets)

        self.assertEqual(self.assets[0].source_url, original_url)


if __name__ == "__main__":
    unittest.main()
