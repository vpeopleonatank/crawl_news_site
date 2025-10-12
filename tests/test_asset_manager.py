import unittest
from unittest.mock import patch

import httpx

from crawler.assets import AssetManager, AssetDownloadError
from crawler.config import IngestConfig, ProxyConfig
from crawler.parsers import AssetType, ParsedAsset


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        if not (200 <= self.status_code < 300):
            raise httpx.HTTPStatusError("error", request=None, response=None)

    def json(self):
        return self._payload


class FakeClient:
    def __init__(self, response_map=None):
        self.response_map = response_map or {}
        self.requested_urls = []

    def get(self, url, timeout=None):
        self.requested_urls.append(url)
        if url not in self.response_map:
            raise httpx.HTTPError("not found")
        return self.response_map[url]

    def stream(self, method, url):  # pragma: no cover - not used in these tests
        raise NotImplementedError


class AssetManagerResolveVideoTestCase(unittest.TestCase):
    def test_resolves_thanhnien_hls_manifest(self) -> None:
        manifest_url = (
            "https://thanhnien.mediacdn.vn/325084952045817856/2025/10/3/"
            "1-1759489185419194083592.mp4.json"
        )
        expected_hls = (
            "https://thanhnien.mediacdn.vn/.hls/325084952045817856/2025/10/3/"
            "1-1759489185419194083592.mp4.master.m3u8?v=f-f3f2838c-1"
        )
        fake_client = FakeClient(
            {
                manifest_url: FakeResponse(
                    payload={
                        "hls": expected_hls,
                        "mhls": "https://thanhnien.mediacdn.vn/.hls/mobile.m3u8",
                    }
                )
            }
        )

        manager = AssetManager(IngestConfig(), client=fake_client)
        source = (
            "https://thanhnien.mediacdn.vn/325084952045817856/2025/10/3/"
            "1-1759489185419194083592.mp4"
        )

        resolved = manager._resolve_video_source(source)

        self.assertEqual(resolved, expected_hls)
        self.assertEqual(fake_client.requested_urls, [manifest_url])

    def test_resolver_falls_back_when_manifest_missing(self) -> None:
        source = "https://thanhnien.mediacdn.vn/video/sample.mp4"
        fake_client = FakeClient({})
        manager = AssetManager(IngestConfig(), client=fake_client)

        resolved = manager._resolve_video_source(source)

        self.assertEqual(resolved, source)


class AssetManagerProxyTestCase(unittest.TestCase):
    def test_applies_proxy_configuration(self) -> None:
        config = IngestConfig()
        config.proxy = ProxyConfig.from_endpoint("127.0.0.1:8080")

        with patch("crawler.assets.httpx.Client") as client_cls:
            manager = AssetManager(config)
            try:
                client_cls.assert_called_once()
                kwargs = client_cls.call_args.kwargs
                self.assertEqual(kwargs.get("proxy"), config.proxy.httpx_proxy())
            finally:
                manager.close()


class AssetManagerDownloadWorkflowTestCase(unittest.TestCase):
    def test_skips_blacklisted_lotus_quiz_assets(self) -> None:
        asset = ParsedAsset(
            source_url="https://challenge.lotus.vn/corona/quiz?_id=5e3bec8324d9b3",
            asset_type=AssetType.IMAGE,
            sequence=1,
        )

        with patch.object(AssetManager, "_stream_to_file") as stream_mock:
            manager = AssetManager(IngestConfig(), client=FakeClient())
            try:
                stored = manager.download_assets("0199d134-ebf2-7f21-9994-b498b1d20456", [asset])
            finally:
                manager.close()

        self.assertEqual(stored, [])
        stream_mock.assert_not_called()

    def test_normalizes_sohatv_embed_videos(self) -> None:
        embed_url = (
            "https://player.sohatv.vn/embed/100387?"
            "vid=nguoiduatinvideo.mediacdn.vn/84299287675052032/2025/3/18/"
            "lv020250318121708-17422764440581360405277.mp4"
            "&poster=https://nguoiduatinvideo.mediacdn.vn/.v-thumb/84299287675052032/2025/3/18/"
            "lv020250318121708-17422764440581360405277.mp4.jpg"
        )
        asset = ParsedAsset(source_url=embed_url, asset_type=AssetType.VIDEO, sequence=1)

        with patch.object(AssetManager, "_stream_to_file", return_value=("checksum", 1024)) as stream_mock:
            manager = AssetManager(IngestConfig(), client=FakeClient())
            try:
                stored = manager.download_assets("0199d131-942a-7a72-a0bb-55944b57ea1d", [asset])
            finally:
                manager.close()

        stream_mock.assert_called_once()
        normalized_url = stream_mock.call_args.args[0]
        self.assertEqual(
            normalized_url,
            "https://nguoiduatinvideo.mediacdn.vn/84299287675052032/2025/3/18/"
            "lv020250318121708-17422764440581360405277.mp4",
        )
        self.assertEqual(stored[0].path.name, "001.mp4")
        self.assertEqual(stored[0].source.source_url, normalized_url)

    def test_applies_referrer_header_when_available(self) -> None:
        asset = ParsedAsset(
            source_url="https://streaming-cms-plo.epicdn.me/video.mp4",
            asset_type=AssetType.VIDEO,
            sequence=1,
            referrer="https://plo.vn/some-article",
        )

        with patch.object(AssetManager, "_stream_to_file", return_value=("checksum", 2048)) as stream_mock:
            manager = AssetManager(IngestConfig(), client=FakeClient())
            try:
                manager.download_assets("0199d5f6-9903-75b0-a394-9f7f15a2e807", [asset])
            finally:
                manager.close()

        stream_mock.assert_called_once()
        headers = stream_mock.call_args.kwargs.get("headers") or {}
        self.assertEqual(headers.get("Referer"), asset.referrer)
        self.assertEqual(headers.get("Origin"), "https://plo.vn")

    def test_extension_falls_back_to_default_when_missing(self) -> None:
        extension = AssetManager._extension_from_url("https://player.sohatv.vn/embed/100387", "mp4")
        self.assertEqual(extension, "mp4")


if __name__ == "__main__":
    unittest.main()
