import unittest
from unittest.mock import patch

import httpx

from crawler.assets import AssetManager, AssetDownloadError
from crawler.config import IngestConfig, ProxyConfig


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


if __name__ == "__main__":
    unittest.main()
