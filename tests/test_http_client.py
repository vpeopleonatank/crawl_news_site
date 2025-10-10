import unittest
from collections import deque

import httpx

from crawler.config import IngestConfig, ProxyConfig
from crawler.http_client import HttpFetchError, HttpFetcher, ProxyRotator


class ProxyRotatorTestCase(unittest.TestCase):
    def test_rotation_respects_min_interval(self) -> None:
        calls = deque()

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url)
            self.assertEqual(request.url.params.get("key"), "secret")
            return httpx.Response(200, json={"status": "ok"})

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport)

        ticks = deque([0.0, 100.0, 241.0])

        def time_source() -> float:
            return ticks.popleft()

        proxy = ProxyConfig(
            scheme="http",
            host="127.0.0.1",
            port=8080,
            api_key="secret",
            change_ip_url="https://proxy.example.com/change",
            min_rotation_interval=240.0,
        )

        rotator = ProxyRotator(proxy, time_source=time_source, client=client)
        try:
            self.assertTrue(rotator.rotate())
            self.assertFalse(rotator.rotate())
            self.assertTrue(rotator.rotate())
        finally:
            rotator.close()

        self.assertEqual(len(calls), 2)


class StubRotator:
    def __init__(self) -> None:
        self.rotate_calls = 0
        self.closed = False

    def should_rotate_response(self, response: httpx.Response) -> bool:
        return response.status_code == httpx.codes.FORBIDDEN

    def rotate(self) -> bool:
        self.rotate_calls += 1
        return True

    def close(self) -> None:
        self.closed = True


class HttpFetcherTestCase(unittest.TestCase):
    def test_fetch_triggers_rotation_on_block(self) -> None:
        fetch_calls = deque()

        def handler(request: httpx.Request) -> httpx.Response:
            fetch_calls.append(str(request.url))
            if len(fetch_calls) == 1:
                return httpx.Response(httpx.codes.FORBIDDEN, headers={"content-type": "text/html"})
            return httpx.Response(200, text="<html>ok</html>", headers={"content-type": "text/html"})

        transport = httpx.MockTransport(handler)
        config = IngestConfig()
        rotator = StubRotator()

        fetcher = HttpFetcher(config, transport=transport, rotator=rotator)
        try:
            text, response = fetcher.fetch_html("https://news.example.com/article")
        finally:
            fetcher.close()

        self.assertEqual(text, "<html>ok</html>")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(rotator.rotate_calls, 1)
        self.assertTrue(rotator.closed)
        self.assertEqual(len(fetch_calls), 2)

    def test_fetch_raises_when_not_blocked(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, headers={"content-type": "text/html"})

        transport = httpx.MockTransport(handler)
        config = IngestConfig()
        rotator = StubRotator()

        fetcher = HttpFetcher(config, transport=transport, rotator=rotator)
        try:
            with self.assertRaises(HttpFetchError):
                fetcher.fetch_html("https://news.example.com/article")
        finally:
            fetcher.close()

        self.assertEqual(rotator.rotate_calls, 0)
        self.assertTrue(rotator.closed)


class ProxyConfigTestCase(unittest.TestCase):
    def test_httpx_proxy_includes_credentials(self) -> None:
        proxy = ProxyConfig.from_endpoint("proxy.example.com:3128:alice:secret")
        self.assertEqual(proxy.httpx_proxy(), "http://alice:secret@proxy.example.com:3128")

    def test_proxy_key_override_preserves_credentials(self) -> None:
        proxy = ProxyConfig.from_endpoint(
            "proxy.example.com:3128:alice:secret",
            api_key="rotate-key",
        )
        self.assertEqual(proxy.username, "alice")
        self.assertEqual(proxy.password, "secret")
        self.assertEqual(proxy.api_key, "rotate-key")


if __name__ == "__main__":
    unittest.main()
