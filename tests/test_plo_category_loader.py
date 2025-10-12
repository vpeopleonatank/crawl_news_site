import json
import unittest
from unittest.mock import patch

import httpx

from crawler.config import ProxyConfig
from crawler.jobs import PloCategoryDefinition, PloCategoryLoader


class FakeResponse:
    def __init__(self, url: str, payload: dict | str, status_code: int = 200) -> None:
        self._url = url
        self._payload = payload
        self.status_code = status_code

    @property
    def text(self) -> str:
        if isinstance(self._payload, str):
            return self._payload
        return json.dumps(self._payload)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("GET", self._url)
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("HTTP error", request=request, response=response)

    def json(self) -> dict:
        if isinstance(self._payload, str):
            return json.loads(self._payload)
        return self._payload


class FakeClient:
    def __init__(self, responses: dict[str, FakeResponse], *args, **kwargs) -> None:
        self._responses = responses

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, url: str) -> FakeResponse:
        return self._responses.get(url, FakeResponse(url, {}, status_code=404))


class PloCategoryLoaderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.category = PloCategoryDefinition(
            slug="phap-luat",
            name="Pháp luật",
            zone_id=114,
            landing_url="https://plo.vn/phap-luat/",
        )

        page_1 = self.category.api_url(1)
        page_2 = self.category.api_url(2)

        self.responses = {
            page_1: FakeResponse(
                page_1,
                {
                    "data": {
                        "contents": [
                            {
                                "url": "/sample-article-post874700.html",
                                "update_time": 1760007000,
                                "avatar_url": "https://image.plo.vn/sample.jpg",
                            },
                            {
                                "url": "/sample-article-post874701.html",
                                "update_time": 1760007100,
                                "avatar_url": "https://image.plo.vn/sample2.jpg",
                            },
                        ]
                    }
                },
            ),
            page_2: FakeResponse(
                page_2,
                {
                    "data": {
                        "contents": [
                            {
                                "url": "https://plo.vn/sample-article-post874700.html",
                                "update_time": 1760007200,
                            },
                            {
                                "url": "/sample-article-post874702.html",
                                "update_time": 1760007300,
                            },
                        ]
                    }
                },
            ),
        }

    def test_loader_emits_unique_urls_and_respects_resume(self) -> None:
        existing = {"https://plo.vn/sample-article-post874701.html"}
        loader = PloCategoryLoader(
            categories=[self.category],
            existing_urls=existing,
            resume=True,
            max_pages=3,
            max_empty_pages=2,
            request_timeout=1.0,
            fetch_retry_backoff=0.0,
        )

        with patch("crawler.jobs.httpx.Client", return_value=FakeClient(self.responses)):
            jobs = list(loader)

        urls = [job.url for job in jobs]
        self.assertEqual(
            urls,
            [
                "https://plo.vn/sample-article-post874700.html",
                "https://plo.vn/sample-article-post874702.html",
            ],
        )
        self.assertEqual(loader.stats.emitted, 2)
        self.assertEqual(loader.stats.skipped_existing, 1)
        self.assertEqual(loader.stats.skipped_duplicate, 1)

    def test_loader_passes_proxy_configuration(self) -> None:
        proxy = ProxyConfig.from_endpoint("127.0.0.1:9000")
        loader = PloCategoryLoader(
            categories=[self.category],
            max_pages=1,
            request_timeout=1.0,
            proxy=proxy,
            fetch_retry_backoff=0.0,
        )

        with patch("crawler.jobs.httpx.Client") as client_cls:
            client_instance = client_cls.return_value.__enter__.return_value
            client_instance.get.side_effect = httpx.HTTPError("boom")
            list(loader)

        kwargs = client_cls.call_args.kwargs
        self.assertEqual(kwargs.get("proxy"), proxy.httpx_proxy())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
