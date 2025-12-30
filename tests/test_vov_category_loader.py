import unittest
from unittest.mock import patch

import httpx

from crawler.config import ProxyConfig
from crawler.jobs import VovCategoryDefinition, VovCategoryLoader


class FakeResponse:
    def __init__(self, url: str, text: str, status_code: int = 200) -> None:
        self._url = url
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("GET", self._url)
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("HTTP error", request=request, response=response)


class FakeClient:
    def __init__(self, responses: dict[str, FakeResponse], *args, **kwargs) -> None:
        self._responses = responses

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, url: str) -> FakeResponse:
        return self._responses.get(url, FakeResponse(url, "", status_code=404))


class VovCategoryLoaderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.category = VovCategoryDefinition(
            slug="phap-luat",
            name="Pháp luật",
            landing_url="https://vov.vn/phap-luat",
        )

        landing_url = self.category.normalized_landing_url()
        page_1 = self.category.page_url(1)
        page_2 = self.category.page_url(2)

        self.responses = {
            landing_url: FakeResponse(
                landing_url,
                """
                <html>
                  <body>
                    <a href="/phap-luat/landing-story-post123456.vov">Landing story</a>
                  </body>
                </html>
                """,
            ),
            page_1: FakeResponse(
                page_1,
                """
                <div class="category">
                  <a href="/phap-luat/page-story-post123457.vov">Page story</a>
                  <a href="https://vov.vn/phap-luat/landing-story-post123456.vov">Duplicate landing</a>
                  <a href="/phap-luat?page=999">Not an article</a>
                </div>
                """,
            ),
            page_2: FakeResponse(page_2, "   "),
        }

    def test_category_loader_emits_unique_urls_and_respects_resume(self) -> None:
        existing = {"https://vov.vn/phap-luat/landing-story-post123456.vov"}
        loader = VovCategoryLoader(
            categories=[self.category],
            existing_urls=existing,
            resume=True,
            max_pages=3,
            max_empty_pages=1,
            request_timeout=1.0,
            fetch_retry_backoff=0.0,
        )

        with patch("crawler.jobs.httpx.Client", return_value=FakeClient(self.responses)):
            jobs = list(loader)

        urls = [job.url for job in jobs]
        self.assertEqual(urls, ["https://vov.vn/phap-luat/page-story-post123457.vov"])
        self.assertEqual(loader.stats.emitted, 1)
        self.assertEqual(loader.stats.skipped_existing, 1)
        self.assertEqual(loader.stats.skipped_duplicate, 1)

    def test_category_loader_passes_proxy_configuration(self) -> None:
        proxy = ProxyConfig.from_endpoint("127.0.0.1:9020")
        loader = VovCategoryLoader(
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

