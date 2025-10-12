import unittest
from unittest.mock import patch

import httpx

from crawler.jobs import ZnewsCategoryDefinition, ZnewsCategoryLoader


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


class ZnewsCategoryLoaderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.category = ZnewsCategoryDefinition(
            slug="phap-luat",
            name="Pháp luật",
            landing_url="https://lifestyle.znews.vn/phap-luat.html",
        )

        landing_url = self.category.page_url(1)
        page_2_url = self.category.page_url(2)
        page_3_url = self.category.page_url(3)

        self.responses = {
            landing_url: FakeResponse(
                landing_url,
                """
                <html>
                    <body>
                        <a href="/tin-tuc-a-post1591001.html">Article A</a>
                        <a href="https://znews.vn/tin-tuc-b-post1591002.html">Article B</a>
                    </body>
                </html>
                """,
            ),
            page_2_url: FakeResponse(
                page_2_url,
                """
                <html>
                    <body>
                        <a data-utm-src="/tin-tuc-c-post1591003.html">Article C</a>
                        <a href="/tin-tuc-b-post1591002.html">Article B duplicate</a>
                    </body>
                </html>
                """,
            ),
            page_3_url: FakeResponse(
                page_3_url,
                """
                <html>
                    <body>
                        <a data-utm-source="/tin-tuc-c-post1591003.html">Article C duplicate</a>
                        <a href="/tin-tuc-b-post1591002.html">Article B duplicate</a>
                    </body>
                </html>
                """,
            ),
        }

    def test_category_loader_emits_unique_articles_respecting_resume(self) -> None:
        existing = {"https://znews.vn/tin-tuc-a-post1591001.html"}
        loader = ZnewsCategoryLoader(
            categories=[self.category],
            existing_urls=existing,
            resume=True,
            max_pages=5,
            request_timeout=1.0,
            fetch_retry_backoff=0.0,
        )

        with patch("crawler.jobs.httpx.Client", return_value=FakeClient(self.responses)):
            jobs = list(loader)

        urls = [job.url for job in jobs]
        self.assertEqual(
            urls,
            [
                "https://znews.vn/tin-tuc-b-post1591002.html",
                "https://znews.vn/tin-tuc-c-post1591003.html",
            ],
        )
        self.assertEqual(loader.stats.emitted, 2)
        self.assertEqual(loader.stats.skipped_existing, 1)
        self.assertEqual(loader.stats.skipped_duplicate, 1)  # duplicate across requests


if __name__ == "__main__":  # pragma: no cover - test runner entrypoint
    unittest.main()
