import unittest
from unittest.mock import patch

import httpx

from crawler.jobs import ThanhnienCategoryDefinition, ThanhnienCategoryLoader


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


class ThanhnienCategoryLoaderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.category = ThanhnienCategoryDefinition(
            slug="thoi-su-phap-luat",
            name="Pháp luật",
            category_id=1855,
            landing_url="https://thanhnien.vn/thoi-su/phap-luat.htm",
        )

        landing_url = self.category.normalized_landing_url()
        timeline_page_1 = self.category.timeline_url(1)
        timeline_page_2 = self.category.timeline_url(2)

        self.responses = {
            landing_url: FakeResponse(
                landing_url,
                """
                <html>
                    <body>
                        <a href="/landing-story-185000000000000001.htm">Landing story</a>
                    </body>
                </html>
                """,
            ),
            timeline_page_1: FakeResponse(
                timeline_page_1,
                """
                <div class="box-category-item">
                    <a href="/timeline-article-185000000000000002.htm">Timeline article 1</a>
                </div>
                """,
            ),
            timeline_page_2: FakeResponse(
                timeline_page_2,
                """
                <div class="box-category-item">
                    <a data-io-canonical-url="/timeline-article-185000000000000003.htm"
                       href="/redirect">Timeline article 2</a>
                </div>
                <div class="box-category-item">
                    <a href="/landing-story-185000000000000001.htm">Duplicate landing story</a>
                </div>
                """,
            ),
        }

    def test_category_loader_emits_unique_articles_respecting_resume(self) -> None:
        existing = {"https://thanhnien.vn/landing-story-185000000000000001.htm"}
        loader = ThanhnienCategoryLoader(
            categories=[self.category],
            existing_urls=existing,
            resume=True,
            max_pages=2,
            request_timeout=1.0,
        )

        with patch("crawler.jobs.httpx.Client", return_value=FakeClient(self.responses)):
            jobs = list(loader)

        urls = [job.url for job in jobs]
        self.assertEqual(
            urls,
            [
                "https://thanhnien.vn/timeline-article-185000000000000002.htm",
                "https://thanhnien.vn/timeline-article-185000000000000003.htm",
            ],
        )
        self.assertEqual(loader.stats.emitted, 2)
        self.assertEqual(loader.stats.skipped_existing, 1)
        self.assertEqual(loader.stats.skipped_duplicate, 1)


if __name__ == "__main__":  # pragma: no cover - test runner entrypoint
    unittest.main()
