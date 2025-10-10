import unittest
from unittest.mock import patch

import httpx

from crawler.config import ProxyConfig
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
            self.category.timeline_url(3): FakeResponse(
                self.category.timeline_url(3),
                """
                <div class="box-category-item">
                    <a href="/timeline-article-185000000000000004.htm">Timeline article 3</a>
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

    def test_category_loader_continues_when_empty_guard_disabled(self) -> None:
        category = self.category
        timeline_page_1 = category.timeline_url(1)
        timeline_page_2 = category.timeline_url(2)
        timeline_page_3 = category.timeline_url(3)
        timeline_page_4 = category.timeline_url(4)

        responses = {
            timeline_page_1: FakeResponse(
                timeline_page_1,
                """
                <div class="box-category-item">
                    <a href="/timeline-article-185000000000000010.htm">Timeline article 10</a>
                </div>
                """,
            ),
            timeline_page_2: FakeResponse(
                timeline_page_2,
                """
                <div class="box-category-item">
                    <a href="/timeline-article-185000000000000010.htm">Duplicate article</a>
                </div>
                """,
            ),
            timeline_page_3: FakeResponse(
                timeline_page_3,
                """
                <div class="box-category-item">
                    <a href="/timeline-article-185000000000000010.htm">Duplicate article</a>
                </div>
                """,
            ),
            timeline_page_4: FakeResponse(
                timeline_page_4,
                """
                <div class="box-category-item">
                    <a href="/timeline-article-185000000000000011.htm">Timeline article 11</a>
                </div>
                """,
            ),
        }

        loader = ThanhnienCategoryLoader(
            categories=[category],
            max_pages=4,
            max_empty_pages=None,
            request_timeout=1.0,
            include_landing_page=False,
        )

        with patch("crawler.jobs.httpx.Client", return_value=FakeClient(responses)):
            jobs = list(loader)

        urls = [job.url for job in jobs]
        self.assertEqual(
            urls,
            [
                "https://thanhnien.vn/timeline-article-185000000000000010.htm",
                "https://thanhnien.vn/timeline-article-185000000000000011.htm",
            ],
        )

    def test_category_loader_passes_proxy_to_httpx_client(self) -> None:
        proxy = ProxyConfig.from_endpoint("127.0.0.1:9191")
        loader = ThanhnienCategoryLoader(
            categories=[self.category],
            max_pages=1,
            request_timeout=1.0,
            proxy=proxy,
        )

        with patch("crawler.jobs.httpx.Client") as client_cls:
            client_instance = client_cls.return_value.__enter__.return_value
            client_instance.get.side_effect = httpx.HTTPError("boom")
            list(loader)

        kwargs = client_cls.call_args.kwargs
        self.assertEqual(kwargs.get("proxy"), proxy.httpx_proxy())


if __name__ == "__main__":  # pragma: no cover - test runner entrypoint
    unittest.main()
