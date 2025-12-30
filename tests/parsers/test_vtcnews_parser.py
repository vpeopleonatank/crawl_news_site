import unittest
from datetime import datetime, timezone, timedelta

from crawler.parsers import AssetType, ParsingError
from crawler.parsers.vtcnews import VtcnewsParser


class VtcnewsParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = VtcnewsParser()

    def test_parse_article_with_ldjson(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="OG title should not win"/>
            <meta name="description" content="Meta description"/>
            <meta property="article:published_time" content="2025-12-27T21:26:50+07:00"/>
            <meta property="article:section" content="Thời sự"/>
            <meta name="keywords" content="VTC News, Việt Nam"/>
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "NewsArticle",
                "headline": "Bài viết thử nghiệm",
                "description": "Mô tả từ ld+json",
                "articleSection": "Thời sự",
                "datePublished": "2025-12-27T21:26:50+07:00",
                "keywords": "VTC, Tin tức",
                "image": ["https://cdn.vtcnews.vn/sample-1.jpg"]
              }
            </script>
          </head>
          <body>
            <article>
              <h1>Bài viết thử nghiệm</h1>
              <p>Đoạn 1.</p>
              <p>Đoạn 2.</p>
              <figure>
                <img src="/sample-2.jpg"/>
                <figcaption>Chú thích ảnh</figcaption>
              </figure>
            </article>
          </body>
        </html>
        """

        result = self.parser.parse("https://vtcnews.vn/bai-viet.html", html)

        self.assertEqual(result.title, "Bài viết thử nghiệm")
        self.assertEqual(result.description, "Meta description")
        self.assertEqual(result.category_name, "Thời sự")
        self.assertEqual(result.category_id, "thời-sự")
        self.assertEqual(
            result.publish_date,
            datetime(2025, 12, 27, 21, 26, 50, tzinfo=timezone(timedelta(hours=7))),
        )
        self.assertEqual(result.content, "Đoạn 1.\n\nĐoạn 2.")
        self.assertIn("VTC", result.tags)
        self.assertIn("Tin tức", result.tags)

        self.assertEqual(len(result.assets), 2)
        self.assertEqual(result.assets[0].asset_type, AssetType.IMAGE)
        self.assertEqual(result.assets[0].source_url, "https://cdn.vtcnews.vn/sample-1.jpg")
        self.assertEqual(result.assets[1].source_url, "https://vtcnews.vn/sample-2.jpg")
        self.assertEqual(result.assets[1].caption, "Chú thích ảnh")

    def test_extracts_content_from_itemprop_article_body(self) -> None:
        html = """
        <html>
          <head>
            <script type="application/ld+json">
              { "@context": "https://schema.org", "@type": "NewsArticle", "headline": "Tựa đề" }
            </script>
          </head>
          <body>
            <div itemprop="articleBody">
              <p>Nội dung 1</p>
              <p>Nội dung 2</p>
            </div>
          </body>
        </html>
        """

        result = self.parser.parse("https://vtcnews.vn/article.html", html)
        self.assertEqual(result.content, "Nội dung 1\n\nNội dung 2")

    def test_missing_title_raises(self) -> None:
        with self.assertRaises(ParsingError):
            self.parser.parse("https://vtcnews.vn/no-title.html", "<html><body><p>n/a</p></body></html>")


if __name__ == "__main__":
    unittest.main()
