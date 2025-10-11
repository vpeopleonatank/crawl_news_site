import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crawler.parsers import AssetType, ParsingError
from crawler.parsers.kenh14 import Kenh14Parser

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "kenh14_sample.html"


class Kenh14ParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = Kenh14Parser()
        self.html = FIXTURE_PATH.read_text(encoding="utf-8")

    def test_parse_valid_article(self) -> None:
        result = self.parser.parse("https://kenh14.vn/sample-article-202410081530.chn", self.html)

        self.assertEqual(result.title, "Sample Kenh14 Article")
        self.assertEqual(result.description, "Short summary of the article.")

        expected_content = "Paragraph 1.\n\nParagraph 2."
        self.assertEqual(result.content, expected_content)

        self.assertEqual(result.category_name, "Pháp luật")
        self.assertEqual(result.category_id, "phap-luat")

        expected_datetime = datetime(2024, 10, 8, 15, 30, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)

        self.assertEqual(result.tags, ["An ninh", "Tội phạm"])
        self.assertIsNone(result.comments)

        self.assertEqual(len(result.assets), 2)

        image_asset = result.assets[0]
        self.assertEqual(image_asset.asset_type, AssetType.IMAGE)
        self.assertEqual(image_asset.source_url, "https://media.kenh14.vn/photo/2024/10/08/sample.jpg")
        self.assertEqual(image_asset.sequence, 1)
        self.assertEqual(image_asset.caption, "Sample caption")

        video_asset = result.assets[1]
        self.assertEqual(video_asset.asset_type, AssetType.VIDEO)
        self.assertEqual(video_asset.source_url, "https://media.kenh14.vn/video/sample.mp4")
        self.assertEqual(video_asset.sequence, 2)
        self.assertIsNone(video_asset.caption)

    def test_missing_body_raises(self) -> None:
        html = "<html><body><h1>Title</h1></body></html>"
        with self.assertRaises(ParsingError):
            self.parser.parse("https://kenh14.vn/no-body.chn", html)


if __name__ == "__main__":
    unittest.main()
