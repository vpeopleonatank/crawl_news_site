import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crawler.parsers import AssetType, ParsingError
from crawler.parsers.thanhnien import ThanhnienParser

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "thanhnien_sample.html"


class ThanhnienParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ThanhnienParser()
        self.html = FIXTURE_PATH.read_text(encoding="utf-8")

    def test_parse_valid_article(self) -> None:
        result = self.parser.parse("https://thanhnien.vn/bai-viet/bao-so-5.htm", self.html)

        self.assertEqual(result.title, "Bão số 5 đổ bộ miền Trung")
        self.assertEqual(result.description, "Gió giật cấp 12 khiến nhiều nhà dân tốc mái")

        expected_content = (
            "Do ảnh hưởng của bão số 5, khu vực miền Trung có mưa rất to.\n\n"
            "Các lực lượng chức năng đã được huy động để hỗ trợ người dân sơ tán.\n\n"
            "Hàng ngàn ngôi nhà bị ảnh hưởng, trong đó có nhiều mái nhà bị tốc."
        )
        self.assertEqual(result.content, expected_content)

        self.assertEqual(result.category_name, "Thời sự")
        self.assertEqual(result.category_id, "thời-sự")

        expected_datetime = datetime(2024, 10, 5, 8, 30, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)

        self.assertEqual(result.tags, ["Bão", "Miền Trung", "Sơ tán"])
        self.assertIsNone(result.comments)

        self.assertEqual(len(result.assets), 2)

        image_asset = result.assets[0]
        self.assertEqual(image_asset.asset_type, AssetType.IMAGE)
        self.assertEqual(image_asset.source_url, "https://cdn.example.com/photos/bao-so-5.jpg")
        self.assertEqual(image_asset.sequence, 1)
        self.assertEqual(image_asset.caption, "Lực lượng cứu hộ di dời người dân.")

        video_asset = result.assets[1]
        self.assertEqual(video_asset.asset_type, AssetType.VIDEO)
        self.assertEqual(video_asset.source_url, "https://cdn.example.com/videos/bao-so-5.mp4")
        self.assertEqual(video_asset.sequence, 2)
        self.assertIsNone(video_asset.caption)

    def test_missing_title_raises(self) -> None:
        html = "<html><body><div class='detail__content'><p>Test</p></div></body></html>"
        with self.assertRaises(ParsingError):
            self.parser.parse("https://thanhnien.vn/bai-viet/khong-title.htm", html)


if __name__ == "__main__":
    unittest.main()
