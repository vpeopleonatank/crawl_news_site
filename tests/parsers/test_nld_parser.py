import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crawler.parsers import AssetType, ParsingError
from crawler.parsers.nld import NldParser

LEGACY_FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "nld_sample.html"
MODERN_FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent / "fixtures" / "nld_modern_sample.html"
)
VIDEO_FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent / "fixtures" / "nld_video_article.html"
)


class NldParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = NldParser()
        self.legacy_html = LEGACY_FIXTURE_PATH.read_text(encoding="utf-8")
        self.modern_html = MODERN_FIXTURE_PATH.read_text(encoding="utf-8")
        self.video_html = VIDEO_FIXTURE_PATH.read_text(encoding="utf-8")

    def test_parse_valid_article(self) -> None:
        result = self.parser.parse(
            "https://nld.com.vn/thoi-su/sample-article-202410050915.htm",
            self.legacy_html,
        )

        self.assertEqual(result.title, "Sample NLD Article")
        self.assertEqual(result.description, "Short NLD summary.")

        expected_content = "Paragraph 1.\n\nParagraph 2."
        self.assertEqual(result.content, expected_content)

        self.assertEqual(result.category_name, "Chính trị")
        self.assertEqual(result.category_id, "chinh-tri")

        expected_datetime = datetime(2024, 10, 5, 9, 15, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)

        self.assertEqual(result.tags, ["Chính trị", "Đối ngoại"])
        self.assertIsNone(result.comments)

        self.assertEqual(len(result.assets), 4)

        image_asset = result.assets[0]
        self.assertEqual(image_asset.asset_type, AssetType.IMAGE)
        self.assertEqual(image_asset.source_url, "https://nld.com.vn/images/sample.jpg")
        self.assertEqual(image_asset.sequence, 1)
        self.assertEqual(image_asset.caption, "Figure caption")

        inline_image = result.assets[1]
        self.assertEqual(inline_image.asset_type, AssetType.IMAGE)
        self.assertEqual(inline_image.source_url, "https://cdn.nld.com.vn/photo/2024/10/05/extra.jpg")
        self.assertEqual(inline_image.sequence, 2)
        self.assertIsNone(inline_image.caption)

        video_asset = result.assets[2]
        self.assertEqual(video_asset.asset_type, AssetType.VIDEO)
        self.assertEqual(video_asset.source_url, "https://nld.com.vn/videos/sample.mp4")
        self.assertEqual(video_asset.sequence, 3)
        self.assertIsNone(video_asset.caption)

        iframe_asset = result.assets[3]
        self.assertEqual(iframe_asset.asset_type, AssetType.VIDEO)
        self.assertEqual(iframe_asset.source_url, "https://player.nld.com.vn/embed/sample")
        self.assertEqual(iframe_asset.sequence, 4)
        self.assertIsNone(iframe_asset.caption)

    def test_parse_modern_layout(self) -> None:
        result = self.parser.parse(
            "https://nld.com.vn/phap-luat/sample-modern-202509261045.htm",
            self.modern_html,
        )

        self.assertEqual(result.title, "Modern NLD Article")
        self.assertEqual(result.description, "Updated layout summary.")
        expected_content = "Modern paragraph one.\n\nQuoted paragraph from the article."
        self.assertEqual(result.content, expected_content)
        self.assertEqual(result.category_name, "Pháp luật")
        self.assertEqual(result.category_id, "phap-luat")
        expected_datetime = datetime(2025, 9, 26, 10, 45, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)
        self.assertEqual(result.tags, ["Pháp luật", "Điện lực"])
        self.assertIsNone(result.comments)

        self.assertEqual(len(result.assets), 4)

        hero_image = result.assets[0]
        self.assertEqual(hero_image.asset_type, AssetType.IMAGE)
        self.assertEqual(hero_image.source_url, "https://nld.mediacdn.vn/modern/hero.jpg")
        self.assertEqual(hero_image.caption, "Hero caption")

        inline_image = result.assets[1]
        self.assertEqual(inline_image.source_url, "https://nld.mediacdn.vn/modern/inline.jpg")

        video_asset = result.assets[2]
        self.assertEqual(video_asset.source_url, "https://nld.mediacdn.vn/videos/clip.mp4")

        iframe_asset = result.assets[3]
        self.assertEqual(iframe_asset.source_url, "https://player.nld.com.vn/embed/modern")

    def test_parse_video_article(self) -> None:
        result = self.parser.parse(
            "https://nld.com.vn/thoi-su/video-article-20250102.htm",
            self.video_html,
        )

        self.assertEqual(result.title, "Video Article Title")
        self.assertEqual(result.description, "Video article summary.")
        self.assertEqual(result.content, "Video article summary.")
        self.assertEqual(result.category_name, "Thời sự")
        self.assertEqual(result.category_id, "thoi-su")

        expected_datetime = datetime(2025, 1, 2, 10, 1, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)
        self.assertEqual(result.tags, ["Thời sự", "Video"])

        self.assertEqual(len(result.assets), 2)

        image_asset = result.assets[0]
        self.assertEqual(image_asset.asset_type, AssetType.IMAGE)
        self.assertEqual(
            image_asset.source_url, "https://nld.mediacdn.vn/thumb_w/750/video-poster.jpg"
        )

        video_asset = result.assets[1]
        self.assertEqual(video_asset.asset_type, AssetType.VIDEO)
        self.assertEqual(
            video_asset.source_url, "https://nld.mediacdn.vn/videos/video-source.mp4"
        )

    def test_missing_body_raises(self) -> None:
        html = "<html><body><h1>Title</h1></body></html>"
        with self.assertRaises(ParsingError):
            self.parser.parse("https://nld.com.vn/no-body-20241005.htm", html)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
