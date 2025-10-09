import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crawler.parsers import AssetType, ParsingError
from crawler.parsers.znews import ZnewsParser

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures"
STANDARD_FIXTURE = FIXTURE_DIR / "znews_standard.html"
VIDEO_FIXTURE = FIXTURE_DIR / "znews_video.html"


class ZnewsParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ZnewsParser()

    def test_parse_standard_article(self) -> None:
        html = STANDARD_FIXTURE.read_text(encoding="utf-8")

        result = self.parser.parse(
            "https://lifestyle.znews.vn/phat-hanh-du-an-tien-ao-post1591568.html",
            html,
        )

        self.assertEqual(
            result.title,
            "Phát hành dự án tiền ảo, chiếm đoạt tiền của hơn 3.000 nhà đầu tư",
        )
        self.assertEqual(
            result.description,
            "Công an TP Hà Nội cho biết lực lượng chức năng triệt phá nhóm phát hành tiền ảo.",
        )

        expected_content = (
            "Từ năm 2022, các đối tượng đã huy động tới 7,86 triệu USD tương đương nhiều tỷ đồng từ nhà đầu tư.\n\n"
            "Phòng An ninh mạng Công an TP Hà Nội phát hiện đường dây quảng bá dự án tiền ảo trái phép.\n\n"
            "Các nghi phạm tổ chức sự kiện quảng bá, cam kết lợi nhuận cao cho nhà đầu tư.\n\n"
            "Cơ quan điều tra đã khuyến cáo người dân cảnh giác trước lời mời."
        )
        self.assertEqual(result.content, expected_content)

        self.assertEqual(result.category_name, "Pháp luật")
        self.assertEqual(result.category_id, "phap-luat")

        expected_datetime = datetime(2025, 10, 9, 12, 35, 44, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)
        self.assertEqual(result.tags, ["Hà Nội", "Tiền mã hóa"])

        self.assertEqual(len(result.assets), 1)
        asset = result.assets[0]
        self.assertEqual(asset.asset_type, AssetType.IMAGE)
        self.assertEqual(asset.sequence, 1)
        self.assertEqual(asset.source_url, "https://photo.znews.vn/Uploaded/sample_image.jpg")
        self.assertEqual(asset.caption, "Ảnh minh họa do cơ quan chức năng cung cấp.")

    def test_parse_video_article(self) -> None:
        html = VIDEO_FIXTURE.read_text(encoding="utf-8")

        result = self.parser.parse(
            "https://lifestyle.znews.vn/video-mua-lon-post1591943.html",
            html,
        )

        self.assertEqual(result.title, "Mưa lớn ở TP.HCM, người dân chật vật lội nước về nhà")
        self.assertEqual(
            result.description,
            "13h chiều 8/10, TP.HCM xuất hiện mưa dông, nhiều tuyến phố ngập sâu.",
        )
        self.assertEqual(
            result.content,
            "13h chiều 8/10, TP.HCM xuất hiện mưa dông, nhiều tuyến phố ngập sâu.",
        )

        self.assertEqual(result.category_name, "Đời sống")
        self.assertEqual(result.category_id, "video-doi-song")

        expected_datetime = datetime(2025, 10, 8, 18, 37, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)
        self.assertEqual(result.tags, ["TP.HCM", "Mưa lớn"])

        self.assertEqual(len(result.assets), 1)
        asset = result.assets[0]
        self.assertEqual(asset.asset_type, AssetType.VIDEO)
        self.assertEqual(asset.sequence, 1)
        self.assertEqual(
            asset.source_url,
            "https://streaming.znews.vn/video/2025/10/08/sample/index.m3u8",
        )
        self.assertIsNone(asset.caption)

    def test_missing_title_raises(self) -> None:
        html = "<html><body><div class='the-article-body'><p>Nội dung</p></div></body></html>"
        with self.assertRaises(ParsingError):
            self.parser.parse("https://znews.vn/khong-title.html", html)


if __name__ == "__main__":
    unittest.main()
