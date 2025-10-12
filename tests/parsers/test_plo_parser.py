import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crawler.parsers import AssetType, ParsingError
from crawler.parsers.plo import PloParser

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures"


class PloParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = PloParser()

    def test_parse_phap_luat_article(self) -> None:
        html = (FIXTURE_DIR / "plo_phap_luat.html").read_text(encoding="utf-8")
        url = "https://plo.vn/vu-vay-ngan-hang-mua-biet-thu-vks-khang-nghi-theo-huong-huy-an-so-tham-post874715.html"

        result = self.parser.parse(url, html)

        self.assertEqual(
            result.title,
            "Vụ vay ngân hàng mua biệt thự: VKS kháng nghị theo hướng hủy án sơ thẩm",
        )
        self.assertEqual(
            result.description,
            "(PLO)- VKSND khu vực 7 - TP.HCM đã kháng nghị tòa bộ bản án sơ thẩm vụ vay ngân hàng mua biệt thự và đề nghị TAND TP.HCM xét xử phúc thẩm theo hướng hủy bản án sơ thẩm.",
        )

        first_paragraph = result.content.split("\n\n")[0]
        self.assertTrue(first_paragraph.startswith("Ngày 9-10, VKSND khu vực 7 - TP.HCM"))

        self.assertEqual(result.category_id, "phap-luat")
        self.assertEqual(result.category_name, "Pháp luật")

        expected_datetime = datetime(2025, 10, 9, 17, 55, 47, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(result.publish_date, expected_datetime)

        self.assertEqual(
            result.tags,
            ["VKSND khu vực 7 - TP.HCM", "vay ngân hàng mua biệt thự", "hủy án sơ thẩm"],
        )

        self.assertIsNone(result.comments)
        self.assertEqual(len(result.assets), 2)

        image_asset = result.assets[0]
        self.assertEqual(image_asset.asset_type, AssetType.IMAGE)
        self.assertEqual(
            image_asset.source_url,
            "https://image.plo.vn/w1000/Uploaded/2025/wpdhnwzdh/2025_10_09/vay-tien-mua-biet-thu-3533-5585.png.webp",
        )
        self.assertEqual(image_asset.sequence, 1)
        self.assertEqual(
            image_asset.caption,
            "HĐXX sơ thẩm tuyên án vụ kiện giữa ngân hàng và vợ chồng ông THS. Ảnh: TP",
        )

    def test_parse_chinh_tri_article(self) -> None:
        html = (FIXTURE_DIR / "plo_chinh_tri.html").read_text(encoding="utf-8")
        url = "https://plo.vn/doan-dai-bieu-tphcm-dang-hoa-tuong-niem-cac-anh-hung-liet-si-tai-binh-duong-ba-ria-vung-tau-cu-post874982.html"

        result = self.parser.parse(url, html)

        self.assertEqual(result.category_id, "chinh-tri")
        self.assertEqual(result.category_name, "Chính trị")
        self.assertGreaterEqual(len(result.content), 1000)
        self.assertGreater(len(result.assets), 5)
        self.assertIn("anh hùng liệt sĩ", result.tags[0].lower())

    def test_missing_body_raises(self) -> None:
        html = "<html><body><h1>Title</h1></body></html>"
        with self.assertRaises(ParsingError):
            self.parser.parse("https://plo.vn/sample-post.html", html)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
