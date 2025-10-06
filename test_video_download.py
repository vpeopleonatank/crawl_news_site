from crawler.assets import AssetManager
from crawler.config import IngestConfig
from crawler.parsers import ParsedAsset, AssetType
from pathlib import Path


config = IngestConfig(storage_root=Path("/tmp/tn_assets"))
manager = AssetManager(config)

asset = ParsedAsset(
 source_url="https://thanhnien.mediacdn.vn/325084952045817856/2025/10/3/1-1759489185419194083592.mp4",
 asset_type=AssetType.VIDEO,
 sequence=1,
)

stored = manager.download_assets("quyen-linh-demo", [asset])
print(stored[0].path)
