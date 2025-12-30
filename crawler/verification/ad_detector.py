"""Advertisement image detection logic."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict
from collections import defaultdict
import hashlib
import re

from PIL import Image
from sqlalchemy.orm import Session

from models import Article, ArticleImage


@dataclass
class AdCheckResult:
    """Result of ad detection analysis for a single image."""
    is_suspicious: bool
    confidence: float  # 0.0 to 1.0
    reasons: List[str] = field(default_factory=list)
    width: Optional[int] = None
    height: Optional[int] = None


@dataclass
class SuspiciousImage:
    """A flagged suspicious image with context."""
    image_id: str
    article_id: str
    article_url: str
    article_title: str
    image_path: str
    source_url: Optional[str]
    check_result: AdCheckResult


# Known ad network domains
AD_DOMAIN_PATTERNS = [
    r"admicro\.vn",
    r"adtima\.vn",
    r"eclick\.vn",
    r"doubleclick\.net",
    r"googlesyndication\.com",
    r"googleadservices\.com",
    r"facebook\.com/tr",  # Facebook pixel
    r"analytics\.",
    r"tracking\.",
    r"pixel\.",
    r"ads\.",
    r"banner\.",
    r"sponsor\.",
    r"promo\.",
]

# Ad filename patterns
AD_FILENAME_PATTERNS = [
    r"banner",
    r"sponsor",
    r"ad[-_]",
    r"promo",
    r"tracking",
    r"pixel",
    r"widget",
    r"sidebar",
]

# Common ad banner dimensions (width x height)
AD_BANNER_DIMENSIONS = [
    (728, 90),   # Leaderboard
    (300, 250),  # Medium Rectangle
    (336, 280),  # Large Rectangle
    (300, 600),  # Half Page
    (320, 50),   # Mobile Leaderboard
    (320, 100),  # Large Mobile Banner
    (160, 600),  # Wide Skyscraper
    (120, 600),  # Skyscraper
    (468, 60),   # Full Banner
    (234, 60),   # Half Banner
    (88, 31),    # Micro Bar
    (120, 90),   # Button 1
    (120, 60),   # Button 2
    (1, 1),      # Tracking pixel
]


class AdImageDetector:
    """Detects advertisement images in crawled content."""

    def __init__(self, storage_root: Path):
        self.storage_root = Path(storage_root)
        self.ad_domain_regex = re.compile(
            "|".join(AD_DOMAIN_PATTERNS), re.IGNORECASE
        )
        self.ad_filename_regex = re.compile(
            "|".join(AD_FILENAME_PATTERNS), re.IGNORECASE
        )
        # Cache for image checksums (for duplicate detection)
        self._checksum_cache: Dict[str, List[str]] = defaultdict(list)

    def _get_image_dimensions(self, image_path: Path) -> tuple:
        """Get image width and height using Pillow."""
        try:
            with Image.open(image_path) as img:
                return img.size  # (width, height)
        except Exception:
            return None, None

    def _compute_checksum(self, image_path: Path) -> Optional[str]:
        """Compute SHA256 checksum of image file."""
        try:
            with open(image_path, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception:
            return None

    def _check_small_dimensions(self, width: int, height: int) -> tuple:
        """Check if image is suspiciously small (tracking pixel)."""
        if width <= 10 and height <= 10:
            return True, f"Tracking pixel ({width}x{height})"
        if width < 50 and height < 50:
            return True, f"Very small image ({width}x{height})"
        if width < 100 and height < 100:
            return True, f"Small image ({width}x{height})"
        return False, ""

    def _check_banner_dimensions(self, width: int, height: int) -> tuple:
        """Check if image matches common ad banner dimensions."""
        tolerance = 5  # Allow small variance
        for ad_w, ad_h in AD_BANNER_DIMENSIONS:
            if abs(width - ad_w) <= tolerance and abs(height - ad_h) <= tolerance:
                return True, f"Banner dimensions ({width}x{height} matches {ad_w}x{ad_h})"
        return False, ""

    def _check_ad_domain(self, source_url: Optional[str]) -> tuple:
        """Check if source URL is from known ad network."""
        if not source_url:
            return False, ""
        if self.ad_domain_regex.search(source_url):
            return True, "Ad network domain in URL"
        return False, ""

    def _check_ad_filename(self, source_url: Optional[str], image_path: str) -> tuple:
        """Check if filename matches ad patterns."""
        check_str = (source_url or "") + image_path
        if self.ad_filename_regex.search(check_str):
            return True, "Ad-related filename pattern"
        return False, ""

    def analyze_image(
        self,
        image_path: Path,
        source_url: Optional[str] = None,
        article_id: Optional[str] = None,
    ) -> AdCheckResult:
        """
        Analyze a single image for ad indicators.

        Returns AdCheckResult with confidence score and reasons.
        """
        reasons = []
        scores = []

        # Get dimensions
        width, height = self._get_image_dimensions(image_path)

        if width and height:
            # Check small dimensions (high confidence)
            is_small, reason = self._check_small_dimensions(width, height)
            if is_small:
                reasons.append(reason)
                scores.append(0.9 if width <= 10 else 0.7 if width < 50 else 0.5)

            # Check banner dimensions (medium confidence)
            is_banner, reason = self._check_banner_dimensions(width, height)
            if is_banner:
                reasons.append(reason)
                scores.append(0.6)

        # Check ad domain (high confidence)
        is_ad_domain, reason = self._check_ad_domain(source_url)
        if is_ad_domain:
            reasons.append(reason)
            scores.append(0.85)

        # Check filename patterns (medium confidence)
        is_ad_filename, reason = self._check_ad_filename(source_url, str(image_path))
        if is_ad_filename:
            reasons.append(reason)
            scores.append(0.5)

        # Compute checksum for duplicate detection
        checksum = self._compute_checksum(image_path)
        if checksum and article_id:
            self._checksum_cache[checksum].append(article_id)
            if len(self._checksum_cache[checksum]) > 3:
                reasons.append(f"Duplicate in {len(self._checksum_cache[checksum])} articles")
                scores.append(0.7)

        # Calculate overall confidence
        confidence = max(scores) if scores else 0.0
        is_suspicious = confidence >= 0.5

        return AdCheckResult(
            is_suspicious=is_suspicious,
            confidence=confidence,
            reasons=reasons,
            width=width,
            height=height,
        )

    def scan_site(
        self,
        site_slug: str,
        session: Session,
        min_confidence: float = 0.5,
    ) -> List[SuspiciousImage]:
        """
        Scan all images for a site and return suspicious ones.

        Args:
            site_slug: Site identifier (e.g., 'vtcnews', 'thanhnien')
            session: SQLAlchemy database session
            min_confidence: Minimum confidence threshold to flag

        Returns:
            List of SuspiciousImage objects
        """
        suspicious = []

        # Query articles with images for this site
        articles = (
            session.query(Article)
            .filter(Article.site_slug == site_slug)
            .all()
        )

        for article in articles:
            for img in article.images:
                image_path = self.storage_root / img.image_path

                if not image_path.exists():
                    continue

                result = self.analyze_image(
                    image_path=image_path,
                    source_url=getattr(img, 'source_url', None),
                    article_id=str(article.id),
                )

                if result.is_suspicious and result.confidence >= min_confidence:
                    suspicious.append(SuspiciousImage(
                        image_id=str(img.id),
                        article_id=str(article.id),
                        article_url=article.url,
                        article_title=article.title or "Untitled",
                        image_path=img.image_path,
                        source_url=getattr(img, 'source_url', None),
                        check_result=result,
                    ))

        return suspicious

    def get_duplicate_stats(self) -> Dict[str, int]:
        """Return statistics about duplicate images."""
        return {
            checksum: len(articles)
            for checksum, articles in self._checksum_cache.items()
            if len(articles) > 1
        }
