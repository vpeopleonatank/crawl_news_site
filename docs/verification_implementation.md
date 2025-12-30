# Crawl Verification Tool - Implementation Guide

## Overview

This document describes the implementation of a verification tool to detect advertisement images that were incorrectly crawled from news sites.

---

## Files to Create

### 1. `crawler/verification/__init__.py`

```python
"""Verification module for crawled content analysis."""

from .ad_detector import AdImageDetector, AdCheckResult, SuspiciousImage

__all__ = ["AdImageDetector", "AdCheckResult", "SuspiciousImage"]
```

---

### 2. `crawler/verification/ad_detector.py`

```python
"""Advertisement image detection logic."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Set
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
        self._checksum_cache: dict[str, List[str]] = defaultdict(list)

    def _get_image_dimensions(self, image_path: Path) -> tuple[Optional[int], Optional[int]]:
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

    def _check_small_dimensions(self, width: int, height: int) -> tuple[bool, str]:
        """Check if image is suspiciously small (tracking pixel)."""
        if width <= 10 and height <= 10:
            return True, f"Tracking pixel ({width}x{height})"
        if width < 50 and height < 50:
            return True, f"Very small image ({width}x{height})"
        if width < 100 and height < 100:
            return True, f"Small image ({width}x{height})"
        return False, ""

    def _check_banner_dimensions(self, width: int, height: int) -> tuple[bool, str]:
        """Check if image matches common ad banner dimensions."""
        tolerance = 5  # Allow small variance
        for ad_w, ad_h in AD_BANNER_DIMENSIONS:
            if abs(width - ad_w) <= tolerance and abs(height - ad_h) <= tolerance:
                return True, f"Banner dimensions ({width}x{height} matches {ad_w}x{ad_h})"
        return False, ""

    def _check_ad_domain(self, source_url: Optional[str]) -> tuple[bool, str]:
        """Check if source URL is from known ad network."""
        if not source_url:
            return False, ""
        if self.ad_domain_regex.search(source_url):
            return True, f"Ad network domain in URL"
        return False, ""

    def _check_ad_filename(self, source_url: Optional[str], image_path: str) -> tuple[bool, str]:
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

    def get_duplicate_stats(self) -> dict[str, int]:
        """Return statistics about duplicate images."""
        return {
            checksum: len(articles)
            for checksum, articles in self._checksum_cache.items()
            if len(articles) > 1
        }
```

---

### 3. `crawler/verification/web_viewer.py`

```python
"""Local web server for viewing suspicious images."""

import http.server
import json
import os
import socketserver
import threading
import webbrowser
from pathlib import Path
from typing import List
from urllib.parse import parse_qs, urlparse

from .ad_detector import SuspiciousImage


HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crawl Verification - Suspicious Images</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
        h1 { margin-bottom: 20px; color: #fff; }
        .stats { background: #16213e; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
        .stats span { margin-right: 30px; }
        .filters { margin-bottom: 20px; }
        .filters select, .filters input { padding: 8px; margin-right: 10px; border-radius: 4px; border: 1px solid #444; background: #16213e; color: #eee; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 20px; }
        .card { background: #16213e; border-radius: 8px; overflow: hidden; transition: transform 0.2s; }
        .card:hover { transform: translateY(-5px); }
        .card img { width: 100%; height: 150px; object-fit: contain; background: #0f0f23; }
        .card-body { padding: 12px; }
        .card-title { font-size: 12px; color: #aaa; margin-bottom: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .confidence { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; }
        .confidence.high { background: #e74c3c; }
        .confidence.medium { background: #f39c12; }
        .confidence.low { background: #27ae60; }
        .reasons { font-size: 11px; color: #888; margin-top: 8px; }
        .reasons li { margin-left: 15px; }
        .dimensions { font-size: 11px; color: #666; margin-top: 5px; }
        .export-btn { background: #3498db; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin-bottom: 20px; }
        .export-btn:hover { background: #2980b9; }
        a { color: #3498db; text-decoration: none; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>Suspicious Images Report</h1>

    <div class="stats">
        <span>Total Images: <strong id="total">0</strong></span>
        <span>High Confidence: <strong id="high">0</strong></span>
        <span>Medium: <strong id="medium">0</strong></span>
    </div>

    <div class="filters">
        <select id="confidence-filter">
            <option value="all">All Confidence Levels</option>
            <option value="high">High (>= 0.8)</option>
            <option value="medium">Medium (0.5 - 0.8)</option>
        </select>
        <button class="export-btn" onclick="exportCSV()">Export to CSV</button>
    </div>

    <div class="grid" id="image-grid"></div>

    <script>
        const images = __IMAGES_DATA__;

        function getConfidenceClass(conf) {
            if (conf >= 0.8) return 'high';
            if (conf >= 0.5) return 'medium';
            return 'low';
        }

        function renderImages(filter = 'all') {
            const grid = document.getElementById('image-grid');
            grid.innerHTML = '';

            let filtered = images;
            if (filter === 'high') filtered = images.filter(i => i.confidence >= 0.8);
            else if (filter === 'medium') filtered = images.filter(i => i.confidence >= 0.5 && i.confidence < 0.8);

            filtered.forEach(img => {
                const card = document.createElement('div');
                card.className = 'card';
                card.innerHTML = `
                    <img src="/image/${encodeURIComponent(img.image_path)}" alt="Suspicious image" loading="lazy">
                    <div class="card-body">
                        <div class="card-title" title="${img.article_title}">${img.article_title}</div>
                        <span class="confidence ${getConfidenceClass(img.confidence)}">${(img.confidence * 100).toFixed(0)}% suspicious</span>
                        <div class="dimensions">${img.width || '?'}x${img.height || '?'}</div>
                        <ul class="reasons">${img.reasons.map(r => `<li>${r}</li>`).join('')}</ul>
                        <div style="margin-top: 8px; font-size: 11px;">
                            <a href="${img.article_url}" target="_blank">View Article</a>
                        </div>
                    </div>
                `;
                grid.appendChild(card);
            });

            document.getElementById('total').textContent = filtered.length;
            document.getElementById('high').textContent = images.filter(i => i.confidence >= 0.8).length;
            document.getElementById('medium').textContent = images.filter(i => i.confidence >= 0.5 && i.confidence < 0.8).length;
        }

        document.getElementById('confidence-filter').addEventListener('change', (e) => {
            renderImages(e.target.value);
        });

        function exportCSV() {
            const headers = ['article_url', 'article_title', 'image_path', 'confidence', 'reasons', 'dimensions'];
            const rows = images.map(i => [
                i.article_url,
                i.article_title,
                i.image_path,
                i.confidence,
                i.reasons.join('; '),
                `${i.width}x${i.height}`
            ]);

            const csv = [headers, ...rows].map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\\n');
            const blob = new Blob([csv], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'suspicious_images.csv';
            a.click();
        }

        renderImages();
    </script>
</body>
</html>'''


class VerificationHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler for verification viewer."""

    images_data: List[dict] = []
    storage_root: Path = Path(".")

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()

            # Inject images data into template
            html = HTML_TEMPLATE.replace(
                "__IMAGES_DATA__",
                json.dumps(self.images_data)
            )
            self.wfile.write(html.encode())

        elif parsed.path.startswith("/image/"):
            # Serve image file
            image_path = self.storage_root / parsed.path[7:]  # Remove /image/
            if image_path.exists():
                self.send_response(200)
                ext = image_path.suffix.lower()
                content_type = {
                    '.jpg': 'image/jpeg',
                    '.jpeg': 'image/jpeg',
                    '.png': 'image/png',
                    '.gif': 'image/gif',
                    '.webp': 'image/webp',
                }.get(ext, 'application/octet-stream')
                self.send_header("Content-Type", content_type)
                self.end_headers()
                with open(image_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404)
        else:
            self.send_error(404)

    def log_message(self, format, *args):
        pass  # Suppress default logging


def start_viewer(
    suspicious_images: List[SuspiciousImage],
    storage_root: Path,
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    """
    Start local web server to view suspicious images.

    Args:
        suspicious_images: List of flagged images to display
        storage_root: Root path for image storage
        port: HTTP server port
        open_browser: Whether to open browser automatically
    """
    # Convert to JSON-serializable format
    images_data = [
        {
            "image_id": img.image_id,
            "article_id": img.article_id,
            "article_url": img.article_url,
            "article_title": img.article_title,
            "image_path": img.image_path,
            "source_url": img.source_url,
            "confidence": img.check_result.confidence,
            "reasons": img.check_result.reasons,
            "width": img.check_result.width,
            "height": img.check_result.height,
        }
        for img in suspicious_images
    ]

    # Sort by confidence (highest first)
    images_data.sort(key=lambda x: x["confidence"], reverse=True)

    # Configure handler
    VerificationHandler.images_data = images_data
    VerificationHandler.storage_root = storage_root

    # Start server
    with socketserver.TCPServer(("", port), VerificationHandler) as httpd:
        url = f"http://localhost:{port}"
        print(f"\nOpening web viewer at {url}")
        print("Press Ctrl+C to stop server\n")

        if open_browser:
            webbrowser.open(url)

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
```

---

### 4. `crawler/verify.py`

```python
"""CLI tool for verifying crawled content."""

import argparse
import csv
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Article, ArticleImage
from crawler.verification import AdImageDetector
from crawler.verification.web_viewer import start_viewer


def main():
    parser = argparse.ArgumentParser(
        description="Verify crawled news site content",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Images command
    images_parser = subparsers.add_parser(
        "images",
        help="Scan images for advertisements",
    )
    images_parser.add_argument(
        "--site",
        required=True,
        help="Site slug to scan (e.g., vtcnews, thanhnien)",
    )
    images_parser.add_argument(
        "--storage-root",
        type=Path,
        default=Path("storage"),
        help="Root path for storage directory",
    )
    images_parser.add_argument(
        "--db-url",
        required=True,
        help="Database connection URL",
    )
    images_parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.5,
        help="Minimum confidence threshold to flag (default: 0.5)",
    )
    images_parser.add_argument(
        "--output",
        type=Path,
        help="Export results to CSV/JSON file",
    )
    images_parser.add_argument(
        "--no-web",
        action="store_true",
        help="Skip opening web viewer",
    )
    images_parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port for web viewer (default: 8765)",
    )

    args = parser.parse_args()

    if args.command == "images":
        run_image_verification(args)
    else:
        parser.print_help()
        sys.exit(1)


def run_image_verification(args):
    """Run image verification for a site."""
    print(f"Scanning {args.site} images...")

    # Connect to database
    engine = create_engine(args.db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Count total images
        total_articles = (
            session.query(Article)
            .filter(Article.site_slug == args.site)
            .count()
        )
        total_images = (
            session.query(ArticleImage)
            .join(Article)
            .filter(Article.site_slug == args.site)
            .count()
        )

        print(f"Found {total_images:,} images across {total_articles:,} articles\n")

        # Run detection
        detector = AdImageDetector(args.storage_root)
        suspicious = detector.scan_site(
            site_slug=args.site,
            session=session,
            min_confidence=args.min_confidence,
        )

        # Print summary
        print(f"Suspicious images detected: {len(suspicious)}")

        # Group by reason type
        reason_counts = {}
        for img in suspicious:
            for reason in img.check_result.reasons:
                key = reason.split("(")[0].strip()
                reason_counts[key] = reason_counts.get(key, 0) + 1

        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"  - {reason}: {count}")

        # Export if requested
        if args.output:
            export_results(suspicious, args.output)
            print(f"\nResults exported to {args.output}")

        # Launch web viewer
        if not args.no_web and suspicious:
            start_viewer(
                suspicious_images=suspicious,
                storage_root=args.storage_root,
                port=args.port,
            )
        elif not suspicious:
            print("\nNo suspicious images found!")

    finally:
        session.close()


def export_results(suspicious, output_path: Path):
    """Export results to CSV or JSON."""
    if output_path.suffix == ".json":
        data = [
            {
                "article_url": img.article_url,
                "article_title": img.article_title,
                "image_path": img.image_path,
                "confidence": img.check_result.confidence,
                "reasons": img.check_result.reasons,
                "width": img.check_result.width,
                "height": img.check_result.height,
            }
            for img in suspicious
        ]
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    else:
        # Default to CSV
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "article_url", "article_title", "image_path",
                "confidence", "reasons", "width", "height"
            ])
            for img in suspicious:
                writer.writerow([
                    img.article_url,
                    img.article_title,
                    img.image_path,
                    img.check_result.confidence,
                    "; ".join(img.check_result.reasons),
                    img.check_result.width,
                    img.check_result.height,
                ])


if __name__ == "__main__":
    main()
```

---

## Dependencies

Add to `requirements.txt`:

```
Pillow>=10.0.0
```

---

## Usage

```bash
# Scan vtcnews images for ads
python -m crawler.verify images \
    --site vtcnews \
    --storage-root /app/storage \
    --db-url postgresql://crawl_user:crawl_password@pgbouncer:6432/crawl_db

# Export to CSV without web viewer
python -m crawler.verify images \
    --site vtcnews \
    --storage-root /app/storage \
    --db-url postgresql://... \
    --output suspicious.csv \
    --no-web

# Higher confidence threshold
python -m crawler.verify images \
    --site thanhnien \
    --storage-root /app/storage \
    --db-url postgresql://... \
    --min-confidence 0.7
```

---

## Web Viewer Features

- **Image grid** with thumbnails
- **Confidence badges** (red = high, yellow = medium)
- **Filter by confidence level**
- **Image dimensions displayed**
- **Reasons for flagging shown**
- **Link to original article**
- **Export to CSV** button

---

## Detection Heuristics

| Check | Confidence | Description |
|-------|------------|-------------|
| Tracking pixel (1x1 to 10x10) | 0.9 | Very small images used for tracking |
| Very small (< 50x50) | 0.7 | Likely icons, badges, or tracking |
| Small (< 100x100) | 0.5 | Potentially irrelevant |
| Ad network domain | 0.85 | URL contains known ad network |
| Banner dimensions | 0.6 | Matches IAB standard ad sizes |
| Ad filename pattern | 0.5 | Contains "banner", "ad-", etc. |
| Duplicate across articles | 0.7 | Same image in 3+ articles |
