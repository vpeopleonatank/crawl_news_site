"""Local web server for viewing suspicious images."""

import http.server
import json
import socketserver
import webbrowser
from pathlib import Path
from typing import List
from urllib.parse import urlparse, unquote

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
        .no-images { text-align: center; padding: 50px; color: #888; }
    </style>
</head>
<body>
    <h1>Suspicious Images Report</h1>

    <div class="stats">
        <span>Total Flagged: <strong id="total">0</strong></span>
        <span>High Confidence: <strong id="high">0</strong></span>
        <span>Medium: <strong id="medium">0</strong></span>
    </div>

    <div class="filters">
        <select id="confidence-filter">
            <option value="all">All Confidence Levels</option>
            <option value="high">High (&gt;= 0.8)</option>
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

        function renderImages(filter) {
            filter = filter || 'all';
            const grid = document.getElementById('image-grid');
            grid.innerHTML = '';

            let filtered = images;
            if (filter === 'high') filtered = images.filter(function(i) { return i.confidence >= 0.8; });
            else if (filter === 'medium') filtered = images.filter(function(i) { return i.confidence >= 0.5 && i.confidence < 0.8; });

            if (filtered.length === 0) {
                grid.innerHTML = '<div class="no-images">No images match the current filter</div>';
            }

            filtered.forEach(function(img) {
                const card = document.createElement('div');
                card.className = 'card';
                const reasonsHtml = img.reasons.map(function(r) { return '<li>' + r + '</li>'; }).join('');
                card.innerHTML =
                    '<img src="/image/' + encodeURIComponent(img.image_path) + '" alt="Suspicious image" loading="lazy" onerror="this.src=\'data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 width=%22100%22 height=%22100%22><text x=%2250%%22 y=%2250%%22 text-anchor=%22middle%22 fill=%22%23888%22>Not Found</text></svg>\'">' +
                    '<div class="card-body">' +
                        '<div class="card-title" title="' + img.article_title + '">' + img.article_title + '</div>' +
                        '<span class="confidence ' + getConfidenceClass(img.confidence) + '">' + (img.confidence * 100).toFixed(0) + '% suspicious</span>' +
                        '<div class="dimensions">' + (img.width || '?') + 'x' + (img.height || '?') + '</div>' +
                        '<ul class="reasons">' + reasonsHtml + '</ul>' +
                        '<div style="margin-top: 8px; font-size: 11px;">' +
                            '<a href="' + img.article_url + '" target="_blank">View Article</a>' +
                        '</div>' +
                    '</div>';
                grid.appendChild(card);
            });

            document.getElementById('total').textContent = filtered.length;
            document.getElementById('high').textContent = images.filter(function(i) { return i.confidence >= 0.8; }).length;
            document.getElementById('medium').textContent = images.filter(function(i) { return i.confidence >= 0.5 && i.confidence < 0.8; }).length;
        }

        document.getElementById('confidence-filter').addEventListener('change', function(e) {
            renderImages(e.target.value);
        });

        function exportCSV() {
            const headers = ['article_url', 'article_title', 'image_path', 'confidence', 'reasons', 'dimensions'];
            const rows = images.map(function(i) {
                return [
                    i.article_url,
                    i.article_title,
                    i.image_path,
                    i.confidence,
                    i.reasons.join('; '),
                    i.width + 'x' + i.height
                ];
            });

            const csvContent = [headers].concat(rows).map(function(r) {
                return r.map(function(c) {
                    return '"' + String(c).replace(/"/g, '""') + '"';
                }).join(',');
            }).join('\\n');

            const blob = new Blob([csvContent], { type: 'text/csv' });
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
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()

            # Inject images data into template
            html = HTML_TEMPLATE.replace(
                "__IMAGES_DATA__",
                json.dumps(self.images_data, ensure_ascii=False)
            )
            self.wfile.write(html.encode('utf-8'))

        elif parsed.path.startswith("/image/"):
            # Serve image file
            image_rel_path = unquote(parsed.path[7:])  # Remove /image/
            image_path = self.storage_root / image_rel_path

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
                self.send_header("Cache-Control", "max-age=3600")
                self.end_headers()
                with open(image_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, f"Image not found: {image_rel_path}")
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
    VerificationHandler.storage_root = Path(storage_root)

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
