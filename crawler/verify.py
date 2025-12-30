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
        epilog="""
Examples:
  # Scan vtcnews images for ads
  python -m crawler.verify images --site vtcnews --db-url postgresql://...

  # Export to CSV without web viewer
  python -m crawler.verify images --site vtcnews --db-url postgresql://... --output suspicious.csv --no-web

  # Higher confidence threshold
  python -m crawler.verify images --site thanhnien --db-url postgresql://... --min-confidence 0.7
        """,
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
        help="Root path for storage directory (default: storage)",
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
        help="Export results to CSV or JSON file (determined by extension)",
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

        if total_images == 0:
            print("No images found for this site.")
            return

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

        # Show duplicate stats
        dup_stats = detector.get_duplicate_stats()
        if dup_stats:
            print(f"\nDuplicate images found: {len(dup_stats)}")
            top_dups = sorted(dup_stats.items(), key=lambda x: -x[1])[:5]
            for checksum, count in top_dups:
                print(f"  - {checksum[:16]}...: appears in {count} articles")

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
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    else:
        # Default to CSV
        with open(output_path, "w", newline="", encoding="utf-8") as f:
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
