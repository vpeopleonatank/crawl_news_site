#!/usr/bin/env python3
import argparse
import os
from typing import Callable, Dict, Iterable

from sqlalchemy import create_engine


TABLE_QUERIES: Dict[str, Callable[[str | None], str]] = {
    "articles": lambda site: (
        "SELECT * FROM articles"
        if site is None
        else "SELECT * FROM articles WHERE site_slug = %s"
    ),
    "article_images": lambda site: (
        "SELECT ai.* FROM article_images ai"
        if site is None
        else "SELECT ai.* FROM article_images ai "
        "JOIN articles a ON a.id = ai.article_id WHERE a.site_slug = %s"
    ),
    "article_videos": lambda site: (
        "SELECT av.* FROM article_videos av"
        if site is None
        else "SELECT av.* FROM article_videos av "
        "JOIN articles a ON a.id = av.article_id WHERE a.site_slug = %s"
    ),
    "pending_video_assets": lambda site: (
        "SELECT * FROM pending_video_assets"
        if site is None
        else "SELECT * FROM pending_video_assets WHERE site_slug = %s"
    ),
    "failed_media_downloads": lambda site: (
        "SELECT * FROM failed_media_downloads"
        if site is None
        else "SELECT * FROM failed_media_downloads WHERE site_slug = %s"
    ),
}


def export_table(
    connection, table: str, site_slug: str | None, output_path: str
) -> None:
    query = TABLE_QUERIES[table](site_slug)
    copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    with connection.cursor() as cursor, open(output_path, "w", encoding="utf-8") as file:
        if site_slug is None:
            cursor.copy_expert(copy_sql, file)
        else:
            cursor.copy_expert(cursor.mogrify(copy_sql, (site_slug,)).decode("utf-8"), file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export article-related tables to CSV, optionally filtered by site_slug."
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="Database URL, e.g. postgresql://user:pass@host:port/db",
    )
    parser.add_argument(
        "--site-slug",
        help="Filter rows by site_slug. Applies to articles and related tables.",
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Directory to write CSV files (default: exports)",
    )
    parser.add_argument(
        "--tables",
        default="articles,article_images,article_videos,pending_video_assets,failed_media_downloads",
        help="Comma-separated list of tables to export.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = [t for t in tables if t not in TABLE_QUERIES]
    if unknown:
        raise SystemExit(f"Unknown tables: {', '.join(unknown)}")

    os.makedirs(args.output_dir, exist_ok=True)

    engine = create_engine(args.db_url)
    with engine.raw_connection() as raw_connection:
        for table in tables:
            output_path = os.path.join(args.output_dir, f"{table}.csv")
            export_table(raw_connection, table, args.site_slug, output_path)
            print(f"Wrote {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
