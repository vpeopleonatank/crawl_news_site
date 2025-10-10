# Thanhnien Category Pagination Discovery

## Overview
Thanhnien.vn category pages support timeline-based pagination that allows fetching articles from the beginning of a category.

## URL Pattern

### Category Page
```
https://thanhnien.vn/chinh-tri.htm
```

### Timeline Pagination
```
https://thanhnien.vn/timelinelist/{category_id}/{limit}.htm
```

**Parameters:**
- `category_id`: Numeric category identifier (e.g., `185227` for Chính trị/Politics)
- `limit`: Number of articles to fetch per page (tested with `100`)

## Example

### Politics Category
- **Main page**: `https://thanhnien.vn/chinh-tri.htm`
- **Timeline URL**: `https://thanhnien.vn/timelinelist/185227/100.htm`

This timeline URL returns older articles from the category, allowing you to paginate backwards through the entire category archive.

## How to Extract Category ID

The category ID appears to be embedded in the page structure. From inspecting the Chính trị category page, the ID `185227` is used in the timeline system.

### Method 1: Inspect Page Source
Look for timeline-related JavaScript or data attributes that reference the category ID.

### Method 2: Network Analysis
Monitor XHR/Fetch requests when scrolling or clicking "Load More" buttons on the category page.

## Article Structure

Each article in the timeline includes:
- Thumbnail image
- Category label
- Article title (h3)
- Brief description/teaser
- Article URL
- Related article link

## Pagination Strategy

To crawl a category from the start:

1. **Discover category ID** from the main category page
2. **Fetch timeline pages** using `/timelinelist/{category_id}/{limit}.htm`
3. **Extract article URLs** from each page
4. **Determine next page** by examining the last article's timestamp or ID
5. **Continue fetching** until reaching recent articles or detecting overlap

### Stopping Conditions

The production loader applies two safeties while paginating:

- `max_pages` caps the absolute number of timeline requests per category (default `10`, disable with `--thanhnien-max-pages 0`).
- `max_empty_pages` stops once *N* consecutive pages fail to emit new URLs (default `2`, disable with `--thanhnien-max-empty-pages 0`).

“Empty” in this context means “no unseen article URLs were yielded,” so resume mode can trigger the guard even when the HTML still lists stories you ingested earlier. To crawl deeper history, disable the guard or run without `--resume` (or clear the corresponding records) so older pages produce fresh jobs.

## Category-Based URL Collection

The preferred implementation path is a dedicated `ThanhnienCategoryLoader` that walks one or more category timelines and emits `ArticleJob` objects for ingestion.

### Loader Responsibilities
- Load the list of available Thanhnien categories (see the next section).
- Accept a user-supplied subset of categories while defaulting to “all known” when none are provided.
- For each selected category:
  1. Yield the article URLs from the category’s landing page to capture the most recent posts.
  2. Page through the timeline endpoint (`/timelinelist/{category_id}/{limit}.htm`) until a stopping condition is reached (resume mode, duplicate detection, or depth limit).
  3. Populate `ArticleJob(url=..., lastmod=...)` so downstream components can dedupe and persist.
- Respect the global ingestion settings (resume mode, request throttling, etc.) that are already enforced by `crawler.ingest`.

### Category Catalog and Selection

Maintain a structured list of categories the loader understands. A simple JSON file such as `data/thanhnien_categories.json` keeps the mapping explicit:

```json
[
  {
    "slug": "chinh-tri",
    "name": "Chính trị",
    "category_id": 185227,
    "landing_url": "https://thanhnien.vn/chinh-tri.htm"
  },
  {
    "slug": "the-gioi",
    "name": "Thế giới",
    "category_id": 185246,
    "landing_url": "https://thanhnien.vn/the-gioi.htm"
  }
]
```

- **Slug**: Stable identifier exposed via CLI (lowercase, hyphenated).
- **Category ID**: Numeric value required by the timeline endpoint.
- **Landing URL**: Optional first page fetch for the latest stories.

Nested paths should be flattened into a single slug (e.g., `https://thanhnien.vn/thoi-su/phap-luat.htm` → `thoi-su-phap-luat`). The corresponding timeline endpoint becomes `https://thanhnien.vn/timelinelist/1855/{limit}.htm`.

The ingestion CLI should expose two complementary arguments:
- `--thanhnien-categories` (comma-separated list of slugs) — opt-in selection.
- `--thanhnien-all-categories` (flag) — override defaults and crawl every known category.

When neither flag is present, ingest a default subset (for example, high-priority desk such as politics, world, society, business). Resume mode continues to dedupe URLs across categories.

### Implementation Outline

```python
class ThanhnienCategoryLoader(JobLoader):
    def __init__(self, categories: Sequence[CategoryDefinition], *, resume: bool, existing_urls: set[str], limit: int = 100):
        ...

    def __iter__(self) -> Iterator[ArticleJob]:
        for category in self._categories:
            yield from self._emit_from_category(category)
```

- `_emit_from_category` downloads the main category page (optional), parses article links, then calls `_paginate_timeline`.
- `_paginate_timeline` iterates `/timelinelist/{category.category_id}/{limit}.htm`, yielding URLs until:
  - no new URLs appear,
  - resume mode detects previously ingested URLs,
  - or a configurable depth limit is reached.
- Track per-category `JobLoaderStats` to inform the user which categories contributed URLs.

## Integration Notes

- Replace the NDJSON job loader wiring for Thanhnien in `crawler/sites.py` with the category loader, keeping NDJSON as a fallback for bespoke jobs.
- The CLI parser in `crawler/ingest.py` should translate `--thanhnien-categories` into a list of slugs, resolve them via the category catalog, and instantiate the loader.
- Stats emitted by the loader should be surfaced at the end of an ingestion run (e.g., “Politics: 180 new, 22 skipped existing”).

### URL Structure in Timeline
Articles maintain their standard URL format:
```
https://thanhnien.vn/{slug}-{article_id}.htm
```

Example:
```
https://thanhnien.vn/bo-truong-gtvt-nguyen-van-thang-lam-bo-truong-tai-chinh-185241128173912665.htm
```

## Category Catalog Status

| Slug | Vietnamese Name | Category ID | Landing URL |
|------|-----------------|-------------|-------------|
| chinh-tri | Chính trị | 185227 | https://thanhnien.vn/chinh-tri.htm |
| thoi-su | Thời sự | TBD | https://thanhnien.vn/thoi-su.htm |
| thoi-su-phap-luat* | Thời sự › Pháp luật | 1855 | https://thanhnien.vn/thoi-su/phap-luat.htm |
| the-gioi* | Thế giới | 185246 | https://thanhnien.vn/the-gioi.htm |
| TBD | ... | ... | ... |

\*Verify before shipping. IDs beyond Chính trị were observed during manual browsing and should be confirmed with fresh captures.

## Next Steps

1. **Populate the category catalog** with verified IDs for priority desks:
   - Thời sự (News)
   - Thế giới (World)
   - Kinh tế (Economy)
   - Đời sống (Lifestyle)
   - Sức khỏe (Health)
   - Giới trẻ (Youth)
   - Giáo dục (Education)
   - Du lịch (Travel)
   - Văn hóa (Culture)
   - Giải trí (Entertainment)
   - Thể thao (Sports)
   - Công nghệ (Technology)
   - Xe (Automotive)

2. **Implement `ThanhnienCategoryLoader`** in `crawler/jobs.py`, aligning with the outline above.

3. **Add CLI support** for category-based ingestion using slugs:
   ```bash
   python -m crawler.ingest \
     --site thanhnien \
     --thanhnien-categories chinh-tri,the-gioi \
     --storage-root /app/storage
   ```

4. **Test pagination depth** per category to understand:
   - Total articles available per category
   - Historical coverage (how far back articles go)
   - Pagination limits or cutoffs

## Example Output from Timeline Page

The timeline page shows articles ordered chronologically (newest first within the batch), with each article containing:
- Image URL
- Title
- Category tag
- Brief description
- Link to full article
- Related article suggestions

Articles are displayed in a clean list format without the category page's hero section or sub-navigation.
