# Znews Category Pagination Analysis

## Overview
Category pages on Znews support pagination but have a **hard limit** where pages beyond the maximum simply repeat the last valid page's content.

## Test Case: Pháp luật (Law) Category
**URL**: `https://lifestyle.znews.vn/phap-luat.html`

### Key Findings

1. **Pagination Pattern**: `/phap-luat/trang{N}.html` where N is the page number
2. **Maximum Valid Page**: Page 50 (as of October 9, 2025)
3. **Invalid Pages Behavior**: Pages 51+ return identical content to page 50
4. **No Pagination Links**: Pages near the limit (48-50) have no pagination navigation

### Date Range Coverage

| Page | Date Range | Notes |
|------|------------|-------|
| Page 1 | Oct 9, 2025 | Newest articles (current) |
| Page 50 | May 25-26, 2025 | Oldest available (~4.5 months back) |
| Page 51+ | May 25-26, 2025 | **Duplicate of page 50** |

### Pagination Behavior

```
Page 1:  [Newest] Oct 9, 2025  ✓ Unique content
Page 2:            ...          ✓ Unique content
...
Page 48:           May 27, 2025 ✓ Unique content
Page 49:           May 26, 2025 ✓ Unique content
Page 50:           May 25, 2025 ✓ Unique content (LAST VALID PAGE)
Page 51: [Oldest] May 25, 2025 ⚠️ DUPLICATE of page 50
Page 52+:          May 25, 2025 ⚠️ DUPLICATE of page 50
```

## Implications for Crawling

### ✅ What Works
- **Recent articles**: Pages 1-50 provide ~4.5 months of coverage
- **Forward pagination**: Start at page 1, continue until duplicate detection
- **Backward pagination**: Start at page 50, work back to page 1

### ⚠️ Limitations
- **Limited history**: Only ~4.5 months of articles available via category pages
- **No older content**: Cannot access articles beyond May 2025 via category pagination
- **No clear end marker**: The site doesn't return 404 or indicate the last page; it just repeats content

### 🎯 Detection Strategy

To detect when you've hit the pagination limit:

```python
def is_duplicate_page(articles_current, articles_previous):
    """Compare article lists to detect pagination limit"""
    if not articles_previous:
        return False

    # Compare first 3 article titles/URLs
    return articles_current[:3] == articles_previous[:3]

# Usage
previous_articles = None
for page_num in range(1, 100):  # Arbitrary high limit
    articles = fetch_page(page_num)

    if is_duplicate_page(articles, previous_articles):
        print(f"Reached pagination limit at page {page_num}")
        break

    previous_articles = articles
```

## Recommended Approach

For **comprehensive historical coverage**, use the **sitemap-based loader** instead:
- Sitemap indices provide access to all published articles
- No 4.5-month limitation
- More reliable for backfilling

For **category-specific crawling** (e.g., only "Pháp luật" articles):
- Use category pagination for recent articles (last 4-5 months)
- Supplement with sitemap filtering by URL pattern
- Implement duplicate detection to stop at pagination limit

## Implementation in the Ingestion Pipeline

The CLI now exposes first-class support for category pagination via the `ZnewsCategoryLoader`:

- `--znews-use-categories`: Switch from sitemap ingestion to category pagination.
- `--znews-categories`: Optional comma-separated list of slugs (e.g. `phap-luat,doi-song`). When omitted, a curated default set is used.
- `--znews-all-categories`: Crawl every category defined in `data/znews_categories.json` (falls back to built-ins when the file is absent).
- `--znews-max-pages`: Cap the number of pages to traverse per category (set `0` to disable).

Example:

```bash
python -m crawler.ingest \
  --site znews \
  --db-url postgresql://crawl_user:crawl_password@postgres:5432/crawl_db \
  --storage-root /app/storage \
  --znews-use-categories \
  --znews-categories phap-luat,doi-song \
  --znews-max-pages 40
```

### Loader Behavior

- Landing page is treated as page 1; subsequent pages follow the `/trang{N}.html` pattern.
- Article URLs are deduplicated globally (across pages and categories) and respect `--resume`.
- Pagination stops automatically when the first three URLs of a page repeat those from the previous page (mirrors the duplicate detection described above).
- Catalog overrides can be supplied at `data/znews_categories.json` with entries like:

```json
[
  {"slug": "phap-luat", "name": "Pháp luật", "landing_url": "https://lifestyle.znews.vn/phap-luat.html"}
]
```

## Testing Commands

```bash
# Test pagination structure
python3 << 'EOF'
import httpx
from bs4 import BeautifulSoup

urls = [
    "https://lifestyle.znews.vn/phap-luat/trang50.html",
    "https://lifestyle.znews.vn/phap-luat/trang51.html"
]

for url in urls:
    response = httpx.get(url, timeout=10.0)
    soup = BeautifulSoup(response.text, 'html.parser')
    articles = soup.select('article h3 a')
    print(f"{url}: {len(articles)} articles")
    print(f"First: {articles[0].get_text(strip=True) if articles else 'None'}")
EOF
```

## Conclusion

**Yes**, you can fetch articles from the start of a category using pagination, but:
- Coverage is limited to ~4-5 months of recent articles
- Maximum ~50 pages per category
- Must implement duplicate detection to avoid infinite loops
- For full historical coverage, use sitemap-based ingestion instead
