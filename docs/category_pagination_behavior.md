# Category Pagination Behavior & Stop Signals

This document explains how each site's category loader handles pagination, including automatic stop signals and configuration options.

## Quick Reference

### Default Pagination Limits

| Site | Default `max_pages` | Will crawl to start by default? |
|------|-------------------|--------------------------------|
| **Thanhnien** | 10 | ❌ No (stops after 10 pages) |
| **Znews** | 50 | ❌ No (stops after 50 pages) |
| **Kenh14** | 600 | ⚠️  Possibly (600 pages is large) |
| **PLO** | None (unlimited) | ✅ Yes (until API exhausted) |
| **Nld** | None (unlimited) | ✅ Yes (until archive exhausted) |

### The `--{site}-max-pages 0` Problem

**⚠️ IMPORTANT**: Passing `--{site}-max-pages 0` does **NOT** mean unlimited for any site!

```bash
# ❌ This will NOT crawl unlimited - it stops immediately!
--thanhnien-max-pages 0  # Exits after landing page only
--znews-max-pages 0      # Exits after page 1 only
--kenh14-max-pages 0     # Exits after landing page only
--plo-max-pages 0        # Exits after landing page only
--nld-max-pages 0        # Exits after landing page only
```

**Why?** The code checks: `if page > max_pages: break`. When `max_pages=0` and `page=1`, the condition `1 > 0` is `True`, causing immediate exit.

**For unlimited crawling:**
- **PLO & Nld**: Omit the flag entirely (already unlimited by default)
- **Thanhnien, Znews, Kenh14**: Pass a very large number like `--thanhnien-max-pages 99999`

---

## Stop Signals by Site

### 1. Thanhnien (`ThanhnienCategoryLoader`)

**Location**: `crawler/jobs.py:714-755`

#### Stop Signals (Priority Order)

**A. HTTP Failure** (Line 732-733)
```python
html = self._fetch_html(client, timeline_url)
if not html:  # HTTP error, timeout, or empty response
    break  # ← IMMEDIATE STOP
```
- Triggers on: HTTP 4xx/5xx, network timeout, empty response body
- No retry mechanism

**B. Empty Page Guard** (Line 740-743)
```python
if not emitted_on_page:  # Zero articles after deduplication
    consecutive_empty_pages += 1
    if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
        break  # ← STOPS AFTER N CONSECUTIVE EMPTY PAGES
```
- **Default**: 2 consecutive empty pages
- Configure via `--thanhnien-max-empty-pages N`
- Pass `0` to disable this guard entirely
- "Empty" means: zero articles after deduplication, or all articles already exist in DB (when using `--resume`)

**C. Max Pages Limit** (Line 727-728)
```python
if self._max_pages is not None and page > self._max_pages:
    break  # ← STOPS AFTER MAX_PAGES
```
- **Default**: 10 pages
- Configure via `--thanhnien-max-pages N`
- Omit flag to use default (NOT unlimited)

#### Configuration Examples

```bash
# Default behavior (stops at 10 pages or 2 consecutive empty pages)
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri

# Unlimited pages (stops only on empty pages or HTTP errors)
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri \
  --thanhnien-max-pages 99999

# Very aggressive (stops on first empty page)
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri \
  --thanhnien-max-empty-pages 1

# Never stop on empty pages (stops only on HTTP error or max_pages)
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri \
  --thanhnien-max-pages 99999 \
  --thanhnien-max-empty-pages 0
```

---

### 2. Znews (`ZnewsCategoryLoader`)

**Location**: `crawler/jobs.py:1293-1346`

#### Stop Signals (Priority Order)

**A. HTTP Failure** (Line 1307-1308)
```python
html = self._fetch_html(client, page_url)
if not html:
    break  # ← IMMEDIATE STOP
```

**B. Hardcoded Empty Page Guard** (Line 1313-1314) ⚠️
```python
if consecutive_empty_pages >= 2:
    break  # ← HARDCODED! Not configurable via CLI
```
- **Hardcoded**: Always stops after 2 consecutive empty pages
- **Cannot be changed** via command-line flags
- Exists in addition to any max_pages limit

**C. Duplicate Pagination Detection** (Line 1320-1331) ⭐
```python
fingerprint = urls[: self._duplicate_fingerprint_size]  # First 3 URLs
if (self._stop_on_duplicate
    and previous_fingerprint is not None
    and fingerprint == previous_fingerprint):
    LOGGER.info("Znews category '%s': detected duplicate pagination at %s; stopping.", ...)
    break  # ← STOPS ON CIRCULAR PAGINATION
```
- Compares first **3 URLs** from consecutive pages
- If pages N and N+1 have identical first 3 URLs → pagination is looping → **stops**
- Prevents infinite loops on sites with circular pagination
- **Enabled by default**, cannot be disabled via CLI
- Only detects consecutive duplicates (not circular patterns across many pages)

**D. Max Pages Limit** (Line 1302-1303)
```python
if self._max_pages is not None and page > self._max_pages:
    break
```
- **Default**: 50 pages
- Configure via `--znews-max-pages N`

#### Configuration Examples

```bash
# Default behavior (stops at 50 pages, or 2 empty pages, or duplicate pagination)
docker compose run --rm test_app python -m crawler.ingest \
  --site znews \
  --znews-use-categories \
  --znews-categories phap-luat

# Unlimited pages (still stops on: 2 empty pages OR duplicate pagination)
docker compose run --rm test_app python -m crawler.ingest \
  --site znews \
  --znews-use-categories \
  --znews-categories phap-luat \
  --znews-max-pages 99999
```

**⚠️ Important**: Even with `--znews-max-pages 99999`, Znews will **always stop** after 2 consecutive empty pages (hardcoded). This cannot be disabled.

---

### 3. Kenh14 (`Kenh14CategoryLoader`)

**Location**: `crawler/jobs.py:852-897`

#### Stop Signals (Priority Order)

**A. HTTP Failure** (Line 870-875) - **Lenient Behavior**
```python
html = self._fetch_payload(client, timeline_url)
if not html:
    consecutive_empty_pages += 1
    if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
        break
    page += 1
    continue  # ← Does NOT stop immediately! Counts as "empty page"
```
- **Unlike other loaders**, HTTP failures don't stop immediately
- Failed requests count toward `consecutive_empty_pages`
- Allows crawler to tolerate temporary network issues

**B. Empty Page Guard** (Line 882-885)
```python
if not emitted_on_page:
    consecutive_empty_pages += 1
    if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
        break
```
- **Default**: 3 consecutive empty/failed pages
- Configure via `--kenh14-max-empty-pages N`
- More tolerant than other loaders (3 vs. 1-2)

**C. Max Pages Limit** (Line 865-866)
```python
if self._max_pages is not None and page > self._max_pages:
    break
```
- **Default**: 600 pages
- Configure via `--kenh14-max-pages N`
- High default suggests deep archive

#### Configuration Examples

```bash
# Default behavior (stops at 600 pages or 3 consecutive empty/failed pages)
docker compose run --rm test_app python -m crawler.ingest \
  --site kenh14 \
  --kenh14-categories phap-luat

# Unlimited pages with default tolerance
docker compose run --rm test_app python -m crawler.ingest \
  --site kenh14 \
  --kenh14-categories phap-luat \
  --kenh14-max-pages 99999

# Very strict (stops on first failed/empty page)
docker compose run --rm test_app python -m crawler.ingest \
  --site kenh14 \
  --kenh14-categories phap-luat \
  --kenh14-max-empty-pages 1

# Very tolerant (tolerates 10 failed/empty pages)
docker compose run --rm test_app python -m crawler.ingest \
  --site kenh14 \
  --kenh14-categories phap-luat \
  --kenh14-max-pages 99999 \
  --kenh14-max-empty-pages 10
```

---

### 4. PLO (`PloCategoryLoader`)

**Location**: `crawler/jobs.py:1043-1086`

#### Stop Signals (Priority Order)

**A. API Failure** (Line 1062-1063)
```python
contents = self._fetch_api_contents(client, api_url)
if contents is None:  # HTTP error, invalid JSON, or missing data section
    break  # ← IMMEDIATE STOP
```
- Triggers on: HTTP error, JSON parse error, or malformed API response
- No retry mechanism
- More strict than HTML-based loaders

**B. Empty Page Guard** (Line 1070-1073)
```python
if not emitted_on_page:
    consecutive_empty_pages += 1
    if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
        break
```
- **Default**: 2 consecutive empty pages
- Configure via `--plo-max-empty-pages N`
- "Empty" includes: valid API response with zero articles

**C. Max Pages Limit** (Line 1057-1058)
```python
if self._max_pages is not None and page > self._max_pages:
    break
```
- **Default**: None (unlimited)
- Configure via `--plo-max-pages N` to add a limit
- **Recommended to leave unlimited** since PLO's API-based pagination is reliable

#### Configuration Examples

```bash
# Default behavior (unlimited pages, stops on API failure or 2 consecutive empty)
docker compose run --rm test_app python -m crawler.ingest \
  --site plo \
  --plo-categories phap-luat,chinh-tri

# Add a safety limit (stops at 500 pages)
docker compose run --rm test_app python -m crawler.ingest \
  --site plo \
  --plo-categories phap-luat \
  --plo-max-pages 500

# Very strict (stops on first empty page)
docker compose run --rm test_app python -m crawler.ingest \
  --site plo \
  --plo-categories phap-luat \
  --plo-max-empty-pages 1

# Very tolerant (tolerates 10 empty pages before stopping)
docker compose run --rm test_app python -m crawler.ingest \
  --site plo \
  --plo-categories phap-luat \
  --plo-max-empty-pages 10
```

---

### 5. Nld (`NldCategoryLoader`)

**Location**: `crawler/jobs.py:155-218`

#### Stop Signals (Priority Order)

**A. HTTP Failure** (Line 175-180) - **Lenient Behavior**
```python
html = self._fetch_html(client, timeline_url)
if not html:
    consecutive_empty_pages += 1
    if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
        break
    page += 1
    continue  # ← Does NOT stop immediately! Counts as "empty page"
```
- Similar to Kenh14, HTTP failures don't stop immediately
- Failed requests count toward `consecutive_empty_pages`

**B. Empty Page Guard** (Line 184-186) - **Most Strict**
```python
if not urls:
    consecutive_empty_pages += 1
    if self._max_empty_pages is not None and consecutive_empty_pages >= self._max_empty_pages:
        break
```
- **Default**: 1 consecutive empty page
- Configure via `--nld-max-empty-pages N`
- **Most aggressive** of all loaders
- "Empty" means: zero URLs extracted from HTML (before deduplication)

**C. Duplicate Pagination Detection** (Line 192-203) ⭐
```python
fingerprint = urls[: self._duplicate_fingerprint_size]  # First 5 URLs
if (self._stop_on_duplicate
    and previous_fingerprint is not None
    and fingerprint == previous_fingerprint):
    LOGGER.info("Nld category '%s': detected duplicate pagination at %s; stopping.", ...)
    break  # ← STOPS ON CIRCULAR PAGINATION
```
- Compares first **5 URLs** from consecutive pages (vs. Znews's 3)
- More sensitive to pagination loops than Znews
- **Enabled by default**, cannot be disabled via CLI
- `duplicate_fingerprint_size=5` is hardcoded, not configurable

**D. Max Pages Limit** (Line 170-171)
```python
if self._max_pages is not None and page > self._max_pages:
    break
```
- **Default**: None (unlimited)
- Configure via `--nld-max-pages N` to add a limit

#### Configuration Examples

```bash
# Default behavior (unlimited pages, stops on: duplicate pagination OR 1 empty page)
docker compose run --rm test_app python -m crawler.ingest \
  --site nld \
  --nld-categories phap-luat,chinh-tri

# Add a safety limit
docker compose run --rm test_app python -m crawler.ingest \
  --site nld \
  --nld-categories phap-luat \
  --nld-max-pages 1000

# More tolerant (allow 5 consecutive empty pages)
docker compose run --rm test_app python -m crawler.ingest \
  --site nld \
  --nld-categories phap-luat \
  --nld-max-empty-pages 5

# Never stop on empty pages (only duplicate detection or max_pages)
# ⚠️ Use with caution - may crawl very far into empty archive
docker compose run --rm test_app python -m crawler.ingest \
  --site nld \
  --nld-categories phap-luat \
  --nld-max-pages 99999 \
  --nld-max-empty-pages 0
```

---

## Comparison Table

### Stop Signal Summary

| Site | HTTP Failure | Empty Page Default | Duplicate Detection | Max Pages Default | Landing Page |
|------|-------------|-------------------|--------------------|--------------------|--------------|
| **Thanhnien** | Immediate stop | 2 consecutive | ❌ No | 10 | Yes (included) |
| **Znews** | Immediate stop | 2 consecutive (hardcoded) | ✅ Yes (3-URL fingerprint) | 50 | Yes (page 1) |
| **Kenh14** | Continue, count as empty | 3 consecutive | ❌ No | 600 | Yes (included) |
| **PLO** | Immediate stop (API) | 2 consecutive | ❌ No | None (unlimited) | No (API-only) |
| **Nld** | Continue, count as empty | 1 consecutive | ✅ Yes (5-URL fingerprint) | None (unlimited) |

### Configuration Flags

| Site | Max Pages Flag | Empty Pages Flag | Default Unlimited? |
|------|---------------|-----------------|-------------------|
| **Thanhnien** | `--thanhnien-max-pages N` | `--thanhnien-max-empty-pages N` | ❌ No (default: 10) |
| **Znews** | `--znews-max-pages N` | N/A (hardcoded to 2) | ❌ No (default: 50) |
| **Kenh14** | `--kenh14-max-pages N` | `--kenh14-max-empty-pages N` | ❌ No (default: 600) |
| **PLO** | `--plo-max-pages N` | `--plo-max-empty-pages N` | ✅ Yes (default: None) |
| **Nld** | `--nld-max-pages N` | `--nld-max-empty-pages N` | ✅ Yes (default: None) |

---

## Best Practices

### For Complete Archive Backfills

Goal: Crawl all available historical content from a category.

**PLO & Nld** (Already unlimited by default):
```bash
docker compose run --rm test_app python -m crawler.ingest \
  --site plo \
  --plo-categories phap-luat,chinh-tri \
  --resume  # Skip already-crawled URLs

docker compose run --rm test_app python -m crawler.ingest \
  --site nld \
  --nld-categories phap-luat,chinh-tri \
  --nld-max-empty-pages 3 \  # More tolerant than default
  --resume
```

**Thanhnien, Znews, Kenh14** (Need explicit large limit):
```bash
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri,thoi-su-phap-luat \
  --thanhnien-max-pages 99999 \
  --thanhnien-max-empty-pages 5 \  # Tolerate gaps in archive
  --resume

docker compose run --rm test_app python -m crawler.ingest \
  --site znews \
  --znews-use-categories \
  --znews-categories phap-luat \
  --znews-max-pages 99999 \
  --resume
  # Note: Still stops after 2 empty pages (hardcoded)

docker compose run --rm test_app python -m crawler.ingest \
  --site kenh14 \
  --kenh14-categories phap-luat \
  --kenh14-max-pages 99999 \
  --kenh14-max-empty-pages 5 \
  --resume
```

### For Recent Content Only

Goal: Crawl only the most recent N pages (e.g., daily updates).

```bash
# Crawl only first 5 pages of each category
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri \
  --thanhnien-max-pages 5

# Crawl until you hit 50 new articles (stops early if DB already has recent content)
docker compose run --rm test_app python -m crawler.ingest \
  --site znews \
  --znews-use-categories \
  --znews-categories phap-luat \
  --znews-max-pages 10 \
  --resume  # Stops early when all articles already exist
```

### For Testing/Development

Goal: Quickly verify parser works without crawling entire archive.

```bash
# Test parser with just landing page + first timeline page
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-categories chinh-tri \
  --thanhnien-max-pages 1 \
  --max-workers 1

# Test until first empty page (validates pagination works)
docker compose run --rm test_app python -m crawler.ingest \
  --site kenh14 \
  --kenh14-categories phap-luat \
  --kenh14-max-empty-pages 1
```

---

## Common Gotchas

### 1. `--{site}-max-pages 0` Does NOT Mean Unlimited

**Problem**: Passing `0` causes immediate exit after landing page.

**Why**: Code checks `if page > max_pages`, so `1 > 0` is `True`.

**Solution**:
- For PLO/Nld: Omit the flag (already unlimited)
- For others: Pass large number like `99999`

### 2. Resume Mode Affects Empty Page Detection

**Problem**: With `--resume`, pages full of already-crawled articles count as "empty."

**Example**:
```bash
# First run: crawls 100 pages successfully
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-max-pages 99999

# Second run: stops after 2 pages (all articles already exist)
docker compose run --rm test_app python -m crawler.ingest \
  --site thanhnien \
  --thanhnien-max-pages 99999 \
  --resume  # ← Causes early stop
```

**Solution**: This is expected behavior. The crawler correctly stops when it reaches content already in the database.

### 3. Znews Hardcoded Empty Page Limit

**Problem**: Cannot crawl Znews past 2 consecutive empty pages, even with high limits.

**Why**: Line 1313 has hardcoded `if consecutive_empty_pages >= 2: break`.

**Workaround**: None via CLI. Would require code modification to change.

### 4. Duplicate Detection May Stop Too Early

**Problem**: Znews/Nld stop when pagination "loops" (same URLs appear twice in a row).

**Example**: Site returns same page twice due to bug → crawler stops, thinking it hit end.

**Workaround**:
- Not configurable via CLI currently
- Would need to modify `stop_on_duplicate` parameter in code
- Or increase `duplicate_fingerprint_size` to be more tolerant

### 5. Different "Empty" Definitions Per Site

**Thanhnien/Kenh14/PLO**: "Empty" = zero articles after deduplication
- If page has 20 URLs but all are duplicates → counts as empty

**Nld**: "Empty" = zero URLs extracted from HTML (before deduplication)
- If page has 20 URLs that are all duplicates → **NOT** empty
- Only triggers if HTML parsing finds zero `<a>` tags

**Znews**: "Empty" = zero URLs extracted (like Nld)

### 6. HTTP Errors Behave Differently

**Immediate stop** (Thanhnien, Znews, PLO):
- Single HTTP error → crawl terminates immediately
- No tolerance for transient network issues

**Lenient** (Kenh14, Nld):
- HTTP errors count toward empty page counter
- Crawl continues if within tolerance threshold
- Better for unreliable networks

---

## Troubleshooting

### Crawl Stops Too Early

**Symptom**: Crawler stops after only a few pages despite large `--max-pages`.

**Diagnosis**:
1. Check logs for which stop signal triggered:
   ```
   INFO Thanhnien category 'chinh-tri': emitted=50 skipped_existing=200 skipped_duplicate=30
   ```

2. Common causes:
   - **Empty page guard triggered**: All recent pages have already-crawled articles (when using `--resume`)
   - **HTTP errors**: Check for network issues or rate limiting
   - **Duplicate detection** (Znews/Nld only): Pagination is looping

**Solutions**:
```bash
# Increase empty page tolerance
--thanhnien-max-empty-pages 10

# Disable empty page guard entirely (use with caution)
--thanhnien-max-empty-pages 0

# Remove --resume flag if you want to re-crawl existing content
# (without --resume, existing articles don't count as "empty")
```

### Crawl Never Stops

**Symptom**: Crawler runs for hours/days without stopping.

**Diagnosis**: Likely misconfigured empty page guard.

**Solutions**:
```bash
# Add a safety max_pages limit
--thanhnien-max-pages 5000

# Reduce empty page tolerance (stops sooner when hitting gaps)
--thanhnien-max-empty-pages 1
```

### Duplicate Pagination Warning (Znews/Nld)

**Symptom**: Logs show `detected duplicate pagination at <URL>; stopping`.

**Diagnosis**: Site is returning the same content for consecutive page numbers.

**This is normal when**:
- You've reached the end of the archive
- Site's pagination wraps around (page 999 returns to page 1)

**This is a bug when**:
- Happens early in the crawl (e.g., page 10)
- You know the site has more content

**Solutions**:
- For end-of-archive case: This is correct behavior, no action needed
- For bug case: Would need code modification to disable duplicate detection

### Rate Limiting / HTTP 429 Errors

**Symptom**: Crawl stops immediately with HTTP 429 or 503 errors.

**Diagnosis**: Site is rate-limiting or blocking the crawler.

**Solutions**:
```bash
# Reduce concurrency
--max-workers 1

# Add delays between requests (not currently supported, would need code modification)

# Use proxy rotation (if configured)
--proxy <proxy_config> \
--proxy-change-url <rotation_url> \
--proxy-rotation-interval 240
```

---

## Implementation Details

### Code Locations

All category loaders are in `crawler/jobs.py`:

| Loader Class | Line Range | Key Methods |
|-------------|-----------|-------------|
| `ThanhnienCategoryLoader` | 669-804 | `_iterate_category` (714-755) |
| `ZnewsCategoryLoader` | 1248-1395 | `_iterate_category` (1293-1346) |
| `Kenh14CategoryLoader` | 807-994 | `_iterate_category` (852-897) |
| `PloCategoryLoader` | 997-1183 | `_iterate_category` (1043-1086) |
| `NldCategoryLoader` | 106-299 | `_iterate_category` (155-218) |

### Default Parameter Locations

Defaults are set in `__init__` methods:

```python
# Thanhnien (line 679-680)
max_pages: int | None = 10
max_empty_pages: int | None = 2

# Znews (line 1258)
max_pages: int | None = 50
# (empty_pages hardcoded to 2 in _iterate_category)

# Kenh14 (line 817-818)
max_pages: int | None = 600
max_empty_pages: int | None = 3

# PLO (line 1007-1008)
max_pages: int | None = None  # Unlimited
max_empty_pages: int | None = 2

# Nld (line 116-117)
max_pages: int | None = None  # Unlimited
max_empty_pages: int | None = 1
```

### Modifying Defaults

To change defaults system-wide, edit the `__init__` method of the relevant loader class.

**Example**: Make Thanhnien unlimited by default:
```python
# In ThanhnienCategoryLoader.__init__ (line 679)
max_pages: int | None = None  # Changed from 10 to None
```

**Example**: Disable Znews hardcoded empty page limit:
```python
# In ZnewsCategoryLoader._iterate_category (line 1313)
# Comment out or remove these lines:
# if consecutive_empty_pages >= 2:
#     break
```

---

## Future Improvements

Potential enhancements to pagination behavior:

1. **Make `0` mean unlimited** - Change logic to `if max_pages > 0 and page > max_pages`
2. **Expose duplicate detection** - Add CLI flags for `stop_on_duplicate` and `duplicate_fingerprint_size`
3. **Remove Znews hardcoded limit** - Make empty page tolerance configurable
4. **Add retry logic** - Tolerate transient HTTP errors (exponential backoff)
5. **Standardize empty detection** - Use consistent definition across all loaders
6. **Add rate limiting** - Built-in delays between requests to avoid bans
7. **Resume from page N** - Allow starting crawl from arbitrary page number
8. **Progress tracking** - Show current page number and estimated remaining pages

---

## Related Documentation

- Main project documentation: `CLAUDE.md`
- Site definitions and parser wiring: `crawler/sites.py`
- Category catalog format: `data/{site}_categories.json`
- Configuration reference: `crawler/config.py`
