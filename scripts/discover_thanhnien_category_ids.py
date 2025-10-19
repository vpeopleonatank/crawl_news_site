"""
Script to discover Thanhnien category IDs by monitoring XHR requests.

For each category URL, this script:
1. Opens the page in a browser
2. Scrolls to trigger infinite load
3. Captures XHR requests to /timelinelist/<category_id>/<page>.htm
4. Extracts the category_id from the URL pattern
"""
import json
import re
import time
from playwright.sync_api import sync_playwright

def discover_category_id(page, url, slug):
    """
    Load a category page and extract the category_id from XHR requests.

    Args:
        page: Playwright page object
        url: Category landing URL
        slug: Category slug for logging

    Returns:
        int: The discovered category_id, or None if not found
    """
    category_id = None

    def handle_request(request):
        nonlocal category_id
        # Look for requests matching /timelinelist/<id>/<page>.htm
        if '/timelinelist/' in request.url and request.url.endswith('.htm'):
            match = re.search(r'/timelinelist/(\d+)/\d+\.htm', request.url)
            if match:
                category_id = int(match.group(1))
                print(f"  ✓ Found category_id={category_id} from XHR: {request.url}")

    # Register request handler
    page.on('request', handle_request)

    try:
        print(f"Loading: {slug} -> {url}")
        page.goto(url, wait_until='networkidle', timeout=30000)

        # Wait a bit for initial load
        time.sleep(2)

        # Scroll down to trigger infinite load
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        # Try clicking "Xem thêm" button if it exists
        try:
            xem_them_button = page.locator('text=Xem thêm').first
            if xem_them_button.is_visible(timeout=2000):
                print(f"  Clicking 'Xem thêm' button...")
                xem_them_button.click()
                time.sleep(2)
        except Exception as e:
            print(f"  No 'Xem thêm' button found (may use infinite scroll)")

        # Additional scroll attempts
        for i in range(3):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)

    except Exception as e:
        print(f"  ✗ Error loading page: {e}")

    finally:
        page.remove_listener('request', handle_request)

    return category_id


def main():
    # Load categories from JSON
    with open('data/thanhnien_categories.json', 'r', encoding='utf-8') as f:
        categories = json.load(f)

    print(f"Discovering category IDs for {len(categories)} categories...\n")

    # Track results
    discovered = {}
    failed = []

    with sync_playwright() as p:
        # Launch browser (headless mode for efficiency)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        try:
            for i, category in enumerate(categories, 1):
                slug = category['slug']
                url = category['landing_url']

                print(f"\n[{i}/{len(categories)}] Processing: {slug}")

                category_id = discover_category_id(page, url, slug)

                if category_id:
                    discovered[slug] = category_id
                    category['category_id'] = category_id
                else:
                    print(f"  ✗ Failed to discover category_id")
                    failed.append(slug)

                # Rate limiting
                time.sleep(1)

        finally:
            browser.close()

    # Save updated categories
    with open('data/thanhnien_categories.json', 'w', encoding='utf-8') as f:
        json.dump(categories, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print(f"Discovery Summary:")
    print(f"  Total categories: {len(categories)}")
    print(f"  Successfully discovered: {len(discovered)}")
    print(f"  Failed: {len(failed)}")

    if failed:
        print(f"\nFailed categories:")
        for slug in failed:
            print(f"  - {slug}")

    print(f"\n✓ Updated data/thanhnien_categories.json")


if __name__ == '__main__':
    main()
