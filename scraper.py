import hashlib
import os
import time
import requests
from bs4 import BeautifulSoup
from database import (
    insert_seen_image,
    is_url_seen,
    is_hash_seen,
    mark_downloaded,
    has_processed_image_for_date_category,
)

BASE_URL = "https://lahore.punjab.gov.pk"

CATEGORIES = {
    "fruits":     "/fruits-rate-list",
    "vegetables": "/vegetables-rate-list",
    "poultry":    "/poultry-rate-list",
}

IMAGES_DIR = os.path.join(os.path.dirname(__file__), "images")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ── Proxy Configuration ────────────────────────────────────────────────────────
# Add your working proxies here. Format: "http://ip:port" or "https://ip:port"
# Example: ["http://proxy1.com:8080", "http://proxy2.com:8080"]
PROXIES_LIST = [
    "http://43.245.131.90:8080",
    "http://103.115.198.177:8082",
    "http://103.205.178.226:8080",
    "http://103.66.149.194:8080",
    "http://202.165.232.238:8080",
    "http://103.197.47.42:8080",
    "http://154.208.58.89:8080",
]

PROXY_INDEX = 0


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_next_proxy() -> dict | None:
    """
    Get the next proxy from the rotation list.
    Returns a dict for requests.get(proxies=dict) or None if no proxies configured.
    """
    global PROXY_INDEX
    
    if not PROXIES_LIST:
        return None
    
    proxy = PROXIES_LIST[PROXY_INDEX % len(PROXIES_LIST)]
    PROXY_INDEX += 1
    
    return {
        "http": proxy,
        "https": proxy,
    }


def md5_of_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def image_save_path(date: str, category: str) -> str:
    folder = os.path.join(IMAGES_DIR, date)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{category}.jpeg")


# ── Core scraping logic ────────────────────────────────────────────────────────

def fetch_page(url: str) -> BeautifulSoup | None:
    """
    Fetch a page with proxy rotation, retry logic, and fallback to no proxy.
    Retries each proxy up to 2 times before moving to the next one.
    """
    proxies_to_try = [None]  # Start with no proxy
    
    if PROXIES_LIST:
        # Try proxies first before falling back to no proxy
        proxies_to_try = [get_next_proxy() for _ in range(min(3, len(PROXIES_LIST)))] + [None]
    
    max_retries = 2  # Retry each proxy up to 2 times
    
    for proxies in proxies_to_try:
        for attempt in range(max_retries + 1):
            try:
                proxy_info = f" (via {proxies['https']})" if proxies else ""
                attempt_info = f" (attempt {attempt + 1}/{max_retries + 1})" if attempt > 0 else ""
                resp = requests.get(url, headers=HEADERS, timeout=30, proxies=proxies)
                resp.raise_for_status()
                print(f"[SCRAPER] Fetched {url}{proxy_info}")
                return BeautifulSoup(resp.text, "html.parser")
            except requests.RequestException as e:
                proxy_info = f" (via {proxies['https']})" if proxies else ""
                attempt_info = f" (attempt {attempt + 1}/{max_retries + 1})" if attempt < max_retries else ""
                print(f"[SCRAPER] Failed to fetch {url}{proxy_info}{attempt_info}: {e}")
                # If we've exhausted retries for this proxy, try the next one
                if attempt == max_retries:
                    continue
                # Otherwise, retry the same proxy
                time.sleep(1)
    
    print(f"[SCRAPER] All attempts failed for {url}")
    return None


def parse_table(soup: BeautifulSoup, category: str) -> list[dict]:
    """
    Extract rows from the market rates table.
    Returns a list of dicts: {date, category, url}
    """
    rows = []
    table = soup.find("table", class_="table")
    if not table:
        print(f"[SCRAPER] No table found for {category}")
        return rows

    for tr in table.find("tbody").find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue

        # Date is in the <time datetime="..."> attribute
        time_tag = cells[0].find("time")
        if not time_tag:
            continue
        date = time_tag["datetime"][:10]  # "2026-03-14T12:00:00Z" → "2026-03-14"

        # Image URL is the href on the <a> tag in the second cell
        link = cells[1].find("a")
        if not link:
            continue
        image_path = link["href"]  # e.g. /system/files?file=fruit_7.jpeg
        full_url = BASE_URL + image_path

        rows.append({"date": date, "category": category, "url": full_url})

    return rows


def download_image(url: str, date: str, category: str) -> str | None:
    """
    Download the image with proxy rotation, check for MD5 dupe, save to disk.
    Returns the local file path if successful, None otherwise.
    """
    proxies_to_try = [None]  # Start with no proxy
    
    if PROXIES_LIST:
        # Try proxies first before falling back to no proxy
        proxies_to_try = [get_next_proxy() for _ in range(min(3, len(PROXIES_LIST)))] + [None]
    
    for proxies in proxies_to_try:
        try:
            proxy_info = f" (via {proxies['https']})" if proxies else ""
            resp = requests.get(url, headers=HEADERS, timeout=30, proxies=proxies)
            resp.raise_for_status()
            break  # Success, stop trying proxies
        except requests.RequestException as e:
            proxy_info = f" (via {proxies['https']})" if proxies else ""
            print(f"[SCRAPER] Download failed for {url}{proxy_info}: {e}")
            continue
    else:
        # All proxy attempts failed
        print(f"[SCRAPER] All proxy attempts exhausted for {url}")
        return None

    image_data = resp.content
    md5 = md5_of_bytes(image_data)

    # Second dupe check — same content, different filename
    if is_hash_seen(md5):
        print(f"[SCRAPER] Duplicate content (MD5 match), skipping: {url}")
        return None

    save_path = image_save_path(date, category)
    with open(save_path, "wb") as f:
        f.write(image_data)

    mark_downloaded(url, md5)
    print(f"[SCRAPER] Saved: {save_path}")
    return save_path


# ── Main entry point ───────────────────────────────────────────────────────────

def run_scraper() -> list[dict]:
    """
    Scrape all three category pages.
    Returns list of newly downloaded images ready for OCR.
    """
    new_images = []

    for category, path in CATEGORIES.items():
        url = BASE_URL + path
        print(f"[SCRAPER] Checking {category} → {url}")

        soup = fetch_page(url)
        if not soup:
            continue

        rows = parse_table(soup, category)
        print(f"[SCRAPER] Found {len(rows)} rows for {category}")

        # Only process the latest (first) image for this category
        if not rows:
            continue
        
        row = rows[0]  # Latest image only
        
        # Skip if we already have a successfully processed image for this date/category
        if has_processed_image_for_date_category(row["date"], row["category"]):
            print(f"[SCRAPER] Already processed for {row['date']} {row['category']}, skipping.")
            continue
        
        # Mark URL as seen immediately (prevents re-download if scheduler
        # fires again before OCR finishes)
        insert_seen_image(row["url"], row["date"], row["category"])

        # Download and check: if new, save; if duplicate, it's deleted automatically
        local_path = download_image(row["url"], row["date"], row["category"])
        if local_path:
            new_images.append({**row, "local_path": local_path})

        # Be polite — small delay between category requests
        time.sleep(2)

    print(f"[SCRAPER] Done. {len(new_images)} new image(s) downloaded.")
    return new_images


if __name__ == "__main__":
    from database import init_db
    init_db()
    results = run_scraper()
    for r in results:
        print(r)
