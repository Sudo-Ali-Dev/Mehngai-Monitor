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

# Force direct networking for now and ignore OS/env proxy variables.
HTTP = requests.Session()
HTTP.trust_env = False


# ── Helpers ────────────────────────────────────────────────────────────────────


def md5_of_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def image_save_path(date: str, category: str) -> str:
    folder = os.path.join(IMAGES_DIR, date)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{category}.jpeg")


# ── Core scraping logic ────────────────────────────────────────────────────────

def fetch_page(url: str) -> BeautifulSoup | None:
    """
    Fetch a page directly (no proxy) with retry logic.
    """
    max_retries = 2

    for attempt in range(max_retries + 1):
        try:
            attempt_info = f" (attempt {attempt + 1}/{max_retries + 1})" if attempt > 0 else ""
            resp = HTTP.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            print(f"[SCRAPER] Fetched {url}")
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            attempt_info = f" (attempt {attempt + 1}/{max_retries + 1})" if attempt < max_retries else ""
            print(f"[SCRAPER] Failed to fetch {url}{attempt_info}: {e}")
            if attempt < max_retries:
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
    Download image directly (no proxy), check for MD5 dupe, and save to disk.
    Returns the local file path if successful, None otherwise.
    """
    try:
        resp = HTTP.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[SCRAPER] Download failed for {url}: {e}")
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

        if not rows:
            continue

        for row in rows:
            if has_processed_image_for_date_category(row["date"], row["category"]):
                print(f"[SCRAPER] Already processed for {row['date']} {row['category']}, skipping.")
                continue

            insert_seen_image(row["url"], row["date"], row["category"])

            local_path = download_image(row["url"], row["date"], row["category"])
            if local_path:
                new_images.append({**row, "local_path": local_path})

            time.sleep(2)

    print(f"[SCRAPER] Done. {len(new_images)} new image(s) downloaded.")
    return new_images


if __name__ == "__main__":
    from database import init_db
    init_db()
    results = run_scraper()
    for r in results:
        print(r)
