import os
import json
import base64
import requests
from dotenv import load_dotenv
from database import get_unprocessed, mark_processed, get_conn
from normalizer import normalize

load_dotenv()  # Load GEMINI_API_KEY from .env file

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-3.1-flash-lite-preview:generateContent?key=" + GEMINI_API_KEY
)

PROMPT = """
This image is an Urdu-language daily market price list from Lahore, Pakistan.

The image contains a table with fruit/vegetable/poultry items and their retail prices per KG (or per piece for some items).

The table has two side-by-side sections (right and left, since Urdu is RTL).
Each section has 3 columns: item name (نام), price 1 - اول, price 2 - دوئم.

Your task:
1. Extract EVERY row from BOTH sections of the table
2. Translate each item name from Urdu to English
3. A dash (-) means price is unavailable — use null for that value
4. Return ONLY a JSON object, no explanation, no markdown, no backticks

Return this exact JSON structure:
{
  "date": "YYYY-MM-DD",
  "category": "auto-detect: fruits or vegetables or poultry",
  "unit": "per kg (or per piece if mentioned)",
  "items": [
    {
      "urdu_name": "original urdu text",
      "english_name": "translated name",
      "price_1": 145,
      "price_2": 138
    },
    {
      "urdu_name": "...",
      "english_name": "...",
      "price_1": null,
      "price_2": null
    }
  ]
}

Important:
- Extract ALL items, do not skip any rows
- Keep urdu_name exactly as written in the image
- USE CONSISTENT ENGLISH NAMES. For example, if it's "Apple Irani", do not write "Apple Iranian". Be strict and simple.
- price_1 is اول (first/higher quality), price_2 is دوئم (second/lower quality)
- Return ONLY the JSON, nothing else
"""


def image_to_base64(image_path: str) -> tuple[str, str]:
    """Read image file and return (base64_data, mime_type)."""
    with open(image_path, "rb") as f:
        data = f.read()
    b64 = base64.b64encode(data).decode("utf-8")
    # Detect mime type from extension
    ext = os.path.splitext(image_path)[1].lower()
    mime = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
    return b64, mime


def call_gemini(image_path: str) -> dict | None:
    """Send image to Gemini and return parsed JSON response."""
    if not GEMINI_API_KEY:
        print("[OCR] ERROR: GEMINI_API_KEY not set")
        return None

    print(f"[OCR] Sending to Gemini: {image_path}")
    b64_data, mime_type = image_to_base64(image_path)

    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "inlineData": {
                            "mimeType": mime_type,
                            "data": b64_data,
                        }
                    },
                    {"text": PROMPT},
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,   # Low temp = more consistent structured output
            "maxOutputTokens": 2048,
        },
    }

    try:
        resp = requests.post(GEMINI_URL, json=payload, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[OCR] Gemini API error: {e}")
        return None

    raw = resp.json()

    # Extract text from Gemini response
    try:
        text = raw["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError) as e:
        print(f"[OCR] Unexpected Gemini response structure: {e}")
        print(raw)
        return None

    # Strip any accidental markdown code fences
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[OCR] Failed to parse JSON from Gemini: {e}")
        print("Raw response:", text[:500])
        return None


def save_to_db(data: dict, date: str, category: str):
    """Insert normalized items into market_rates table."""
    items = data.get("items", [])
    if not items:
        print("[OCR] No items found in response")
        return

    inserted = 0
    skipped  = 0
    with get_conn() as conn:
        for item in items:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO market_rates
                       (date, category, item_name, min_price, max_price, unit, price_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        date,
                        category,
                        item["english_name"],
                        item.get("price_1"),
                        item.get("price_2"),
                        data.get("unit", "per kg"),
                        "retail",
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"[OCR] DB error for '{item.get('english_name')}': {e}")

    print(f"[OCR] Saved {inserted} new, skipped {skipped} duplicate rows "
          f"for {category} on {date}")
    if data.get("skipped"):
        print(f"[OCR] Normalizer dropped {len(data['skipped'])} item(s): "
              f"{[s['name'] for s in data['skipped']]}")


def run_ocr():
    """Process all downloaded-but-not-yet-OCR'd images."""
    pending = get_unprocessed()

    if not pending:
        print("[OCR] No new images to process.")
        return

    print(f"[OCR] {len(pending)} image(s) to process.")

    for row in pending:
        image_path = os.path.join(
            os.path.dirname(__file__),
            "images",
            row["date"],
            f"{row['category']}.jpeg",
        )

        if not os.path.exists(image_path):
            print(f"[OCR] Image file not found: {image_path}")
            continue

        result = call_gemini(image_path)
        if result:
            # ── Normalize before persisting ──────────────────────────────
            clean = normalize(result)
            
            # ── AI Canonicalization ──────────────────────────────────────
            # Fetch all known names for this category to map inconsistencies
            with get_conn() as conn:
                known = conn.execute(
                    "SELECT DISTINCT item_name FROM market_rates WHERE category = ?",
                    (row["category"],)
                ).fetchall()
                known_names = [n[0] for n in known]
            
            from normalizer import canonicalize_names
            clean["items"] = canonicalize_names(clean["items"], known_names)
            
            save_to_db(clean, row["date"], row["category"])
            mark_processed(row["url"])
        else:
            print(f"[OCR] Failed to process {image_path}")


if __name__ == "__main__":
    run_ocr()
