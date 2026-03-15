"""
normalizer.py — OCR output normalization layer.

Sits between Gemini's raw JSON and the database.
Every field is cleaned and validated here before DB insert.
"""

import re
import re
import json
import os
import requests
from datetime import datetime, timezone

# ── Paths & Config ──────────────────────────────────────────────────────────

_LOG_DIR  = os.path.join(os.path.dirname(__file__), "data")
_LOG_FILE = os.path.join(_LOG_DIR, "normalization_log.jsonl")
_CANON_LOG = os.path.join(_LOG_DIR, "canon_log.jsonl")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-3.1-flash-lite-preview:generateContent?key=" + GEMINI_API_KEY
)


# ── Unit canonicalization ────────────────────────────────────────────────────

# Maps substrings (lowercased) to canonical short unit strings.
# Checked in order — first match wins.
_UNIT_MAP: list[tuple[str, str]] = [
    # Specific multi-unit poultry strings must be matched FIRST
    # e.g. "per kg / per dozen / per crate" and "per kg (or per piece/dozen)"
    # These all reduce to "per kg" as the primary unit.
    ("per kg",     "per kg"),   # covers all "per kg ..." variants
    ("per 40 kg",  "per 40 kg"),
    ("40 kg",      "per 40 kg"),
    ("40kg",       "per 40 kg"),
    ("per maund",  "per maund"),
    ("maund",      "per maund"),
    # Only match "per dozen" / "per piece" when kg is NOT present
    ("per dozen",  "per dozen"),
    ("dozen",      "per dozen"),
    ("per piece",  "per piece"),
    ("each",       "per piece"),
]

def normalize_unit(raw: str | None) -> str:
    """Return a short, canonical unit string."""
    if not raw:
        return "per kg"
    lower = raw.lower().strip()
    for fragment, canonical in _UNIT_MAP:
        if fragment in lower:
            return canonical
    return "per kg"   # safe default


# ── Item name canonicalization ───────────────────────────────────────────────

# (lowercased key) → canonical title-case value
ITEM_NAMES: dict[str, str] = {
    # Apples
    "apple iranian":        "Apple Irani",
    "apple iran":           "Apple Irani",
    "apple irani":          "Apple Irani",
    "apple kala kulu mountain": "Apple Kala Kulu Mountain",
    "apple kala kulu plain":"Apple Kala Kulu Plain",
    "apple white a grade":  "Apple White A-Grade",
    "apple white a-grade":  "Apple White A-Grade",
    "apple golden":         "Apple Golden",
    "apple kashmiri":       "Apple Kashmiri",

    # Common vegetables
    "potato regular":       "Potato",
    "potato":               "Potato",
    "tomato regular":       "Tomato",
    "tomato":               "Tomato",
    "onion regular":        "Onion",
    "onion":                "Onion",
    "garlic":               "Garlic",
    "ginger":               "Ginger",
    "green chilli":         "Green Chilli",
    "green chili":          "Green Chilli",
    "green pepper":         "Green Chilli",
    "capsicum":             "Capsicum",
    "spinach":              "Spinach",
    "peas":                 "Peas",
    "cauliflower":          "Cauliflower",
    "carrot":               "Carrot",
    "radish":               "Radish",
    "turnip":               "Turnip",
    "eggplant":             "Eggplant",
    "brinjal":              "Eggplant",
    "cucumber":             "Cucumber",
    "pumpkin":              "Pumpkin",
    "bitter gourd":         "Bitter Gourd",
    "bottle gourd":         "Bottle Gourd",
    "lady finger":          "Lady Finger",
    "okra":                 "Lady Finger",
    "tinda":                "Tinda",
    "zucchini":             "Zucchini",

    # Citrus / fruits
    "orange":               "Orange",
    "orange kinnow":        "Kinnow",
    "kinnow":               "Kinnow",
    "mandarin":             "Kinnow",
    "lemon":                "Lemon",
    "banana":               "Banana",
    "banana a grade":       "Banana A-Grade",
    "banana a-grade":       "Banana A-Grade",
    "banana b grade":       "Banana B-Grade",
    "banana b-grade":       "Banana B-Grade",
    "mango":                "Mango",
    "grape":                "Grape",
    "grapes":               "Grape",
    "papaya":               "Papaya",
    "guava":                "Guava",
    "pomegranate":          "Pomegranate",
    "watermelon":           "Watermelon",
    "melon":                "Muskmelon",
    "cantaloupe":           "Cantaloupe",
    "pear":                 "Pear",
    "peach":                "Peach",
    "plum":                 "Plum",
    "apricot":              "Apricot",
    "fig":                  "Fig",
    "strawberry":           "Strawberry",
    "coconut":              "Coconut",
    "dates":                "Dates",
    "date":                 "Dates",

    # Poultry / meat
    "broiler":              "Broiler (live)",
    "broiler live":         "Broiler (live)",
    "broiler chicken":      "Broiler (live)",
    "farm chicken":         "Farm Chicken",
    "desi chicken":         "Desi Chicken",
    "rooster":              "Rooster",
    "eggs":                 "Eggs",
    "egg":                  "Eggs",
    "desi egg":             "Desi Eggs",
    "desi eggs":            "Desi Eggs",
}

# Patterns to strip from names before lookup
_STRIP_PATTERNS = [
    r"\(per (kg|piece|dozen|maund)\)",   # "(per kg)" etc embedded in names
    r"\(regular\)",
    r"\s{2,}",                            # multiple spaces
]

def normalize_name(raw: str | None, urdu_fallback: str = "") -> str:
    """Return a canonical, consistently-cased item name."""
    if not raw:
        raw = urdu_fallback or "Unknown"

    # Strip embedded unit qualifiers and extra whitespace
    cleaned = raw.strip()
    for pat in _STRIP_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip()

    # Title-case then check lookup table
    title = cleaned.title()
    lookup = ITEM_NAMES.get(cleaned.lower())
    return lookup if lookup else title


# ── Price validation ─────────────────────────────────────────────────────────

# Absolute price bounds per category (Rs). Outside = likely OCR error.
_PRICE_BOUNDS: dict[str, tuple[float, float]] = {
    "fruits":     (1,   50_000),
    "vegetables": (1,   50_000),
    "poultry":    (1,  200_000),
    "default":    (1, 1_000_000),
}

def validate_price(value, category: str) -> float | None:
    """Return the price if valid, else None."""
    if value is None:
        return None
    try:
        p = float(value)
    except (TypeError, ValueError):
        return None
    lo, hi = _PRICE_BOUNDS.get(category, _PRICE_BOUNDS["default"])
    if not (lo <= p <= hi):
        return None
    return p


# ── Normalization log ────────────────────────────────────────────────────────

def _log_event(event_type: str, item_name: str, reason: str, original: dict):
    """Append one JSONL line to the normalization log."""
    os.makedirs(_LOG_DIR, exist_ok=True)
    entry = {
        "ts":        datetime.now(timezone.utc).isoformat(),
        "event":     event_type,     # "modified" | "skipped" | "swapped"
        "item":      item_name,
        "reason":    reason,
        "original":  original,
    }
    with open(_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ── Main normalize() entry point ─────────────────────────────────────────────

def normalize(data: dict) -> dict:
    """
    Clean and validate a raw Gemini OCR result dict.

    Input  (raw Gemini output):
        {
          "date":     "2026-03-14",
          "category": "fruits",
          "unit":     "per kg (or per piece if mentioned)",
          "items": [
            {"urdu_name": "...", "english_name": "Apple Iranian",
             "price_1": 145, "price_2": 138},
            ...
          ]
        }

    Output (normalized, ready for DB insert):
        Same structure with cleaned fields and a "skipped" key listing
        any items that were entirely dropped.
    """
    category = (data.get("category") or "default").lower().strip()
    unit      = normalize_unit(data.get("unit"))

    seen_names: set[str] = set()
    clean_items: list[dict] = []
    skipped:     list[dict] = []

    for raw_item in data.get("items", []):
        original = dict(raw_item)

        # ── Name ────────────────────────────────────────────────────────
        name = normalize_name(
            raw_item.get("english_name"),
            raw_item.get("urdu_name", ""),
        )

        # ── Deduplication ────────────────────────────────────────────────
        key = name.lower()
        if key in seen_names:
            _log_event("skipped", name, "duplicate item in same OCR result", original)
            skipped.append({"name": name, "reason": "duplicate"})
            continue
        seen_names.add(key)

        # ── Prices ──────────────────────────────────────────────────────
        p1 = validate_price(raw_item.get("price_1"), category)
        p2 = validate_price(raw_item.get("price_2"), category)

        if raw_item.get("price_1") is not None and p1 is None:
            _log_event("modified", name, f"price_1 out of range: {raw_item.get('price_1')}", original)

        if raw_item.get("price_2") is not None and p2 is None:
            _log_event("modified", name, f"price_2 out of range: {raw_item.get('price_2')}", original)

        # price_1 should always be >= price_2 (اول = higher quality)
        if p1 is not None and p2 is not None and p1 < p2:
            p1, p2 = p2, p1
            _log_event("swapped", name, "price_1 < price_2; swapped", original)

        clean_items.append({
            "english_name": name,
            "urdu_name":    raw_item.get("urdu_name", ""),
            "price_1":      p1,
            "price_2":      p2,
        })

    return {
        "date":     data.get("date", ""),
        "category": category,
        "unit":     unit,
        "items":    clean_items,
        "skipped":  skipped,
    }


# ── AI Name Canonicalization ─────────────────────────────────────────────────

def _call_gemini_canonicalize(new_names: list[str], known_names: list[str]) -> dict[str, str]:
    """Ask Gemini to map new OCR names to known DB names."""
    if not GEMINI_API_KEY:
        print("[CANON] Skipping AI canonicalize: GEMINI_API_KEY not set")
        return {}

    prompt = f"""
You are resolving OCR inconsistencies for a daily market price tracker.

We have a list of 'newly_extracted_names' from today's OCR, and 'known_canonical_names' already in our database.
Map each new name to a known name ONLY IF they refer to the exact same item and quality grade.
For example, "Guava Awal" and "Guava A-Grade" are the same. "Corn Cobs" and "Corn Cob" are the same.
If a new name is genuinely a new product or a different grade, do NOT map it.

Known names (do not invent new ones):
{json.dumps(known_names)}

New names to map:
{json.dumps(new_names)}

Return ONLY a JSON dictionary where keys are new names and values are the mapped known names.
If a name should not be mapped, do not include it in the dictionary.
Do not include markdown or backticks.
"""
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 1024},
    }

    try:
        resp = requests.post(GEMINI_URL, json=payload, timeout=30)
        resp.raise_for_status()
        text = resp.json()[( "candidates")][0]["content"]["parts"][0]["text"]
        
        # Strip markdown
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()
        
        mapping = json.loads(text)
        return {str(k): str(v) for k, v in mapping.items() if v in known_names and k != v}
    except Exception as e:
        print(f"[CANON] Gemini API error: {e}")
        return {}


def canonicalize_names(items: list[dict], known_names: list[str]) -> list[dict]:
    """Map inconsistent item names to known canonical names using AI."""
    if not items or not known_names:
        return items

    # Find which names are actually new
    known_set = set(known_names)
    current_names = [item["english_name"] for item in items]
    new_names = [n for n in current_names if n not in known_set]

    if not new_names:
        return items  # Everything is already perfectly canonical

    print(f"[CANON] Asking AI to map {len(new_names)} new names against {len(known_names)} known names...")
    
    mapping = _call_gemini_canonicalize(new_names, known_names)
    if not mapping:
        return items

    # Log the AI decisions
    os.makedirs(_LOG_DIR, exist_ok=True)
    with open(_CANON_LOG, "a", encoding="utf-8") as f:
        log_entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "mapping": mapping
        }
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    # Apply mapping in-place
    mapped_count = 0
    for item in items:
        orig = item["english_name"]
        if orig in mapping:
            item["english_name"] = mapping[orig]
            mapped_count += 1
            print(f"[CANON] Mapped: {orig!r} -> {mapping[orig]!r}")

    if mapped_count > 0:
        print(f"[CANON] Successfully remapped {mapped_count} items via AI.")
        
    return items
