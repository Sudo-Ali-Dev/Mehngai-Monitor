"""
normalizer.py — OCR output normalization layer.

Sits between Gemini's raw JSON and the database.
Every field is cleaned and validated here before DB insert.
"""

import re
import json
import os
from datetime import datetime, timezone

# ── Paths ───────────────────────────────────────────────────────────────────

_LOG_DIR  = os.path.join(os.path.dirname(__file__), "data")
_LOG_FILE = os.path.join(_LOG_DIR, "normalization_log.jsonl")


# ── Unit canonicalization ────────────────────────────────────────────────────

# Maps substrings (lowercased) to canonical short unit strings.
# Checked in order — first match wins.
_UNIT_MAP: list[tuple[str, str]] = [
    ("per dozen",  "per dozen"),
    ("dozen",      "per dozen"),
    ("per 40 kg",  "per 40 kg"),
    ("40 kg",      "per 40 kg"),
    ("40kg",       "per 40 kg"),
    ("per maund",  "per maund"),
    ("maund",      "per maund"),
    # "per kg" must come before "per piece" — the verbose OCR string
    # "per kg (or per piece if mentioned)" must resolve to "per kg"
    ("per kg",     "per kg"),
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
