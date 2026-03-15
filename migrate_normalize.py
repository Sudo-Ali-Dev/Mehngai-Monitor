"""
migrate_normalize.py — One-time migration to apply normalizer rules to existing DB rows.

What it does:
  1. Reads every row from market_rates
  2. Applies normalize_unit() and normalize_name() to each row
  3. Handles collisions: if two rows in the same (date, category) would get
     the same normalized name, merge them (keep the first one's prices, drop duplicate)
  4. Updates rows in-place using UPDATE; uses DELETE for merged duplicates
  5. Applies the UNIQUE INDEX via init_db() if not already present

Run once with:
    python migrate_normalize.py
"""

import sys
import os
import sqlite3
sys.path.insert(0, os.path.dirname(__file__))

# Load env before importing normalizer
from dotenv import load_dotenv
load_dotenv()

from database import get_conn, init_db
from normalizer import normalize_unit, normalize_name


def migrate():
    # Ensure UNIQUE index exists (safe — uses IF NOT EXISTS)
    init_db()

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, date, category, item_name, min_price, max_price, unit FROM market_rates"
        ).fetchall()

    print(f"[MIGRATE] Found {len(rows)} rows to process.")

    # Group by (date, category) so we can detect within-group collisions
    # after normalization
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["date"], r["category"])
        groups.setdefault(key, []).append(dict(r))

    updates = 0
    deletes = 0

    with get_conn() as conn:
        for (date, category), items in groups.items():
            seen: dict[str, int] = {}   # normalized_name → first row id we kept

            for item in items:
                new_name = normalize_name(item["item_name"])
                new_unit = normalize_unit(item["unit"])
                orig_id  = item["id"]

                norm_key = new_name.lower()

                if norm_key in seen:
                    # Collision — this is a duplicate after normalization
                    # Delete the later occurrence, keep the first
                    conn.execute("DELETE FROM market_rates WHERE id = ?", (orig_id,))
                    deletes += 1
                    print(f"  [MERGE]  Deleted duplicate id={orig_id}: "
                          f"{item['item_name']!r} -> {new_name!r} "
                          f"(kept id={seen[norm_key]})")
                else:
                    # Check if anything actually changed
                    name_changed = new_name != item["item_name"]
                    unit_changed = new_unit != item["unit"]

                    if name_changed or unit_changed:
                        conn.execute(
                            "UPDATE market_rates SET item_name = ?, unit = ? WHERE id = ?",
                            (new_name, new_unit, orig_id),
                        )
                        updates += 1
                        if name_changed:
                            print(f"  [NAME]   id={orig_id}: {item['item_name']!r} -> {new_name!r}")
                        if unit_changed:
                            print(f"  [UNIT]   id={orig_id}: {item['unit']!r} -> {new_unit!r}")

                    seen[norm_key] = orig_id

    total = conn.execute("SELECT COUNT(*) FROM market_rates").fetchone()[0]
    print(f"\n[MIGRATE] Done.")
    print(f"  Updated : {updates} rows")
    print(f"  Deleted : {deletes} duplicate rows")
    print(f"  Remaining rows: {total}")


def migrate_canonicalize():
    """Second pass: AI Semantic Name Canonicalization."""
    print("\n[MIGRATE] Starting AI Canonicalization phase...")
    updates = 0
    deletes = 0

    # We need to do this per category
    for category in ["fruits", "vegetables", "poultry"]:
        with get_conn() as conn:
            # 1. Get all distinct names for this category across the WHOLE db
            names = conn.execute(
                "SELECT DISTINCT item_name FROM market_rates WHERE category = ? ORDER BY item_name",
                (category,)
            ).fetchall()
            all_names = [n[0] for n in names]
            
            if not all_names:
                continue

            print(f"  [CANON] {category}: {len(all_names)} distinct names to process")
            
            # 2. Call Gemini to map them
            # We trick canonicalize_names by passing 'items' as the new names
            # and 'known_names' as the exact same list. Gemini will map the variants
            # to the most canonical-looking ones.
            dummy_items = [{"english_name": n} for n in all_names]
            
            # We need a dedicated prompt for global deduplication, because
            # the daily _call_gemini_canonicalize assumes known_names are pristine.
            import json
            import requests
            from normalizer import GEMINI_URL, GEMINI_API_KEY
            
            if not GEMINI_API_KEY:
                print(f"  [CANON] Skipping AI canonicalize: GEMINI_API_KEY not set")
                continue
                
            prompt = f"""
You are a master data cleaner resolving OCR inconsistencies for a database of market prices.
Here is a list of ALL distinct item names currently in the database for the '{category}' category.
Your job is to find clusters of names that mean the EXACT same thing, and elect ONE single best name to represent them all.

Rules for electing the best name:
1. Prefer English terms over transliterated Urdu (e.g., 'A-Grade' instead of 'Awal' or 'First')
2. Prefer standard spelling (e.g., 'Irani' instead of 'Iranian')
3. Prefer singular forms logically if it represents a unit (e.g., 'Corn Cob' over 'Corn Cobs')
4. DO NOT merge different grades (e.g., 'A-Grade' is different from 'B-Grade')
5. DO NOT merge genuinely different varieties (e.g., 'Apple Golden' vs 'Apple Irani')

List of names:
{json.dumps(all_names)}

Return a JSON mapping where the KEYS are the messy names that should be renamed, and the VALUES are the single elected best name for that cluster.
Only include names that need to change. If a name is already the elected best name or is totally unique, omit it.
"""
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.0, "maxOutputTokens": 1024},
            }

            print(f"  [CANON] Calling Gemini for {category} global deduplication...")
            try:
                resp = requests.post(GEMINI_URL, json=payload, timeout=45)
                resp.raise_for_status()
                text = resp.json()[( "candidates")][0]["content"]["parts"][0]["text"]
                text = text.strip()
                if text.startswith("```"):
                    text = text.split("```")[1]
                    if text.startswith("json"):
                        text = text[4:]
                text = text.strip()
                mapping = json.loads(text)
            except Exception as e:
                print(f"  [CANON] Gemini API error: {e}")
                continue

            
            if not mapping:
                print(f"  [CANON] No mappings found for {category}")
                continue

            # 3. Apply the mapping to the DB
            # We have to group by date again to handle collisions that result from the mapping
            rows = conn.execute(
                "SELECT id, date, item_name FROM market_rates WHERE category = ?",
                (category,)
            ).fetchall()

            groups: dict[str, list[dict]] = {}
            for r in rows:
                groups.setdefault(r["date"], []).append(dict(r))

            for date, date_items in groups.items():
                seen: dict[str, int] = {}  # mapped_name -> kept id
                
                for item in date_items:
                    orig_name = item["item_name"]
                    orig_id = item["id"]
                    
                    # Either it got mapped, or it stays the same
                    new_name = mapping.get(orig_name, orig_name)
                    norm_key = new_name.lower()
                    
                    if norm_key in seen:
                        # Collision! Delete the duplicate
                        conn.execute("DELETE FROM market_rates WHERE id = ?", (orig_id,))
                        deletes += 1
                        print(f"    [MERGE]  {date} Deleted duplicate id={orig_id}: "
                              f"{orig_name!r} -> {new_name!r} "
                              f"(kept id={seen[norm_key]})")
                    else:
                        if orig_name != new_name:
                            try:
                                conn.execute(
                                    "UPDATE market_rates SET item_name = ? WHERE id = ?",
                                    (new_name, orig_id),
                                )
                                updates += 1
                                print(f"    [UPDATE] {date} id={orig_id}: {orig_name!r} -> {new_name!r}")
                            except sqlite3.IntegrityError:
                                # The target name already exists on this date!
                                # This is a duplicate we must drop.
                                conn.execute("DELETE FROM market_rates WHERE id = ?", (orig_id,))
                                deletes += 1
                                print(f"    [MERGE DB] {date} Deleted duplicate id={orig_id}: "
                                      f"{orig_name!r} -> {new_name!r} (target already exists)")
                        
                        seen[norm_key] = orig_id

    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM market_rates").fetchone()[0]
    print(f"\n[MIGRATE CANON] Done.")
    print(f"  Updated : {updates} rows")
    print(f"  Deleted : {deletes} duplicate rows")
    print(f"  Remaining rows: {total}")

if __name__ == "__main__":
    migrate()
    migrate_canonicalize()

