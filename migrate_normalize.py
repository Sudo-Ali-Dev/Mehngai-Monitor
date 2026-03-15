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
sys.path.insert(0, os.path.dirname(__file__))

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


if __name__ == "__main__":
    migrate()
