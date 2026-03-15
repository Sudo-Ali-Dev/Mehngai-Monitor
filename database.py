import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "market_rates.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    with get_conn() as conn:
        conn.executescript("""
            -- Tracks every image URL we've seen so we don't reprocess
            CREATE TABLE IF NOT EXISTS seen_images (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT    NOT NULL UNIQUE,
                md5_hash    TEXT,
                date        TEXT    NOT NULL,   -- ISO format: 2026-03-14
                category    TEXT    NOT NULL,   -- fruits / vegetables / poultry
                downloaded  INTEGER DEFAULT 0,  -- 1 once file saved to disk
                processed   INTEGER DEFAULT 0,  -- 1 once OCR done
                created_at  TEXT    DEFAULT (datetime('now'))
            );

            -- Stores the final extracted + translated price data
            CREATE TABLE IF NOT EXISTS market_rates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                category    TEXT    NOT NULL,
                item_name   TEXT    NOT NULL,
                min_price   REAL,
                max_price   REAL,
                unit        TEXT,
                price_type  TEXT,               -- retail / wholesale
                created_at  TEXT    DEFAULT (datetime('now'))
            );

            -- Prevent duplicate rows when OCR is re-run on the same image
            CREATE UNIQUE INDEX IF NOT EXISTS uq_market_rates_item
                ON market_rates (date, category, item_name);
        """)
    print("[DB] Tables ready.")


def is_url_seen(url: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM seen_images WHERE url = ?", (url,)
        ).fetchone()
        return row is not None


def is_hash_seen(md5_hash: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM seen_images WHERE md5_hash = ?", (md5_hash,)
        ).fetchone()
        return row is not None


def insert_seen_image(url: str, date: str, category: str):
    with get_conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO seen_images (url, date, category)
               VALUES (?, ?, ?)""",
            (url, date, category),
        )


def mark_downloaded(url: str, md5_hash: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE seen_images SET downloaded = 1, md5_hash = ? WHERE url = ?",
            (md5_hash, url),
        )


def mark_processed(url: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE seen_images SET processed = 1 WHERE url = ?", (url,)
        )


def get_unprocessed():
    """Return all images downloaded but not yet OCR'd."""
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM seen_images WHERE downloaded = 1 AND processed = 0"
        ).fetchall()
