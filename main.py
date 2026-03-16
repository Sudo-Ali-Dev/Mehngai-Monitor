import os
from dotenv import load_dotenv

load_dotenv()  # Loads GEMINI_API_KEY from .env file

from database import init_db
from scraper import run_scraper
from ocr import run_ocr
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from database import get_conn

app = FastAPI()
templates = Jinja2Templates(directory="templates")


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index(request: Request, category: str = "fruits"):
    with get_conn() as conn:
        # Get latest date available for this category
        latest = conn.execute(
            "SELECT MAX(date) as date FROM market_rates WHERE category = ?",
            (category,),
        ).fetchone()["date"]

        rates = []
        if latest:
            raw_rates = conn.execute(
                """SELECT item_name, min_price, max_price, unit
                   FROM market_rates
                   WHERE category = ? AND date = ?
                   ORDER BY item_name""",
                (category, latest),
            ).fetchall()

            # Get previous date for trend comparison
            prev_date_row = conn.execute(
                """SELECT DISTINCT date FROM market_rates
                   WHERE category = ? AND date < ?
                   ORDER BY date DESC LIMIT 1""",
                (category, latest),
            ).fetchone()

            prev_prices = {}
            if prev_date_row:
                prev_rows = conn.execute(
                    """SELECT item_name, min_price, max_price
                       FROM market_rates
                       WHERE category = ? AND date = ?""",
                    (category, prev_date_row["date"]),
                ).fetchall()
                for p in prev_rows:
                    avg = ((p["min_price"] or 0) + (p["max_price"] or p["min_price"] or 0)) / 2
                    prev_prices[p["item_name"]] = avg

            rates = []
            for r in raw_rates:
                row = dict(r)
                curr_avg = ((r["min_price"] or 0) + (r["max_price"] or r["min_price"] or 0)) / 2
                prev_avg = prev_prices.get(r["item_name"])
                if prev_avg is None or curr_avg == 0:
                    row["trend"] = "flat"
                elif curr_avg > prev_avg * 1.005:
                    row["trend"] = "up"
                elif curr_avg < prev_avg * 0.995:
                    row["trend"] = "down"
                else:
                    row["trend"] = "flat"
                rates.append(row)

        # Get last 30 available dates for this category
        dates = conn.execute(
            """SELECT DISTINCT date FROM market_rates
               WHERE category = ?
               ORDER BY date DESC LIMIT 30""",
            (category,),
        ).fetchall()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "category": category,
        "rates": rates,
        "latest_date": latest,
        "dates": [d["date"] for d in dates],
        "categories": ["fruits", "vegetables", "poultry"],
    })


@app.get("/date/{category}/{date}", response_class=HTMLResponse)
def by_date(request: Request, category: str, date: str):
    with get_conn() as conn:
        raw_rates = conn.execute(
            """SELECT item_name, min_price, max_price, unit
               FROM market_rates
               WHERE category = ? AND date = ?
               ORDER BY item_name""",
            (category, date),
        ).fetchall()

        # Get previous date for trend comparison
        prev_date_row = conn.execute(
            """SELECT DISTINCT date FROM market_rates
               WHERE category = ? AND date < ?
               ORDER BY date DESC LIMIT 1""",
            (category, date),
        ).fetchone()

        prev_prices = {}
        if prev_date_row:
            prev_rows = conn.execute(
                """SELECT item_name, min_price, max_price
                   FROM market_rates
                   WHERE category = ? AND date = ?""",
                (category, prev_date_row["date"]),
            ).fetchall()
            for p in prev_rows:
                avg = ((p["min_price"] or 0) + (p["max_price"] or p["min_price"] or 0)) / 2
                prev_prices[p["item_name"]] = avg

        rates = []
        for r in raw_rates:
            row = dict(r)
            curr_avg = ((r["min_price"] or 0) + (r["max_price"] or r["min_price"] or 0)) / 2
            prev_avg = prev_prices.get(r["item_name"])
            if prev_avg is None or curr_avg == 0:
                row["trend"] = "flat"
            elif curr_avg > prev_avg * 1.005:
                row["trend"] = "up"
            elif curr_avg < prev_avg * 0.995:
                row["trend"] = "down"
            else:
                row["trend"] = "flat"
            rates.append(row)

        dates = conn.execute(
            """SELECT DISTINCT date FROM market_rates
               WHERE category = ?
               ORDER BY date DESC LIMIT 30""",
            (category,),
        ).fetchall()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "category": category,
        "rates": rates,
        "latest_date": date,
        "dates": [d["date"] for d in dates],
        "categories": ["fruits", "vegetables", "poultry"],
    })


@app.get("/trend/{category}/{item_name}", response_class=HTMLResponse)
def item_trend(request: Request, category: str, item_name: str):
    with get_conn() as conn:
        # Fetch historical prices ordered by date for Charting
        history = conn.execute(
            """SELECT date, min_price, max_price, unit
               FROM market_rates
               WHERE category = ? AND item_name = ?
               ORDER BY date ASC""",
            (category, item_name),
        ).fetchall()

    # Extract units (usually consistent, we'll take the most recent one if present)
    unit = history[-1]["unit"] if history else ""

    return templates.TemplateResponse("trend.html", {
        "request": request,
        "category": category,
        "item_name": item_name,
        "history": history,
        "unit": unit
    })


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Lahore Market Rates")
    print("=" * 50)

    # 1. Init DB
    init_db()

    # 2. Scrape new images
    print("\n[STEP 1] Checking for new images...")
    new_images = run_scraper()

    # 3. OCR any new images
    print("\n[STEP 2] Processing new images with Gemini...")
    run_ocr()

    # 4. Start web server
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"\n[STEP 3] Starting web app at http://{host}:8000")
    print("=" * 50)
    uvicorn.run(app, host=host, port=8000)