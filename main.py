import os
from dotenv import load_dotenv

load_dotenv()  # Loads GEMINI_API_KEY from .env file

from database import init_db
from scraper import run_scraper
from ocr import run_ocr
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from database import get_conn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from statistics import mean

app = FastAPI()
templates = Jinja2Templates(directory="templates")

CATEGORIES = ["fruits", "vegetables", "poultry"]


def _avg_price(min_price, max_price) -> float:
    a = min_price or 0
    b = max_price or min_price or 0
    return (a + b) / 2


def _get_prev_date(conn, category: str, date: str):
    row = conn.execute(
        """SELECT DISTINCT date FROM market_rates
           WHERE category = ? AND date < ?
           ORDER BY date DESC LIMIT 1""",
        (category, date),
    ).fetchone()
    return row["date"] if row else None


def _get_rates_with_trend(conn, category: str, date: str) -> list[dict]:
    raw_rates = conn.execute(
        """SELECT item_name, min_price, max_price, unit
           FROM market_rates
           WHERE category = ? AND date = ?
           ORDER BY item_name""",
        (category, date),
    ).fetchall()

    prev_date = _get_prev_date(conn, category, date)
    prev_prices = {}
    if prev_date:
        prev_rows = conn.execute(
            """SELECT item_name, min_price, max_price
               FROM market_rates
               WHERE category = ? AND date = ?""",
            (category, prev_date),
        ).fetchall()
        for p in prev_rows:
            prev_prices[p["item_name"]] = _avg_price(p["min_price"], p["max_price"])

    rates = []
    for r in raw_rates:
        row = dict(r)
        curr_avg = _avg_price(r["min_price"], r["max_price"])
        prev_avg = prev_prices.get(r["item_name"])
        if prev_avg and prev_avg > 0:
            pct_change = ((curr_avg - prev_avg) / prev_avg) * 100
        else:
            pct_change = 0.0

        if prev_avg is None or curr_avg == 0:
            trend = "flat"
        elif curr_avg > prev_avg * 1.005:
            trend = "up"
        elif curr_avg < prev_avg * 0.995:
            trend = "down"
        else:
            trend = "flat"

        row["trend"] = trend
        row["pct_change"] = round(pct_change, 2)
        row["avg_price"] = round(curr_avg, 2)
        rates.append(row)
    return rates


def _category_time_series(conn, category: str, end_date: str | None, days: int = 365) -> list[dict]:
    if end_date:
        rows = conn.execute(
            """SELECT date,
                      AVG(COALESCE((min_price + COALESCE(max_price, min_price)) / 2.0, min_price, max_price)) AS avg_price
               FROM market_rates
               WHERE category = ? AND date <= ?
               GROUP BY date
               ORDER BY date DESC
               LIMIT ?""",
            (category, end_date, days),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT date,
                      AVG(COALESCE((min_price + COALESCE(max_price, min_price)) / 2.0, min_price, max_price)) AS avg_price
               FROM market_rates
               WHERE category = ?
               GROUP BY date
               ORDER BY date DESC
               LIMIT ?""",
            (category, days),
        ).fetchall()
    return [dict(r) for r in reversed(rows)]


def _sector_performance(conn, anchor_date: str | None) -> list[dict]:
    output = []
    for cat in CATEGORIES:
        if anchor_date:
            latest_row = conn.execute(
                "SELECT MAX(date) AS date FROM market_rates WHERE category = ? AND date <= ?",
                (cat, anchor_date),
            ).fetchone()
        else:
            latest_row = conn.execute(
                "SELECT MAX(date) AS date FROM market_rates WHERE category = ?",
                (cat,),
            ).fetchone()
        latest_date = latest_row["date"] if latest_row else None
        if not latest_date:
            output.append({"category": cat, "pct_change": 0.0, "trend": "flat"})
            continue

        prev_date = _get_prev_date(conn, cat, latest_date)

        curr_avg_row = conn.execute(
            """SELECT AVG(COALESCE((min_price + COALESCE(max_price, min_price)) / 2.0, min_price, max_price)) AS avg_price
               FROM market_rates WHERE category = ? AND date = ?""",
            (cat, latest_date),
        ).fetchone()
        curr_avg = curr_avg_row["avg_price"] or 0

        prev_avg = 0
        if prev_date:
            prev_avg_row = conn.execute(
                """SELECT AVG(COALESCE((min_price + COALESCE(max_price, min_price)) / 2.0, min_price, max_price)) AS avg_price
                   FROM market_rates WHERE category = ? AND date = ?""",
                (cat, prev_date),
            ).fetchone()
            prev_avg = prev_avg_row["avg_price"] or 0

        pct_change = ((curr_avg - prev_avg) / prev_avg * 100) if prev_avg else 0.0
        trend = "up" if pct_change > 0.3 else "down" if pct_change < -0.3 else "flat"
        output.append({
            "category": cat,
            "pct_change": round(pct_change, 2),
            "trend": trend,
            "latest_date": latest_date,
        })
    return output


def _dashboard_insights(category: str, rates: list[dict], sector: list[dict]) -> dict:
    movers = sorted(rates, key=lambda r: abs(r["pct_change"]), reverse=True)
    top = movers[0] if movers else None
    ups = sum(1 for r in rates if r["trend"] == "up")
    downs = sum(1 for r in rates if r["trend"] == "down")
    total = len(rates) or 1
    volatility = mean([abs(r["pct_change"]) for r in rates]) if rates else 0

    if top and top["pct_change"] > 1:
        supply = (
            f"{top['item_name']} is leading today's move at +{top['pct_change']}%. "
            "Expect short-term pressure on nearby substitutes."
        )
    elif top and top["pct_change"] < -1:
        supply = (
            f"{top['item_name']} is cooling fastest at {top['pct_change']}%. "
            "Retail rates may soften if this trend sustains."
        )
    else:
        supply = "No major single-item shock detected today. Supply conditions look relatively balanced."

    if ups / total > 0.55:
        sentiment = "Broad upward pressure is visible across today's basket. Buyers should monitor high-turnover items closely."
    elif downs / total > 0.55:
        sentiment = "Market shows cooling momentum across most items with selective rebounds."
    elif volatility >= 3:
        sentiment = "Mixed but volatile board: both upward and downward pockets are active across categories."
    else:
        sentiment = "Sideways market with mild day-over-day movement and no dominant directional bias."

    return {
        "supply_alert": supply,
        "market_sentiment": sentiment,
        "top_movers": movers[:5],
        "up_count": ups,
        "down_count": downs,
        "flat_count": total - ups - downs,
        "volatility": round(volatility, 2),
    }


def _dashboard_context(conn, category: str, selected_date: str | None):
    if selected_date:
        latest = selected_date
    else:
        latest_row = conn.execute(
            "SELECT MAX(date) AS date FROM market_rates WHERE category = ?",
            (category,),
        ).fetchone()
        latest = latest_row["date"] if latest_row else None

    rates = _get_rates_with_trend(conn, category, latest) if latest else []

    dates = conn.execute(
        """SELECT DISTINCT date FROM market_rates
           WHERE category = ?
           ORDER BY date DESC LIMIT 30""",
        (category,),
    ).fetchall()

    sector = _sector_performance(conn, latest)
    series = _category_time_series(conn, category, latest, 365)
    insights = _dashboard_insights(category, rates, sector)

    vol = insights["volatility"]
    volatility_label = "Low" if vol < 1.5 else "Medium" if vol < 3.5 else "High"

    return {
        "category": category,
        "rates": rates,
        "latest_date": latest,
        "dates": [d["date"] for d in dates],
        "categories": CATEGORIES,
        "sector_performance": sector,
        "category_series": series,
        "insights": insights,
        "volatility_label": volatility_label,
    }


def _dashboard_page_context(conn, category: str, view: str):
    dates = conn.execute(
        """SELECT DISTINCT date FROM market_rates
           WHERE category = ?
           ORDER BY date DESC LIMIT 30""",
        (category,),
    ).fetchall()
    date_list = [d["date"] for d in dates]

    latest = date_list[0] if date_list else None
    yesterday = date_list[1] if len(date_list) > 1 else latest

    selected_date = latest
    if view == "yesterday" and yesterday:
        selected_date = yesterday

    base = _dashboard_context(conn, category, selected_date)

    weekly_points = base["category_series"][-7:] if base["category_series"] else []
    weekly_avg = round(mean([p["avg_price"] for p in weekly_points]), 2) if weekly_points else 0

    highest = sorted(base["rates"], key=lambda r: r["avg_price"], reverse=True)[:3]
    volatility_label = "Low" if base["insights"]["volatility"] < 1.5 else "Medium" if base["insights"]["volatility"] < 3.5 else "High"

    return {
        **base,
        "view": view,
        "available_dates": date_list,
        "highest_items": highest,
        "weekly_avg": weekly_avg,
        "volatility_label": volatility_label,
    }


# ── Scheduler for Daily Scraping ───────────────────────────────────────────────

def scheduled_scrape_job():
    """Background job to run scraper and OCR daily."""
    print("\n[SCHEDULED] Running daily scraper job...")
    try:
        new_images = run_scraper()
        run_ocr()
        print("[SCHEDULED] Daily job completed successfully.")
    except Exception as e:
        print(f"[SCHEDULED] Error during job: {e}")


# Initialize and start the scheduler
scheduler = BackgroundScheduler()
pakistan_tz = pytz.timezone("Asia/Karachi")

# Schedule to run every day at 12:00 PM Pakistan time
scheduler.add_job(
    scheduled_scrape_job,
    trigger=CronTrigger(hour=12, minute=0, timezone=pakistan_tz),
    id="daily_scrape",
    name="Daily scraper at 12 PM Pakistan time",
    replace_existing=True,
)
scheduler.start()

# Run scraper on startup
@app.on_event("startup")
async def startup_scrape():
    """Run scraper and OCR immediately on app startup."""
    print("\n[STARTUP] Running scraper on app startup...")
    try:
        run_scraper()
        run_ocr()
        print("[STARTUP] Startup scraper job completed successfully.")
    except Exception as e:
        print(f"[STARTUP] Error during startup job: {e}")


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/analytics", response_class=HTMLResponse)
def index(request: Request, category: str = "fruits"):
    with get_conn() as conn:
        context = _dashboard_context(conn, category, None)

    return templates.TemplateResponse("index.html", {
        "request": request,
        **context,
    })


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, category: str = "fruits", view: str = "today"):
    view = view if view in {"today", "yesterday", "weekly"} else "today"
    with get_conn() as conn:
        context = _dashboard_page_context(conn, category, view)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        **context,
    })


@app.get("/index")
def index_redirect(category: str = "fruits", view: str = "today"):
    return RedirectResponse(url=f"/dashboard?category={category}&view={view}", status_code=307)


@app.get("/date/{category}/{date}", response_class=HTMLResponse)
def by_date(request: Request, category: str, date: str):
    with get_conn() as conn:
        context = _dashboard_context(conn, category, date)

    return templates.TemplateResponse("index.html", {
        "request": request,
        **context,
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

    history = [dict(row) for row in history]

    series = []
    for h in history:
        avg = _avg_price(h.get("min_price"), h.get("max_price"))
        series.append({"date": h["date"], "avg_price": round(avg, 2)})

    pct_changes = []
    for i in range(1, len(series)):
        prev = series[i - 1]["avg_price"]
        curr = series[i]["avg_price"]
        if prev > 0:
            pct_changes.append(((curr - prev) / prev) * 100)

    latest_avg = series[-1]["avg_price"] if series else 0
    prev_avg = series[-2]["avg_price"] if len(series) > 1 else latest_avg
    latest_change = ((latest_avg - prev_avg) / prev_avg * 100) if prev_avg else 0

    sentiment = "Stable"
    if latest_change > 1:
        sentiment = "Rising"
    elif latest_change < -1:
        sentiment = "Cooling"

    # Extract units (usually consistent, we'll take the most recent one if present)
    unit = history[-1]["unit"] if history else ""

    return templates.TemplateResponse("trend.html", {
        "request": request,
        "category": category,
        "item_name": item_name,
        "history": history,
        "unit": unit,
        "history_series": series,
        "latest_avg": round(latest_avg, 2),
        "latest_change": round(latest_change, 2),
        "volatility": round(mean([abs(x) for x in pct_changes]), 2) if pct_changes else 0,
        "sentiment": sentiment,
    })


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Lahore Market Rates")
    print("=" * 50)

    # 1. Init DB
    init_db()

    # 2. Scrape new images on startup
    print("\n[STEP 1] Checking for new images...")
    new_images = run_scraper()

    # 3. OCR any new images
    print("\n[STEP 2] Processing new images with Gemini...")
    run_ocr()

    # 4. Start web server with scheduler running in background
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"\n[STEP 3] Daily scraper scheduled for 12:00 PM Pakistan time (Asia/Karachi)")
    print(f"[STEP 4] Starting web app at http://{host}:8000")
    print("=" * 50)
    
    try:
        uvicorn.run(app, host=host, port=8000)
    finally:
        # Shutdown scheduler on exit
        if scheduler.running:
            scheduler.shutdown()