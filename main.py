import os
from urllib.parse import quote, urlencode
from xml.sax.saxutils import escape

from dotenv import load_dotenv

load_dotenv()  # Loads GEMINI_API_KEY from .env file

from database import init_db
from scraper import run_scraper
from ocr import run_ocr
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from database import get_conn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from statistics import mean

app = FastAPI()
templates = Jinja2Templates(directory="templates")

CATEGORIES = ["fruits", "vegetables", "poultry"]
SITE_NAME = os.environ.get("SITE_NAME", "Mehngai Monitor")
SITE_BASE_URL = os.environ.get("SITE_BASE_URL", "http://mehngai.duckdns.org").rstrip("/")
SITE_LANG = os.environ.get("SITE_LANG", "en")
SITE_LOCALE = os.environ.get("SITE_LOCALE", "en_PK")
TWITTER_CARD = os.environ.get("TWITTER_CARD", "summary")


def _get_base_url(request: Request) -> str:
    base_url = SITE_BASE_URL.strip()
    if base_url:
        return base_url
    return str(request.base_url).rstrip("/")


def _encode_path_segment(segment: str) -> str:
    return quote(segment, safe="")


def _build_url(base_url: str, path: str, params: dict | None = None) -> str:
    url = f"{base_url}{path}"
    if params:
        clean_params = {k: v for k, v in params.items() if v is not None}
        query = urlencode(clean_params)
        if query:
            url = f"{url}?{query}"
    return url


def _seo_webpage_structured_data(
    title: str,
    description: str,
    url: str,
    base_url: str,
    about: dict | None = None,
) -> dict:
    data = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": title,
        "description": description,
        "url": url,
        "inLanguage": SITE_LANG,
        "isPartOf": {
            "@type": "WebSite",
            "name": SITE_NAME,
            "url": f"{base_url}/",
        },
    }
    if about:
        data["about"] = about
    return data


def _build_seo_context(
    request: Request,
    title: str,
    description: str,
    path: str,
    params: dict | None = None,
    og_type: str = "website",
    about: dict | None = None,
    structured_data: dict | None = None,
) -> dict:
    base_url = _get_base_url(request)
    canonical = _build_url(base_url, path, params)
    if structured_data is None:
        structured_data = _seo_webpage_structured_data(
            title,
            description,
            canonical,
            base_url,
            about=about,
        )

    return {
        "title": title,
        "description": description,
        "canonical": canonical,
        "site_name": SITE_NAME,
        "base_url": base_url,
        "og_type": og_type,
        "og_locale": SITE_LOCALE,
        "twitter_card": TWITTER_CARD,
        "structured_data": structured_data,
    }


def _sitemap_entry(
    loc: str,
    lastmod: str | None = None,
    changefreq: str | None = None,
    priority: str | None = None,
) -> str:
    parts = [f"<url><loc>{escape(loc)}</loc>"]
    if lastmod:
        parts.append(f"<lastmod>{escape(lastmod)}</lastmod>")
    if changefreq:
        parts.append(f"<changefreq>{escape(changefreq)}</changefreq>")
    if priority:
        parts.append(f"<priority>{escape(priority)}</priority>")
    parts.append("</url>")
    return "".join(parts)


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

    category_label = category.capitalize()
    latest_date = context.get("latest_date")
    date_suffix = f" for {latest_date}" if latest_date else ""
    seo_title = f"{SITE_NAME} - {category_label} price analytics"
    seo_description = (
        f"Lahore {category_label} price analytics with volatility, sector performance, "
        f"and top movers{date_suffix}."
    )
    seo_about = {
        "@type": "Thing",
        "name": f"{category_label} market prices",
    }
    seo = _build_seo_context(
        request,
        seo_title,
        seo_description,
        "/analytics",
        params={"category": category},
        about=seo_about,
    )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            **context,
            "seo": seo,
        },
    )


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, category: str = "fruits", view: str = "today"):
    view = view if view in {"today", "yesterday", "weekly"} else "today"
    with get_conn() as conn:
        context = _dashboard_page_context(conn, category, view)

    category_label = category.capitalize()
    latest_date = context.get("latest_date")
    view_label = {"today": "Today", "yesterday": "Yesterday", "weekly": "Weekly"}.get(view, "Today")
    date_suffix = f" for {latest_date}" if latest_date else ""
    seo_title = f"{SITE_NAME} - {category_label} daily prices ({view_label})"
    seo_description = (
        f"Lahore {category_label} daily prices with movers and volatility. "
        f"View: {view_label}{date_suffix}."
    )
    seo_about = {
        "@type": "Thing",
        "name": f"{category_label} daily prices",
    }
    seo = _build_seo_context(
        request,
        seo_title,
        seo_description,
        "/dashboard",
        params={"category": category, "view": view},
        about=seo_about,
    )

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "request": request,
            **context,
            "seo": seo,
        },
    )


@app.get("/index")
def index_redirect(category: str = "fruits", view: str = "today"):
    return RedirectResponse(url=f"/dashboard?category={category}&view={view}", status_code=307)


@app.get("/date/{category}/{date}", response_class=HTMLResponse)
def by_date(request: Request, category: str, date: str):
    with get_conn() as conn:
        context = _dashboard_context(conn, category, date)

    category_label = category.capitalize()
    seo_title = f"{SITE_NAME} - {category_label} prices on {date}"
    seo_description = f"Lahore {category_label} market prices for {date}."
    seo_about = {
        "@type": "Thing",
        "name": f"{category_label} market prices",
    }
    seo = _build_seo_context(
        request,
        seo_title,
        seo_description,
        f"/date/{category}/{date}",
        about=seo_about,
    )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            **context,
            "seo": seo,
        },
    )


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

    category_label = category.capitalize()
    seo_title = f"{item_name} price trend - {SITE_NAME}"
    seo_description = (
        f"Historical {category_label} price trend for {item_name} in Lahore. "
        f"Latest average PKR {round(latest_avg, 2)} with {sentiment.lower()} momentum."
    )
    item_slug = _encode_path_segment(item_name)
    seo_about = {
        "@type": "Product",
        "name": item_name,
        "category": category_label,
    }
    seo = _build_seo_context(
        request,
        seo_title,
        seo_description,
        f"/trend/{category}/{item_slug}",
        og_type="article",
        about=seo_about,
    )

    return templates.TemplateResponse(
        request=request,
        name="trend.html",
        context={
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
            "seo": seo,
        },
    )


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt(request: Request):
    base_url = _get_base_url(request)
    lines = [
        "User-agent: *",
        "Allow: /",
        f"Sitemap: {base_url}/sitemap.xml",
    ]
    return "\n".join(lines)


@app.get("/sitemap.xml", response_class=Response)
def sitemap_xml(request: Request):
    base_url = _get_base_url(request)
    urls: list[str] = []

    with get_conn() as conn:
        for cat in CATEGORIES:
            latest_row = conn.execute(
                "SELECT MAX(date) AS date FROM market_rates WHERE category = ?",
                (cat,),
            ).fetchone()
            latest_date = latest_row["date"] if latest_row else None

            for view in ("today", "yesterday", "weekly"):
                loc = _build_url(base_url, "/dashboard", {"category": cat, "view": view})
                urls.append(_sitemap_entry(loc, latest_date, "daily", "0.9"))

            loc = _build_url(base_url, "/analytics", {"category": cat})
            urls.append(_sitemap_entry(loc, latest_date, "daily", "0.8"))

            date_rows = conn.execute(
                """SELECT DISTINCT date FROM market_rates
                   WHERE category = ?
                   ORDER BY date DESC LIMIT 30""",
                (cat,),
            ).fetchall()
            for row in date_rows:
                date_value = row["date"]
                loc = _build_url(base_url, f"/date/{cat}/{date_value}")
                urls.append(_sitemap_entry(loc, date_value, "daily", "0.7"))

            item_rows = conn.execute(
                """SELECT item_name, MAX(date) AS date
                   FROM market_rates
                   WHERE category = ?
                   GROUP BY item_name""",
                (cat,),
            ).fetchall()
            for row in item_rows:
                item_name = row["item_name"]
                item_slug = _encode_path_segment(item_name)
                loc = _build_url(base_url, f"/trend/{cat}/{item_slug}")
                urls.append(_sitemap_entry(loc, row["date"], "weekly", "0.6"))

    xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    xml += "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
    xml += "".join(urls)
    xml += "</urlset>"
    return Response(content=xml, media_type="application/xml")


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Mehngai Monitor")
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