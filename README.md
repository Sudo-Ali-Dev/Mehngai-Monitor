# Mehngai Monitor

Mehngai Monitor tracks daily Lahore market rates for fruits, vegetables, and poultry, then turns raw image sheets into a searchable analytics dashboard.

The app scrapes official rate-list images, runs OCR + normalization, stores clean records in SQLite, and serves trend views with FastAPI.

## Highlights

- Daily automated scraping from official Lahore market pages
- OCR extraction via Gemini API from Urdu image tables
- Normalization pipeline for units, names, and noisy prices
- Duplicate protection for images and market rows
- Dashboard for latest rates with trend labels
- Analytics board with category-level volatility chart
- Item-level historical trend pages
- Background scheduler for daily refresh at 12:00 PM Asia/Karachi

## Stack

- Backend: FastAPI, Jinja2
- Data: SQLite
- Scheduling: APScheduler
- Scraping: requests, BeautifulSoup
- OCR/LLM: Gemini API
- Frontend: Tailwind CSS + Plotly

## Project Layout

- main.py: FastAPI app, routes, analytics context, scheduler
- scraper.py: category page crawler + image downloader
- ocr.py: Gemini OCR call + parsing + DB writes
- normalizer.py: cleanup and canonicalization logic
- database.py: SQLite schema and data access helpers
- templates/: dashboard, analytics, and trend pages
- data/: SQLite DB and normalization/canonical logs
- images/: downloaded daily source sheets

## Quick Start (Local)

### 1. Clone and enter project

```bash
git clone <your-repo-url>
cd "Mehngai Monitor"
```

### 2. Create virtual environment

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment

Create a .env file in project root:

```env
GEMINI_API_KEY=your_api_key_here
```

Note: OCR needs GEMINI_API_KEY. The web app can still start without it, but OCR ingestion will be skipped/fail.

### 5. Run app

```bash
python main.py
```

Open:

- http://127.0.0.1:8000

## Useful Commands

Run only scraper:

```bash
python scraper.py
```

Run only OCR on pending images:

```bash
python ocr.py
```

Run tests:

```bash
python -m unittest test_normalizer -v
```

## Routes

- / or /dashboard: primary dashboard (today/yesterday/weekly switch)
- /analytics: category analytics board
- /date/{category}/{date}: analytics snapshot for a selected day
- /trend/{category}/{item_name}: item-level historical trend

## Deployment

See DEPLOYMENT.md for full Linux/systemd deployment steps.

## Public Repo Checklist

Before publishing:

- Ensure .env is not committed
- Rotate any previously exposed API keys
- Confirm local DB snapshots/log files are safe to share
- Verify service paths in mehngai-monitor.service and setup-server.sh

## Data Source

District Lahore, Government of the Punjab market rate pages:

- https://lahore.punjab.gov.pk/market_rates

## Notes

This project is focused on practical price monitoring and rapid iteration. OCR quality can vary by source image quality, so normalization and canonicalization are designed to minimize noisy output over time.
