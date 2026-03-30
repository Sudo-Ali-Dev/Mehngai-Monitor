# Mehngai Monitor

Mehngai Monitor collects daily Lahore market rates for fruits, vegetables, and poultry, then shows them in a web dashboard.

## What It Does

- Scrapes official Lahore market rate list images
- Runs OCR with Gemini and normalizes extracted rows
- Stores clean data in SQLite
- Serves dashboard, analytics, and item trend pages with FastAPI

## Quick Start

```bash
git clone <your-repo-url>
cd "Mehngai Monitor"
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a .env file in the project root:

```env
GEMINI_API_KEY=your_api_key_here
```

Run the app:

```bash
python main.py
```

Open http://127.0.0.1:8000

## Common Commands

```bash
python scraper.py
python ocr.py
python -m unittest test_normalizer -v
```

## Tech

- FastAPI + Jinja2
- SQLite
- APScheduler
- requests + BeautifulSoup
- Tailwind CSS + Plotly

## Known Limitations

- The website is not mobile optimized yet.
- OCR quality depends on source image quality and Gemini extraction output.

## Data Source

https://lahore.punjab.gov.pk/market_rates

## Deployment

See DEPLOYMENT.md for Linux/systemd setup.
