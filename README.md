# COA Annual Audit Report Scraper

Scrapes Annual Audit Report (AAR) PDF and ZIP files from the [Philippine Commission on Audit (COA)](https://www.coa.gov.ph) website and tracks them in a local CSV database. Designed to run periodically to pick up new reports as they are published.

## What it collects

| Category | Description |
|---|---|
| **NGA** | National Government Agencies |
| **LGU** | Local Government Units |
| **GOCC** | Government-Owned and/or Controlled Corporations |

Each agency has two files per year: a **ZIP** (full consolidated audit report) and a **PDF** (executive summary).

## Requirements

- Python 3.10+
- Google Chrome (installed, not just Chromium)
- [Playwright](https://playwright.dev/python/)

```bash
pip install playwright
playwright install chromium
```

## Setup

1. **Clone the repo**

   ```bash
   git clone https://github.com/tordecilla/coa-scraper.git
   cd coa-scraper
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

3. **Run Chrome once to pass Cloudflare**

   On first run, Chrome will open a Cloudflare challenge page for the COA website. Solve it once manually — the scraper stores the session in `.chrome_profile/` and reuses it automatically on subsequent runs.

## Usage

```bash
# Full run: discover new reports and download them
python scraper.py

# Discover only — find new links without downloading
python scraper.py --discover-only

# Scrape one category only
python scraper.py --category NGA
python scraper.py --category LGU
python scraper.py --category GOCC

# Discover reports from all years (default: most recent year only)
python scraper.py --all-years

# Skip scraping, download all pending/failed entries
python scraper.py --download-only

# Retry previously failed downloads
python scraper.py --retry-failed

# Re-scrape only years that have failed entries (picks up files at new URLs)
python scraper.py --rediscover-failed

# Print summary stats from the CSV
python scraper.py --stats
```

## File structure

```
coa-scraper/
├── scraper.py              # Main script
├── generate_catalogs.py    # Generates catalog CSVs from reports.csv
├── config.example.py       # Template — copy to config.py and fill in your paths
├── requirements.txt
├── data/
│   ├── reports.csv         # Master database — one row per discovered file
│   ├── catalog_nga.csv     # Generated catalog for National Government Agencies
│   ├── catalog_lgu.csv     # Generated catalog for Local Government Units
│   └── pdfs/               # Downloaded files
│       ├── NGA/{year}/{agency}/
│       ├── LGU/{year}/{agency}/
│       └── GOCC/{year}/{agency}/
└── logs/
    └── scraper.log
```

## Database schema (`data/reports.csv`)

One row per discovered file.

| Field | Description |
|---|---|
| `id` | Auto-increment integer |
| `category` | `NGA`, `LGU`, or `GOCC` |
| `year` | e.g. `2024` |
| `agency` | Agency grouping as scraped from the COA website |
| `title` | Report title |
| `url` | Source URL — unique per row |
| `file_path` | Local path after download (empty until downloaded) |
| `status` | `discovered` → `downloaded` / `failed` / `skipped` |
| `first_seen` | ISO 8601 datetime |
| `downloaded_at` | ISO 8601 datetime (empty until downloaded) |
| `error_msg` | Last error if status is `failed` |

## Catalog CSVs (`generate_catalogs.py`)

Run `python generate_catalogs.py` to generate two structured catalogs from `reports.csv`:

- **`data/catalog_nga.csv`** — NGA reports with columns: `Year, Main Agency, Agency, Description, File Type, Filename, Path, Contents, Source URL, Extraction Notes`
- **`data/catalog_lgu.csv`** — LGU reports with columns: `Year, Region, Province, LGU Name, LGU Type, Description, File Type, Filename, Path, Contents, Source URL, Extraction Notes`

`Contents` lists the files inside each ZIP (pipe-separated). `Extraction Notes` flags any rows where automatic field extraction was uncertain.

## How Cloudflare bypass works

The COA website is protected by Cloudflare, which blocks standard HTTP requests and headless browsers. This scraper launches Chrome with automation detection flags disabled and maintains a dedicated browser profile in `.chrome_profile/`. On first run, Chrome may show a Cloudflare challenge — solve it manually once, and subsequent runs reuse the stored session automatically.

The scraper does not use proxies, CAPTCHA-solving services, or any credentials.

## Notes

- Files are streamed to disk to handle large ZIPs without memory issues.
- The scraper saves progress to CSV after each year (scraping) and after each file (downloading), so interrupted runs pick up where they left off.
- HTTP 404 responses mark a file as `skipped` (not retried). Cloudflare transient errors (521–524) are retried with exponential backoff.
- ZIPs are kept as-is; no automatic extraction is performed.
