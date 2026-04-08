# COA Report Scraper

## Project Overview
A personal scraping tool that collects Annual Audit Report (AAR) PDF and ZIP files from the Philippine Commission on Audit (COA) website and tracks them in a CSV database. Designed to run periodically to pick up new reports.

**Owner:** Jaemark Tordecilla  
**Purpose:** Build and maintain a local repository of COA audit reports for the COA Beat Assistant.

## Target URLs
```
NGA:  https://www.coa.gov.ph/audit-audit-reports/aar-ngs/
LGU:  https://www.coa.gov.ph/reports/annual-audit-reports/aar-local-government-units/
GOCC: https://www.coa.gov.ph/reports/annual-audit-reports/aar-government-owned-and-or-controlled-corporations/
```

Each page has year tabs (2013–2024). Each year lists agencies with links to PDF and ZIP reports.

## Site Challenges
- **Cloudflare protection** — must use a real browser, not plain HTTP requests
- **Lazy loading** — year tab content loads dynamically via JavaScript; must click each tab and wait for content
- **Inconsistent formatting** — agency names and PDF link structure vary across years and categories

## Stack
- **Python 3.10+**
- **Playwright** (`playwright` + `playwright install chromium`) — real browser for Cloudflare bypass and JS rendering
- **csv** — stdlib, no extras needed
- **pathlib / os** — file management
- No framework, no ORM, no task queue. Keep it a single script with helper functions.

## File Structure
```
coa-scraper/
├── CLAUDE.md
├── scraper.py          ← Main script, entry point
├── downloader.py       ← PDF download logic (optional split)
├── data/
│   ├── reports.csv     ← The database
│   └── pdfs/           ← Downloaded files: pdfs/{category}/{year}/{agency_slug}/file.{zip|pdf}
├── .chrome_profile/    ← Persistent Chrome profile for Cloudflare bypass
├── logs/
│   └── scraper.log     ← Append-only run log
├── requirements.txt
└── .gitignore          ← Ignore logs/
```

## CSV Schema (`data/reports.csv`)
One row per discovered PDF. This is the database — do not restructure it mid-project.

```
id, category, year, agency, title, url, file_path, status, first_seen, downloaded_at, error_msg
```

| Field | Values / Notes |
|---|---|
| `id` | Auto-increment integer |
| `category` | `NGA`, `LGU`, or `GOCC` |
| `year` | e.g. `2023` |
| `agency` | Agency name as scraped |
| `title` | Report title or filename label |
| `url` | Full URL to PDF — **must be unique** |
| `file_path` | Local path after download, empty until downloaded |
| `status` | `discovered` → `downloaded` / `failed` / `skipped` |
| `first_seen` | ISO datetime |
| `downloaded_at` | ISO datetime, empty until downloaded |
| `error_msg` | Last error message if status is `failed` |

## Key Rules

### Scraping
- Load `reports.csv` into a set of known URLs at startup. Skip any URL already in the set.
- For each category page: launch Playwright, click every year tab, wait for content to load, extract all PDF links.
- Insert newly discovered URLs immediately with `status = discovered`.
- Random delay of 3–6 seconds between tab clicks to avoid triggering rate limits.
- Use a realistic User-Agent string (copy from a real Chrome browser).
- If a year tab fails to load after 10 seconds, log the error and continue to the next tab — never crash.

### Downloading
- Only download files with `status = discovered` (or `failed` with fewer than 3 prior attempts).
- Each agency has two files: a **ZIP** (full consolidated audit report) and a **PDF** (executive summary). Both are discovered and downloaded.
- Save to `data/pdfs/{CATEGORY}/{year}/{agency_slug}/original_filename.{ext}` — extension is whatever the file actually is (`.zip` or `.pdf`).
- `agency_slug`: lowercase, spaces → underscores, strip special characters.
- Stream downloads to handle large files without memory issues.
- On failure: update `status = failed`, write error to `error_msg`. Retry on next run.
- On HTTP 404: set `status = skipped`, do not retry.

### CSV Handling
- Always read the full CSV at startup, write the full CSV at shutdown.
- Use Python's `csv.DictReader` / `csv.DictWriter` with `extrasaction='ignore'`.
- Never append mid-run — hold state in memory (a list of dicts), write once at the end.
- If the CSV doesn't exist yet, create it with headers on first run.

### Logging
- Print a one-line summary to console for each action (tab scraped, PDF found, download result).
- Append the same lines to `logs/scraper.log` with timestamps.
- End of run: print total new discovered, downloaded, failed counts.

## CLI Usage
```bash
# Full run: scrape all categories, download new PDFs
python scraper.py

# Discover only — no downloads
python scraper.py --discover-only

# Scrape one category
python scraper.py --category LGU

# Retry failed downloads only
python scraper.py --retry-failed

# Skip scraping, download all discovered/failed entries
python scraper.py --download-only

# Show summary stats from CSV
python scraper.py --stats
```

## Requirements (`requirements.txt`)
```
playwright
```
Install: `pip install playwright && playwright install chromium`

## References
- data/reports.csv — The live database
- logs/scraper.log — Run history