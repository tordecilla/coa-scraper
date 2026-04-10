"""
COA Report Scraper
Scrapes Annual Audit Reports from the Philippine Commission on Audit website.

Site uses WP File Download (wpfd) plugin. Structure:
  Root category → Year subcategories → Agency subcategories → Files
All data is available via JSON API — no tab clicking needed.

Cloudflare bypass: persistent Chrome profile with automation flags disabled.
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import signal
import time
from datetime import datetime, timezone
UTC = timezone.utc
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CATEGORIES = {
    "NGA":  {"url": "https://www.coa.gov.ph/audit-audit-reports/aar-ngs/",              "root_id": 49},
    "LGU":  {"url": "https://www.coa.gov.ph/reports/annual-audit-reports/aar-local-government-units/", "root_id": 167},
    "GOCC": {"url": "https://www.coa.gov.ph/reports/annual-audit-reports/aar-government-owned-and-or-controlled-corporations/", "root_id": 199},
}

DATA_DIR     = Path("data")
PDF_DIR      = DATA_DIR / "pdfs"
CSV_PATH     = DATA_DIR / "reports.csv"
LOG_DIR      = Path("logs")
LOG_PATH     = LOG_DIR / "scraper.log"
PROFILE_DIR  = Path(".chrome_profile")

AJAX_BASE = "https://www.coa.gov.ph/wp-admin/admin-ajax.php"

CSV_FIELDS = [
    "id", "category", "year", "agency", "title",
    "url", "file_path", "status", "first_seen", "downloaded_at", "error_msg",
]

CF_POLL_INTERVAL     = 2    # seconds between title polls
CF_MAX_WAIT          = 60   # seconds max to wait for Cloudflare to clear
DOWNLOAD_DELAY_LOW   = 1
DOWNLOAD_DELAY_HIGH  = 3
PAGE_DEFAULT_TIMEOUT = 30_000  # ms — caps all Playwright calls so nothing hangs forever

# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------

for d in (DATA_DIR, PDF_DIR, LOG_DIR, PROFILE_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Signal handling — force-exit on Ctrl+C even when Playwright is blocking
# ---------------------------------------------------------------------------

_active_ctx = None   # set to the live BrowserContext so SIGINT can close it


def _install_sigint_handler() -> None:
    """
    Replace the default SIGINT handler with one that force-closes the browser
    and calls os._exit().  We use os._exit() (not sys.exit()) because the
    main thread may be blocked inside a Playwright C extension call that will
    never check for a pending KeyboardInterrupt.
    """
    def _handler(sig, frame):
        log.warning("Interrupted — closing browser and saving progress…")
        os._exit(130)   # 130 = 128 + SIGINT; kill immediately — ctx.close() blocks

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_csv() -> list[dict]:
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_csv(rows: list[dict]) -> None:
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def next_id(rows: list[dict]) -> int:
    return max((int(r["id"]) for r in rows if r.get("id")), default=0) + 1


def known_urls(rows: list[dict]) -> set[str]:
    return {r["url"] for r in rows}


# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------

MAX_SLUG_LEN = 60  # keeps full path under Windows MAX_PATH (260 chars)

def agency_slug(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s_]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    name = name or "unknown"
    if len(name) > MAX_SLUG_LEN:
        name = name[:MAX_SLUG_LEN].rstrip("_")
    return name


# ---------------------------------------------------------------------------
# Browser helpers
# ---------------------------------------------------------------------------

def _setup_profile_prefs(profile_dir: Path) -> None:
    """
    Write Chrome prefs so PDFs are downloaded instead of opened in the viewer.
    """
    prefs_dir = profile_dir / "Default"
    prefs_dir.mkdir(parents=True, exist_ok=True)
    prefs_path = prefs_dir / "Preferences"
    try:
        prefs = json.loads(prefs_path.read_text(encoding="utf-8")) if prefs_path.exists() else {}
    except (json.JSONDecodeError, OSError):
        prefs = {}
    prefs.setdefault("plugins", {})["always_open_pdf_externally"] = True
    prefs_path.write_text(json.dumps(prefs), encoding="utf-8")


def launch_browser(pw):
    """Launch a persistent Chrome context that bypasses Cloudflare detection."""
    _setup_profile_prefs(PROFILE_DIR)
    ctx = pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR.resolve()),
        channel="chrome",
        headless=False,
        viewport={"width": 1280, "height": 900},
        args=["--disable-blink-features=AutomationControlled"],
        ignore_default_args=["--enable-automation"],
    )
    # Cap every Playwright call so a frozen browser can't zombie the process.
    # The SIGINT handler is the last-resort kill; this prevents reaching it.
    ctx.set_default_timeout(PAGE_DEFAULT_TIMEOUT)
    return ctx


def wait_past_cloudflare(page, url: str) -> bool:
    """
    Navigate to url and poll until Cloudflare challenge is resolved.
    Returns True if page loaded successfully, False on timeout or error.
    """
    log.info(f"Loading: {url}")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as exc:
        log.error(f"Navigation error for {url}: {exc}")
        return False

    elapsed = 0
    while elapsed < CF_MAX_WAIT:
        time.sleep(CF_POLL_INTERVAL)
        elapsed += CF_POLL_INTERVAL
        try:
            title = page.title()
            tl = title.lower()
            if "just a moment" in tl:
                log.debug(f"  [{elapsed}s] Cloudflare challenge pending…")
                continue
            # CF error pages (522 connection timeout, 503, etc.) are not
            # recoverable — treat them the same as a timeout.
            if any(code in tl for code in ("522", "503", "error", "timed out", "connection timeout")):
                log.error(f"  [{elapsed}s] Cloudflare error page: {title}")
                return False
            log.info(f"Page ready [{elapsed}s]: {title}")
            return True
        except Exception:
            # Navigation still in progress — keep waiting
            pass

    log.error(f"Cloudflare challenge did not clear after {CF_MAX_WAIT}s")
    return False


class BrowserClosedError(RuntimeError):
    """Raised when the browser/page is closed unexpectedly."""


class APIEmptyError(RuntimeError):
    """Raised when an API call returns an empty response — likely CF session expiry."""


def api_get(page, url: str) -> dict | None:
    """
    Fetch a JSON URL using the browser's fetch() so session cookies are included.
    Returns parsed dict or None on error.
    Raises BrowserClosedError if the browser was closed mid-run.
    Raises APIEmptyError if the response is empty (Cloudflare session likely expired).
    """
    try:
        raw = page.evaluate(f"""async () => {{
            const r = await fetch("{url}", {{credentials: "include"}});
            return await r.text();
        }}""")
        if not raw or not raw.strip():
            raise APIEmptyError(f"Empty response from {url}")
        return json.loads(raw.strip())
    except (APIEmptyError, BrowserClosedError):
        raise
    except Exception as exc:
        msg = str(exc)
        if "Target page, context or browser has been closed" in msg:
            raise BrowserClosedError(msg)
        log.error(f"API error for {url}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def fetch_subcategories(page, cat_id: int) -> list[dict]:
    """Return direct subcategories of cat_id from the wpfd categories API."""
    url = (
        f"{AJAX_BASE}?juwpfisadmin=false&action=wpfd"
        f"&task=categories.display&view=categories&id={cat_id}"
    )
    data = api_get(page, url)
    if not data:
        return []
    return data.get("categories", [])


def fetch_files(page, cat_id: int, root_id: int) -> list[dict]:
    """Return all files in cat_id from the wpfd files API."""
    url = (
        f"{AJAX_BASE}?juwpfisadmin=false&action=wpfd"
        f"&task=files.display&view=files&id={cat_id}"
        f"&rootcat={root_id}&orderCol=ordering&orderDir=asc&show_files=1"
    )
    data = api_get(page, url)
    if not data:
        return []
    return data.get("files", [])


def _make_row(cat_key, year_name, agency_name, f, existing_urls) -> dict | None:
    """Build a row dict for a file entry, or None if already known."""
    download_url = f.get("linkdownload", "")
    if not download_url or download_url in existing_urls:
        return None
    existing_urls.add(download_url)
    return {
        "id":            "",
        "category":      cat_key,
        "year":          year_name,
        "agency":        agency_name,
        "title":         f.get("post_title", ""),
        "url":           download_url,
        "file_path":     "",
        "status":        "discovered",
        "first_seen":    datetime.now(UTC).isoformat(timespec="seconds"),
        "downloaded_at": "",
        "error_msg":     "",
    }


def scrape_category(page, cat_key: str, cat_cfg: dict, existing_urls: set[str],
                    all_years: bool = False, target_years: set[str] | None = None):
    """
    Generator — scrapes one section (NGA/LGU/GOCC).
    Yields (year_name, [row, ...]) after each year so the caller can save incrementally.
    Raises BrowserClosedError if the browser is closed mid-run.
    By default only checks the most recent year; pass all_years=True to check all,
    or target_years={'2023','2022'} to check specific years only.
    """
    root_id = cat_cfg["root_id"]
    url     = cat_cfg["url"]

    if not wait_past_cloudflare(page, url):
        log.error(f"[{cat_key}] Skipping — could not load page")
        return

    time.sleep(2)

    years = fetch_subcategories(page, root_id)
    log.info(f"[{cat_key}] {len(years)} year(s) found")

    # Filter to valid year categories, sort descending
    year_cats = [
        yc for yc in years
        if re.fullmatch(r"20\d{2}", str(yc.get("name", "")))
    ]
    year_cats.sort(key=lambda yc: yc.get("name", ""), reverse=True)
    if target_years is not None:
        year_cats = [yc for yc in year_cats if yc.get("name") in target_years]
        log.info(f"[{cat_key}] Targeting years: {sorted(target_years, reverse=True)}")
    elif not all_years:
        year_cats = year_cats[:1]
        log.info(f"[{cat_key}] Checking most recent year: "
                 f"{[yc.get('name') for yc in year_cats]} (use --all-years to check all)")

    for year_cat in year_cats:
        year_id   = year_cat.get("term_id")
        year_name = year_cat.get("name", str(year_id))

        log.info(f"[{cat_key}] Year {year_name}: fetching agencies…")
        year_rows: list[dict] = []

        # Fetch agencies for this year, reloading CF session if the response is empty.
        try:
            agencies = fetch_subcategories(page, year_id)
        except APIEmptyError:
            log.warning(f"[{cat_key}] CF session expired fetching agencies for {year_name} — reloading…")
            if _reload_cf_session(page, url):
                time.sleep(2)
                try:
                    agencies = fetch_subcategories(page, year_id)
                except APIEmptyError:
                    log.error(f"[{cat_key}] Still empty after CF reload — skipping year {year_name}")
                    continue
            else:
                log.error(f"[{cat_key}] CF reload failed — skipping year {year_name}")
                continue

        for agency_cat in agencies:
            agency_id   = agency_cat.get("term_id")
            agency_name = agency_cat.get("name", str(agency_id))
            agency_rows: list[dict] = []

            def walk(cat_id: int, path: list[str], depth: int = 0) -> None:
                """Recursively collect files under cat_id, building agency name from path."""
                if depth > 6:
                    log.warning(f"[{cat_key}] Max depth reached at {' / '.join(path)}, skipping")
                    return
                label = " / ".join(path)
                for f in fetch_files(page, cat_id, root_id):
                    row = _make_row(cat_key, year_name, label, f, existing_urls)
                    if row:
                        agency_rows.append(row)
                        log.info(f"[{cat_key}] {year_name} / {label}: {f.get('post_title')}")
                for sub_cat in fetch_subcategories(page, cat_id):
                    sub_name = sub_cat.get("name", str(sub_cat.get("term_id")))
                    walk(sub_cat.get("term_id"), path + [sub_name], depth + 1)

            # Retry walk once with a CF reload if the session expired mid-agency.
            # Already-seen URLs are deduplicated via existing_urls, so a partial
            # walk followed by a retry won't produce duplicate rows.
            for attempt in range(2):
                try:
                    walk(agency_id, [agency_name])
                    break
                except APIEmptyError:
                    if attempt == 0:
                        log.warning(
                            f"[{cat_key}] CF session expired scraping {agency_name} — reloading…"
                        )
                        if _reload_cf_session(page, url):
                            time.sleep(2)
                        else:
                            log.error(f"[{cat_key}] CF reload failed — skipping {agency_name}")
                            break
                    else:
                        log.error(
                            f"[{cat_key}] Still empty after CF reload — skipping {agency_name}"
                        )

            # Yield per agency so the caller saves to CSV after each one —
            # prevents losing all discoveries if the run is interrupted mid-year.
            yield year_name, agency_rows


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------

def _dest_for(row: dict) -> tuple[Path, Path]:
    url      = row["url"]
    filename = url.split("/")[-1] or "report"
    dest_dir = PDF_DIR / row["category"] / row["year"] / agency_slug(row["agency"])
    return dest_dir, dest_dir / filename


def _mark_downloaded(row: dict, dest_path: Path, size: int) -> None:
    row["file_path"]     = str(dest_path)
    row["status"]        = "downloaded"
    row["downloaded_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    row["error_msg"]     = ""
    log.info(f"  Saved ({size:,} bytes): {dest_path}")


# HTTP status codes that mean the file is permanently unavailable.
# 404 = gone for good.  521-524 are temporary server outages — handled by the
# transient-retry loop, NOT skipped.
_SKIP_STATUSES = {404}

# 521-524 are Cloudflare errors meaning COA's origin server is temporarily down.
# These are worth retrying indefinitely with backoff.
_TRANSIENT_STATUSES = {521, 522, 523, 524}

RETRY_WAIT_INIT =   60   # first retry wait (seconds)
RETRY_WAIT_MAX  =  900   # cap at 15 minutes


def _is_transient_error(error_msg: str) -> bool:
    """True when the error is a temporary server outage worth retrying forever."""
    m = error_msg.lower()
    # PDF path: "HTTP 522" etc.
    if any(m == f"http {c}" for c in _TRANSIENT_STATUSES):
        return True
    # ZIP path: server returned a CF error HTML page
    if "server returned page" in m and any(str(c) in m for c in _TRANSIENT_STATUSES):
        return True
    return False


# Errors that are clearly NOT caused by a Cloudflare session expiry.
# After these failures we should NOT burn time reloading the CF session.
def _is_cf_unrelated(error_msg: str) -> bool:
    m = error_msg.lower()
    # Local filesystem errors (e.g. path too long)
    if "no such file or directory" in m or "errno" in m:
        return True
    # Any plain HTTP error — server-side, not a CF session issue
    if m.startswith("http "):
        return True
    # Transient server errors: reloading CF session won't help while site is down
    if _is_transient_error(error_msg):
        return True
    return False


CF_RELOAD_RETRIES    = 3   # max attempts before giving up and continuing anyway
CF_RELOAD_RETRY_WAIT = 30  # seconds between retry attempts


def _reload_cf_session(page, url: str) -> bool:
    """
    Try to reload the Cloudflare session up to CF_RELOAD_RETRIES times.
    Returns True if a reload succeeds.  On total failure logs a warning and
    returns False — callers should continue rather than abort.
    """
    for attempt in range(1, CF_RELOAD_RETRIES + 1):
        if attempt > 1:
            log.info(f"CF reload attempt {attempt}/{CF_RELOAD_RETRIES} "
                     f"(waiting {CF_RELOAD_RETRY_WAIT}s)…")
            time.sleep(CF_RELOAD_RETRY_WAIT)
        if wait_past_cloudflare(page, url):
            return True
    log.warning(
        f"CF session reload failed after {CF_RELOAD_RETRIES} attempts — "
        "continuing with existing session"
    )
    return False


def download_file(ctx, page, row: dict) -> None:
    """
    Download a report file via the browser (page.goto + expect_download).
    Both PDFs and ZIPs go through Chrome's network stack so Cloudflare sees
    a real browser TLS fingerprint.  Chrome prefs are pre-written to force
    PDFs to download rather than open in the built-in viewer.
    """
    dest_dir, dest_path = _dest_for(row)
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        row["status"]    = "failed"
        row["error_msg"] = str(exc)
        log.error(f"  FAILED creating directory {dest_dir}: {exc}")
        return
    url = row["url"]
    ext = dest_path.suffix.lower()

    if dest_path.exists():
        log.info(f"  Already on disk: {dest_path}")
        _mark_downloaded(row, dest_path, dest_path.stat().st_size)
        return

    log.info(f"  Downloading {ext.lstrip('.').upper()}: {url}")
    _html_title: str | None = None
    try:
        with page.expect_download(timeout=180_000) as dl_info:
            try:
                page.goto(url, wait_until="commit", timeout=60_000)
            except Exception as exc:
                # Playwright raises "Download is starting" when the server
                # responds with a file attachment — expected, download captured.
                if "download is starting" not in str(exc).lower():
                    raise
            else:
                # page.goto returned normally — server sent HTML (e.g. "File Not Found")
                # instead of a file.  Raise now to exit expect_download immediately.
                _html_title = page.title()
                raise RuntimeError(_html_title)
        dl = dl_info.value
        if dl.failure():
            raise RuntimeError(f"Download failed: {dl.failure()}")
        dl.save_as(str(dest_path))
        _mark_downloaded(row, dest_path, dest_path.stat().st_size)
    except PlaywrightTimeoutError:
        row["status"]    = "failed"
        row["error_msg"] = "Download did not start within timeout"
        log.error(f"  FAILED {url}: download did not start")
    except Exception as exc:
        if "target page, context or browser has been closed" in str(exc).lower():
            raise BrowserClosedError(str(exc))
        if _html_title is not None:
            tl = _html_title.lower()
            if any(kw in tl for kw in ("not found", "404", "file not found", "page not found")):
                row["status"]    = "skipped"
                row["error_msg"] = "File not found on server"
                log.warning(f"  Skipping — file not found on server: {url}")
            else:
                row["status"]    = "failed"
                row["error_msg"] = f"Server returned page: {_html_title!r}"
                log.error(f"  FAILED — server returned page {_html_title!r}: {url}")
        else:
            row["status"]    = "failed"
            row["error_msg"] = str(exc)
            log.error(f"  FAILED {url}: {exc}")


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def print_stats(rows: list[dict]) -> None:
    from collections import Counter

    ALL_STATUSES = ["discovered", "downloaded", "failed", "skipped"]

    def status_counts(subset):
        c = Counter(r["status"] for r in subset)
        return {s: c.get(s, 0) for s in ALL_STATUSES}

    def fmt_row(counts):
        return "  ".join(f"{s}: {counts[s]:>5}" for s in ALL_STATUSES)

    total = status_counts(rows)
    W = 72

    print(f"\n{'='*W}")
    print(f"  Total rows: {len(rows)}")
    print(f"  {fmt_row(total)}")

    # --- By category ---
    print(f"\n  {'BY CATEGORY':─<{W-2}}")
    cats = sorted({r["category"] for r in rows})
    for cat in cats:
        subset = [r for r in rows if r["category"] == cat]
        label = {"NGA": "National Gov't Agencies", "LGU": "Local Gov't Units",
                 "GOCC": "Gov't-Owned/Controlled Corps"}.get(cat, cat)
        print(f"  {cat} ({label})")
        print(f"    {fmt_row(status_counts(subset))}")

    # --- By year (descending), broken down per category ---
    print(f"\n  {'BY YEAR':─<{W-2}}")
    years = sorted({r["year"] for r in rows if r["year"]}, reverse=True)
    cat_w = max(len(c) for c in cats) if cats else 4
    header_parts = "  ".join(f"{s}: {0:>5}" for s in ALL_STATUSES)
    # column widths for alignment
    for year in years:
        year_rows = [r for r in rows if r["year"] == year]
        print(f"  {year}  (total: {len(year_rows)})")
        for cat in cats:
            subset = [r for r in year_rows if r["category"] == cat]
            if not subset:
                continue
            print(f"    {cat:<{cat_w}}  {fmt_row(status_counts(subset))}")

    print(f"{'='*W}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="COA Annual Audit Report scraper")
    p.add_argument("--discover-only", action="store_true",
                   help="Scrape for new links but do not download")
    p.add_argument("--category", choices=list(CATEGORIES), default=None,
                   help="Scrape only this category")
    p.add_argument("--retry-failed", action="store_true",
                   help="Only retry previously failed downloads (no scraping)")
    p.add_argument("--download-only", action="store_true",
                   help="Skip scraping, download all discovered/failed entries")
    p.add_argument("--all-years", action="store_true",
                   help="Scrape all years (default: only the most recent)")
    p.add_argument("--years", default=None,
                   help="Scrape specific years: range '2014-2019' or list '2014,2015,2016'")
    p.add_argument("--rediscover-failed", action="store_true",
                   help="Re-scrape only the category/year combos that have failed entries, "
                        "to pick up replacement files at new URLs")
    p.add_argument("--stats", action="store_true",
                   help="Print summary stats from CSV and exit")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global _active_ctx

    _install_sigint_handler()

    args = parse_args()

    # Parse --years into a set of year strings
    explicit_years: set[str] | None = None
    if args.years:
        raw = args.years.strip()
        m = re.fullmatch(r"(20\d{2})-(20\d{2})", raw)
        if m:
            lo, hi = sorted([int(m.group(1)), int(m.group(2))])
            explicit_years = {str(y) for y in range(lo, hi + 1)}
        else:
            explicit_years = {y.strip() for y in raw.split(",")}
        # Validate
        bad = [y for y in explicit_years if not re.fullmatch(r"20\d{2}", y)]
        if bad:
            raise SystemExit(f"Invalid year(s) in --years: {', '.join(bad)}")
        log.info(f"--years filter: {sorted(explicit_years, reverse=True)}")

    rows = load_csv()
    log.info(f"Loaded {len(rows)} existing rows from CSV")

    if args.stats:
        print_stats(rows)
        return

    urls_seen = known_urls(rows)

    # ------------------------------------------------------------------
    # Scraping phase
    # ------------------------------------------------------------------
    if not args.retry_failed and not args.download_only:
        cats_to_scrape = (
            {args.category: CATEGORIES[args.category]}
            if args.category
            else CATEGORIES
        )

        # Build per-category target_years from failed rows when --rediscover-failed
        failed_years_by_cat: dict[str, set[str]] = {}
        if args.rediscover_failed:
            for r in rows:
                if r["status"] == "failed":
                    failed_years_by_cat.setdefault(r["category"], set()).add(r["year"])
            if not failed_years_by_cat:
                log.info("No failed entries found — nothing to rediscover")
            else:
                for cat, yrs in failed_years_by_cat.items():
                    log.info(f"[{cat}] Will rediscover years with failures: {sorted(yrs, reverse=True)}")

        with sync_playwright() as pw:
            ctx = launch_browser(pw)
            _active_ctx = ctx

            try:
                for cat_key, cat_cfg in cats_to_scrape.items():
                    if args.rediscover_failed and cat_key not in failed_years_by_cat:
                        log.info(f"[{cat_key}] No failed entries — skipping rediscovery")
                        continue
                    # Fresh page per category — a stale/frozen page from a
                    # previous CF failure won't poison the next category.
                    page = ctx.new_page()
                    cat_new = 0
                    if args.rediscover_failed:
                        target_yrs = failed_years_by_cat.get(cat_key)
                    elif args.years:
                        target_yrs = explicit_years
                    else:
                        target_yrs = None
                    try:
                        for year_name, year_rows in scrape_category(
                            page, cat_key, cat_cfg, urls_seen,
                            all_years=args.all_years, target_years=target_yrs,
                        ):
                            for row in year_rows:
                                row["id"] = next_id(rows)
                                rows.append(row)
                            cat_new += len(year_rows)
                            # Save after each year — if browser closes mid-run, progress is kept
                            save_csv(rows)
                            log.info(f"[{cat_key}] {year_name}: +{len(year_rows)} ({len(rows)} total)")
                        log.info(f"[{cat_key}] Done — +{cat_new} new URLs")
                    except BrowserClosedError:
                        log.error("Browser was closed — saving progress and stopping scrape")
                        save_csv(rows)
                        break
                    finally:
                        try:
                            page.close()
                        except Exception:
                            pass
            finally:
                _active_ctx = None
                try:
                    ctx.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Download phase
    # ------------------------------------------------------------------
    if not args.discover_only:
        cat_filter = {args.category} if args.category else set(CATEGORIES)
        if args.retry_failed:
            to_download = [r for r in rows if r["status"] == "failed" and r["category"] in cat_filter]
        else:
            to_download = [
                r for r in rows
                if r["status"] in ("discovered", "failed") and r["category"] in cat_filter
            ]

        log.info(f"Downloading {len(to_download)} file(s)…")

        if to_download:
            with sync_playwright() as pw:
                ctx = launch_browser(pw)
                _active_ctx = ctx

                # Group by category so we load each category page once
                # (establishes Cloudflare session cookies for that category)
                rows_by_cat = {}
                for r in to_download:
                    rows_by_cat.setdefault(r["category"], []).append(r)

                try:
                    for cat_key, cat_rows in rows_by_cat.items():
                        # Fresh page per category — avoids carrying over frozen state
                        page = ctx.new_page()
                        cat_url = CATEGORIES[cat_key]["url"]
                        try:
                            if not _reload_cf_session(page, cat_url):
                                log.error(f"Could not load {cat_key} page after retries — skipping its downloads")
                                continue
                            time.sleep(2)

                            downloads_since_reload = 0
                            for row in cat_rows:
                                # Proactive reload every 30 downloads to keep CF session alive
                                if downloads_since_reload > 0 and downloads_since_reload % 30 == 0:
                                    log.info(f"Proactive CF session refresh after {downloads_since_reload} downloads…")
                                    if _reload_cf_session(page, cat_url):
                                        time.sleep(2)
                                    downloads_since_reload = 0

                                retry_wait = RETRY_WAIT_INIT
                                while True:
                                    download_file(ctx, page, row)
                                    downloads_since_reload += 1

                                    if row["status"] == "failed" and _is_transient_error(row.get("error_msg", "")):
                                        log.warning(
                                            f"Server unavailable ({row['error_msg']}) — "
                                            f"waiting {retry_wait}s then retrying…"
                                        )
                                        save_csv(rows)
                                        time.sleep(retry_wait)
                                        retry_wait = min(retry_wait * 2, RETRY_WAIT_MAX)
                                        # Attempt to re-establish CF session;
                                        # failure is fine — we'll retry the download regardless
                                        _reload_cf_session(page, cat_url)
                                        continue
                                    break

                                # Reload CF session after a non-transient failure — but only
                                # if the error could plausibly be CF-related.
                                if row["status"] == "failed" and not _is_cf_unrelated(row.get("error_msg", "")):
                                    log.warning(f"Download failed ({row.get('error_msg', '')})"
                                                f" — reloading CF session before continuing…")
                                    if _reload_cf_session(page, cat_url):
                                        time.sleep(2)
                                    downloads_since_reload = 0

                                time.sleep(random.uniform(DOWNLOAD_DELAY_LOW, DOWNLOAD_DELAY_HIGH))
                                # Save after each file so progress isn't lost on crash
                                save_csv(rows)
                        except BrowserClosedError:
                            log.error("Browser was closed — saving progress and stopping downloads")
                            save_csv(rows)
                            raise  # break out of the category loop via the outer finally
                        finally:
                            try:
                                page.close()
                            except Exception:
                                pass
                finally:
                    _active_ctx = None
                    try:
                        ctx.close()
                    except Exception:
                        pass

            save_csv(rows)
            log.info("CSV saved after downloads")

    # ------------------------------------------------------------------
    # Run summary
    # ------------------------------------------------------------------
    from collections import Counter
    s = Counter(r["status"] for r in rows)
    log.info(
        f"Run complete — discovered: {s.get('discovered',0)}, "
        f"downloaded: {s.get('downloaded',0)}, "
        f"failed: {s.get('failed',0)}, "
        f"skipped: {s.get('skipped',0)}"
    )


if __name__ == "__main__":
    main()
