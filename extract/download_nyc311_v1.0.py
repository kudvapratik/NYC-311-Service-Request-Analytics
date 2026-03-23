# ============================================================
# NYC 311 Source Data Downloader v2.0
# Features:
#   - Year range input (e.g. 2022 to 2024)
#   - Auto-save checkpoint every N pages
#   - Resume from failed page
#   - One CSV per year
# Author  : Pratik Kudva
# Dataset : NYC Open Data erm2-nwe9
# ============================================================

import requests
import csv
import os
import json
import logging
from datetime import datetime

# ── LOGGING ──────────────────────────────────────────────────
# New timestamped log file created every run
# e.g. nyc311_download_20260322_210145.log
# Old logs are never overwritten — full history preserved
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE      = f"nyc311_download_{RUN_TIMESTAMP}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),           # console output
        logging.FileHandler(LOG_FILE),     # timestamped log file
    ]
)
log = logging.getLogger(__name__)
log.info(f"Log file: {LOG_FILE}")

# ── CONFIG ───────────────────────────────────────────────────
BASE_URL        = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE       = 50_000    # rows per API call (max 50000)
SAVE_EVERY      = 5         # save CSV every N pages
                            # lower = safer but slower
                            # higher = faster but more data lost if crash
CHECKPOINT_FILE = "nyc311_checkpoint.json"

COLUMNS = [
    "unique_key", "created_date", "closed_date",
    "agency", "agency_name", "complaint_type", "descriptor",
    "location_type", "incident_zip", "incident_address",
    "city", "borough", "latitude", "longitude",
    "status", "resolution_description", "community_board",
]

# ── CHECKPOINT HELPERS ────────────────────────────────────────
def save_checkpoint(year: int, page: int, offset: int,
                    rows_saved: int) -> None:
    """
    Save progress to JSON file.
    If download fails — resume from here next run.
    """
    checkpoint = {
        "year":       year,
        "page":       page,
        "offset":     offset,
        "rows_saved": rows_saved,
        "saved_at":   datetime.now().isoformat(),
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)
    log.info(f"Checkpoint saved — year {year} page {page} "
             f"offset {offset:,} rows {rows_saved:,}")


def load_checkpoint() -> dict | None:
    """
    Load checkpoint if it exists.
    Returns None if no checkpoint found.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE) as f:
            cp = json.load(f)
        log.info(f"Checkpoint found — year {cp['year']} "
                 f"page {cp['page']} offset {cp['offset']:,}")
        return cp
    except Exception as e:
        log.warning(f"Could not read checkpoint: {e}")
        return None


def clear_checkpoint() -> None:
    """Remove checkpoint after successful year completion."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — year complete")


# ── CSV HELPERS ───────────────────────────────────────────────
def get_csv_path(year: int) -> str:
    """Returns CSV filename for a given year."""
    return f"nyc311_{year}.csv"


def append_to_csv(records: list, year: int,
                  write_header: bool = False) -> None:
    """
    Append records to CSV file.
    write_header=True only on first write — avoids duplicate headers.
    Using append mode — does not overwrite existing data.
    """
    filepath = get_csv_path(year)
    mode = "a"  # append — never overwrite!

    with open(filepath, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS,
                                extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(records)

    size_mb = os.path.getsize(filepath) / 1024 / 1024
    log.info(f"Saved {len(records):,} rows to {filepath} "
             f"({size_mb:.1f} MB total)")


# ── API ───────────────────────────────────────────────────────
def fetch_page(year: int, offset: int, limit: int) -> list:
    """Fetch one page of data from NYC Open Data API."""
    params = {
        "$limit":   limit,
        "$offset":  offset,
        "$order":   "created_date ASC",
        "$where":   (f"created_date >= '{year}-01-01T00:00:00'"
                     f" AND created_date < '{year + 1}-01-01T00:00:00'"),
        "$select":  ", ".join(COLUMNS),
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        log.error("Request timed out — API slow. Save checkpoint and retry.")
        raise
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP error: {e}")
        raise
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        raise


def clean_row(row: dict) -> dict:
    """Light cleaning — strip whitespace. Heavy cleaning in SQL ETL."""
    return {
        col: row.get(col, "").strip()
        if isinstance(row.get(col, ""), str)
        else row.get(col, "")
        for col in COLUMNS
    }


# ── DOWNLOAD ONE YEAR ─────────────────────────────────────────
def download_year(year: int,
                  resume_page: int = 1,
                  resume_offset: int = 0,
                  resume_rows: int = 0) -> int:
    """
    Download all 311 requests for one year.
    Saves CSV every SAVE_EVERY pages.
    Returns total rows downloaded.

    resume_page/offset/rows: used when resuming after failure
    """
    csv_path    = get_csv_path(year)
    page        = resume_page
    offset      = resume_offset
    total_rows  = resume_rows
    batch_rows  = []           # accumulate rows between saves
    write_header = not os.path.exists(csv_path)  # header if new file

    log.info("-" * 52)
    log.info(f"Year {year} — starting from page {page} "
             f"offset {offset:,}")
    log.info("-" * 52)

    while True:
        log.info(f"Year {year} | Page {page:>4} | "
                 f"offset {offset:>9,} | fetching {PAGE_SIZE:,} rows...")

        try:
            raw_batch = fetch_page(year, offset, PAGE_SIZE)
        except Exception as e:
            # Save whatever we have before crashing
            if batch_rows:
                append_to_csv(batch_rows, year, write_header)
                total_rows += len(batch_rows)
                write_header = False
            save_checkpoint(year, page, offset, total_rows)
            log.error(f"Download failed on page {page}. "
                      f"Resume with --resume flag.")
            raise

        # Empty page = no more data for this year
        if not raw_batch:
            log.info(f"Year {year} — empty page, all data fetched!")
            break

        # Clean rows lightly
        cleaned = [clean_row(row) for row in raw_batch]
        batch_rows.extend(cleaned)
        total_rows += len(cleaned)
        offset     += PAGE_SIZE

        log.info(f"Year {year} | Page {page:>4} | "
                 f"fetched {len(raw_batch):>6,} | "
                 f"total {total_rows:>9,}")

        # Save every SAVE_EVERY pages
        if page % SAVE_EVERY == 0:
            append_to_csv(batch_rows, year, write_header)
            write_header = False
            save_checkpoint(year, page + 1, offset, total_rows)
            batch_rows = []  # clear buffer after saving

        # Partial page = last page of year
        if len(raw_batch) < PAGE_SIZE:
            log.info(f"Year {year} — partial page, end of data")
            break

        page += 1

    # Save any remaining rows not yet written
    if batch_rows:
        append_to_csv(batch_rows, year, write_header)

    # Year complete — clear checkpoint
    clear_checkpoint()

    log.info(f"Year {year} complete — {total_rows:,} rows "
             f"in {csv_path}")
    return total_rows


# ── USER INPUT ────────────────────────────────────────────────
def get_year_range() -> tuple[int, int]:
    """Ask user for start and end year."""
    print("\n" + "=" * 52)
    print("NYC 311 Data Downloader v2.0")
    print("=" * 52)

    while True:
        try:
            start = int(input("Enter start year (e.g. 2022): ").strip())
            end   = int(input("Enter end year   (e.g. 2024): ").strip())
            if 2010 <= start <= end <= 2025:
                return start, end
            print("Years must be between 2010 and 2025, "
                  "start <= end.")
        except ValueError:
            print("Please enter valid years e.g. 2022")


def check_resume() -> dict | None:
    """
    Check if a checkpoint exists and ask user to resume.
    Returns checkpoint dict if resuming, None if fresh start.
    """
    cp = load_checkpoint()
    if not cp:
        return None

    print(f"\nCheckpoint found!")
    print(f"  Year      : {cp['year']}")
    print(f"  Page      : {cp['page']}")
    print(f"  Rows saved: {cp['rows_saved']:,}")
    print(f"  Saved at  : {cp['saved_at']}")

    while True:
        choice = input("\nResume from checkpoint? (y/n): ").strip().lower()
        if choice == "y":
            return cp
        if choice == "n":
            clear_checkpoint()
            return None
        print("Enter y or n")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    start_time = datetime.now()

    # Check for existing checkpoint first
    checkpoint = check_resume()

    if checkpoint:
        # Resume single year from checkpoint
        year_start = checkpoint["year"]
        year_end   = checkpoint["year"]
        log.info(f"Resuming year {year_start} from page "
                 f"{checkpoint['page']}")
    else:
        # Fresh start — get year range from user
        year_start, year_end = get_year_range()

    years  = list(range(year_start, year_end + 1))
    totals = {}

    log.info("=" * 52)
    log.info(f"Downloading years: {years}")
    log.info(f"Page size        : {PAGE_SIZE:,}")
    log.info(f"Save every       : {SAVE_EVERY} pages")
    log.info("=" * 52)

    for year in years:
        try:
            if checkpoint and year == checkpoint["year"]:
                # Resume this year from checkpoint
                rows = download_year(
                    year,
                    resume_page   = checkpoint["page"],
                    resume_offset = checkpoint["offset"],
                    resume_rows   = checkpoint["rows_saved"],
                )
                checkpoint = None  # clear after first resumed year
            else:
                # Fresh download for this year
                rows = download_year(year)

            totals[year] = rows

        except Exception as e:
            log.error(f"Year {year} failed: {e}")
            log.error("Fix the issue and re-run — "
                      "checkpoint saved, will resume.")
            break

    # Final summary
    elapsed = datetime.now() - start_time
    log.info("=" * 52)
    log.info("DOWNLOAD SUMMARY")
    log.info("-" * 52)
    for year, rows in totals.items():
        size_mb = os.path.getsize(get_csv_path(year)) / 1024 / 1024
        log.info(f"  {year} : {rows:>9,} rows  "
                 f"{size_mb:>6.1f} MB  {get_csv_path(year)}")
    log.info("-" * 52)
    log.info(f"Total rows : {sum(totals.values()):>9,}")
    log.info(f"Elapsed    : {str(elapsed).split('.')[0]}")
    log.info("=" * 52)


if __name__ == "__main__":
    main()
