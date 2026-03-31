# ============================================================
# NYC 311 — Load CSV to SQL Server Staging
# Script 2 of 4
# Reads nyc311_YYYY.csv → stg.raw_311_requests
#
# Usage:
#   python nyc311_load_staging.py             # auto-detects all nyc311_YYYY.csv files
#   python nyc311_load_staging.py 2024        # single year
#   python nyc311_load_staging.py 2022 2024   # year range
#
# Author  : Pratik Kudva
# ============================================================

import sys
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from datetime import datetime


# ============================================================
# STEP 1 — RESOLVE YEARS (before anything else)
# ============================================================
def resolve_years() -> list[int]:
    """
    Determine which years to load based on CLI arguments.
      no args  : auto-detect all nyc311_YYYY.csv in current directory
      1 arg    : single year  e.g. 2024
      2 args   : year range   e.g. 2022 2024
    """
    current_year = datetime.now().year

    def validate_year(y: str) -> int:
        # Distinguish non-integer input from out-of-range year
        try:
            val = int(y)
        except ValueError:
            print(f"Invalid input '{y}' — expected a 4-digit year e.g. 2024.")
            sys.exit(1)
        if not (2010 <= val <= current_year):
            print(f"Year {val} out of range. Must be between 2010 and {current_year}.")
            sys.exit(1)
        return val

    if len(sys.argv) == 2:
        return [validate_year(sys.argv[1])]

    elif len(sys.argv) == 3:
        start = validate_year(sys.argv[1])
        end   = validate_year(sys.argv[2])
        if start > end:
            print(f"Start year {start} must be <= end year {end}.")
            sys.exit(1)
        return list(range(start, end + 1))

    else:
        # Auto-detect all nyc311_YYYY.csv files in current directory
        found = sorted(glob.glob("nyc311_*.csv"))
        years = []
        for f in found:
            stem = os.path.basename(f).replace("nyc311_", "").replace(".csv", "")
            try:
                y = int(stem)
                if 2010 <= y <= current_year:
                    years.append(y)
            except ValueError:
                continue

        if not years:
            print(
                "No nyc311_YYYY.csv files found in current directory.\n"
                "Run download_nyc311_v2.py first to generate them."
            )
            sys.exit(1)

        print(f"Auto-detected CSV files for years: {years}")
        return years


YEARS = resolve_years()


# ============================================================
# STEP 2 — ARCHIVE PROMPT (before logging starts)
# Only prompts for the staging log file.
# nyc311_checkpoint.json belongs to the downloader — not touched here.
# ============================================================
LOG_FILE = "nyc311_load_staging.log"


def prompt_archive(filepath: str, label: str) -> None:
    """
    If filepath exists, ask user to archive it (rename with timestamp)
    or continue appending.
    Archives are renamed: filename_YYYYMMDD_HHMMSS.ext
    """
    if not os.path.exists(filepath):
        return

    size_kb  = os.path.getsize(filepath) / 1024
    modified = datetime.fromtimestamp(os.path.getmtime(filepath))
    ext      = os.path.splitext(filepath)[1]
    base     = filepath[: -len(ext)] if ext else filepath

    print("\n" + "=" * 55)
    print(f"Existing {label} found: {filepath}")
    print(f"  Size     : {size_kb:.1f} KB")
    print(f"  Modified : {modified.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)
    print("  [A] Archive — rename with timestamp, start fresh")
    print("  [K] Keep    — append to existing log")
    print("=" * 55)

    while True:
        choice = input("Choice (A/K): ").strip().upper()
        if choice == "A":
            ts           = modified.strftime("%Y%m%d_%H%M%S")
            archive_name = f"{base}_{ts}{ext}"
            os.rename(filepath, archive_name)
            print(f"Archived to : {archive_name}\n")
            return
        if choice == "K":
            print(f"Keeping existing {label}.\n")
            return
        print("Enter A or K.")


prompt_archive(LOG_FILE, "log file")


# ============================================================
# STEP 3 — LOGGING (after archive prompt so file handler is clean)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ]
)
log = logging.getLogger(__name__)
log.info(f"Log file : {LOG_FILE}")


# ============================================================
# STEP 4 — CONFIG (.env)
# ============================================================
# Credentials are read from a .env file in the repo root.
# Copy .env.example to .env and fill in your values.
# Never commit .env to git — it is listed in .gitignore.
load_dotenv()

SQL_SERVER   = os.getenv("SQL_SERVER",   "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "NYC311_Analytics")
SQL_AUTH     = os.getenv("SQL_AUTH",     "windows")   # "windows" or "sql"

if SQL_AUTH == "sql":
    SQL_USER = os.getenv("SQL_USER", "")
    SQL_PASS = os.getenv("SQL_PASS", "")
    CONN_STRING = (
        f"mssql+pyodbc://{SQL_USER}:{SQL_PASS}@{SQL_SERVER}/{SQL_DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
else:
    # Windows Authentication (default)
    CONN_STRING = (
        f"mssql+pyodbc://{SQL_SERVER}/{SQL_DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
    )

# ── BATCH SIZE ────────────────────────────────────────────────
# Do NOT use method="multi" with pyodbc — it hits SQL Server's
# ~2100 parameter marker limit on large batches.
# Use method=None + fast_executemany=True on the engine instead.
BATCH_SIZE = 1_000      # rows per to_sql chunk — safe with fast_executemany

# Columns in CSV that map to stg table
STG_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "location_type",
    "incident_zip",
    "incident_address",
    "city",
    "borough",
    "latitude",
    "longitude",
    "status",
    "resolution_description",
    "community_board",
]


# ============================================================
# EXTRACT
# ============================================================
def load_csv(filepath: str) -> pd.DataFrame:
    """
    Read CSV into pandas DataFrame.
    All columns stay as strings — staging is raw!
    No type conversion here — that happens in stg → dw ETL.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"CSV not found: {filepath}\n"
            f"Run download_nyc311_v2.py first to generate this file."
        )

    log.info(f"Reading CSV: {filepath}")

    df = pd.read_csv(
        filepath,
        dtype=str,              # EVERYTHING as string — no auto conversion
        keep_default_na=False,  # keep empty strings — don't convert to NaN
        encoding="utf-8",
        low_memory=False        # suppress mixed type warnings
    )

    log.info(f"CSV loaded: {len(df):,} rows, {len(df.columns)} columns")

    # Keep only columns we need
    available = [c for c in STG_COLUMNS if c in df.columns]
    missing   = [c for c in STG_COLUMNS if c not in df.columns]

    if missing:
        log.warning(f"Missing columns in CSV: {missing}")

    df = df[available]
    log.info(f"Keeping {len(available)} columns for staging")

    return df


# ============================================================
# TRANSFORM
# ============================================================
def prepare_for_staging(df: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    """
    Minimal preparation before staging insert.
    We do NOT clean data here — staging is raw!
    We only add audit columns.
    """
    df = df.copy()

    # Replace empty strings with None — SQL Server NULL
    df = df.replace("", None)

    # Add audit columns
    df["stg_load_date"]     = datetime.now()
    df["stg_batch_id"]      = batch_id
    df["stg_is_processed"]  = 0          # 0 = not yet loaded to dw
    df["stg_error_message"] = None

    return df


# ============================================================
# LOAD
# ============================================================
def get_staging_count(engine) -> int:
    """Return current row count of stg.raw_311_requests."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stg.raw_311_requests"))
        return result.fetchone()[0]


def truncate_staging(engine) -> None:
    """Clear staging table before fresh load."""
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE stg.raw_311_requests"))
        conn.commit()
    log.info("Staging table truncated — ready for fresh load")


def load_to_staging(df: pd.DataFrame, engine, batch_id: int) -> int:
    """
    Bulk insert DataFrame to staging table in chunks.
    Returns the number of rows actually committed to the database.

    Uses engine.begin() to open an explicit transaction — to_sql() is
    passed the live connection object (not the engine) so the insert
    commits when the with-block exits cleanly, or rolls back on error.

    This fixes the silent rollback issue where to_sql() runs without
    raising an exception but nothing lands in the database.

    method=None + fast_executemany=True avoids the pyodbc parameter
    marker limit that occurs with method='multi' on large batches.
    """
    total_rows = len(df)
    log.info(f"Loading {total_rows:,} rows from CSV to stg.raw_311_requests")
    log.info(f"Chunk size : {BATCH_SIZE:,} rows")

    df_prepared = prepare_for_staging(df, batch_id)

    rows_before = get_staging_count(engine)

    # engine.begin() opens an explicit transaction
    # con=conn passes the live connection — commits on clean exit
    with engine.begin() as conn:
        df_prepared.to_sql(
            name      = "raw_311_requests",
            schema    = "stg",
            con       = conn,           # connection object, not engine
            if_exists = "append",
            index     = False,
            chunksize = BATCH_SIZE,
            method    = None,           # single-row INSERT — safe with fast_executemany
        )

    # Verify rows actually landed in DB — do not trust len(df)
    rows_after    = get_staging_count(engine)
    rows_inserted = rows_after - rows_before

    if rows_inserted == 0:
        log.warning(
            f"0 rows inserted despite {total_rows:,} rows in CSV. "
            f"Check DB connection and table permissions."
        )
    elif rows_inserted != total_rows:
        log.warning(
            f"Row count mismatch — CSV had {total_rows:,} rows "
            f"but only {rows_inserted:,} were inserted."
        )
    else:
        log.info(f"DB confirmed: {rows_inserted:,} rows inserted")

    return rows_inserted


# ============================================================
# VERIFY
# ============================================================
def verify_staging(engine) -> None:
    """Quick sanity check after all years are loaded."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*)                                                AS total_rows,
                SUM(CASE WHEN unique_key   IS NULL THEN 1 ELSE 0 END)  AS null_keys,
                SUM(CASE WHEN agency       IS NULL THEN 1 ELSE 0 END)  AS null_agency,
                SUM(CASE WHEN borough      IS NULL THEN 1 ELSE 0 END)  AS null_borough,
                SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END)  AS null_dates,
                COUNT(DISTINCT agency)                                  AS distinct_agencies,
                COUNT(DISTINCT borough)                                 AS distinct_boroughs,
                COUNT(DISTINCT complaint_type)                          AS distinct_complaints
            FROM stg.raw_311_requests
        """))
        stats = result.fetchone()

    log.info("=" * 55)
    log.info("STAGING VERIFICATION")
    log.info(f"Total rows loaded  : {stats[0]:>10,}")
    log.info(f"Null unique_keys   : {stats[1]:>10,}")
    log.info(f"Null agencies      : {stats[2]:>10,}")
    log.info(f"Null boroughs      : {stats[3]:>10,}")
    log.info(f"Null created_dates : {stats[4]:>10,}")
    log.info(f"Distinct agencies  : {stats[5]:>10,}")
    log.info(f"Distinct boroughs  : {stats[6]:>10,}")
    log.info(f"Distinct complaints: {stats[7]:>10,}")
    log.info("=" * 55)


# ============================================================
# ORCHESTRATE
# ============================================================
def run():
    totals = {}

    log.info("=" * 55)
    log.info("NYC 311 Staging Load starting")
    log.info(f"Years    : {YEARS}")
    log.info(f"Target   : stg.raw_311_requests")
    log.info("=" * 55)

    try:
        # fast_executemany=True — efficient parameter binding at driver level
        # Must pair with method=None in to_sql (not method='multi')
        engine = create_engine(
            CONN_STRING,
            connect_args={"fast_executemany": True}
        )
        log.info("SQL Server connection established")

        # Pre-flight — check at least one CSV exists before truncating the table
        # Without this, all years could be skipped leaving staging empty
        loadable = [y for y in YEARS if os.path.exists(f"nyc311_{y}.csv")]
        if not loadable:
            log.error(
                f"No CSV files found for any requested year {YEARS}. "
                f"Aborting — staging table has NOT been truncated."
            )
            return

        if len(loadable) < len(YEARS):
            skipped = [y for y in YEARS if y not in loadable]
            log.warning(f"CSV files missing for years: {skipped} — these will be skipped")

        # Single batch_id for the entire run — generated once before the loop
        # Avoids collision when multiple years load within the same second
        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))
        log.info(f"Batch ID : {batch_id}")

        # Truncate once — safe now that we know at least one file exists
        truncate_staging(engine)

        for year in YEARS:
            csv_file = f"nyc311_{year}.csv"

            log.info("-" * 55)
            log.info(f"Loading year {year} — {csv_file}")
            log.info("-" * 55)

            try:
                df           = load_csv(csv_file)
                rows_inserted = load_to_staging(df, engine, batch_id)
                totals[year] = rows_inserted   # DB-confirmed count, not len(df)

            except FileNotFoundError as e:
                log.warning(f"Skipping year {year} — {e}")
                continue

            except Exception as e:
                log.error(f"Year {year} failed: {e}", exc_info=True)
                raise

        # Only verify if at least one year was successfully loaded
        if not totals:
            log.warning("No years were successfully loaded — skipping verification.")
        else:
            verify_staging(engine)

        # Final summary
        log.info("=" * 55)
        log.info("LOAD SUMMARY")
        log.info("-" * 55)
        for year, rows in totals.items():
            log.info(f"  {year} : {rows:>9,} rows")
        if totals:
            log.info(f"  Total : {sum(totals.values()):>9,} rows")
        log.info("=" * 55)
        log.info("STAGING LOAD SUCCEEDED" if totals else "STAGING LOAD COMPLETED — NO ROWS LOADED")

    except Exception as e:
        log.error(f"STAGING LOAD FAILED: {e}", exc_info=True)
        raise
    finally:
        log.info("Staging load finished")


if __name__ == "__main__":
    start = datetime.now()
    run()
    elapsed = datetime.now() - start
    log.info(f"Elapsed: {str(elapsed).split('.')[0]}")