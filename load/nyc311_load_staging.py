# ============================================================
# NYC 311 — Load CSV to SQL Server Staging
# Script 2 of 4
# Loops through nyc311_YYYY.csv files → stg.raw_311_requests
# Features:
#   - Per-run timestamped log file (new file every run)
#   - Checkpoint file to resume from point of failure
#   - Processes all nyc311_YYYY.csv files in DATA_DIR
# Author  : Pratik Kudva
# ============================================================

import os
import glob
import json
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# ── CONFIG ───────────────────────────────────────────────────
SQL_SERVER   = "SURFACE"
SQL_DATABASE = "NYC311_Analytics"

# Folder containing nyc311_YYYY.csv files
DATA_DIR        = "."

# Rows per INSERT batch
BATCH_SIZE      = 250000

# Checkpoint file — tracks progress across files and chunks
CHECKPOINT_FILE = "nyc311_checkpoint.json"

# Columns to load into staging
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


# ── LOGGING ───────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    """
    Creates a NEW timestamped log file every single run.

    Fix for 'same log file reused' bug:
    Python's logging.getLogger() caches loggers by name for the
    entire interpreter lifetime. In PyCharm, the interpreter stays
    alive across runs, so the same file handler is reused.
    Solution: use a unique logger name per run (includes timestamp).
    """
    run_ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir  = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"nyc311_load_staging_{run_ts}.log")

    fmt = "%(asctime)s | %(levelname)s | %(message)s"

    # Unique name per run — guarantees a fresh logger with no cached handlers
    logger_name = f"nyc311_{run_ts}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(fmt))
    logger.addHandler(ch)

    # File handler — this run only
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)

    logger.info(f"Log file : {log_path}")
    return logger


# ── DATABASE CONNECTION ───────────────────────────────────────
def build_engine(log: logging.Logger):
    """
    Build SQLAlchemy engine for SQL Server with fast_executemany.

    Fix for 'pyodbc.Connection has no attribute fast_executemany' bug:
    fast_executemany is a CURSOR-level attribute in newer pyodbc versions,
    not a connection-level attribute. The correct way to enable it in
    SQLAlchemy is via connect_args — SQLAlchemy passes it to the cursor
    automatically through the DBAPI execution layer.
    """
    conn_str = (
        f"mssql+pyodbc://{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
        f"&Trusted_Connection=yes"
    )

    engine = create_engine(
        conn_str,
        connect_args={"fast_executemany": True},  # cursor-level, correct approach
        echo=False,
    )

    # Verify connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    log.info("SQL Server connection OK")
    log.info(f"  Server   : {SQL_SERVER}")
    log.info(f"  Database : {SQL_DATABASE}")
    log.info(f"  fast_executemany : enabled")
    return engine


# ── CHECKPOINT ────────────────────────────────────────────────
def load_checkpoint() -> dict:
    """
    Load checkpoint state from disk.
    {
      "nyc311_2023.csv": {"status": "done",    "last_chunk": 644},
      "nyc311_2024.csv": {"status": "partial", "last_chunk": 17},
      "nyc311_2025.csv": {"status": "pending", "last_chunk": -1}
    }
    """
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoint: dict) -> None:
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


def reset_checkpoint() -> None:
    """Delete checkpoint to force full reload from scratch."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint reset — next run starts from scratch")


# ── DISCOVER FILES ────────────────────────────────────────────
def discover_csv_files(data_dir: str, log: logging.Logger) -> list:
    pattern = os.path.join(data_dir, "nyc311_????.csv")
    files   = sorted(glob.glob(pattern))

    if not files:
        log.warning(f"No nyc311_YYYY.csv files found in: {os.path.abspath(data_dir)}")
    else:
        log.info(f"Found {len(files)} file(s):")
        for f in files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            log.info(f"  {os.path.basename(f)}  ({size_mb:,.1f} MB)")

    return files


# ── EXTRACT ───────────────────────────────────────────────────
def load_csv(filepath: str, log: logging.Logger) -> pd.DataFrame:
    """Read CSV — all columns as strings. Staging is raw."""
    log.info(f"Reading: {filepath}")

    df = pd.read_csv(
        filepath,
        dtype           = str,
        keep_default_na = False,
        encoding        = "utf-8",
        low_memory      = False,
    )

    log.info(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")

    # Normalise column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    available = [c for c in STG_COLUMNS if c in df.columns]
    missing   = [c for c in STG_COLUMNS if c not in df.columns]

    if missing:
        log.warning(f"  Columns missing from CSV (will be NULL): {missing}")

    return df[available]


# ── TRANSFORM ─────────────────────────────────────────────────
def prepare_for_staging(df: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    """Add audit columns. No cleaning — staging is raw."""
    df = df.copy()
    df = df.replace("", None)

    df["stg_load_date"]     = datetime.now()
    df["stg_batch_id"]      = batch_id          # YYYYMMDD — fits in SQL Server INT
    df["stg_is_processed"]  = 0
    df["stg_error_message"] = None
    return df


# ── LOAD ──────────────────────────────────────────────────────
def truncate_staging(engine, log: logging.Logger) -> None:
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE stg.raw_311_requests"))
        conn.commit()
    log.info("  Staging table truncated")


def insert_chunk(chunk: pd.DataFrame, engine) -> None:
    """
    Insert one chunk to staging.
    fast_executemany is enabled at engine level — no extra config needed here.
    """
    chunk.to_sql(
        name      = "raw_311_requests",
        schema    = "stg",
        con       = engine,
        if_exists = "append",
        index     = False,
        chunksize = len(chunk),   # send whole chunk as one batch
    )


def load_file_to_staging(
    filepath:   str,
    engine,
    batch_id:   int,
    checkpoint: dict,
    log:        logging.Logger,
) -> bool:
    """
    Load one CSV file to staging in chunks.
    Resumes from last successful chunk if checkpoint exists.
    Returns True on full success, False on failure.
    """
    filename = os.path.basename(filepath)
    file_cp  = checkpoint.get(filename, {"status": "pending", "last_chunk": -1})

    # Note: "done" files are skipped by the orchestrator before reaching here
    start_chunk = file_cp["last_chunk"] + 1
    log.info(f"  {filename} — resuming from chunk {start_chunk}")

    df            = load_csv(filepath, log)
    df            = prepare_for_staging(df, batch_id)
    total_rows    = len(df)
    total_chunks  = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    rows_inserted = 0

    log.info(f"  Total rows   : {total_rows:,}")
    log.info(f"  Chunk size   : {BATCH_SIZE:,}")
    log.info(f"  Total chunks : {total_chunks:,}")

    chunk_idx = start_chunk  # track outside loop for except block
    try:
        for chunk_idx in range(start_chunk, total_chunks):
            start_row = chunk_idx * BATCH_SIZE
            end_row   = min(start_row + BATCH_SIZE, total_rows)
            chunk     = df.iloc[start_row:end_row]

            insert_chunk(chunk, engine)

            rows_inserted += len(chunk)
            pct = (rows_inserted / total_rows) * 100
            log.info(
                f"  [{filename}] Chunk {chunk_idx + 1}/{total_chunks} "
                f"rows {start_row:,}–{end_row:,} "
                f"({pct:.1f}%)"
            )

            # Checkpoint after every successful chunk
            checkpoint[filename] = {
                "status":     "partial",
                "last_chunk": chunk_idx,
                "rows_done":  rows_inserted,
                "total_rows": total_rows,
                "updated_at": datetime.now().isoformat(),
            }
            save_checkpoint(checkpoint)

        # All chunks done
        checkpoint[filename] = {
            "status":       "done",
            "last_chunk":   total_chunks - 1,
            "rows_done":    total_rows,
            "total_rows":   total_rows,
            "completed_at": datetime.now().isoformat(),
        }
        save_checkpoint(checkpoint)
        log.info(f"  {filename} — COMPLETE ({total_rows:,} rows)")
        return True

    except Exception as e:
        last_good = chunk_idx - 1
        log.error(f"  {filename} — FAILED at chunk {chunk_idx}: {e}", exc_info=True)
        checkpoint[filename] = {
            "status":      "partial",
            "last_chunk":  last_good,
            "rows_done":   max(0, last_good + 1) * BATCH_SIZE,
            "total_rows":  total_rows,
            "failed_at":   datetime.now().isoformat(),
            "error":       str(e),
        }
        save_checkpoint(checkpoint)
        return False


# ── VERIFY ────────────────────────────────────────────────────
def verify_staging(engine, log: logging.Logger) -> None:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*)                                               AS total_rows,
                SUM(CASE WHEN unique_key   IS NULL THEN 1 ELSE 0 END) AS null_keys,
                SUM(CASE WHEN agency       IS NULL THEN 1 ELSE 0 END) AS null_agency,
                SUM(CASE WHEN borough      IS NULL THEN 1 ELSE 0 END) AS null_borough,
                SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) AS null_dates,
                COUNT(DISTINCT agency)                                 AS distinct_agencies,
                COUNT(DISTINCT borough)                                AS distinct_boroughs,
                COUNT(DISTINCT complaint_type)                         AS distinct_complaints,
                MIN(stg_batch_id)                                      AS first_batch,
                MAX(stg_batch_id)                                      AS last_batch
            FROM stg.raw_311_requests
        """))
        s = result.fetchone()

    log.info("=" * 60)
    log.info("STAGING VERIFICATION")
    log.info(f"  Total rows         : {s[0]:>12,}")
    log.info(f"  Null unique_keys   : {s[1]:>12,}")
    log.info(f"  Null agencies      : {s[2]:>12,}")
    log.info(f"  Null boroughs      : {s[3]:>12,}")
    log.info(f"  Null created_dates : {s[4]:>12,}")
    log.info(f"  Distinct agencies  : {s[5]:>12,}")
    log.info(f"  Distinct boroughs  : {s[6]:>12,}")
    log.info(f"  Distinct complaints: {s[7]:>12,}")
    log.info(f"  Batch ID range     : {s[8]} → {s[9]}")
    log.info("=" * 60)


# ── ORCHESTRATE ───────────────────────────────────────────────
def run():
    log        = setup_logging()
    # YYYYMMDD format — max 20991231, well within SQL Server INT (2,147,483,647)
    # Previous format %Y%m%d%H%M%S produced 20260323004951 which overflowed INT
    batch_id   = int(datetime.now().strftime("%Y%m%d"))
    checkpoint = load_checkpoint()

    log.info("=" * 60)
    log.info("NYC 311 Staging Load — START")
    log.info(f"Batch ID   : {batch_id}")
    log.info(f"Data dir   : {os.path.abspath(DATA_DIR)}")
    log.info(f"Batch size : {BATCH_SIZE:,} rows/chunk")
    log.info(f"Checkpoint : {CHECKPOINT_FILE}")
    log.info("=" * 60)

    csv_files = discover_csv_files(DATA_DIR, log)
    if not csv_files:
        log.error("No files found — exiting")
        return

    try:
        engine = build_engine(log)
    except Exception as e:
        log.error(f"Cannot connect to SQL Server: {e}")
        raise

    results = {}
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        file_cp  = checkpoint.get(filename, {"status": "pending", "last_chunk": -1})

        log.info("-" * 60)
        log.info(f"Processing : {filename}")
        log.info(f"CP status  : {file_cp['status']}  |  last completed chunk: {file_cp['last_chunk']}")


        if file_cp["status"] == "done":
            log.info("  Already complete — skipping")
            results[filename] = "SUCCESS"
            continue


        is_resuming = file_cp["status"] == "partial" and file_cp["last_chunk"] >= 0

        if not is_resuming:
            # Fresh start — safe to truncate, nothing committed yet for this file
            log.info("  Fresh start — truncating staging table")
            truncate_staging(engine, log)
        else:
            # RESUMING — do NOT truncate.
            # Chunks 0..last_chunk are already committed in the table.
            # Truncating here would permanently lose that data.
            log.warning(
                f"  RESUMING from chunk {file_cp['last_chunk'] + 1} — "
                f"truncate SKIPPED to preserve "
                f"{file_cp.get('rows_done', 0):,} already-loaded rows"
            )

        success = load_file_to_staging(filepath, engine, batch_id, checkpoint, log)
        results[filename] = "SUCCESS" if success else "FAILED"

        if not success:
            log.error(f"Stopping — {filename} failed. Re-run to resume from checkpoint.")
            break

    log.info("=" * 60)
    log.info("RUN SUMMARY")
    for fname, status in results.items():
        icon = "OK" if status == "SUCCESS" else "!!"
        log.info(f"  [{icon}] {status:8s}  {fname}")
    log.info("=" * 60)

    if any(v == "SUCCESS" for v in results.values()):
        verify_staging(engine, log)

    log.info("CHECKPOINT STATUS")
    for fname, cp in checkpoint.items():
        log.info(
            f"  {fname:30s}  status={cp['status']:8s}  "
            f"rows={cp.get('rows_done', 0):>10,} / {cp.get('total_rows', 0):>10,}"
        )


if __name__ == "__main__":
    start   = datetime.now()
    run()
    elapsed = datetime.now() - start
    logging.getLogger(__name__).info(f"Total elapsed: {str(elapsed).split('.')[0]}")
