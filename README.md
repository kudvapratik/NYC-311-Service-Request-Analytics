# NYC 311 Service Request Analytics

An end-to-end analytics pipeline on New York City's public 311 dataset — 3.4 million service requests in 2024. Built with Python, SQL Server, and Power BI.

---
**Stack:** Python · SQL Server 2019 · T-SQL Stored Procedures · Power BI

---
## 📊 Dashboard Preview

### Executive Summary
![Executive Summary](screenshots/Executive_Summary.png)

### Agency Summary
![Agency Summary](screenshots/Agency_Summary.png)

### Borough Map
![Borough Map](screenshots/Borough_Map.png)

### Complaint Analysis
![Complaint Analysis](screenshots/Complaint_Analysis.png)

### Open Cases
![Open Cases](screenshots/Open_Requests.png)

---

## Source Data

| Property | Detail |
|---|---|
| Dataset name | 311 Service Requests from 2010 to Present |
| Dataset ID | erm2-nwe9 |
| Publisher | NYC Open Data |
| URL | https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9 |
| API endpoint | https://data.cityofnewyork.us/resource/erm2-nwe9.json |
| Format | JSON API / CSV download |
| Update frequency | Daily |
| Total rows | 35+ million (2010 to present) |
| 2024 rows | 3.4 million |
| Columns | 41 per row |

The dataset uses the Socrata Open Data API (SODA). No API key is required for public access. Each row represents one service request — from the moment a resident calls 311 to the moment the assigned agency closes the case.

**Known data quality issues in source:**
- `agency_name` column contains incorrect values — use `agency` (short code) as the reliable join key
- `borough` field contains `Unspecified` for approximately 3% of records — filtered out during staging to DW load
- `latitude` and `longitude` arrive as strings — some rows contain `0` or blank — treated as NULL in the warehouse
- `closed_date` is NULL for all open cases — expected, not a data quality error

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      NYC Open Data (SODA API)                   │
│               data.cityofnewyork.us — 311 dataset               │
└─────────────────────────┬───────────────────────────────────────┘
                          │  download_nyc311_v2.py
                          │  (paginated, checkpoint-resume)
                          ▼
                    nyc311_raw.csv
                          │  nyc311_load_staging.py
                          │  (10k-row batch INSERT, validation)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  SQL Server — NYC311_Analytics                                  │
│                                                                 │
│  stg.raw_311_requests       ← raw landing zone (all strings)   │
│          │                                                      │
│          │  00_run_all_etl.sql (steps 01 → 05)                 │
│          ▼                                                      │
│  dw.dim_date                ← 2020-2030 calendar spine         │
│  dw.dim_agency              ← 15 agencies + SLA targets        │
│  dw.dim_location            ← borough + zip + coordinates      │
│  dw.dim_complaint_type      ← 186 types → 6 categories         │
│  dw.fact_service_requests   ← 3M+ rows/year, computed resolution│
│          │                                                      │
│  rpt.vw_service_requests    ← flattened reporting view         │
│  rpt.vw_agency_summary      ← pre-aggregated agency metrics    │
└─────────────────────────┬───────────────────────────────────────┘
                          │  Direct Query / Import
                          ▼
               Power BI — nyc311_analytics.pbip
               5 pages · DAX measures · drill-through
```

---

## 🗄️ Star Schema

| Table | Rows | Description |
|-------|------|-------------|
| `dw.dim_date` | 4,018 | One row per calendar day 2020–2030. Required for DAX time intelligence. |
| `dw.dim_agency` | 116 | NYC agencies with SLA day targets (NYPD=2, HPD=10, DPR=14, etc.) |
| `dw.dim_location` | 719 | Borough + community board + zip code + lat/long |
| `dw.dim_complaint_type` | 1321 | Complaint types grouped into 7 categories via keyword enrichment |
| `dw.fact_service_requests` | 9M+ | One row per 311 request. `resolution_days` is a PERSISTED computed column. |

**Foreign keys:** All 4 dimension keys enforced on the fact table. `TRUNCATE` resets via FK drop/recreate pattern (see `99_clear_dw_tables.sql`).

**ETL audit trail:** `etl.etl_log` captures batch ID, row counts (read / inserted / skipped / rejected) and duration per step. `etl.etl_errors` captures every rejected row with a typed reason code.

---

## 📁 Repository Structure

```
NYC-311-Service-Request-Analytics/
NYC-311-Service-Request-Analytics/
│
├── extract/
│   └── download_nyc311.py                       # SODA API downloader — paginated, checkpoint-resume
│
├── load/
│   └── nyc311_load_staging.py                   # CSV → stg.raw_311_requests in 10k-row batches
│
├── transform/
│   ├── nyc311_create_tables.sql              # DDL — database, schemas, star schema, indexes, FKs, views
│   ├── 00_etl_log_tables.sql                    # One-time setup — etl schema, log + error tables
│   ├── 00_run_all_etl.sql                       # Master orchestrator — runs steps 01 → 05 in sequence
│   ├── 01_load_dim_date.sql                     # Generates 2020–2030 date spine via recursive CTE
│   ├── 02_load_dim_agency.sql                   # Loads agencies + sets SLA day targets
│   ├── 03_load_dim_location.sql                 # Loads borough / zip / coordinate dimension
│   ├── 03a_alter_dim_location_drop_city.sql     # Patch — removes redundant city column
│   ├── 04_load_dim_complaint_type.sql           # Loads 186 types, derives 6 categories via LIKE
│   ├── 05_usp_load_fact_service_requests.sql    # Stored procedure — 5-stage temp table ETL pipeline
│   ├── dw.ToPascalCase.sql                      # Scalar UDF — title-cases agency names
│   ├── nyc311_stg_to_dw.sql                     # Legacy single-file ETL (pre-modular reference)
│   └── 99_clear_dw_tables.sql                   # Drop FKs → TRUNCATE all → recreate FKs
│
├── verify/
│   └── nyc311_verify.sql                        # 7-section data quality report
│
├── dashboard/
│   └── NYC_311_2024_Report/
│       ├── NYC_311_2024_Report.Report/          # Visual definitions — 5 report pages as JSON
│       └── NYC_311_2024_Report.SemanticModel/   # Data model — tables, measures, relationships, RLS
│
├── data/
│   ├── 311_ServiceRequest_DataDictionary.xlsx   # NYC Open Data official field definitions
│   └── nyc311_sample_100rows.csv                # 100-row sample for schema reference
│
├── docs/
│   └── nyc311_powerbi_guide.html                # Step-by-step Power BI build guide — visuals, DAX, formatting
│
├── Screenshots/
│   ├── Executive Summary.png
│   ├── Agency Summary.png
│   ├── Borough Map.png
│   ├── Complaint Analysis.png
│   └── Open Requests.png
│
├── .gitattributes
├── .gitignore
└── README.md
```

---

## ⚙️ Setup & Run Order

### Prerequisites

- SQL Server 2019+ with ODBC Driver 17 for SQL Server
- Python 3.9+ — install dependencies: `pip install pandas sqlalchemy pyodbc requests`
- Power BI Desktop (June 2024+) for the `.pbip` file

### Step 1 — Download Raw Data from NYC Open Data API

```bash
python python/download_nyc311.py
```

Downloads all 311 requests for the configured year via the SODA API. Saves paginated results to `nyc311_raw.csv`. Supports checkpoint-resume if the download is interrupted — re-run with `--resume` flag.

### Step 2 — Create Database, Schema & Tables

Run in SSMS in this order:

```sql
-- 1. Create database, schemas (stg / dw / rpt), all tables, indexes, FKs, reporting views
sql/nyc311_create_tables_v5.sql

-- 2. Create ETL audit schema and log/error tables (one-time only)
sql/00_etl_log_tables.sql
```

### Step 3 — Load Staging Table

```bash
python python/nyc311_load_staging.py
```

Reads `nyc311_raw.csv` → truncates `stg.raw_311_requests` → loads in 10,000-row batches → prints a verification summary (row count, null checks, distinct agency/borough/complaint counts).

### Step 4 — Run Full ETL (Staging → Data Warehouse)

```sql
-- Runs all 5 dimension + fact load steps in dependency order
sql/00_run_all_etl.sql
```

Each step logs to `etl.etl_log`. Rejected rows go to `etl.etl_errors` with typed reason codes (`NULL_KEY`, `BAD_DATE`, `NO_DIM_MATCH`, `DUPLICATE`, etc.).

### Step 5 — Verify Data Quality

```sql
sql/nyc311_verify.sql
```

Runs 7 checks: row counts across all tables, fact quality (negatives, nulls), top 10 complaint types, borough volumes, agency SLA performance, monthly trend, and a reporting view spot-check.

### Step 6 — Open in Power BI

Open `powerbi/nyc311_analytics.pbip` directly in Power BI Desktop, or follow the step-by-step guide at `powerbi/nyc311_powerbi_guide.html` to build it from scratch.

> **To reset and reload:** run `sql/99_clear_dw_tables.sql` — this drops foreign keys, truncates all DW tables (identity reset), then recreates the FK constraints. Then re-run `00_run_all_etl.sql`.

---

## 🔑 Key Findings — 2024 Data

### Volume & Closure

| Metric | Value |
|--------|-------|
| Total service requests | **3.32M** |
| Closed requests | **3.28M** |
| Open requests | **38.8K** |
| Closure rate | **98.83%** (+0.3% vs 2023) |
| Avg resolution time | **25.21 days** (−12.17 days vs 2023) |
| SLA breach rate | **16.12%** ⚠️ above 10% threshold |
| YoY volume growth | **+7.61%** vs 2023 |

### Open Cases Backlog

| Metric | Value |
|--------|-------|
| Total open cases | **169K** (across all years) |
| Avg days waiting | **503.81 days** (+493 days over SLA target) |
| Stale cases (90+ days) | **168K** |
| Cases waiting 365+ days | **90K** |
| Longest unresolved case | **1,184 days** — DEP, Adopt-a-basket, Jan 2023 |
| Highest open backlog agency | **DPR: 47K open** |

### Borough Breakdown

| Borough | Requests | Closed | Open | SLA Breach % |
|---------|----------|--------|------|--------------|
| Brooklyn | 1,013K | 1,000K | 11.43K | 16.78% |
| Queens | 790.89K | 776.81K | 14.08K | 13.97% |
| Bronx | 700.64K | 697.25K | 3.39K | 14.70% |
| Manhattan | 696.29K | 688.52K | 7.77K | 19.07% |
| Staten Island | 115.64K | 113.51K | 2.12K | 15.87% |

### Agency Performance

| Agency | Requests | Avg Resolution Days | SLA Breach % |
|--------|----------|---------------------|--------------|
| NYPD | 1,472,166 | 1.0 | 0.07% ✅ |
| DSNY | 283,921 | 6.6 | 24.13% |
| DEP | 190,476 | 7.6 | 11.56% |
| DOT | 185,927 | 15.8 | 10.87% |
| HPD | 728,730 | 15.2 | 28.75% |
| DOHMH | — | — | 37.72% |
| DOB | 102,377 | 73.0 | 51.01% |
| TLC | 32,962 | 85.5 | 85.50% ❌ |
| DPR | 132,280 | 128.3 | 56.15% ❌ |
| EDC | 27,894 | 53.4 | 91.63% ❌ |

**Average across all agencies: 25.2 days**

### Top Complaint Types

| Rank | Complaint Type | Volume |
|------|---------------|--------|
| 1 | Illegal Parking | 472K |
| 2 | Noise – Residential | 374K |
| 3 | Heat / Hot Water | 262K |
| 4 | Blocked Driveway | 168K |
| 5 | Noise – Street/Sidewalk | 150K |
| 6 | Unsanitary Condition | 120K |
| 7 | Plumbing | 69K |
| 8 | Noise – Commercial | 67K |
| 9 | Water System | 65K |
| 10 | Abandoned Vehicle | 65K |

**Complaint categories** (6 total): Other (1.024M) · Noise (726K) · Vehicle (658K) · Housing (455K) · Infrastructure (217K) · Environment (202K)

### Resolution Time Distribution

| Bucket | Requests |
|--------|----------|
| Same Day | 1.53M |
| 1–3 Days | 0.90M |
| 4–7 Days | 0.23M |
| 8–30 Days | 0.31M |
| Over 30 Days | 0.28M |
| Open | 0.06M |

---

## 📄 ETL Design Notes

**Temp table pipeline (fact load):** The stored procedure `dw.usp_load_fact_service_requests` processes staging data through 5 named temp tables (`#t1_raw → #t2_valid_dates → #t3_dim_matched → #t4_deduped → INSERT`). Each stage isolates one validation class so failures are diagnosable at the exact step they occur.

**Error capture:** Typed error codes are stored in `etl.etl_errors` — `NULL_KEY`, `BAD_DATE`, `NEGATIVE_RESOLUTION`, `NO_DIM_AGENCY`, `NO_DIM_LOCATION`, `NO_DIM_COMPLAINT`, `NO_DIM_DATE`, `DUPLICATE_KEY`, `INVALID_STATUS`. Capped at 1,000 rows per error type per batch to prevent log bloat.

**Category enrichment:** The `complaint_type` column from the source has 186 distinct values with no category label. The ETL derives a `category` column using ordered LIKE pattern matching (`Noise → Vehicle → Housing → Infrastructure → Environment → Safety → Other`). This enrichment happens in `04_load_dim_complaint_type.sql`.

**Idempotent SLA updates:** Agency SLA day targets are hard-coded in `02_load_dim_agency.sql` as `UPDATE` statements that run every batch — ensuring values stay correct even if an agency row was inserted by a prior run.

---

## 💡 Technical Highlights

- **PERSISTED computed column** on `resolution_days` — stored on disk, indexable, calculated automatically from `created_date` and `closed_date`
- **Recursive CTE** for date dimension generation — produces 4,018 gapless rows from 2020-01-01 to 2030-12-31
- **Checkpoint-resume download** — the SODA API downloader saves progress every N pages and can resume from the last successful page after a timeout or failure
- **10K-row batch inserts** — staging load uses `pandas` chunked `to_sql` to keep memory usage bounded on large CSV files
- **FK drop/recreate for TRUNCATE** — `99_clear_dw_tables.sql` uses the correct SQL Server pattern: `ALTER TABLE DROP CONSTRAINT` → `TRUNCATE` → `ALTER TABLE ADD CONSTRAINT` (NOCHECK alone does not allow TRUNCATE on referenced tables)
- **Power BI Project format (.pbip)** — saved as JSON-based project files instead of binary `.pbix`, enabling proper Git diffing and version control

---

## 👤 Author

**Pratik Kudva**  
Data Engineer / Analyst  
[GitHub](https://github.com/kudvapratik) · [LinkedIn](https://linkedin.com/in/pratik-kudva)

---

*Data source: [NYC Open Data — 311 Service Requests from 2010 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)*