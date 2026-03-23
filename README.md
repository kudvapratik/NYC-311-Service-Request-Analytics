# NYC 311 Service Request Analytics

An end-to-end analytics pipeline on New York City's public 311 dataset — 3.4 million service requests in 2024. Built with Python, SQL Server, and Power BI.

---

## Problem Statement

New York City receives over 3.4 million 311 service requests every year across 30+ city agencies. While the raw data is publicly available, it exists as a single wide flat file with 40+ columns, repeated text, inconsistent casing, and no analytical structure.

This project answers three operational questions:

**1. What are residents complaining about?**
Illegal Parking accounts for 14.8% of all requests — the single largest complaint type. The top 10 complaint types drive 60% of total volume. Without a structured pipeline, identifying these patterns requires manual effort on a 2GB CSV file.

**2. Which agencies are failing their SLAs?**
The Department of Buildings (DOB) has a 52.7% SLA breach rate — more than half of all closed cases exceeded the target resolution time. NYPD resolves cases in 0.2 days on average. The gap between best and worst performing agencies is invisible in the raw data.

**3. Where are the problem areas?**
Brooklyn leads raw volume at 1.02 million requests. The Bronx has the highest noise complaint rate per capita. Heat complaints average 2.1 days resolution — but the 95th percentile is 31 days. Someone in the Bronx waited a month without heat in January.

This pipeline transforms raw open data into a structured, queryable, visualisable analytics system.

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

## Data Architecture

```
NYC Open Data API (JSON)
        │
        ▼
download_nyc311_v2.py          Python script
Paginates API → saves CSV      One CSV per year
nyc311_2022.csv                Auto-saves every 5 pages
nyc311_2023.csv                Resumes from checkpoint on failure
nyc311_2024.csv
        │
        ▼
nyc311_load_staging.py         Python + pandas + SQLAlchemy
CSV → SQL Server               Bulk insert in 10,000 row batches
stg.raw_311_requests           All columns VARCHAR — no type conversion
                               stg_is_processed = 0 (unprocessed)
        │
        ▼
stg to dw					   (documentation coming soon)
        │
        ▼
Power BI Dashboard             (documentation coming soon)
```

**Technology stack:**

| Layer | Technology |
|---|---|
| Download | Python 3.11, requests, csv |
| Staging load | Python 3.11, pandas, SQLAlchemy, pyodbc |
| Database | SQL Server 2019 |
| ETL | T-SQL stored procedures and scripts |
| Reporting | Power BI Desktop |

---
