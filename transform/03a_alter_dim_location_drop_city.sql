-- ============================================================
-- NYC 311 — One-time DDL: Remove city from dw.dim_location
-- Run ONCE before re-running 03_load_dim_location.sql
--
-- Why: city column is inconsistent in source data —
--   same zip+board has multiple city name variants
--   e.g. "LONG ISLAND CITY" vs "QUEENS" for zip 11101.
--   City is also fully derivable from borough+zip — it adds
--   no analytical value that the other columns don't cover.
--   Keeping it causes duplicate dim rows on re-load.
-- Author : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

-- Step 1 — clear existing bad data first
-- (dim_location duplicates from previous loads)
DELETE FROM dw.fact_service_requests;
PRINT 'fact_service_requests cleared';

DELETE FROM dw.dim_location;
PRINT 'dim_location cleared';
GO

-- Step 2 — drop city column from dim_location
ALTER TABLE dw.dim_location DROP COLUMN city;
PRINT 'city column dropped from dw.dim_location';
GO

-- Step 3 — drop city from the reporting view and recreate
-- (view references city — must be dropped and recreated)
DROP VIEW IF EXISTS rpt.vw_service_requests;
GO

CREATE VIEW rpt.vw_service_requests AS
SELECT
    f.request_key,
    f.unique_key,
    f.created_date,
    f.closed_date,
    f.status,
    f.resolution_days,
    d.year,
    d.quarter_name,
    d.month,
    d.month_name,
    d.month_short,
    d.day_name,
    d.is_weekend,
    d.is_holiday,
    a.agency_code,
    a.agency_name,
    a.department,
    a.sla_days,
    l.borough,
    l.community_board,
    l.zip_code,
    -- city removed — inconsistent in source, redundant with borough+zip
    l.latitude,
    l.longitude,
    l.neighborhood,
    ct.complaint_type,
    ct.descriptor,
    ct.category,
    CASE WHEN f.resolution_days > a.sla_days THEN 1 ELSE 0 END  AS is_sla_breach,
    CASE WHEN f.status != 'Closed'
         THEN DATEDIFF(DAY, f.created_date, GETDATE())
         ELSE NULL END                                           AS days_open_if_pending,
    CASE
        WHEN f.resolution_days IS NULL THEN 'Open'
        WHEN f.resolution_days = 0     THEN 'Same Day'
        WHEN f.resolution_days <= 3    THEN '1-3 Days'
        WHEN f.resolution_days <= 7    THEN '4-7 Days'
        WHEN f.resolution_days <= 30   THEN '8-30 Days'
        ELSE 'Over 30 Days'
    END                                                         AS resolution_bucket
FROM      dw.fact_service_requests  f
JOIN      dw.dim_date           d   ON f.date_key      = d.date_key
JOIN      dw.dim_agency         a   ON f.agency_key    = a.agency_key
JOIN      dw.dim_location       l   ON f.location_key  = l.location_key
JOIN      dw.dim_complaint_type ct  ON f.complaint_key = ct.complaint_key;
GO

PRINT 'rpt.vw_service_requests recreated without city column';

-- Step 4 — reset staging so ETL reprocesses all rows
UPDATE stg.raw_311_requests SET stg_is_processed = 0;
PRINT 'stg_is_processed reset — ready for full reload';
GO

PRINT '============================================================';
PRINT 'DDL fix complete. Now run in order:';
PRINT '  03_load_dim_location.sql';
PRINT '  04_load_dim_complaint_type.sql  (if not already loaded)';
PRINT '  EXEC dw.usp_load_fact_service_requests';
PRINT '============================================================';
GO
