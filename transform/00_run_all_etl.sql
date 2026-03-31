-- ============================================================
-- NYC 311 — Master ETL Orchestrator
-- Runs all 5 steps in correct dependency order
-- Run this script to execute a full stg → dw load
--
-- Run order:
--   00  etl_log_tables.sql         (one-time setup only)
--   01  load_dim_date.sql          (no dependency)
--   02  load_dim_agency.sql        (needs stg data)
--   03  load_dim_location.sql      (needs stg data)
--   04  load_dim_complaint_type.sql(needs stg data)
--   05  usp_load_fact_sp.sql       (needs all 4 dims)
--   THIS FILE — runs 01–05 in sequence
--
-- Author : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

DECLARE @batch_id INT = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT);
PRINT '============================================================';
PRINT 'NYC 311 STG → DW ETL — Master Run';
PRINT 'Batch ID : ' + CAST(@batch_id AS VARCHAR);
PRINT 'Started  : ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '============================================================';
GO

-- Step 1 — dim_date
:r 01_load_dim_date.sql
GO

-- Step 2 — dim_agency
:r 02_load_dim_agency.sql
GO

-- Step 3 — dim_location
:r 03_load_dim_location.sql
GO

-- Step 4 — dim_complaint_type
:r 04_load_dim_complaint_type.sql
GO

 Step 5 — fact table via stored procedure
EXEC dw.usp_load_fact_service_requests
    @batch_id   = NULL,  -- defaults to today
    @debug_mode = 0;
GO

-- ── LOAD SUMMARY ─────────────────────────────────────────────
PRINT '============================================================';
PRINT 'ETL RUN SUMMARY';
PRINT '============================================================';

SELECT
    step_number,
    step_name,
    status,
    rows_read,
    rows_inserted,
    rows_skipped,
    rows_rejected,
    duration_sec,
    error_message
FROM etl.etl_log
WHERE batch_id = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT)
ORDER BY step_number;
GO

-- ── ERROR SUMMARY ─────────────────────────────────────────────
SELECT
    step_name,
    error_type,
    COUNT(*)                AS error_count,
    MIN(error_message)      AS example_message
FROM etl.etl_errors
WHERE batch_id = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT)
GROUP BY step_name, error_type
ORDER BY step_name, error_count DESC;
GO

PRINT 'Master ETL complete — check results above';
GO
