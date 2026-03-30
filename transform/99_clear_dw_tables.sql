-- ============================================================
-- NYC 311 — Clear All DW Tables (No Drop)
-- Strategy  : DROP foreign keys → TRUNCATE all → RECREATE FKs
-- Why not NOCHECK: SQL Server blocks TRUNCATE even with
--   constraints disabled via NOCHECK. The FK registration
--   itself must be removed for TRUNCATE to succeed.
--   DELETE FROM is the alternative but is logged row-by-row
--   and extremely slow on millions of rows. DROP/RECREATE FK
--   is the correct production pattern.
-- USE WITH CAUTION — irreversible without a backup
-- Author    : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

PRINT '============================================================';
PRINT 'NYC 311 — DW Table Clear';
PRINT 'Started : ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '============================================================';

-- ── STEP 1: DROP all foreign key constraints ──────────────────
-- NOCHECK CONSTRAINT is not enough — SQL Server still blocks
-- TRUNCATE on referenced tables even when the FK is disabled.
-- The FK definition itself must be removed before TRUNCATE.
-- We recreate them identically in Step 4.
PRINT 'Dropping FK constraints...';

-- All 4 FKs live on fact_service_requests
ALTER TABLE dw.fact_service_requests DROP CONSTRAINT FK_fact_date;
ALTER TABLE dw.fact_service_requests DROP CONSTRAINT FK_fact_agency;
ALTER TABLE dw.fact_service_requests DROP CONSTRAINT FK_fact_location;
ALTER TABLE dw.fact_service_requests DROP CONSTRAINT FK_fact_complaint;
GO

PRINT '  FK_fact_date, FK_fact_agency, FK_fact_location, FK_fact_complaint dropped';
GO

-- ── STEP 2: TRUNCATE fact first, then dims ────────────────────
-- Fact first is still best practice even with FKs dropped —
-- keeps the script logically correct and safe to re-order.
PRINT 'Truncating tables...';

TRUNCATE TABLE dw.fact_service_requests;
PRINT '  fact_service_requests truncated — identity reset to 1';

TRUNCATE TABLE dw.dim_date;
PRINT '  dim_date truncated';

TRUNCATE TABLE dw.dim_agency;
PRINT '  dim_agency truncated — identity reset to 1';

TRUNCATE TABLE dw.dim_location;
PRINT '  dim_location truncated — identity reset to 1';

TRUNCATE TABLE dw.dim_complaint_type;
PRINT '  dim_complaint_type truncated — identity reset to 1';
GO

-- ── STEP 3: RECREATE all foreign key constraints ──────────────
-- Recreate identically to the original DDL in
-- nyc311_create_tables_v5.sql. If these names or columns ever
-- change in the DDL, update here too.
PRINT 'Recreating FK constraints...';

ALTER TABLE dw.fact_service_requests
    ADD CONSTRAINT FK_fact_date
        FOREIGN KEY (date_key)
        REFERENCES dw.dim_date (date_key);

ALTER TABLE dw.fact_service_requests
    ADD CONSTRAINT FK_fact_agency
        FOREIGN KEY (agency_key)
        REFERENCES dw.dim_agency (agency_key);

ALTER TABLE dw.fact_service_requests
    ADD CONSTRAINT FK_fact_location
        FOREIGN KEY (location_key)
        REFERENCES dw.dim_location (location_key);

ALTER TABLE dw.fact_service_requests
    ADD CONSTRAINT FK_fact_complaint
        FOREIGN KEY (complaint_key)
        REFERENCES dw.dim_complaint_type (complaint_key);
GO

PRINT '  All 4 FK constraints recreated';
GO

-- ── STEP 4: Reset staging processed flag (optional) ──────────
-- Uncomment if you want the next ETL run to re-process all
-- staging rows from scratch. Only do this if staging data
-- is still intact and you want a full reload.
/*
PRINT 'Resetting stg_is_processed flag...';
UPDATE stg.raw_311_requests SET stg_is_processed = 0;
PRINT '  stg.raw_311_requests reset — all rows stg_is_processed = 0';
*/

-- ── STEP 5: Clear ETL log and error tables (optional) ─────────
-- Uncomment if you also want a clean audit trail.
-- Leave commented to preserve load history for comparison.
/*
PRINT 'Clearing ETL log tables...';
TRUNCATE TABLE etl.etl_errors;
TRUNCATE TABLE etl.etl_log;
PRINT '  etl.etl_errors cleared';
PRINT '  etl.etl_log cleared';
*/
GO

-- ── VERIFY: row counts + FK integrity ────────────────────────
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT
    'dw.dim_date'                  AS table_name, COUNT(*) AS row_count FROM dw.dim_date
UNION ALL SELECT 'dw.dim_agency',               COUNT(*) FROM dw.dim_agency
UNION ALL SELECT 'dw.dim_location',             COUNT(*) FROM dw.dim_location
UNION ALL SELECT 'dw.dim_complaint_type',       COUNT(*) FROM dw.dim_complaint_type
UNION ALL SELECT 'dw.fact_service_requests',    COUNT(*) FROM dw.fact_service_requests;

-- Confirm FKs are back in place
SELECT
    fk.name                        AS constraint_name,
    tp.name                        AS parent_table,
    tr.name                        AS referenced_table,
    fk.is_disabled                 AS is_disabled
FROM sys.foreign_keys              fk
JOIN sys.tables                    tp ON fk.parent_object_id    = tp.object_id
JOIN sys.tables                    tr ON fk.referenced_object_id = tr.object_id
WHERE tp.schema_id = SCHEMA_ID('dw')
ORDER BY fk.name;
GO

PRINT 'Clear complete — all DW tables empty, all FKs active';
PRINT 'Run 00_run_all_etl.sql to reload from staging';
GO
