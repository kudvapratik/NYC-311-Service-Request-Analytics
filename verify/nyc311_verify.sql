-- ============================================================
-- NYC 311 — Verification & Data Quality Report
-- Script 4 of 4
-- Run after stg → dw load to confirm everything is correct
-- Author  : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

PRINT '============================================';
PRINT 'NYC 311 DATA QUALITY REPORT';
PRINT CAST(GETDATE() AS VARCHAR);
PRINT '============================================';

-- ============================================================
-- SECTION 1 — Row Counts
-- ============================================================
PRINT '';
PRINT '--- ROW COUNTS ---';

SELECT
    'stg.raw_311_requests'       AS table_name,
    COUNT(*)                      AS total_rows,
    SUM(CASE WHEN stg_is_processed = 1 THEN 1 ELSE 0 END) AS processed,
    SUM(CASE WHEN stg_is_processed = 0 THEN 1 ELSE 0 END) AS pending
FROM stg.raw_311_requests
UNION ALL
SELECT 'dw.dim_date',           COUNT(*), NULL, NULL FROM dw.dim_date
UNION ALL
SELECT 'dw.dim_agency',         COUNT(*), NULL, NULL FROM dw.dim_agency
UNION ALL
SELECT 'dw.dim_location',       COUNT(*), NULL, NULL FROM dw.dim_location
UNION ALL
SELECT 'dw.dim_complaint_type', COUNT(*), NULL, NULL FROM dw.dim_complaint_type
UNION ALL
SELECT 'dw.fact_service_requests', COUNT(*), NULL, NULL FROM dw.fact_service_requests;
GO

-- ============================================================
-- SECTION 2 — Fact Table Data Quality
-- ============================================================
PRINT '';
PRINT '--- FACT TABLE QUALITY ---';

SELECT
    COUNT(*)                                            AS total_rows,
    SUM(CASE WHEN closed_date IS NULL     THEN 1 ELSE 0 END) AS open_cases,
    SUM(CASE WHEN closed_date IS NOT NULL THEN 1 ELSE 0 END) AS closed_cases,
    SUM(CASE WHEN resolution_days < 0    THEN 1 ELSE 0 END) AS negative_days,
    SUM(CASE WHEN resolution_days > 365  THEN 1 ELSE 0 END) AS over_365_days,
    MIN(created_date)                                   AS earliest_date,
    MAX(created_date)                                   AS latest_date,
    AVG(CAST(resolution_days AS FLOAT))                 AS avg_resolution_days
FROM dw.fact_service_requests;
GO

-- ============================================================
-- SECTION 3 — Top 10 Complaint Types
-- Quick sanity check — should match NYC Open Data stats
-- ============================================================
PRINT '';
PRINT '--- TOP 10 COMPLAINT TYPES ---';

SELECT TOP 10
    ct.complaint_type,
    ct.category,
    a.agency_code,
    COUNT(*)                                AS total_requests,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                   AS pct_of_total,
    AVG(CAST(f.resolution_days AS FLOAT))   AS avg_days
FROM dw.fact_service_requests   f
JOIN dw.dim_complaint_type      ct ON f.complaint_key = ct.complaint_key
JOIN dw.dim_agency              a  ON f.agency_key    = a.agency_key
GROUP BY ct.complaint_type, ct.category, a.agency_code
ORDER BY total_requests DESC;
GO

-- ============================================================
-- SECTION 4 — Volume by Borough
-- ============================================================
PRINT '';
PRINT '--- VOLUME BY BOROUGH ---';

SELECT
    l.borough,
    COUNT(*)                                AS total_requests,
    SUM(CASE WHEN f.status = 'Closed'
             THEN 1 ELSE 0 END)             AS closed,
    SUM(CASE WHEN f.status != 'Closed'
             THEN 1 ELSE 0 END)             AS open,
    AVG(CAST(f.resolution_days AS FLOAT))   AS avg_resolution_days
FROM dw.fact_service_requests   f
JOIN dw.dim_location            l  ON f.location_key = l.location_key
GROUP BY l.borough
ORDER BY total_requests DESC;
GO

-- ============================================================
-- SECTION 5 — Agency SLA Performance
-- ============================================================
PRINT '';
PRINT '--- AGENCY SLA PERFORMANCE ---';

SELECT
    a.agency_code,
    a.sla_days                              AS sla_threshold,
    COUNT(*)                                AS total_closed,
    AVG(CAST(f.resolution_days AS FLOAT))   AS avg_days,
    SUM(CASE WHEN f.resolution_days > a.sla_days
             THEN 1 ELSE 0 END)             AS sla_breaches,
    CAST(
        SUM(CASE WHEN f.resolution_days > a.sla_days
                 THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
        AS DECIMAL(5,1)
    )                                       AS breach_pct
FROM dw.fact_service_requests   f
JOIN dw.dim_agency              a  ON f.agency_key = a.agency_key
WHERE f.status = 'Closed'
GROUP BY a.agency_code, a.sla_days
ORDER BY breach_pct DESC;
GO

-- ============================================================
-- SECTION 6 — Monthly Trend 2024
-- ============================================================
PRINT '';
PRINT '--- MONTHLY TREND 2024 ---';

SELECT
    d.month,
    d.month_short,
    COUNT(*)                                AS request_count,
    SUM(CASE WHEN f.status = 'Closed'
             THEN 1 ELSE 0 END)             AS closed_count,
    CAST(
        SUM(CASE WHEN f.status = 'Closed' THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
        AS DECIMAL(5,1)
    )                                       AS closure_rate_pct
FROM dw.fact_service_requests   f
JOIN dw.dim_date                d  ON f.date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.month, d.month_short
ORDER BY d.month;
GO

-- ============================================================
-- SECTION 7 — Reporting View Check
-- Confirms Power BI can connect successfully
-- ============================================================
PRINT '';
PRINT '--- REPORTING VIEW CHECK ---';

SELECT TOP 5
    year,
    month_name,
    agency_code,
    borough,
    complaint_type,
    category,
    status,
    resolution_days,
    is_sla_breach
FROM rpt.vw_service_requests
ORDER BY created_date DESC;
GO

PRINT '';
PRINT '============================================';
PRINT 'DATA QUALITY REPORT COMPLETE';
PRINT '============================================';
GO
