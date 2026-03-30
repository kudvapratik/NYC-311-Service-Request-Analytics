-- ============================================================
-- NYC 311 Analytics — Star Schema
-- Version  : 4.0
-- SQL Server 2019+
-- Author   : Pratik Kudva
-- ============================================================

USE master;
GO
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'NYC311_Analytics')
    DROP DATABASE NYC311_Analytics;
GO
CREATE DATABASE NYC311_Analytics
    COLLATE SQL_Latin1_General_CP1_CI_AS;
GO
USE NYC311_Analytics;
GO
-- ============================================================
-- SCHEMAS
-- stg = raw data landing zone
-- dw  = clean star schema
-- rpt = reporting views for Power BI
-- ============================================================
CREATE SCHEMA stg;
GO
CREATE SCHEMA dw;
GO
CREATE SCHEMA rpt;
GO
-- ============================================================
-- DIM_DATE
-- One row per calendar day 2020-2030
-- Required by Power BI DAX time intelligence functions
-- ============================================================
CREATE TABLE dw.dim_date (
    date_key        INT          NOT NULL,  -- YYYYMMDD e.g. 20240115
    full_date       DATE         NOT NULL,
    year            INT          NOT NULL,
    quarter         INT          NOT NULL,
    quarter_name    VARCHAR(6)   NOT NULL,  -- Q1 Q2 Q3 Q4
    month           INT          NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,  -- January
    month_short     VARCHAR(3)   NOT NULL,  -- Jan
    week_of_year    INT          NOT NULL,
    day_of_month    INT          NOT NULL,
    day_of_week     INT          NOT NULL,  -- 1=Sunday 7=Saturday
    day_name        VARCHAR(10)  NOT NULL,  -- Monday
    is_weekend      BIT          NOT NULL,  -- 1=Sat or Sun
    is_holiday      BIT          NOT NULL DEFAULT 0
    CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_key)
);
GO
CREATE NONCLUSTERED INDEX IX_dim_date_year
    ON dw.dim_date (year)
    INCLUDE (month, quarter, month_name, month_short, is_weekend);
GO
CREATE NONCLUSTERED INDEX IX_dim_date_full_date
    ON dw.dim_date (full_date)
    INCLUDE (date_key, year, month, quarter);
GO
-- ============================================================
-- DIM_AGENCY
-- One row per NYC city agency (~30 rows)
-- sla_days varies per agency — NYPD=2, HPD=10, DOB=15
-- ============================================================
CREATE TABLE dw.dim_agency (
    agency_key      INT           NOT NULL IDENTITY(1,1),
    agency_code     VARCHAR(10)   NOT NULL,  -- NYPD HPD DOT etc
    agency_name     VARCHAR(100)  NOT NULL,  -- Full name stored once
    department      VARCHAR(100)  NULL,
    sla_days        INT           NOT NULL DEFAULT 10,
    is_active       BIT           NOT NULL DEFAULT 1
    CONSTRAINT PK_dim_agency      PRIMARY KEY CLUSTERED (agency_key),
    CONSTRAINT UQ_dim_agency_code UNIQUE (agency_code)
);
GO
CREATE NONCLUSTERED INDEX IX_dim_agency_code
    ON dw.dim_agency (agency_code)
    INCLUDE (agency_key, agency_name, sla_days);
GO
-- ============================================================
-- DIM_LOCATION
-- One row per unique borough + zip combination (~50K rows)
-- Lat/long stored here for Power BI map visuals
-- DECIMAL(9,6) chosen over FLOAT — avoids map pin drift
-- ============================================================
CREATE TABLE dw.dim_location (
    location_key    INT            NOT NULL IDENTITY(1,1),
    borough         VARCHAR(50)    NOT NULL,  -- Stored UPPER case
    community_board VARCHAR(50)    NULL,       -- 59 NYC boards
    zip_code        VARCHAR(10)    NULL,       -- VARCHAR — zips start with 0
    city            VARCHAR(50)    NULL,
    latitude        DECIMAL(9,6)   NULL,       -- 6dp = ~0.1 metre accuracy
    longitude       DECIMAL(9,6)   NULL,       -- Negative for NYC (west)
    neighborhood    VARCHAR(100)   NULL        -- Enriched post-load
    CONSTRAINT PK_dim_location PRIMARY KEY CLUSTERED (location_key)
);
GO
CREATE NONCLUSTERED INDEX IX_dim_location_borough
    ON dw.dim_location (borough)
    INCLUDE (location_key, zip_code, community_board, latitude, longitude);
GO
CREATE NONCLUSTERED INDEX IX_dim_location_zip
    ON dw.dim_location (zip_code)
    INCLUDE (location_key, borough);
GO
-- ============================================================
-- DIM_COMPLAINT_TYPE
-- One row per unique complaint + descriptor (~400 rows)
-- category column is DERIVED — not in source data!
-- Groups 186 complaint types into 6 categories
-- ============================================================
CREATE TABLE dw.dim_complaint_type (
    complaint_key   INT           NOT NULL IDENTITY(1,1),
    complaint_type  VARCHAR(100)  NOT NULL,  -- Illegal Parking
    descriptor      VARCHAR(200)  NULL,       -- Blocked Hydrant
    category        VARCHAR(50)   NOT NULL DEFAULT 'Other'--,
    --agency_code     VARCHAR(10)   NOT NULL
    CONSTRAINT PK_dim_complaint_type PRIMARY KEY CLUSTERED (complaint_key),
    CONSTRAINT UQ_dim_complaint      UNIQUE (complaint_type, descriptor)
);
GO
CREATE NONCLUSTERED INDEX IX_dim_complaint_type
    ON dw.dim_complaint_type (complaint_type)
    INCLUDE (complaint_key, category--, agency_code
    );
GO
CREATE NONCLUSTERED INDEX IX_dim_complaint_category
    ON dw.dim_complaint_type (category)
    INCLUDE (complaint_key, complaint_type--, agency_code
    );
GO
-- ============================================================
-- FACT_SERVICE_REQUESTS
-- One row per 311 service request (3.4M rows for 2024)
-- Stores integer FK keys — no repeated text
-- resolution_days is PERSISTED computed column
-- ============================================================
CREATE TABLE dw.fact_service_requests (
    request_key     INT          NOT NULL IDENTITY(1,1),
    unique_key      VARCHAR(20)  NOT NULL,  -- NYC source system key
    date_key        INT          NOT NULL,  -- FK → dim_date (YYYYMMDD)
    agency_key      INT          NOT NULL,  -- FK → dim_agency
    location_key    INT          NOT NULL,  -- FK → dim_location
    complaint_key   INT          NOT NULL,  -- FK → dim_complaint_type
    created_date    DATETIME     NOT NULL,  -- When resident called 311
    closed_date     DATETIME     NULL,       -- NULL if still open
    status          VARCHAR(20)  NOT NULL,  -- Open/Closed/Pending

    -- Computed — SQL Server calculates automatically
    -- PERSISTED = stored on disk, faster reads, can be indexed
    -- NULL for open cases (closed_date IS NULL)
    resolution_days AS (
        CASE
            WHEN closed_date IS NOT NULL
            THEN DATEDIFF(DAY, created_date, closed_date)
            ELSE NULL
        END
    ) PERSISTED,

    -- Audit columns
    dw_created_date DATETIME     NOT NULL DEFAULT GETDATE(),
    dw_batch_id     INT          NULL,

    CONSTRAINT PK_fact_service_requests PRIMARY KEY CLUSTERED (request_key),
    CONSTRAINT UQ_fact_unique_key        UNIQUE (unique_key),

    CONSTRAINT FK_fact_date
        FOREIGN KEY (date_key)      REFERENCES dw.dim_date (date_key),
    CONSTRAINT FK_fact_agency
        FOREIGN KEY (agency_key)    REFERENCES dw.dim_agency (agency_key),
    CONSTRAINT FK_fact_location
        FOREIGN KEY (location_key)  REFERENCES dw.dim_location (location_key),
    CONSTRAINT FK_fact_complaint
        FOREIGN KEY (complaint_key) REFERENCES dw.dim_complaint_type (complaint_key),

    -- Data quality guards
    CONSTRAINT CK_fact_resolution_days
        CHECK (resolution_days IS NULL OR resolution_days >= 0),
    CONSTRAINT CK_fact_status
        CHECK (status IN ('Open','Closed','Pending',
                          'In Progress','Unspecified','Assigned'))
);
GO
-- ── FACT TABLE INDEXES ────────────────────────────────────────
-- Fix 1: removed duplicate IX_fact_date_agency
-- Fix 2: removed filtered index on computed column resolution_days
--        SQL Server does not allow WHERE on computed columns in indexes
--        Instead use a regular index — optimizer handles NULLs

CREATE NONCLUSTERED INDEX IX_fact_date_key
    ON dw.fact_service_requests (date_key)
    INCLUDE (agency_key, complaint_key, location_key,
             status, resolution_days);
GO
CREATE NONCLUSTERED INDEX IX_fact_agency_key
    ON dw.fact_service_requests (agency_key)
    INCLUDE (date_key, status, resolution_days, complaint_key);
GO
CREATE NONCLUSTERED INDEX IX_fact_status
    ON dw.fact_service_requests (status)
    INCLUDE (agency_key, date_key, location_key, created_date);
GO
CREATE NONCLUSTERED INDEX IX_fact_location_key
    ON dw.fact_service_requests (location_key)
    INCLUDE (complaint_key, status, resolution_days);
GO
CREATE NONCLUSTERED INDEX IX_fact_complaint_key
    ON dw.fact_service_requests (complaint_key)
    INCLUDE (date_key, agency_key, status, resolution_days);
GO
-- Fix 2: regular index on resolution_days — no WHERE filter
-- PERSISTED computed column CAN be indexed — just no filter
CREATE NONCLUSTERED INDEX IX_fact_resolution_days
    ON dw.fact_service_requests (resolution_days)
    INCLUDE (agency_key, complaint_key, status);
GO
-- Composite — covers common date + agency queries
CREATE NONCLUSTERED INDEX IX_fact_date_agency
    ON dw.fact_service_requests (date_key, agency_key)
    INCLUDE (status, resolution_days, complaint_key, location_key);
GO
-- ============================================================
-- STAGING TABLE
-- Raw CSV data lands here first — all VARCHAR, unvalidated
-- stg_is_processed = 0 means not yet loaded to dw
-- Enables incremental loads — next run skips processed rows
-- ============================================================
CREATE TABLE stg.raw_311_requests (
    unique_key              VARCHAR(20)   NULL,
    created_date            VARCHAR(50)   NULL,  -- Raw string "01/15/2024 08:23:45 AM"
    closed_date             VARCHAR(50)   NULL,  -- NULL for open cases
    agency                  VARCHAR(20)   NULL,  -- Short code e.g. NYPD
    agency_name             VARCHAR(200)  NULL,  -- Has bugs in source — use agency
    complaint_type          VARCHAR(200)  NULL,
    descriptor              VARCHAR(500)  NULL,
    location_type           VARCHAR(200)  NULL,  -- Not loaded to dw
    incident_zip            VARCHAR(20)   NULL,  -- VARCHAR — zips start with 0
    incident_address        VARCHAR(500)  NULL,  -- Not loaded to dw
    city                    VARCHAR(100)  NULL,
    borough                 VARCHAR(100)  NULL,
    latitude                VARCHAR(50)   NULL,  -- Validate before DECIMAL cast
    longitude               VARCHAR(50)   NULL,  -- Validate before DECIMAL cast
    status                  VARCHAR(50)   NULL,
    resolution_description  VARCHAR(MAX)  NULL,  -- Not loaded to dw — too large
    community_board         VARCHAR(100)  NULL,

    -- Audit columns
    stg_load_date           DATETIME      NOT NULL DEFAULT GETDATE(),
    stg_batch_id            BIGINT        NULL,
    stg_is_processed        BIT           NOT NULL DEFAULT 0,  -- 0=pending 1=done
    stg_error_message       VARCHAR(MAX)  NULL
);
GO
CREATE NONCLUSTERED INDEX IX_stg_is_processed
    ON stg.raw_311_requests (stg_is_processed)
    INCLUDE (unique_key, agency, borough, complaint_type, created_date);
GO
CREATE NONCLUSTERED INDEX IX_stg_unique_key
    ON stg.raw_311_requests (unique_key)
    WHERE unique_key IS NOT NULL;
GO
-- ============================================================
-- REPORTING VIEWS
-- Power BI connects to rpt schema only — never stg or dw directly
-- Views hide JOIN complexity — analysts see one clean flat table
-- ============================================================

CREATE VIEW rpt.vw_service_requests AS
SELECT
    f.request_key,
    f.unique_key,
    f.created_date,
    f.closed_date,
    f.status,
    f.resolution_days,
    -- Date
    d.year,
    d.quarter_name,
    d.month,
    d.month_name,
    d.month_short,
    d.day_name,
    d.is_weekend,
    d.is_holiday,
    -- Agency
    a.agency_code,
    a.agency_name,
    a.department,
    a.sla_days,
    -- Location
    l.borough,
    l.community_board,
    l.zip_code,
    l.latitude,
    l.longitude,
    l.neighborhood,
    -- Complaint
    ct.complaint_type,
    ct.descriptor,
    ct.category,
    -- Calculated
    CASE WHEN f.resolution_days > a.sla_days THEN 1 ELSE 0 END AS is_sla_breach,
    CASE WHEN f.status != 'Closed'
         THEN DATEDIFF(DAY, f.created_date, GETDATE())
         ELSE NULL END                                          AS days_open_if_pending,
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
CREATE VIEW rpt.vw_agency_summary AS
SELECT
    a.agency_code,
    a.agency_name,
    a.sla_days,
    d.year,
    d.month,
    d.month_name,
    COUNT(*)                                                        AS total_requests,
    SUM(CASE WHEN f.status = 'Closed'  THEN 1 ELSE 0 END)          AS total_closed,
    SUM(CASE WHEN f.status != 'Closed' THEN 1 ELSE 0 END)          AS total_open,
    AVG(CAST(f.resolution_days AS FLOAT))                           AS avg_resolution_days,
    MAX(f.resolution_days)                                          AS max_resolution_days,
    SUM(CASE WHEN f.resolution_days > a.sla_days THEN 1 ELSE 0 END) AS sla_breaches,
    CAST(
        SUM(CASE WHEN f.resolution_days > a.sla_days THEN 1.0 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.status = 'Closed'  THEN 1.0 ELSE 0 END), 0)
        * 100 AS DECIMAL(5,1)
    )                                                               AS sla_breach_pct
FROM      dw.fact_service_requests  f
JOIN      dw.dim_agency         a   ON f.agency_key = a.agency_key
JOIN      dw.dim_date           d   ON f.date_key   = d.date_key
GROUP BY  a.agency_code, a.agency_name, a.sla_days,
          d.year, d.month, d.month_name;
GO
CREATE VIEW rpt.vw_borough_summary AS
SELECT
    l.borough,
    d.year,
    d.month,
    ct.category,
    COUNT(*)                                                         AS total_requests,
    SUM(CASE WHEN f.status = 'Closed'  THEN 1 ELSE 0 END)           AS total_closed,
    AVG(CAST(f.resolution_days AS FLOAT))                            AS avg_resolution_days,
    SUM(CASE WHEN f.resolution_days > a.sla_days THEN 1 ELSE 0 END)  AS sla_breaches
FROM      dw.fact_service_requests  f
JOIN      dw.dim_location       l   ON f.location_key  = l.location_key
JOIN      dw.dim_date           d   ON f.date_key      = d.date_key
JOIN      dw.dim_complaint_type ct  ON f.complaint_key = ct.complaint_key
JOIN      dw.dim_agency         a   ON f.agency_key    = a.agency_key
GROUP BY  l.borough, d.year, d.month, ct.category;
GO
PRINT '====================================================';
PRINT 'NYC 311 Analytics — schema created successfully';
PRINT 'Schemas : stg | dw | rpt';
PRINT 'Tables  : dim_date, dim_agency, dim_location,';
PRINT '          dim_complaint_type, fact_service_requests';
PRINT '          stg.raw_311_requests';
PRINT 'Indexes : 7 on fact | 2 per dim | 2 on stg';
PRINT 'Views   : vw_service_requests';
PRINT '          vw_agency_summary';
PRINT '          vw_borough_summary';
PRINT '====================================================';
GO