-- ============================================================
-- NYC 311 — Staging to Data Warehouse ETL
-- Script 3 of 4
-- Moves stg.raw_311_requests → dw dimension + fact tables
-- Author  : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

-- ============================================================
-- STEP 1 — Load dim_date
-- Generate ALL dates for 2020-2030
-- WHY: Power BI needs a complete date table — no gaps!
--      Even days with zero complaints must exist in dim_date
-- ============================================================
PRINT 'Step 1: Loading dim_date...';

-- Generate date rows using a recursive CTE
WITH date_series AS (
    -- Anchor: start date
    SELECT CAST('2020-01-01' AS DATE) AS full_date
    UNION ALL
    -- Recursive: add one day at a time
    SELECT DATEADD(DAY, 1, full_date)
    FROM   date_series
    WHERE  full_date < '2030-12-31'
)
INSERT INTO dw.dim_date (
    date_key, full_date, year, quarter, quarter_name,
    month, month_name, month_short, week_of_year,
    day_of_month, day_of_week, day_name,
    is_weekend, is_holiday
)
SELECT
    -- date_key = YYYYMMDD integer e.g. 20240115
    CAST(CONVERT(VARCHAR, full_date, 112) AS INT)   AS date_key,
    full_date,
    YEAR(full_date)                                  AS year,
    DATEPART(QUARTER, full_date)                     AS quarter,
    'Q' + CAST(DATEPART(QUARTER, full_date) AS VARCHAR) AS quarter_name,
    MONTH(full_date)                                 AS month,
    DATENAME(MONTH, full_date)                       AS month_name,
    LEFT(DATENAME(MONTH, full_date), 3)              AS month_short,
    DATEPART(WEEK, full_date)                        AS week_of_year,
    DAY(full_date)                                   AS day_of_month,
    DATEPART(WEEKDAY, full_date)                     AS day_of_week,
    DATENAME(WEEKDAY, full_date)                     AS day_name,
    CASE WHEN DATEPART(WEEKDAY, full_date) IN (1,7)
         THEN 1 ELSE 0 END                           AS is_weekend,
    0                                                AS is_holiday
FROM date_series
-- Avoid duplicates on re-run
WHERE CAST(CONVERT(VARCHAR, full_date, 112) AS INT)
      NOT IN (SELECT date_key FROM dw.dim_date)
OPTION (MAXRECURSION 5000);  -- allow up to 5000 recursive steps
GO

PRINT 'dim_date loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- ============================================================
-- STEP 2 — Load dim_agency
-- WHY: Must exist before fact table — FK constraint
--      DISTINCT ensures one row per agency
--      Default SLA = 10 days — update per agency after load
-- ============================================================
PRINT 'Step 2: Loading dim_agency...';

INSERT INTO dw.dim_agency (
    agency_code, agency_name, department, sla_days, is_active
)
SELECT DISTINCT
    UPPER(LTRIM(RTRIM(agency)))         AS agency_code,
    LTRIM(RTRIM(
        CASE
            -- Map known agency codes to correct names
            -- WHY: agency_name column has bugs in source data!
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'NYPD'  THEN 'New York City Police Department'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'HPD'   THEN 'Housing Preservation and Development'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'DOT'   THEN 'Department of Transportation'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'DSNY'  THEN 'Department of Sanitation'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'DEP'   THEN 'Department of Environmental Protection'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'DPR'   THEN 'Department of Parks and Recreation'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'DOB'   THEN 'Department of Buildings'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'TLC'   THEN 'Taxi and Limousine Commission'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'FDNY'  THEN 'Fire Department of New York'
            WHEN UPPER(LTRIM(RTRIM(agency))) = 'DOE'   THEN 'Department of Education'
            ELSE LTRIM(RTRIM(agency_name))  -- fallback to source
        END
    ))                                  AS agency_name,
    'City Agency'                       AS department,
    10                                  AS sla_days,     -- default SLA
    1                                   AS is_active
FROM stg.raw_311_requests (nolock)
WHERE agency IS NOT NULL
  AND stg_is_processed = 0
  -- Avoid duplicates on re-run
  AND UPPER(LTRIM(RTRIM(agency))) NOT IN (
      SELECT agency_code FROM dw.dim_agency
  );
GO

PRINT 'dim_agency loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- Update SLA days per agency — based on NYC published SLAs
UPDATE dw.dim_agency SET sla_days =  2 WHERE agency_code = 'NYPD';
UPDATE dw.dim_agency SET sla_days = 10 WHERE agency_code = 'HPD';
UPDATE dw.dim_agency SET sla_days = 15 WHERE agency_code = 'DOT';
UPDATE dw.dim_agency SET sla_days =  3 WHERE agency_code = 'DSNY';
UPDATE dw.dim_agency SET sla_days = 10 WHERE agency_code = 'DEP';
UPDATE dw.dim_agency SET sla_days = 14 WHERE agency_code = 'DPR';
UPDATE dw.dim_agency SET sla_days = 15 WHERE agency_code = 'DOB';
UPDATE dw.dim_agency SET sla_days =  5 WHERE agency_code = 'TLC';
GO

PRINT 'dim_agency SLA days updated';
GO

-- ============================================================
-- STEP 3 — Load dim_location
-- WHY: Store unique borough/zip combinations once
--      TRY_CAST for lat/long — source has bad values!
--      DISTINCT — many requests share same location
-- ============================================================
PRINT 'Step 3: Loading dim_location...';

INSERT INTO dw.dim_location (
    borough, community_board, zip_code,
    city, latitude, longitude, neighborhood
)
SELECT DISTINCT
    UPPER(LTRIM(RTRIM(borough)))            AS borough,
    LTRIM(RTRIM(community_board))           AS community_board,
    LTRIM(RTRIM(incident_zip))              AS zip_code,
    LTRIM(RTRIM(city))                      AS city,
    TRY_CAST(latitude  AS DECIMAL(9,6))     AS latitude,
    TRY_CAST(longitude AS DECIMAL(9,6))     AS longitude,
    NULL                                    AS neighborhood  -- enrich later
FROM stg.raw_311_requests s (nolock)
WHERE borough IS NOT NULL
  AND borough != 'Unspecified'
  AND stg_is_processed = 0
  -- Avoid duplicates on re-run
  AND NOT EXISTS (
      SELECT 1 FROM dw.dim_location l
      WHERE  l.borough         = UPPER(LTRIM(RTRIM(s.borough)))
      AND    ISNULL(l.zip_code,'') = ISNULL(LTRIM(RTRIM(s.incident_zip)),'')
      AND TRY_CAST(l.latitude  AS DECIMAL(9,6))=TRY_CAST(s.latitude  AS DECIMAL(9,6))
      AND TRY_CAST(l.longitude  AS DECIMAL(9,6))=TRY_CAST(s.longitude  AS DECIMAL(9,6))
  );
GO
-- alias for self-reference in NOT EXISTS
-- Note: SQL Server requires table alias in FROM for subquery
PRINT 'dim_location loaded';
GO

-- ============================================================
-- STEP 4 — Load dim_complaint_type
-- WHY: 186 unique complaint types stored once
--      category column ENRICHES source data — not in raw!
--      CASE statement classifies into 6 categories
-- ============================================================
PRINT 'Step 4: Loading dim_complaint_type...';

INSERT INTO dw.dim_complaint_type (
    complaint_type, descriptor, category--, agency_code
)
SELECT DISTINCT
    UPPER(LTRIM(RTRIM(complaint_type)))     AS complaint_type,
    LTRIM(RTRIM(descriptor))                AS descriptor,
    -- ENRICHMENT — category not in source data!
    -- We derive it from complaint_type keywords
    CASE
        WHEN UPPER(complaint_type) LIKE '%NOISE%'           THEN 'Noise'
        WHEN UPPER(complaint_type) LIKE '%PARK%'            THEN 'Vehicle'
        WHEN UPPER(complaint_type) LIKE '%VEHICLE%'         THEN 'Vehicle'
        WHEN UPPER(complaint_type) LIKE '%HEAT%'            THEN 'Housing'
        WHEN UPPER(complaint_type) LIKE '%PLUMB%'           THEN 'Housing'
        WHEN UPPER(complaint_type) LIKE '%PAINT%'           THEN 'Housing'
        WHEN UPPER(complaint_type) LIKE '%WATER%'           THEN 'Housing'
        WHEN UPPER(complaint_type) LIKE '%LIGHT%'           THEN 'Infrastructure'
        WHEN UPPER(complaint_type) LIKE '%STREET%'          THEN 'Infrastructure'
        WHEN UPPER(complaint_type) LIKE '%SEWER%'           THEN 'Infrastructure'
        WHEN UPPER(complaint_type) LIKE '%TREE%'            THEN 'Environment'
        WHEN UPPER(complaint_type) LIKE '%LITTER%'          THEN 'Environment'
        WHEN UPPER(complaint_type) LIKE '%DIRTY%'           THEN 'Environment'
        ELSE 'Other'
    END                                     AS category
    --,
    --UPPER(LTRIM(RTRIM(agency)))             AS agency_code
    --INTO #A
FROM stg.raw_311_requests (nolock)
WHERE complaint_type IS NOT NULL
  AND stg_is_processed = 0
  AND UPPER(LTRIM(RTRIM(complaint_type))) NOT IN (
      SELECT complaint_type FROM dw.dim_complaint_type
  );
GO

PRINT 'dim_complaint_type loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- ============================================================
-- STEP 5 — Load fact_service_requests
-- WHY: Always loaded LAST — needs all dimension keys!
--      JOIN to each dimension to get integer surrogate keys
--      TRY_CAST for dates — source has bad date values
--      Only load stg_is_processed = 0 (unprocessed rows)
-- ============================================================
PRINT 'Step 5: Loading fact_service_requests...';

INSERT INTO dw.fact_service_requests (
    unique_key,
    date_key,
    agency_key,
    location_key,
    complaint_key,
    created_date,
    closed_date,
    status,
    dw_batch_id
)
SELECT 
    s.unique_key,

    -- date_key — convert created_date to YYYYMMDD integer
    CAST(CONVERT(VARCHAR,
        TRY_CAST(s.created_date AS DATE), 112)
    AS INT)                                         AS date_key,

    -- agency_key — lookup in dim_agency
    a.agency_key,

    -- location_key — lookup in dim_location
    l.location_key,

    -- complaint_key — lookup in dim_complaint_type
    ct.complaint_key,

    -- dates — TRY_CAST handles bad values safely
    TRY_CAST(s.created_date AS DATETIME)            AS created_date,
    TRY_CAST(s.closed_date  AS DATETIME)            AS closed_date,

    LTRIM(RTRIM(ISNULL(s.status, 'Unknown')))       AS status,

    s.stg_batch_id                                  AS dw_batch_id

FROM stg.raw_311_requests s

-- Join to dimensions — get surrogate keys
JOIN dw.dim_agency a
    ON  UPPER(LTRIM(RTRIM(s.agency))) = a.agency_code

JOIN dw.dim_location l
    ON  UPPER(LTRIM(RTRIM(s.borough))) = l.borough
    AND ISNULL(LTRIM(RTRIM(s.incident_zip)),'')
      = ISNULL(l.zip_code,'')
      AND TRY_CAST(s.latitude  AS DECIMAL(9,6))=l.latitude
      AND TRY_CAST(s.longitude  AS DECIMAL(9,6))=l.longitude

JOIN dw.dim_complaint_type ct
    ON  UPPER(LTRIM(RTRIM(s.complaint_type))) = ct.complaint_type
    AND UPPER(LTRIM(RTRIM(s.descriptor)))=ct.descriptor

-- Guard clauses — only valid processable rows
WHERE s.stg_is_processed = 0
  AND s.unique_key        IS NOT NULL
  AND s.created_date      IS NOT NULL
  AND s.agency            IS NOT NULL
  AND s.borough           IS NOT NULL
  AND s.borough           != 'Unspecified'
  AND TRY_CAST(s.created_date AS DATETIME) IS NOT NULL
  AND ISNULL(DATEDIFF(DAY, created_date, closed_date),0)>=0

  -- Avoid duplicates on re-run
  AND s.unique_key NOT IN (
      SELECT unique_key FROM dw.fact_service_requests
  );
  
GO



PRINT 'fact_service_requests loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- ============================================================
-- STEP 6 — Mark staging rows as processed
-- WHY: stg_is_processed = 1 means "already in dw"
--      Next run skips these rows — incremental load!
--      Never re-process what is already loaded
-- ============================================================
PRINT 'Step 6: Marking staging rows as processed...';

UPDATE stg.raw_311_requests
SET    stg_is_processed = 1
WHERE  stg_is_processed = 0;
GO

PRINT 'Staging rows marked as processed: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

PRINT '============================================';
PRINT 'STG → DW ETL COMPLETE';
PRINT '============================================';
GO
