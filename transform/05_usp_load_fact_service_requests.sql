-- ============================================================
-- NYC 311 — Step 5 of 5: Stored Procedure for fact_service_requests
-- Run order  : Execute AFTER scripts 01 → 02 → 03 → 04
-- Dependency : All four dimension tables must be loaded first
--
-- Design:
--   Temp table pipeline — each stage isolates one validation
--   so failures are diagnosable at the exact step they occur.
--
--   #t1_raw          Raw unprocessed rows from staging
--   #t2_valid_dates  Passed date validation
--   #t3_dim_matched  Successfully joined to all 4 dimensions
--   #t4_deduped      Not already in fact table (unique_key check)
--   #t_errors        All rejected rows with typed reason codes
--
-- Captures:
--   NULL/blank unique_key
--   Unparseable created_date
--   created_date > closed_date (negative resolution)
--   No matching dim_agency
--   No matching dim_location
--   No matching dim_complaint_type
--   No matching dim_date (date out of 2020-2030 range)
--   Duplicate unique_key already in fact table
--   Status values not in allowed list
--
-- Author     : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

CREATE OR ALTER PROCEDURE dw.usp_load_fact_service_requests
    @batch_id   INT  = NULL,   -- defaults to today YYYYMMDD
    @debug_mode BIT  = 0       -- 1 = print row counts at each stage
AS
BEGIN

--EXEC dw.usp_load_fact_service_requests NULL,1

    SET NOCOUNT ON;
    SET XACT_ABORT ON;  -- auto-rollback on any error inside transaction

    -- ── INIT ─────────────────────────────────────────────────
    IF @batch_id IS NULL
        SET @batch_id = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT);

    DECLARE
        @step_name   VARCHAR(100) = 'fact_service_requests',
        @step_number TINYINT      = 5,
        @log_id      INT,
        @rows_read   INT          = 0,
        @rows_ins    INT          = 0,
        @rows_skip   INT          = 0,
        @rows_rej    INT          = 0,
        @err_msg     VARCHAR(MAX),
        @step_msg    VARCHAR(200);

    -- ── LOG: STARTED ─────────────────────────────────────────
    INSERT INTO etl.etl_log (batch_id, step_name, step_number, status)
    VALUES (@batch_id, @step_name, @step_number, 'STARTED');
    SET @log_id = SCOPE_IDENTITY();

    PRINT '============================================================';
    PRINT 'usp_load_fact_service_requests — batch ' + CAST(@batch_id AS VARCHAR);
    PRINT '============================================================';

    BEGIN TRY

        -- ══════════════════════════════════════════════════════
        -- STAGE 1: Pull all unprocessed rows from staging
        -- ══════════════════════════════════════════════════════
        DROP TABLE IF EXISTS #t1_raw;

        SELECT
            unique_key,
            created_date        AS raw_created,
            closed_date         AS raw_closed,
            agency,
            borough,
            incident_zip,
            complaint_type,
            descriptor,
            latitude,
            longitude,
            status              AS raw_status,
            city,
            community_board,
            stg_batch_id
        INTO #t1_raw
        FROM stg.raw_311_requests WITH (NOLOCK)
        WHERE stg_is_processed = 0;

        SET @rows_read = @@ROWCOUNT;

        IF @debug_mode = 1
            PRINT 'Stage 1 — raw rows from staging: ' + CAST(@rows_read AS VARCHAR);

        -- ══════════════════════════════════════════════════════
        -- STAGE 2: Error table — collect ALL rejections here
        --          then bulk insert to etl.etl_errors at end
        -- ══════════════════════════════════════════════════════
        DROP TABLE IF EXISTS #t_errors;

        CREATE TABLE #t_errors (
            unique_key    VARCHAR(20)   NULL,
            error_type    VARCHAR(50)   NOT NULL,
            error_message VARCHAR(MAX)  NOT NULL,
            raw_agency    VARCHAR(20)   NULL,
            raw_borough   VARCHAR(100)  NULL,
            raw_complaint VARCHAR(200)  NULL,
            raw_created   VARCHAR(50)   NULL,
            raw_closed    VARCHAR(50)   NULL,
            raw_latitude  VARCHAR(50)   NULL,
            raw_longitude VARCHAR(50)   NULL,
            raw_status    VARCHAR(50)   NULL
        );

        -- ══════════════════════════════════════════════════════
        -- STAGE 3: Validate unique_key
        -- ══════════════════════════════════════════════════════
        -- Capture nulls/blanks — cannot load to fact without PK
        INSERT INTO #t_errors
        SELECT
            unique_key,
            'NULL_UNIQUE_KEY',
            'unique_key is NULL or blank — cannot load to fact table',
            agency, borough, complaint_type,
            raw_created, raw_closed, latitude, longitude, raw_status
        FROM #t1_raw
        WHERE unique_key IS NULL
           OR LTRIM(RTRIM(unique_key)) = '';

        --Remove them from pipeline
        DELETE FROM #t1_raw
        WHERE unique_key IS NULL
           OR LTRIM(RTRIM(unique_key)) = '';

        -- ══════════════════════════════════════════════════════
        -- STAGE 3B: Additional validation — reject NULL/blank city
        -- Reject rows where borough, incident_zip, latitude and
        -- longitude exist but city is NULL or blank
        -- ══════════════════════════════════════════════════════
        
        INSERT INTO #t_errors
        SELECT
            unique_key,
            'CITY_MISSING',
            'borough, incident_zip, latitude and longitude present but city is NULL/blank',
            agency, borough, complaint_type,
            raw_created, raw_closed, latitude, longitude, raw_status
        FROM #t1_raw
        WHERE borough IS NOT NULL
          AND LTRIM(RTRIM(borough)) <> ''
          AND incident_zip IS NOT NULL
          AND LTRIM(RTRIM(incident_zip)) <> ''
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND (city IS NULL OR LTRIM(RTRIM(city)) = '');
        
        DELETE FROM #t1_raw
        WHERE borough IS NOT NULL
          AND LTRIM(RTRIM(borough)) <> ''
          AND incident_zip IS NOT NULL
          AND LTRIM(RTRIM(incident_zip)) <> ''
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND (city IS NULL OR LTRIM(RTRIM(city)) = '');
        
       IF @debug_mode = 1
            DECLARE @cnt3 INT; SELECT @cnt3 = COUNT(*) FROM #t1_raw;
            PRINT 'Stage 3 — after unique_key validation: ' + CAST(@cnt3 AS VARCHAR);
    
        -- ══════════════════════════════════════════════════════
        -- STAGE 4: Validate and parse dates
        -- ══════════════════════════════════════════════════════
        DROP TABLE IF EXISTS #t2_valid_dates;

        SELECT
            r.*,
            TRY_CAST(r.raw_created AS DATETIME) AS created_date,
            TRY_CAST(r.raw_closed  AS DATETIME) AS closed_date,
            CAST(CONVERT(VARCHAR,
                TRY_CAST(r.raw_created AS DATE), 112)
            AS INT)                             AS date_key
        INTO #t2_valid_dates
        FROM #t1_raw r;

        -- Reject: unparseable created_date
        INSERT INTO #t_errors
        SELECT
            unique_key,
            'INVALID_CREATED_DATE',
            'created_date cannot be parsed as DATETIME: [' + ISNULL(raw_created,'NULL') + ']',
            agency, borough, complaint_type,
            raw_created, raw_closed, latitude, longitude, raw_status
        FROM #t2_valid_dates
        WHERE created_date IS NULL;

        DELETE FROM #t2_valid_dates WHERE created_date IS NULL;

        -- Reject: date_key out of dim_date range (2020–2030)
        INSERT INTO #t_errors
        SELECT
            unique_key,
            'DATE_OUT_OF_RANGE',
            'created_date ' + ISNULL(raw_created,'NULL')
                + ' produces date_key '
                + ISNULL(CAST(date_key AS VARCHAR),'NULL')
                + ' not in dim_date (2020-2030)',
            agency, borough, complaint_type,
            raw_created, raw_closed, latitude, longitude, raw_status
        FROM #t2_valid_dates
        WHERE date_key NOT BETWEEN 20200101 AND 20301231;

        DELETE FROM #t2_valid_dates
        WHERE date_key NOT BETWEEN 20200101 AND 20301231;

        -- Reject: closed_date before created_date (data quality)
        INSERT INTO #t_errors
        SELECT
            unique_key,
            'NEGATIVE_RESOLUTION',
            'closed_date [' + ISNULL(raw_closed,'NULL')
                + '] is before created_date [' + raw_created + ']',
            agency, borough, complaint_type,
            raw_created, raw_closed, latitude, longitude, raw_status
        FROM #t2_valid_dates
        WHERE closed_date IS NOT NULL
          AND closed_date < created_date;

        DELETE FROM #t2_valid_dates
        WHERE closed_date IS NOT NULL
          AND closed_date < created_date;
        


        IF @debug_mode = 1
            DECLARE @cnt4 INT; SELECT @cnt4 = COUNT(*) FROM #t2_valid_dates;
            PRINT 'Stage 4 — after date validation: ' + CAST(@cnt4 AS VARCHAR);

        -- ══════════════════════════════════════════════════════
        -- STAGE 5: Normalise status values
        -- Map source values → allowed set in CK_fact_status
        -- ══════════════════════════════════════════════════════
        -- Map common variants — source has mixed casing and synonyms
        UPDATE #t2_valid_dates
        SET raw_status = CASE
            WHEN UPPER(LTRIM(RTRIM(raw_status))) IN ('CLOSED','CLOSE','RESOLVED')
                THEN 'Closed'
            WHEN UPPER(LTRIM(RTRIM(raw_status))) IN ('OPEN','OPENED')
                THEN 'Open'
            WHEN UPPER(LTRIM(RTRIM(raw_status))) IN ('PENDING','HOLD','ON HOLD')
                THEN 'Pending'
            WHEN UPPER(LTRIM(RTRIM(raw_status))) IN ('IN PROGRESS','IN-PROGRESS','INPROGRESS','STARTED')
                THEN 'In Progress'
            WHEN UPPER(LTRIM(RTRIM(raw_status))) IN ('ASSIGNED','DISPATCHED')
                THEN 'Assigned'
            WHEN raw_status IS NULL OR LTRIM(RTRIM(raw_status)) = ''
                THEN 'Unspecified'
            ELSE raw_status
        END;

        -- Reject: status still not in allowed list after mapping
        INSERT INTO #t_errors
        SELECT
            unique_key,
            'INVALID_STATUS',
            'status value [' + ISNULL(raw_status,'NULL')
                + '] not in allowed list (Open/Closed/Pending/In Progress/Assigned/Unspecified)',
            agency, borough, complaint_type,
            raw_created, raw_closed, latitude, longitude, raw_status
        FROM #t2_valid_dates
        WHERE raw_status NOT IN
            ('Open','Closed','Pending','In Progress','Unspecified','Assigned');

        DELETE FROM #t2_valid_dates
        WHERE raw_status NOT IN
            ('Open','Closed','Pending','In Progress','Unspecified','Assigned');

        -- ══════════════════════════════════════════════════════
        -- STAGE 6: Join to all four dimensions
        --          Capture rows that fail to match any dimension
        -- ══════════════════════════════════════════════════════
        DROP TABLE IF EXISTS #t3_dim_matched;

        SELECT
            v.unique_key,
            v.date_key,
            v.created_date,
            v.closed_date,
            v.raw_status                        AS status,
            v.stg_batch_id,
            -- Surrogate keys from dimensions
            a.agency_key,
            l.location_key,
            ct.complaint_key,
            -- Carry raw values for error reporting
            v.agency                            AS raw_agency,
            v.borough                           AS raw_borough,
            v.complaint_type                    AS raw_complaint,
            v.raw_created,
            v.raw_closed,
            v.latitude                          AS raw_lat,
            v.longitude                         AS raw_long,
            v.raw_status                        AS raw_status_orig,
            v.community_board                  AS raw_community_board
        INTO #t3_dim_matched
        FROM #t2_valid_dates v
        -- dim_agency
        LEFT JOIN dw.dim_agency a
            ON  UPPER(LTRIM(RTRIM(v.agency))) = a.agency_code
        -- dim_location (NYC bounding-box validated lat/long)
        --LEFT JOIN dw.dim_location l
        --    ON  UPPER(LTRIM(RTRIM(v.borough))) = l.borough
        --    AND ISNULL(LTRIM(RTRIM(v.incident_zip)), '') = ISNULL(l.zip_code, '')
        --    AND ISNULL(TRY_CAST(v.latitude  AS DECIMAL(9,6)), -999) = ISNULL(l.latitude,  -999)
        --    AND ISNULL(TRY_CAST(v.longitude AS DECIMAL(9,6)), -999) = ISNULL(l.longitude, -999)
        --    AND UPPER(LTRIM(RTRIM(v.community_board))) = l.community_board
        LEFT JOIN dw.dim_location l
    ON  UPPER(LTRIM(RTRIM(v.borough)))             = l.borough
    AND ISNULL(LTRIM(RTRIM(v.incident_zip)),   '')  = ISNULL(l.zip_code, '')
    AND ISNULL(LTRIM(RTRIM(v.community_board)),'')  = ISNULL(l.community_board, '')
        -- dim_complaint_type
        LEFT JOIN dw.dim_complaint_type ct
            ON  UPPER(LTRIM(RTRIM(v.complaint_type))) = ct.complaint_type
            AND ISNULL(LTRIM(RTRIM(v.descriptor)), '') = ct.descriptor;

        -- Reject: no agency match
        INSERT INTO #t_errors
        SELECT
            unique_key, 'NO_DIM_AGENCY_MATCH',
            'agency code [' + ISNULL(CAST(agency_key as varchar(10)),'NULL') + '] not found in dim_agency',
            raw_agency, raw_borough, raw_complaint,
            raw_created, raw_closed, raw_lat, raw_long, raw_status_orig
        FROM #t3_dim_matched
        WHERE agency_key IS NULL;

        DELETE FROM #t3_dim_matched WHERE agency_key IS NULL;

        -- Reject: no location match
        INSERT INTO #t_errors
        SELECT
            unique_key, 'NO_DIM_LOCATION_MATCH',
            'borough/zip/lat/long combo not found in dim_location',
            raw_agency, raw_borough, raw_complaint,
            raw_created, raw_closed, raw_lat, raw_long, raw_status_orig
        FROM #t3_dim_matched
        WHERE location_key IS NULL;

        DELETE FROM #t3_dim_matched WHERE location_key IS NULL;

        -- Reject: no complaint match
        INSERT INTO #t_errors
        SELECT
            unique_key, 'NO_DIM_COMPLAINT_MATCH',
            'complaint_type/descriptor combo not found in dim_complaint_type',
            raw_agency, raw_borough, raw_complaint,
            raw_created, raw_closed, raw_lat, raw_long, raw_status_orig
        FROM #t3_dim_matched
        WHERE complaint_key IS NULL;

        DELETE FROM #t3_dim_matched WHERE complaint_key IS NULL;

        IF @debug_mode = 1
            DECLARE @cnt6 INT; SELECT @cnt6 = COUNT(*) FROM #t3_dim_matched;
            PRINT 'Stage 6 — after dim joins: ' + CAST(@cnt6 AS VARCHAR);

        -- ══════════════════════════════════════════════════════
        -- STAGE 7: Dedup — remove unique_keys already in fact
        -- ══════════════════════════════════════════════════════
        DROP TABLE IF EXISTS #t4_deduped;

        SELECT m.*
        INTO #t4_deduped
        FROM #t3_dim_matched m
        WHERE NOT EXISTS (
            SELECT 1
            FROM dw.fact_service_requests f
            WHERE f.unique_key = m.unique_key
        );

        DECLARE @cnt_matched INT, @cnt_deduped INT;
        SELECT @cnt_matched = COUNT(*) FROM #t3_dim_matched;
        SELECT @cnt_deduped = COUNT(*) FROM #t4_deduped;
        SET @rows_skip = @cnt_matched - @cnt_deduped;

        -- Capture duplicates in error log (informational, not true errors)
        INSERT INTO #t_errors
        SELECT
            m.unique_key, 'DUPLICATE_UNIQUE_KEY',
            'unique_key already exists in fact_service_requests — skipped',
            m.raw_agency, m.raw_borough, m.raw_complaint,
            m.raw_created, m.raw_closed, m.raw_lat, m.raw_long, m.raw_status_orig
        FROM #t3_dim_matched m
        WHERE NOT EXISTS (
            SELECT 1 FROM #t4_deduped d WHERE d.unique_key = m.unique_key
        );

        IF @debug_mode = 1
            DECLARE @cnt7 INT; SELECT @cnt7 = COUNT(*) FROM #t4_deduped;
            PRINT 'Stage 7 — after dedup: ' + CAST(@cnt7 AS VARCHAR);

        -- ══════════════════════════════════════════════════════
        -- STAGE 8: Final INSERT into fact table
        -- ══════════════════════════════════════════════════════
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
            unique_key,
            date_key,
            agency_key,
            location_key,
            complaint_key,
            created_date,
            closed_date,
            status,
            @batch_id
        FROM #t4_deduped;

        SET @rows_ins = @@ROWCOUNT;

        IF @debug_mode = 1
            PRINT 'Stage 8 — inserted into fact: ' + CAST(@rows_ins AS VARCHAR);

        -- ══════════════════════════════════════════════════════
        -- STAGE 9: Flush error temp table → etl.etl_errors
        -- ══════════════════════════════════════════════════════
        SELECT @rows_rej = COUNT(*) FROM #t_errors;

        IF @rows_rej > 0
        BEGIN
            INSERT INTO etl.etl_errors (
                log_id, batch_id, step_name,
                unique_key, error_type, error_message,
                raw_agency, raw_borough, raw_complaint,
                raw_created, raw_closed, raw_latitude, raw_longitude, raw_status
            )
            SELECT
                @log_id, @batch_id, @step_name,
                unique_key, error_type, error_message,
                raw_agency, raw_borough, raw_complaint,
                raw_created, raw_closed, raw_latitude, raw_longitude, raw_status
            FROM #t_errors;
        END;

        -- ══════════════════════════════════════════════════════
        -- STAGE 10: Mark staging rows as processed
        -- Only mark rows that made it through OR were rejected
        -- Unprocessed rows from other batches are untouched
        -- ══════════════════════════════════════════════════════
        UPDATE stg.raw_311_requests
        SET    stg_is_processed = 1
        WHERE  stg_is_processed = 0;

        PRINT 'Staging rows marked as processed: ' + CAST(@@ROWCOUNT AS VARCHAR);

        -- ── LOG: SUCCESS ─────────────────────────────────────
        UPDATE etl.etl_log
        SET    status        = 'SUCCESS',
               rows_read     = @rows_read,
               rows_inserted = @rows_ins,
               rows_skipped  = @rows_skip,
               rows_rejected = @rows_rej,
               end_time      = GETDATE()
        WHERE  log_id = @log_id;

        PRINT '============================================================';
        PRINT 'fact_service_requests SUCCESS'
        PRINT '  Rows read     : ' + CAST(@rows_read AS VARCHAR);
        PRINT '  Rows inserted : ' + CAST(@rows_ins  AS VARCHAR);
        PRINT '  Rows skipped  : ' + CAST(@rows_skip AS VARCHAR) + '  (duplicates)';
        PRINT '  Rows rejected : ' + CAST(@rows_rej  AS VARCHAR) + '  (see etl.etl_errors)';
        PRINT '============================================================';

    END TRY
    BEGIN CATCH

        SET @err_msg = ERROR_MESSAGE();

        UPDATE etl.etl_log
        SET    status        = 'FAILED',
               error_message = @err_msg,
               end_time      = GETDATE()
        WHERE  log_id = @log_id;

        PRINT 'fact_service_requests FAILED: ' + @err_msg;
        THROW;

    END CATCH;

END;
GO

PRINT 'Stored procedure dw.usp_load_fact_service_requests created';
GO

-- ============================================================
-- USAGE EXAMPLES
-- ============================================================
-- Standard run (batch_id defaults to today):
--   EXEC dw.usp_load_fact_service_requests;
--
-- With specific batch ID:
--   EXEC dw.usp_load_fact_service_requests @batch_id = 20260323;
--
-- With debug output (prints row counts at each stage):
--   EXEC dw.usp_load_fact_service_requests @debug_mode = 1;
--
-- Review load stats:
--   SELECT * FROM etl.etl_log ORDER BY log_id DESC;
--
-- Review errors for last run:
--   SELECT error_type, COUNT(*) AS cnt, MIN(error_message) AS example
--   FROM etl.etl_errors
--   WHERE batch_id = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT)
--   GROUP BY error_type
--   ORDER BY cnt DESC;
--
-- Error breakdown by type:
--   SELECT step_name, error_type, COUNT(*) AS cnt
--   FROM etl.etl_errors
--   WHERE batch_id = 20260323
--   GROUP BY step_name, error_type
--   ORDER BY step_name, cnt DESC;
-- ============================================================





