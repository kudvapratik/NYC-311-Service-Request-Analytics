-- ============================================================
-- NYC 311 — Step 2 of 5: Load dw.dim_agency
-- Run order : 01 → 02 → 03 → 04 → 05_fact_sp
-- Dependency : stg.raw_311_requests must be loaded
-- Strategy   : DISTINCT agency codes from staging
--              Known agency names hardcoded (source has bugs)
--              SLA days updated per NYC published targets
--              Idempotent — safe to re-run, skips existing codes
-- Author     : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

DECLARE
    @batch_id    INT          = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT),
    @step_name   VARCHAR(100) = 'dim_agency',
    @step_number TINYINT      = 2,
    @log_id      INT,
    @rows_read   INT          = 0,
    @rows_ins    INT          = 0,
    @rows_skip   INT          = 0,
    @rows_rej    INT          = 0,
    @err_msg     VARCHAR(MAX);

-- ── LOG: STARTED ─────────────────────────────────────────────
INSERT INTO etl.etl_log (batch_id, step_name, step_number, status)
VALUES (@batch_id, @step_name, @step_number, 'STARTED');
SET @log_id = SCOPE_IDENTITY();
PRINT 'Step 2: Loading dim_agency (batch ' + CAST(@batch_id AS VARCHAR) + ')...';

BEGIN TRY

    -- ── TEMP: distinct valid agencies from staging ────────────
    -- Only unprocessed rows with non-null agency codes
    DROP TABLE IF EXISTS #stg_agency;

    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(agency)))   AS agency_code,
        LTRIM(RTRIM(agency_name))     AS raw_agency_name
    INTO #stg_agency
    FROM stg.raw_311_requests WITH (NOLOCK)
    WHERE stg_is_processed = 0
      AND agency IS NOT NULL
      AND LTRIM(RTRIM(agency)) <> '';

    SET @rows_read = @@ROWCOUNT;

    -- ── TEMP: agencies already in dw (for skip count) ────────
    SET @rows_skip = (
        SELECT COUNT(*)
        FROM   #stg_agency s
        WHERE  s.agency_code IN (SELECT agency_code FROM dw.dim_agency)
    );

    -- ── INSERT new agencies only ──────────────────────────────
    INSERT INTO dw.dim_agency (
        agency_code, agency_name, department, sla_days, is_active
    )
    SELECT
        s.agency_code,
        -- Known agency names hardcoded — source agency_name column
        -- contains inconsistencies and typos. Code is reliable.
        CASE s.agency_code
            WHEN 'NYPD'   THEN 'New York City Police Department'
            WHEN 'HPD'    THEN 'Housing Preservation and Development'
            WHEN 'DOT'    THEN 'Department of Transportation'
            WHEN 'DSNY'   THEN 'Department of Sanitation'
            WHEN 'DEP'    THEN 'Department of Environmental Protection'
            WHEN 'DPR'    THEN 'Department of Parks and Recreation'
            WHEN 'DOB'    THEN 'Department of Buildings'
            WHEN 'TLC'    THEN 'Taxi and Limousine Commission'
            WHEN 'FDNY'   THEN 'Fire Department of New York'
            WHEN 'DOE'    THEN 'Department of Education'
            WHEN 'DHS'    THEN 'Department of Homeless Services'
            WHEN 'ACS'    THEN 'Administration for Children Services'
            WHEN 'HRA'    THEN 'Human Resources Administration'
            WHEN 'EDC'    THEN 'Economic Development Corporation'
            WHEN 'DDC'    THEN 'Department of Design and Construction'
            WHEN 'OMB'    THEN 'Office of Management and Budget'
            WHEN 'DOHMH'  THEN 'Department of Health and Mental Hygiene'
            WHEN 'DCAS'   THEN 'Department of Citywide Administrative Services'
            ELSE LTRIM(RTRIM(ISNULL(s.raw_agency_name, s.agency_code)))
        END                     AS agency_name,
        'City Agency'           AS department,
        10                      AS sla_days,    -- default; updated below
        1                       AS is_active
    FROM #stg_agency s
    -- Only insert codes not already in dw
    WHERE s.agency_code NOT IN (SELECT agency_code FROM dw.dim_agency);

    SET @rows_ins = @@ROWCOUNT;

    -- ── UPDATE SLA days per NYC published targets ─────────────
    -- Run every time — idempotent, ensures values stay correct
    UPDATE dw.dim_agency SET sla_days =  2 WHERE agency_code = 'NYPD';
    UPDATE dw.dim_agency SET sla_days = 10 WHERE agency_code = 'HPD';
    UPDATE dw.dim_agency SET sla_days = 15 WHERE agency_code = 'DOT';
    UPDATE dw.dim_agency SET sla_days =  3 WHERE agency_code = 'DSNY';
    UPDATE dw.dim_agency SET sla_days = 10 WHERE agency_code = 'DEP';
    UPDATE dw.dim_agency SET sla_days = 14 WHERE agency_code = 'DPR';
    UPDATE dw.dim_agency SET sla_days = 15 WHERE agency_code = 'DOB';
    UPDATE dw.dim_agency SET sla_days =  5 WHERE agency_code = 'TLC';
    UPDATE dw.dim_agency SET sla_days =  7 WHERE agency_code = 'FDNY';
    UPDATE dw.dim_agency SET sla_days = 30 WHERE agency_code = 'DOE';
    UPDATE dw.dim_agency SET sla_days =  5 WHERE agency_code = 'DOHMH';

    -- ── CAPTURE: agencies with no valid code (rejected) ───────
    -- These cannot be loaded to dw — log them for investigation
    SET @rows_rej = (
        SELECT COUNT(*)
        FROM stg.raw_311_requests WITH (NOLOCK)
        WHERE stg_is_processed = 0
          AND (agency IS NULL OR LTRIM(RTRIM(agency)) = '')
    );

    IF @rows_rej > 0
    BEGIN
        INSERT INTO etl.etl_errors (
            log_id, batch_id, step_name,
            unique_key, error_type, error_message,
            raw_agency, raw_borough, raw_complaint, raw_created
        )
        SELECT TOP 1000          -- cap at 1000 to avoid log bloat
            @log_id,
            @batch_id,
            @step_name,
            unique_key,
            'NULL_AGENCY_CODE',
            'Agency code is NULL or blank — cannot load to dim_agency',
            agency,
            borough,
            complaint_type,
            created_date
        FROM stg.raw_311_requests WITH (NOLOCK)
        WHERE stg_is_processed = 0
          AND (agency IS NULL OR LTRIM(RTRIM(agency)) = '');
    END;

    -- ── LOG: SUCCESS ─────────────────────────────────────────
    UPDATE etl.etl_log
    SET    status        = 'SUCCESS',
           rows_read     = @rows_read,
           rows_inserted = @rows_ins,
           rows_skipped  = @rows_skip,
           rows_rejected = @rows_rej,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_agency SUCCESS'
        + ' | read: '     + CAST(@rows_read AS VARCHAR)
        + ' | inserted: ' + CAST(@rows_ins  AS VARCHAR)
        + ' | skipped: '  + CAST(@rows_skip AS VARCHAR)
        + ' | rejected: ' + CAST(@rows_rej  AS VARCHAR);

END TRY
BEGIN CATCH

    SET @err_msg = ERROR_MESSAGE();

    UPDATE etl.etl_log
    SET    status        = 'FAILED',
           error_message = @err_msg,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_agency FAILED: ' + @err_msg;
    THROW;

END CATCH;
GO
