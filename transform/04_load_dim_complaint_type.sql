-- ============================================================
-- NYC 311 — Step 4 of 5: Load dw.dim_complaint_type
-- Run order : 01 → 02 → 03 → 04 → 05_fact_sp
-- Dependency : stg.raw_311_requests must be loaded
-- Strategy   : DISTINCT complaint_type + descriptor combos
--              category DERIVED via CASE — not in source data
--              descriptor NULLs normalised to empty string
--              idempotent — dedup on complaint_type + descriptor
-- Author     : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

DECLARE
    @batch_id    INT          = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT),
    @step_name   VARCHAR(100) = 'dim_complaint_type',
    @step_number TINYINT      = 4,
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
PRINT 'Step 4: Loading dim_complaint_type (batch ' + CAST(@batch_id AS VARCHAR) + ')...';

BEGIN TRY

    -- ── TEMP 1: all distinct raw complaint combos ─────────────
    DROP TABLE IF EXISTS #raw_complaints;

    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(complaint_type)))         AS complaint_type,
        -- Normalise descriptor: NULL and blank → empty string
        -- Prevents duplicate dim rows for NULL vs '' descriptor
        ISNULL(LTRIM(RTRIM(descriptor)), '')        AS descriptor
    INTO #raw_complaints
    FROM stg.raw_311_requests WITH (NOLOCK)
    WHERE stg_is_processed = 0
      AND complaint_type IS NOT NULL
      AND LTRIM(RTRIM(complaint_type)) <> '';

    SET @rows_read = @@ROWCOUNT;

    -- ── CAPTURE: rows with null complaint_type → etl_errors ───
    SET @rows_rej = (
        SELECT COUNT(*)
        FROM stg.raw_311_requests WITH (NOLOCK)
        WHERE stg_is_processed = 0
          AND (complaint_type IS NULL OR LTRIM(RTRIM(complaint_type)) = '')
    );

    IF @rows_rej > 0
    BEGIN
        INSERT INTO etl.etl_errors (
            log_id, batch_id, step_name,
            unique_key, error_type, error_message,
            raw_agency, raw_borough, raw_complaint, raw_created
        )
        SELECT TOP 1000
            @log_id,
            @batch_id,
            @step_name,
            unique_key,
            'NULL_COMPLAINT_TYPE',
            'complaint_type is NULL or blank — cannot load to dim_complaint_type',
            agency,
            borough,
            complaint_type,
            created_date
        FROM stg.raw_311_requests WITH (NOLOCK)
        WHERE stg_is_processed = 0
          AND (complaint_type IS NULL OR LTRIM(RTRIM(complaint_type)) = '');
    END;

    -- ── TEMP 2: enrich with derived category ──────────────────
    -- Category not in source — derived here from complaint keywords
    -- Ordered from most-specific to least-specific CASE branches
    DROP TABLE IF EXISTS #enriched_complaints;

    SELECT
        complaint_type,
        descriptor,
        CASE
            -- Noise complaints
            WHEN complaint_type LIKE '%NOISE%'                          THEN 'Noise'
            -- Vehicle / parking
            WHEN complaint_type LIKE '%ILLEGAL PARKING%'               THEN 'Vehicle'
            WHEN complaint_type LIKE '%VEHICLE%'                        THEN 'Vehicle'
            WHEN complaint_type LIKE '%TRAFFIC%'                        THEN 'Vehicle'
            -- Housing / building conditions
            WHEN complaint_type LIKE '%HEAT%'                           THEN 'Housing'
            WHEN complaint_type LIKE '%HOT WATER%'                      THEN 'Housing'
            WHEN complaint_type LIKE '%PLUMBING%'                       THEN 'Housing'
            WHEN complaint_type LIKE '%PAINT%'                          THEN 'Housing'
            WHEN complaint_type LIKE '%MOLD%'                           THEN 'Housing'
            WHEN complaint_type LIKE '%ELEVATOR%'                       THEN 'Housing'
            WHEN complaint_type LIKE '%HOUSING%'                        THEN 'Housing'
            -- Infrastructure
            WHEN complaint_type LIKE '%STREET LIGHT%'                   THEN 'Infrastructure'
            WHEN complaint_type LIKE '%STREET CONDITION%'               THEN 'Infrastructure'
            WHEN complaint_type LIKE '%SEWER%'                          THEN 'Infrastructure'
            WHEN complaint_type LIKE '%POTHOLE%'                        THEN 'Infrastructure'
            WHEN complaint_type LIKE '%SIDEWALK%'                       THEN 'Infrastructure'
            WHEN complaint_type LIKE '%WATER SYSTEM%'                   THEN 'Infrastructure'
            -- Environment / sanitation
            WHEN complaint_type LIKE '%TREE%'                           THEN 'Environment'
            WHEN complaint_type LIKE '%LITTER%'                         THEN 'Environment'
            WHEN complaint_type LIKE '%DIRTY%'                          THEN 'Environment'
            WHEN complaint_type LIKE '%SANITATION%'                     THEN 'Environment'
            WHEN complaint_type LIKE '%RODENT%'                         THEN 'Environment'
            WHEN complaint_type LIKE '%PEST%'                           THEN 'Environment'
            -- Safety
            WHEN complaint_type LIKE '%FIRE%'                           THEN 'Safety'
            WHEN complaint_type LIKE '%HAZARD%'                         THEN 'Safety'
            WHEN complaint_type LIKE '%DRUG%'                           THEN 'Safety'
            WHEN complaint_type LIKE '%WEAPON%'                         THEN 'Safety'
            WHEN complaint_type LIKE '%ASSAULT%'                        THEN 'Safety'
            ELSE 'Other'
        END  AS category
    INTO #enriched_complaints
    FROM #raw_complaints;

    -- ── INSERT new complaint type + descriptor combos only ────
    INSERT INTO dw.dim_complaint_type (
        complaint_type, descriptor, category
    )
    SELECT
        e.complaint_type,
        e.descriptor,
        e.category
    FROM #enriched_complaints e
    WHERE NOT EXISTS (
        SELECT 1
        FROM dw.dim_complaint_type ct
        WHERE ct.complaint_type = e.complaint_type
          AND ct.descriptor     = e.descriptor
    );

    SET @rows_ins  = @@ROWCOUNT;
    SET @rows_skip = @rows_read - @rows_ins;

    -- ── LOG: SUCCESS ─────────────────────────────────────────
    UPDATE etl.etl_log
    SET    status        = 'SUCCESS',
           rows_read     = @rows_read,
           rows_inserted = @rows_ins,
           rows_skipped  = @rows_skip,
           rows_rejected = @rows_rej,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_complaint_type SUCCESS'
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

    PRINT 'dim_complaint_type FAILED: ' + @err_msg;
    THROW;

END CATCH;
GO
