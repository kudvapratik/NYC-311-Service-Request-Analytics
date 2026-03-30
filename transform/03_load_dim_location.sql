-- ============================================================
-- NYC 311 — Step 3 of 5: Load dw.dim_location
-- Run order : 01 → 02 → 03 → 04 → 05_fact_sp
-- Dependency : stg.raw_311_requests must be loaded
--
-- Grain (v3 — final):
--   borough + zip_code + community_board
--   Guaranteed unique — no lat/long, no city in key.
--
-- v2 issue: city column caused duplicates even after lat/long
--   was removed from the key. Same zip+board had multiple city
--   name variants in source (e.g. "LONG ISLAND CITY" vs "QUEENS"
--   for zip 11101) — both passed the NOT EXISTS dedup check
--   because city was still selected into the temp table and
--   treated as a distinct combination.
--
-- v3 fix: city column dropped entirely from dim_location.
--   It is inconsistent, redundant with borough+zip, and adds
--   no analytical value. borough+zip+community_board is the
--   correct stable grain for a location dimension.
--   Lat/long stored as AVG per zip+board — one map pin per area.
--
-- Author : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

DECLARE
    @batch_id    INT          = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT),
    @step_name   VARCHAR(100) = 'dim_location',
    @step_number TINYINT      = 3,
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
PRINT 'Step 3: Loading dim_location (batch ' + CAST(@batch_id AS VARCHAR) + ')...';

BEGIN TRY

    -- ── TEMP 1: aggregate to correct grain ───────────────────
    -- Grain: borough + zip_code + community_board (one row per area)
    -- Lat/long: AVG across all source rows in the zip+board group
    --   → single representative map pin per area
    --   → eliminates the multiple-coordinates-per-zip problem
    -- City: most frequent value in the group (via TOP 1 subquery)
    DROP TABLE IF EXISTS #raw_locations;

    SELECT
        UPPER(LTRIM(RTRIM(borough)))                    AS borough,
        ISNULL(LTRIM(RTRIM(community_board)), '')       AS community_board,
        ISNULL(LTRIM(RTRIM(incident_zip)),    '')       AS zip_code,
        -- city column removed — inconsistent in source data
        -- (e.g. zip 11101 appears as both "LONG ISLAND CITY" and "QUEENS")
        -- borough + zip + community_board fully identifies the area.
        -- Average coordinates — one representative pin per zip+board
        AVG(TRY_CAST(latitude  AS DECIMAL(9,6)))        AS latitude,
        AVG(TRY_CAST(longitude AS DECIMAL(9,6)))        AS longitude
    INTO #raw_locations
    FROM stg.raw_311_requests s WITH (NOLOCK)
    WHERE stg_is_processed = 0
      AND borough IS NOT NULL
      AND UPPER(LTRIM(RTRIM(borough))) NOT IN ('UNSPECIFIED', '')
    GROUP BY
        UPPER(LTRIM(RTRIM(borough))),
        ISNULL(LTRIM(RTRIM(community_board)), ''),
        ISNULL(LTRIM(RTRIM(incident_zip)),    '');

    SELECT @rows_read = COUNT(*) FROM #raw_locations;

    -- ── TEMP 2: valid locations ───────────────────────────────
    -- After averaging, coordinates should be well within NYC.
    -- Any borough+zip combo whose average lands outside the
    -- bounding box has fundamentally bad source data.
    DROP TABLE IF EXISTS #valid_locations;

    SELECT *
    INTO #valid_locations
    FROM #raw_locations
    WHERE latitude  IS NOT NULL
      AND longitude IS NOT NULL
      AND latitude  BETWEEN 40.4 AND 40.9
      AND longitude BETWEEN -74.3 AND -73.7;

    -- ── TEMP 3: invalid locations → error log ────────────────
    DROP TABLE IF EXISTS #invalid_locations;

    SELECT *
    INTO #invalid_locations
    FROM #raw_locations
    WHERE latitude  IS NULL
       OR longitude IS NULL
       OR latitude  NOT BETWEEN 40.4 AND 40.9
       OR longitude NOT BETWEEN -74.3 AND -73.7;

    SELECT @rows_rej = COUNT(*) FROM #invalid_locations;

    IF @rows_rej > 0
    BEGIN
        INSERT INTO etl.etl_errors (
            log_id, batch_id, step_name,
            unique_key, error_type, error_message,
            raw_borough, raw_latitude, raw_longitude
        )
        SELECT TOP 1000
            @log_id,
            @batch_id,
            @step_name,
            NULL,
            CASE
                WHEN i.latitude  IS NULL OR i.longitude IS NULL
                    THEN 'NULL_AVG_COORDINATES'
                ELSE 'OUT_OF_RANGE_AVG_COORDINATES'
            END,
            'borough=' + ISNULL(i.borough, 'NULL')
                + ' zip='  + ISNULL(i.zip_code, 'NULL')
                + ' board='+ ISNULL(i.community_board, 'NULL')
                + ' avg_lat='  + ISNULL(CAST(i.latitude  AS VARCHAR), 'NULL')
                + ' avg_long=' + ISNULL(CAST(i.longitude AS VARCHAR), 'NULL'),
            i.borough,
            CAST(i.latitude  AS VARCHAR),
            CAST(i.longitude AS VARCHAR)
        FROM #invalid_locations i;
    END;

    -- ── INSERT new locations only ─────────────────────────────
    -- Dedup on the new grain: borough + zip_code + community_board
    -- This is now guaranteed unique — no lat/long in the key
    INSERT INTO dw.dim_location (
        borough, community_board, zip_code,
        latitude, longitude, neighborhood
    )
    SELECT
        v.borough,
        v.community_board,
        v.zip_code,
        v.latitude,
        v.longitude,
        NULL    AS neighborhood
    FROM #valid_locations v
    WHERE NOT EXISTS (
        SELECT 1
        FROM dw.dim_location l
        WHERE l.borough        = v.borough
          AND l.zip_code       = v.zip_code
          AND l.community_board = v.community_board
    );

    SET @rows_ins  = @@ROWCOUNT;
    SET @rows_skip = @rows_read - @rows_ins - @rows_rej;

    -- ── LOG: SUCCESS ─────────────────────────────────────────
    UPDATE etl.etl_log
    SET    status        = 'SUCCESS',
           rows_read     = @rows_read,
           rows_inserted = @rows_ins,
           rows_skipped  = @rows_skip,
           rows_rejected = @rows_rej,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_location SUCCESS'
        + ' | read: '     + CAST(@rows_read AS VARCHAR)
        + ' | inserted: ' + CAST(@rows_ins  AS VARCHAR)
        + ' | skipped: '  + CAST(@rows_skip AS VARCHAR)
        + ' | rejected: ' + CAST(@rows_rej  AS VARCHAR);

    IF @rows_rej > 0
        PRINT '  WARNING: ' + CAST(@rows_rej AS VARCHAR)
            + ' borough/zip/board combos rejected — check etl.etl_errors';

END TRY
BEGIN CATCH

    SET @err_msg = ERROR_MESSAGE();

    UPDATE etl.etl_log
    SET    status        = 'FAILED',
           error_message = @err_msg,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_location FAILED: ' + @err_msg;
    THROW;

END CATCH;
GO
