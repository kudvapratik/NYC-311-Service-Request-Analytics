-- ============================================================
-- NYC 311 — Step 1 of 5: Load dw.dim_date
-- Run order : 01 → 02 → 03 → 04 → 05_fact_sp
-- Dependency : none (no FK to other dims)
-- Strategy   : generate all dates 2020–2030 via recursive CTE
--              idempotent — safe to re-run, skips existing dates
-- Author     : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

DECLARE
    @batch_id    INT          = CAST(CONVERT(VARCHAR, GETDATE(), 112) AS INT),
    @step_name   VARCHAR(100) = 'dim_date',
    @step_number TINYINT      = 1,
    @log_id      INT,
    @rows_ins    INT          = 0,
    @rows_skip   INT          = 0,
    @err_msg     VARCHAR(MAX);

-- ── LOG: STARTED ─────────────────────────────────────────────
INSERT INTO etl.etl_log (batch_id, step_name, step_number, status)
VALUES (@batch_id, @step_name, @step_number, 'STARTED');
SET @log_id = SCOPE_IDENTITY();
PRINT 'Step 1: Loading dim_date (batch ' + CAST(@batch_id AS VARCHAR) + ')...';

BEGIN TRY

    -- Count what already exists (for skip count reporting)
    SET @rows_skip = (SELECT COUNT(*) FROM dw.dim_date);

    -- Generate and insert all dates 2020–2030
    -- Recursive CTE: anchor at 2020-01-01, add one day per recursion
    WITH date_series AS (
        SELECT CAST('2020-01-01' AS DATE) AS full_date
        UNION ALL
        SELECT DATEADD(DAY, 1, full_date)
        FROM   date_series
        WHERE  full_date < '2030-12-31'
    )
    INSERT INTO dw.dim_date (
        date_key, full_date,
        year, quarter, quarter_name,
        month, month_name, month_short,
        week_of_year, day_of_month, day_of_week, day_name,
        is_weekend, is_holiday
    )
    SELECT
        CAST(CONVERT(VARCHAR, full_date, 112) AS INT)        AS date_key,
        full_date,
        YEAR(full_date)                                       AS year,
        DATEPART(QUARTER, full_date)                          AS quarter,
        'Q' + CAST(DATEPART(QUARTER, full_date) AS VARCHAR)  AS quarter_name,
        MONTH(full_date)                                      AS month,
        DATENAME(MONTH,    full_date)                         AS month_name,
        LEFT(DATENAME(MONTH, full_date), 3)                   AS month_short,
        DATEPART(WEEK,     full_date)                         AS week_of_year,
        DAY(full_date)                                        AS day_of_month,
        DATEPART(WEEKDAY,  full_date)                         AS day_of_week,
        DATENAME(WEEKDAY,  full_date)                         AS day_name,
        CASE WHEN DATEPART(WEEKDAY, full_date) IN (1,7)
             THEN 1 ELSE 0 END                                AS is_weekend,
        0                                                     AS is_holiday
    FROM date_series
    -- Idempotent guard — skip dates already loaded
    WHERE CAST(CONVERT(VARCHAR, full_date, 112) AS INT)
          NOT IN (SELECT date_key FROM dw.dim_date)
    OPTION (MAXRECURSION 5000);

    SET @rows_ins  = @@ROWCOUNT;
    SET @rows_skip = @rows_skip; -- already captured above

    -- ── LOG: SUCCESS ─────────────────────────────────────────
    UPDATE etl.etl_log
    SET    status       = 'SUCCESS',
           rows_inserted = @rows_ins,
           rows_skipped  = @rows_skip,
           rows_rejected = 0,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_date SUCCESS — inserted: ' + CAST(@rows_ins  AS VARCHAR)
        + '  skipped (already loaded): ' + CAST(@rows_skip AS VARCHAR);

END TRY
BEGIN CATCH

    SET @err_msg = ERROR_MESSAGE();

    -- ── LOG: FAILED ──────────────────────────────────────────
    UPDATE etl.etl_log
    SET    status        = 'FAILED',
           error_message = @err_msg,
           end_time      = GETDATE()
    WHERE  log_id = @log_id;

    PRINT 'dim_date FAILED: ' + @err_msg;
    THROW;  -- re-raise so caller knows it failed

END CATCH;
GO
