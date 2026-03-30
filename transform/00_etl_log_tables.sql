-- ============================================================
-- NYC 311 — ETL Log & Error Tables
-- Run AFTER nyc311_create_tables_v5.sql
-- Author  : Pratik Kudva
-- ============================================================

USE NYC311_Analytics;
GO

-- ============================================================
-- etl.etl_log
-- One row per ETL step execution (dim or fact load)
-- Captures row counts, timing, batch ID, pass/fail
-- ============================================================
CREATE SCHEMA etl;
GO

CREATE TABLE etl.etl_log (
    log_id          INT           NOT NULL IDENTITY(1,1),
    batch_id        INT           NOT NULL,              -- YYYYMMDD
    step_name       VARCHAR(100)  NOT NULL,              -- e.g. 'dim_agency'
    step_number     TINYINT       NOT NULL,              -- 1=date 2=agency etc.
    status          VARCHAR(10)   NOT NULL,              -- STARTED / SUCCESS / FAILED
    rows_read       INT           NULL,                  -- rows found in stg
    rows_inserted   INT           NULL,                  -- rows written to dw
    rows_rejected   INT           NULL,                  -- rows sent to error table
    rows_skipped    INT           NULL,                  -- already in dw (dedup)
    start_time      DATETIME      NOT NULL DEFAULT GETDATE(),
    end_time        DATETIME      NULL,
    duration_sec    AS (DATEDIFF(SECOND, start_time, end_time)) PERSISTED,
    error_message   VARCHAR(MAX)  NULL,
    run_by          VARCHAR(100)  NOT NULL DEFAULT SYSTEM_USER,
    server_name     VARCHAR(100)  NOT NULL DEFAULT @@SERVERNAME,

    CONSTRAINT PK_etl_log PRIMARY KEY CLUSTERED (log_id)
);
GO

CREATE NONCLUSTERED INDEX IX_etl_log_batch
    ON etl.etl_log (batch_id, step_number)
    INCLUDE (step_name, status, rows_inserted, rows_rejected, duration_sec);
GO

-- ============================================================
-- etl.etl_errors
-- One row per rejected record — full data + reason stored
-- Allows manual inspection and potential reprocessing
-- ============================================================
CREATE TABLE etl.etl_errors (
    error_id        INT           NOT NULL IDENTITY(1,1),
    log_id          INT           NOT NULL,              -- FK → etl_log
    batch_id        INT           NOT NULL,
    step_name       VARCHAR(100)  NOT NULL,
    unique_key      VARCHAR(20)   NULL,                  -- source record ID
    error_type      VARCHAR(50)   NOT NULL,              -- NULL_KEY / BAD_DATE / NO_DIM_MATCH etc.
    error_message   VARCHAR(MAX)  NOT NULL,
    -- Raw source values that caused the error
    raw_agency      VARCHAR(20)   NULL,
    raw_borough     VARCHAR(100)  NULL,
    raw_complaint   VARCHAR(200)  NULL,
    raw_created     VARCHAR(50)   NULL,
    raw_closed      VARCHAR(50)   NULL,
    raw_latitude    VARCHAR(50)   NULL,
    raw_longitude   VARCHAR(50)   NULL,
    raw_status      VARCHAR(50)   NULL,
    created_at      DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_etl_errors    PRIMARY KEY CLUSTERED (error_id),
    CONSTRAINT FK_errors_log    FOREIGN KEY (log_id) REFERENCES etl.etl_log (log_id)
);
GO

CREATE NONCLUSTERED INDEX IX_etl_errors_batch
    ON etl.etl_errors (batch_id, step_name)
    INCLUDE (error_type, unique_key);
GO

CREATE NONCLUSTERED INDEX IX_etl_errors_type
    ON etl.etl_errors (error_type)
    INCLUDE (batch_id, step_name, unique_key, error_message);
GO

PRINT 'etl schema, etl_log and etl_errors tables created';
GO
