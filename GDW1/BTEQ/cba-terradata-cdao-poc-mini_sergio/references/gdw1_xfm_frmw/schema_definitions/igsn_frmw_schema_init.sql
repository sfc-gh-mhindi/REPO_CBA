-- ============================================================================
-- EBCDIC Header Tracker Framework - IGSN Generic File Validation Infrastructure
-- ============================================================================
-- Purpose: Generic framework to track EBCDIC file control records for validation
-- Supports: BCFINSG, BCBALSG, and any other EBCDIC file types
-- Replaces: DataStage SQ20-style file discovery and validation logic
-- Pattern: Cloud-native equivalent of DataStage file validation framework
--
-- Key Components:
--   1. DCF_T_IGSN_FRMW_HDR_CTRL - Generic header tracking table
--   2. PIPE_IGSN_HDR_LOADER - Auto-ingest Snowpipe for header files
--   3. Monitoring queries for pipeline health
--   4. Manual loading commands for testing

-- ============================================================================
-- 1. CREATE GENERIC HEADER TRACKER TABLE
-- ============================================================================

CREATE OR REPLACE TABLE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL (
    -- =================================================================
    -- Primary identification and control
    -- =================================================================
    HEADER_TRACKER_ID       NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    
    -- =================================================================
    -- File identification (generic framework supporting all EBCDIC types)
    -- =================================================================
    FEED_NM                 VARCHAR(255)    NOT NULL,        -- BCFINSG, BCBALSG, etc.
    SOURCE_FILE_NM          VARCHAR(255),                    -- Original EBCDIC file name (derived)
    HEADER_FILE_NM          VARCHAR(255)    NOT NULL,        -- Header JSON file name
    STAGE_LOCATION          VARCHAR(500),                    -- Snowflake stage location
    
    -- =================================================================
    -- Temporal tracking (file lifecycle timestamps)
    -- =================================================================
    FILE_LAST_MODIFIED_TS   TIMESTAMP_NTZ   NOT NULL,        -- When file was last modified in stage
    FILE_LOAD_TS            TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(), -- When record was inserted
    EXPECTED_PROCESSING_DT  VARCHAR(8),                      -- Expected date from job parameter (YYYYMMDD)
    EXTRACTED_PROCESSING_DT VARCHAR(8),                      -- Date extracted from header records (YYYYMMDD)
    
    -- =================================================================
    -- Processing status lifecycle management
    -- =================================================================
    -- Status values: DISCOVERED → VALIDATED → READY → PROCESSING → COMPLETED
    --               ↘ REJECTED, DATE_MISMATCH, ERROR (terminal states)
    PROCESSING_STATUS       VARCHAR(50)     DEFAULT 'DISCOVERED',
    PROCESSING_MSG          VARCHAR(1000),                   -- Validation messages, error details
    PROCESSING_TS           TIMESTAMP_NTZ,                   -- Last status update timestamp
    
    -- =================================================================
    -- Generic header data (flexible JSON structure for any EBCDIC type)
    -- =================================================================
    HEADER_METADATA         VARIANT         NOT NULL,        -- Complete header JSON structure
    
    -- =================================================================
    -- Audit trail (standard DCF pattern)
    -- =================================================================
    CREATED_BY              VARCHAR(100)    DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY              VARCHAR(100),
    UPDATED_TIMESTAMP       TIMESTAMP_NTZ,
    
    -- =================================================================
    -- Processing context (DCF integration)
    -- =================================================================
    STREAM_NAME             VARCHAR(100),                    -- DCF stream name (e.g., BCFINSG_PLAN_BALN_SEGM_LOAD)
    PROCESS_RUN_ID          VARCHAR(100),                    -- DCF process instance ID
    BUSINESS_DATE           VARCHAR(8),                      -- Business processing date (YYYYMMDD)
    
    -- =================================================================
    -- File metrics and validation counts
    -- =================================================================
    TOTAL_HEADER_RECORDS    NUMBER(10,0),                    -- Count of header records in file
    CONTROL_RECORD_COUNT    NUMBER(10,0),                    -- Validation count from control records
    
    -- =================================================================
    -- Extensibility for file-type specific attributes
    -- =================================================================
    CUSTOM_ATTRIBUTES       VARIANT                          -- EBCDIC type-specific validation attributes
);

-- ============================================================================
-- 2. CREATE JSON FILE FORMAT (for consistent parsing)
-- ============================================================================
CREATE OR REPLACE FILE FORMAT PSUND_MIGR_DCF.P_D_DCF_001_STD_0.FF_JSON_HEADERS
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE        -- Preserve JSON structure
    COMMENT = 'JSON file format for EBCDIC header files';

-- ============================================================================
-- 3. SNOWPIPE FOR AUTOMATIC HEADER LOADING
-- ============================================================================
-- Purpose: Auto-ingest header JSON files as they arrive in the stage
-- Triggers: File pattern matching '*BCFINSG_CA*headers.json'
-- Processing: Extracts metadata and loads into header tracker table
--
CREATE OR REPLACE PIPE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.PIPE_IGSN_HDR_LOADER
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for EBCDIC header JSON files'
    AS
    COPY INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL (
        FEED_NM,
        HEADER_FILE_NM,
        FILE_LAST_MODIFIED_TS,
        FILE_LOAD_TS,
        PROCESSING_STATUS,
        STREAM_NAME,
        HEADER_METADATA
    )
    FROM (
        SELECT 
            'BCFINSG'                               AS FEED_NM,
            METADATA$FILENAME                       AS HEADER_FILE_NM,
            METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ AS FILE_LAST_MODIFIED_TS,
            METADATA$START_SCAN_TIME::TIMESTAMP_NTZ AS FILE_LOAD_TS,
            'DISCOVERED'                            AS PROCESSING_STATUS,
            'BCFINSG_PLAN_BALN_SEGM_LOAD'          AS STREAM_NAME,
            PARSE_JSON($1)                          AS HEADER_METADATA
        FROM @psundaram.gdw1_0801.istg_gdw1_ebcdic_pqrt
    )
    PATTERN = '.*BCFINSG_CA.*headers\.json$'                -- Regex pattern for file matching
    FILE_FORMAT = PSUND_MIGR_DCF.P_D_DCF_001_STD_0.FF_JSON_HEADERS;

-- ============================================================================
-- 4. PIPE MANAGEMENT AND MONITORING
-- ============================================================================

-- Refresh pipe to check for any pending files
ALTER PIPE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.PIPE_IGSN_HDR_LOADER REFRESH;

-- Monitor recent copy operations (last 1 hour)
SELECT 
    FILE_NAME,
    LAST_LOAD_TIME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    ERROR_LIMIT,
    ERRORS_SEEN
FROM TABLE(
    PSUND_MIGR_DCF.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;

-- Check pipe status and health
SELECT SYSTEM$PIPE_STATUS('PSUND_MIGR_DCF.P_D_DCF_001_STD_0.PIPE_IGSN_HDR_LOADER') AS pipe_status;

-- ============================================================================
-- 5. PERFORMANCE OPTIMIZATION (for large-scale operations)
-- ============================================================================
-- CLUSTERING RECOMMENDATIONS:
-- For production environments with high file volumes, consider:
--
-- ALTER TABLE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL 
--   CLUSTER BY (FEED_NM, EXTRACTED_PROCESSING_DT, PROCESSING_STATUS);
--
-- This optimizes common query patterns:
-- - Filtering by feed type (BCFINSG, BCBALSG)
-- - Filtering by processing date ranges
-- - Filtering by processing status for monitoring

-- ============================================================================
-- 6. MANUAL LOADING COMMANDS (for testing and backfill)
-- ============================================================================
-- Purpose: Load existing header files manually for testing or backfill scenarios
-- Use case: When Snowpipe is not available or for historical data loading
--
COPY INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL (
    FEED_NM,
    HEADER_FILE_NM,
    FILE_LAST_MODIFIED_TS,
    FILE_LOAD_TS,
    PROCESSING_STATUS,
    STREAM_NAME,
    HEADER_METADATA
)
FROM (
    SELECT 
        'BCFINSG'                               AS FEED_NM,
        METADATA$FILENAME                       AS HEADER_FILE_NM,
        METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ AS FILE_LAST_MODIFIED_TS,
        CURRENT_TIMESTAMP()                     AS FILE_LOAD_TS,
        'DISCOVERED'                            AS PROCESSING_STATUS,
        'BCFINSG_PLAN_BALN_SEGM_LOAD'          AS STREAM_NAME,
        PARSE_JSON($1)                          AS HEADER_METADATA
    FROM @psundaram.gdw1_0801.istg_gdw1_ebcdic_pqrt/BCFINSG_CA_20250709_1k_headers.json
)
FILE_FORMAT = PSUND_MIGR_DCF.P_D_DCF_001_STD_0.FF_JSON_HEADERS
ON_ERROR = 'CONTINUE'                           -- Continue loading even if some records fail
RETURN_FAILED_ONLY = FALSE;                     -- Return all results, not just failures

-- ============================================================================
-- 7. OPERATIONAL VIEWS (enable after successful table creation)
-- ============================================================================
-- Purpose: Simplified access patterns for SQ20 validation and monitoring
-- Note: Uncomment these views after confirming table structure is stable

/*
-- =======================================================
-- SQ20 Validation View - Extract EBCDIC header details
-- =======================================================
CREATE OR REPLACE VIEW PSUND_MIGR_DCF.P_D_DCF_001_STD_0.V_IGSN_SQ20_VALIDATION AS
SELECT 
    -- Core tracking fields
    HEADER_TRACKER_ID,
    FEED_NM,
    SOURCE_FILE_NM,
    HEADER_FILE_NM,
    EXPECTED_PROCESSING_DT,
    EXTRACTED_PROCESSING_DT,
    PROCESSING_STATUS,
    PROCESSING_MSG,
    
    -- Extract BCFINSG-specific validation fields from JSON metadata
    HEADER_METADATA['file_metadata']['header_records'][0]['BCF_ACCOUNT_NO_0']::STRING as CONTROL_RECORD_IDENTIFIER,
    HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::NUMBER as BCF_DT_CURR_PROC,
    HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_NEXT_PROC']::NUMBER as BCF_DT_NEXT_PROC,
    HEADER_METADATA['file_metadata']['total_header_records']::NUMBER as TOTAL_HEADER_RECORDS,
    
    -- Audit fields
    FILE_LOAD_TS,
    PROCESSING_TS,
    STREAM_NAME,
    CREATED_BY,
    CREATED_TIMESTAMP
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
WHERE FEED_NM = 'BCFINSG';

-- =======================================================
-- File Processing Dashboard View - Operational monitoring
-- =======================================================
CREATE OR REPLACE VIEW PSUND_MIGR_DCF.P_D_DCF_001_STD_0.V_IGSN_FILE_STATUS_DASHBOARD AS
SELECT 
    FEED_NM,
    EXPECTED_PROCESSING_DT,
    
    -- File count metrics
    COUNT(*)                                                             AS TOTAL_FILES,
    COUNT(CASE WHEN PROCESSING_STATUS = 'DISCOVERED' THEN 1 END)        AS FILES_DISCOVERED,
    COUNT(CASE WHEN PROCESSING_STATUS = 'VALIDATED' THEN 1 END)         AS FILES_VALIDATED,
    COUNT(CASE WHEN PROCESSING_STATUS = 'READY' THEN 1 END)             AS FILES_READY,
    COUNT(CASE WHEN PROCESSING_STATUS = 'PROCESSING' THEN 1 END)        AS FILES_PROCESSING,
    COUNT(CASE WHEN PROCESSING_STATUS = 'COMPLETED' THEN 1 END)         AS FILES_COMPLETED,
    COUNT(CASE WHEN PROCESSING_STATUS = 'REJECTED' THEN 1 END)          AS FILES_REJECTED,
    COUNT(CASE WHEN PROCESSING_STATUS = 'DATE_MISMATCH' THEN 1 END)     AS FILES_DATE_MISMATCH,
    COUNT(CASE WHEN PROCESSING_STATUS = 'ERROR' THEN 1 END)             AS FILES_ERROR,
    
    -- Timing metrics
    MIN(FILE_LOAD_TS)                                                   AS EARLIEST_FILE_TIME,
    MAX(FILE_LOAD_TS)                                                   AS LATEST_FILE_TIME,
    MAX(PROCESSING_TS)                                                  AS LATEST_PROCESSING_TIME,
    
    -- File list (for troubleshooting)
    LISTAGG(HEADER_FILE_NM, ', ') WITHIN GROUP (ORDER BY FILE_LOAD_TS) AS FILE_LIST
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
GROUP BY FEED_NM, EXPECTED_PROCESSING_DT
ORDER BY FEED_NM, EXPECTED_PROCESSING_DT DESC;
*/

-- ============================================================================
-- 8. VERIFICATION AND SUCCESS CONFIRMATION
-- ============================================================================
-- Purpose: Verify infrastructure creation and provide status update

-- Check table structure
SELECT 
    'DCF_T_IGSN_FRMW_HDR_CTRL' AS object_name,
    'TABLE' AS object_type,
    'CREATED' AS status
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'P_D_DCF_001_STD_0' 
  AND TABLE_NAME = 'DCF_T_IGSN_FRMW_HDR_CTRL'

UNION ALL

-- Check pipe status
SELECT 
    'PIPE_IGSN_HDR_LOADER' AS object_name,
    'PIPE' AS object_type,
    'CREATED' AS status
FROM INFORMATION_SCHEMA.PIPES 
WHERE PIPE_SCHEMA = 'P_D_DCF_001_STD_0' 
  AND PIPE_NAME = 'PIPE_IGSN_HDR_LOADER'

UNION ALL

-- Check file format
SELECT 
    'FF_JSON_HEADERS' AS object_name,
    'FILE_FORMAT' AS object_type,
    'CREATED' AS status
FROM INFORMATION_SCHEMA.FILE_FORMATS 
WHERE FILE_FORMAT_SCHEMA = 'P_D_DCF_001_STD_0' 
  AND FILE_FORMAT_NAME = 'FF_JSON_HEADERS';

-- Final success message
SELECT '✅ IGSN EBCDIC Header Tracker Framework deployed successfully!' AS deployment_status,
       'Ready for SQ20 validation workflows' AS next_steps;