-- ============================================================================
-- DCF (Data Control Framework) - Consolidated DDL Script
-- ============================================================================
-- Purpose: Single script to deploy all DCF tables and infrastructure
-- Author: Data Engineering Team - GDW1 DataStage to dbt Migration
-- Version: 1.0
-- Created: January 2025
--
-- Components Included:
--   1. Core DCF Tables (streams, processes, execution logging)
--   2. IGSN Header Framework (file validation)
--   3. Error Management Tables (transformation errors)
--   4. Process Instance Tracking
--   5. Supporting Views and Functions
--
-- Consolidated from these source files:
--   - dcf_schema_init.sql (core DCF tables)
--   - dcf_process_table.sql (process configuration and instances)
--   - igsn_frmw_schema_init.sql (header validation framework)
--   - xfm_err_dtl_ddl.sql (error management)
--
-- Deployment Order:
--   - Core DCF tables first (dependencies)
--   - Header framework (file validation)
--   - Error management (transformation tracking)
--   - Supporting infrastructure
--
-- Usage:
--   snowsql -c pupad_svc -f schema_definitions/dcf_ddl.sql
-- ============================================================================

-- Set context
USE DATABASE psund_migr_dcf;
USE SCHEMA p_d_dcf_001_std_0;

-- ============================================================================
-- SECTION 0: CLEANUP - DROP EXISTING OBJECTS (FOR CLEAN DEPLOYMENT)
-- ============================================================================

-- Drop views first (they depend on tables)
DROP VIEW IF EXISTS V_DCF_PROCESS_INSTANCES;
DROP VIEW IF EXISTS V_DCF_STREAM_STATUS;
DROP VIEW IF EXISTS VW_XFM_ERR_DTL_FLAT;

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS XFM_ERR_DTL;
DROP TABLE IF EXISTS DCF_T_EXEC_LOG;
DROP TABLE IF EXISTS DCF_T_PRCS_INST;
DROP TABLE IF EXISTS DCF_T_STRM_INST;
DROP TABLE IF EXISTS DCF_T_PROC;
DROP TABLE IF EXISTS DCF_T_STRM_BUS_DT;
DROP TABLE IF EXISTS DCF_T_IGSN_FRMW_HDR_CTRL;
DROP TABLE IF EXISTS DCF_T_STRM;

-- Drop file formats
DROP FILE FORMAT IF EXISTS FF_JSON_HEADERS;

-- ============================================================================
-- SECTION 1: CORE DCF FRAMEWORK TABLES
-- ============================================================================

-- ============================================================================
-- 1.1 DCF_T_STRM - Stream Configuration Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS DCF_T_STRM (
    STRM_ID NUMBER(38,0) NOT NULL,
    STRM_NAME VARCHAR(16777216) NOT NULL,
    STRM_DESC VARCHAR(16777216),
    STRM_TYPE VARCHAR(16777216) NOT NULL DEFAULT 'DAILY', -- DAILY, WEEKLY, MONTHLY, ADHOC
    CYCLE_FREQ_CODE NUMBER(38,0) NOT NULL DEFAULT 1, -- Frequency: 1 = Daily, 7 = Weekly, etc.
    MAX_CYCLES_PER_DAY NUMBER(38,0) NOT NULL DEFAULT 1,
    ALLOW_MULTIPLE_CYCLES BOOLEAN NOT NULL DEFAULT FALSE,
    BUSINESS_DOMAIN VARCHAR(100), -- APPLICATIONS, RETAIL, COMMERCIAL, etc.
    TARGET_SCHEMA VARCHAR(100), -- Target schema for output tables
    TARGET_TABLE VARCHAR(500), -- Comma-separated list of target tables
    DBT_TAG VARCHAR(100), -- dbt tag for stream execution
    CTL_ID NUMBER(38,0) NOT NULL, -- Control ID for integration
    STRM_STATUS VARCHAR(16777216) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, INACTIVE, DEPRECATED
    INST_TS TIMESTAMP_NTZ DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    UPDT_TS TIMESTAMP_NTZ DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    CREATED_BY VARCHAR(16777216) DEFAULT CURRENT_USER() NOT NULL,
    
    -- Primary key
    CONSTRAINT PK_DCF_T_STRM PRIMARY KEY (STRM_ID),
    
    -- Unique constraint on stream name
    CONSTRAINT UQ_DCF_T_STRM_NAME UNIQUE (STRM_NAME)
);

-- ============================================================================
-- 1.2 DCF_T_STRM_BUS_DT - Stream Business Date Tracking (Complete GDW2 Structure)
-- ============================================================================
CREATE TABLE IF NOT EXISTS DCF_T_STRM_BUS_DT (
    STRM_ID NUMBER(38,0) NOT NULL,
    STRM_NAME VARCHAR(16777216) NOT NULL,
    BUS_DT DATE NOT NULL,
    PREV_BUS_DT DATE NOT NULL,
    NEXT_BUS_DT DATE NOT NULL,
    BUSINESS_DATE_CYCLE_NUM NUMBER(38,0) DEFAULT 1 NOT NULL,
    BUSINESS_DATE_CYCLE_START_TS TIMESTAMP_NTZ NOT NULL,
    PROCESSING_FLAG NUMBER(38,0) NOT NULL DEFAULT 0, -- 0=READY, 1=RUNNING, 2=COMPLETED, 3=ERROR
    STREAM_STATUS VARCHAR(16777216) DEFAULT 'READY' NOT NULL,
    INST_TS TIMESTAMP_NTZ DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    UPDT_TS TIMESTAMP_NTZ DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    CREATED_BY VARCHAR(16777216) DEFAULT CURRENT_USER() NOT NULL,
    
    -- Primary key
    CONSTRAINT PK_DCF_T_STRM_BUS_DT PRIMARY KEY (STRM_ID, BUS_DT, BUSINESS_DATE_CYCLE_NUM)
    
    -- Note: Foreign key to DCF_T_STRM commented out until DCF_T_STRM has proper indexing
    -- CONSTRAINT FK_DCF_T_STRM_BUS_DT_STRM FOREIGN KEY (STRM_ID) REFERENCES DCF_T_STRM(STRM_ID),
    
    -- Note: CHECK constraints not supported in Snowflake
    -- CONSTRAINT CK_DCF_T_STRM_BUS_DT_FLAG CHECK (PROCESSING_FLAG IN (0, 1, 2, 3))
);

-- ============================================================================
-- 1.3 DCF_T_STRM_INST - Stream Instance Tracking Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS DCF_T_STRM_INST (
    STRM_ID NUMBER(38,0) NOT NULL,
    STRM_NAME VARCHAR(16777216) NOT NULL,
    STRM_START_TS TIMESTAMP_NTZ(9) NOT NULL,
    STRM_END_TS TIMESTAMP_NTZ(9) NOT NULL,
    STRM_STATUS VARCHAR(16777216) NOT NULL,
    STRM_PARMS VARCHAR(16777216),
    INST_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    UPDT_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    CREATED_BY VARCHAR(16777216) NOT NULL DEFAULT CURRENT_USER()
);

-- ============================================================================
-- 1.4 DCF_T_PROC - Process Configuration Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS DCF_T_PROC (
    PROC_ID NUMBER(38,0) NOT NULL,
    STRM_ID NUMBER(38,0) NOT NULL,
    PROC_NAME VARCHAR(16777216) NOT NULL,
    PROC_DESC VARCHAR(16777216),
    PROC_TYPE VARCHAR(16777216) NOT NULL, -- VALIDATION, LOAD, TRANSFORM, etc.
    PROC_ORDER NUMBER(38,0) NOT NULL,
    PROC_STATUS VARCHAR(16777216) DEFAULT 'ACTIVE', -- ACTIVE, INACTIVE, DEPRECATED
    DBT_MODEL_NAME VARCHAR(16777216), -- Corresponding dbt model name
    DBT_TAG VARCHAR(16777216), -- Corresponding dbt tag
    CREATED_BY VARCHAR(16777216) DEFAULT CURRENT_USER(),
    CREATED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(16777216) DEFAULT CURRENT_USER(),
    UPDATED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Primary key
    CONSTRAINT PK_DCF_T_PROC PRIMARY KEY (PROC_ID),
    
    -- Unique constraint on stream + process order
    CONSTRAINT UQ_DCF_T_PROC_STRM_ORDER UNIQUE (STRM_ID, PROC_ORDER),
    
    -- Unique constraint on process name (global uniqueness)
    CONSTRAINT UQ_DCF_T_PROC_NAME UNIQUE (PROC_NAME)
    
    -- Note: Foreign key to DCF_T_STRM commented out until DCF_T_STRM has proper primary key
    -- CONSTRAINT FK_DCF_T_PROC_STRM FOREIGN KEY (STRM_ID) REFERENCES DCF_T_STRM(STRM_ID)
);

-- ============================================================================
-- 1.5 DCF_T_PRCS_INST - Process Instance Tracking Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS DCF_T_PRCS_INST (
    PRCS_INST_ID NUMBER(38,0) IDENTITY(1,1) NOT NULL, -- Auto-incrementing process instance ID
    STRM_ID NUMBER(38,0) NOT NULL,
    STRM_NAME VARCHAR(16777216) NOT NULL,
    PRCS_NAME VARCHAR(16777216) NOT NULL,
    PRCS_BUS_DT DATE NOT NULL,
    PRCS_START_TS TIMESTAMP_NTZ(9) NOT NULL,
    PRCS_END_TS TIMESTAMP_NTZ(9),
    PRCS_STATUS VARCHAR(16777216) NOT NULL DEFAULT 'RUNNING', -- RUNNING, COMPLETED, FAILED, ABORTED
    PRCS_RETRY_CNT NUMBER(38,0) DEFAULT 0,
    PRCS_PARMS VARCHAR(16777216),
    INST_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    UPDT_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    CREATED_BY VARCHAR(16777216) NOT NULL DEFAULT CURRENT_USER(),
    
    -- Primary key
    CONSTRAINT PK_DCF_T_PRCS_INST PRIMARY KEY (PRCS_INST_ID),
    
    -- Note: Foreign key to DCF_T_STRM commented out until DCF_T_STRM has proper primary key
    -- CONSTRAINT FK_DCF_T_PRCS_INST_STRM FOREIGN KEY (STRM_ID) REFERENCES DCF_T_STRM(STRM_ID),
    
    -- Unique constraint on stream + process + business date (prevent duplicate runs)
    CONSTRAINT UQ_DCF_T_PRCS_INST_UNIQUE_RUN UNIQUE (STRM_ID, PRCS_NAME, PRCS_BUS_DT)
);

-- ============================================================================
-- 1.6 DCF_T_EXEC_LOG - Execution Logging Table (Complete GDW2 Structure)
-- ============================================================================
CREATE TABLE IF NOT EXISTS DCF_T_EXEC_LOG (
    STRM_ID NUMBER(38,0) NOT NULL,
    STRM_NAME VARCHAR(16777216) NOT NULL,
    PRCS_NAME VARCHAR(16777216) NOT NULL,
    BUSINESS_DATE DATE,
    BUSINESS_DATE_CYCLE_NUM NUMBER(38,0),
    STEP_ID VARCHAR(16777216),
    STEP_NAME VARCHAR(16777216),
    STEP_STATUS VARCHAR(16777216),
    MESSAGE_TYPE NUMBER(38,0) NOT NULL,  -- 1=Info, 2=Warning, 3=Error, 4=Debug
    MESSAGE_TEXT VARCHAR(16777216),
    ERROR_CODE NUMBER(38,0),
    ERROR_MESSAGE VARCHAR(16777216),
    SQL_TEXT VARCHAR(16777216),
    SQL_ACTIVITY_COUNT NUMBER(38,0),
    QUERY_ID VARCHAR(16777216),
    SESSION_ID VARCHAR(16777216) DEFAULT CURRENT_SESSION() NOT NULL,
    WAREHOUSE_NAME VARCHAR(16777216) DEFAULT CURRENT_WAREHOUSE() NOT NULL,
    CREATED_TS TIMESTAMP_NTZ DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(6)),
    CREATED_BY VARCHAR(16777216) DEFAULT CURRENT_USER() NOT NULL
);

-- ============================================================================
-- SECTION 2: IGSN HEADER FRAMEWORK (FILE VALIDATION)
-- ============================================================================

-- ============================================================================
-- 2.1 DCF_T_IGSN_FRMW_HDR_CTRL - Header Tracker Table
-- ============================================================================
CREATE OR REPLACE TABLE DCF_T_IGSN_FRMW_HDR_CTRL (
    -- =================================================================
    -- Primary identification and control
    -- =================================================================
    HEADER_TRACKER_ID       NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    
    -- =================================================================
    -- File identification (generic framework supporting all EBCDIC types)
    -- =================================================================
    FEED_NM                 VARCHAR(255)    NOT NULL,        -- BCFINSG, BCBALSG, CSEL4, etc.
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
    -- Generic header/trailer data (flexible JSON structure for any EBCDIC type)
    -- =================================================================
    HEADER_METADATA         VARIANT         NOT NULL,        -- Complete header JSON structure
    TRAILER_METADATA        VARIANT,                         -- Complete trailer JSON structure (if available)
    
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
-- 2.2 JSON File Format for Header Processing
-- ============================================================================
CREATE OR REPLACE FILE FORMAT FF_JSON_HEADERS
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE        -- Preserve JSON structure
    COMMENT = 'JSON file format for EBCDIC header files - DCF Framework';

-- ============================================================================
-- SECTION 3: ERROR MANAGEMENT TABLES
-- ============================================================================

-- ============================================================================
-- 3.1 XFM_ERR_DTL - Transformation Error Detail Table
-- ============================================================================
CREATE OR REPLACE TABLE XFM_ERR_DTL (
    -- Primary identifiers
    ERR_ID                  NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    
    -- Process tracking
    STRM_NM                 VARCHAR(100) NOT NULL,      -- Stream name (e.g., BCFINSG_PLAN_BALN_SEGM_LOAD)
    PRCS_NM                 VARCHAR(100) NOT NULL,      -- Process name (e.g., XfmPlanBalnSegmMstr)
    PRCS_DT                 DATE NOT NULL,              -- Processing date

    SRCE_KEY_NM             VARCHAR(100) NOT NULL,      -- Source record identifier
    
    -- Source information
    SRCE_FILE_NM            VARCHAR(255),               -- Source file name
    SRCE_ROW_NUM            NUMBER(10,0),               -- Source row number
    
    -- Error details (JSON structure)
    ERR_DTLS_JSON           VARIANT NOT NULL,           -- JSON array of all errors for this record
    
    -- Processing metadata
    PRCS_INST_ID            VARCHAR(100),               -- Process instance identifier
    REC_INS_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    INS_USR_NM              VARCHAR(100) DEFAULT CURRENT_USER()
);

-- ============================================================================
-- 3.2 VW_XFM_ERR_DTL_FLAT - Error Flattening View
-- ============================================================================
CREATE OR REPLACE VIEW VW_XFM_ERR_DTL_FLAT AS
SELECT 
    -- Main record identifiers
    e.ERR_ID,
    e.SRCE_KEY_NM,
    e.STRM_NM,
    e.PRCS_NM,
    e.PRCS_DT,
    
    -- Source information
    e.SRCE_FILE_NM,
    e.SRCE_ROW_NUM,
    
    -- Flattened error details (one row per error)
    f.index as ERR_SEQ_NUM,
    f.value:column_name::VARCHAR(100) as ERR_COLM_NM,
    f.value:error_type::VARCHAR(100) as ERR_TYPE_NM,
    f.value:error_message::VARCHAR(500) as ERR_MSG_TXT,
    f.value:original_value::VARCHAR(255) as ORIG_VAL_TXT,
    f.value:attempted_value::VARCHAR(255) as ATMPTD_VAL_TXT,
    
    -- Processing metadata
    e.PRCS_INST_ID,
    e.REC_INS_TS,
    e.INS_USR_NM
    
FROM XFM_ERR_DTL e,
     LATERAL FLATTEN(input => e.ERR_DTLS_JSON) f;

-- ============================================================================
-- SECTION 4: OPERATIONAL VIEWS AND INDEXES
-- ============================================================================

-- ============================================================================
-- 4.1 V_DCF_STREAM_STATUS - Stream Status Dashboard View
-- ============================================================================
CREATE OR REPLACE VIEW V_DCF_STREAM_STATUS AS
SELECT 
    s.STRM_ID,
    s.STRM_NAME,
    s.STRM_DESC,
    s.STRM_TYPE,
    s.BUSINESS_DOMAIN,
    s.TARGET_SCHEMA,
    s.TARGET_TABLE,
    s.STRM_STATUS,
    
    -- Current business date info
    bd.BUS_DT as CURRENT_BUS_DT,
    bd.BUSINESS_DATE_CYCLE_NUM as CURRENT_CYCLE,
    bd.PROCESSING_FLAG,
    
    CASE bd.PROCESSING_FLAG
        WHEN 0 THEN 'READY'
        WHEN 1 THEN 'RUNNING'
        WHEN 2 THEN 'COMPLETED'
        WHEN 3 THEN 'ERROR'
        ELSE 'UNKNOWN'
    END as PROCESSING_STATUS,
    
    bd.BUSINESS_DATE_CYCLE_START_TS as STRM_START_TS,
    NULL as STRM_END_TS,  -- End timestamp not tracked in business date table
    
    -- Calculate runtime if running
    CASE 
        WHEN bd.PROCESSING_FLAG = 1 THEN 
            DATEDIFF(second, bd.BUSINESS_DATE_CYCLE_START_TS, CURRENT_TIMESTAMP())
        ELSE NULL
    END as RUNTIME_SECONDS

FROM DCF_T_STRM s
LEFT JOIN DCF_T_STRM_BUS_DT bd 
    ON s.STRM_ID = bd.STRM_ID 
    AND bd.BUS_DT = (
        SELECT MAX(BUS_DT) 
        FROM DCF_T_STRM_BUS_DT bd2 
        WHERE bd2.STRM_ID = s.STRM_ID
    )
WHERE s.STRM_STATUS = 'ACTIVE'
ORDER BY s.STRM_NAME;

-- ============================================================================
-- 4.2 V_DCF_PROCESS_INSTANCES - Process Instance Status View
-- ============================================================================
CREATE OR REPLACE VIEW V_DCF_PROCESS_INSTANCES AS
SELECT 
    pi.PRCS_INST_ID,
    pi.STRM_NAME,
    pi.PRCS_NAME,
    pi.PRCS_BUS_DT,
    pi.PRCS_STATUS,
    pi.PRCS_START_TS,
    pi.PRCS_END_TS,
    pi.PRCS_RETRY_CNT,
    
    -- Calculate runtime
    CASE 
        WHEN pi.PRCS_STATUS = 'RUNNING' THEN 
            DATEDIFF(second, pi.PRCS_START_TS, CURRENT_TIMESTAMP())
        WHEN pi.PRCS_STATUS IN ('COMPLETED', 'FAILED', 'ABORTED') THEN 
            DATEDIFF(second, pi.PRCS_START_TS, pi.PRCS_END_TS)
        ELSE NULL
    END as RUNTIME_SECONDS,
    
    -- Process configuration info
    p.PROC_TYPE,
    p.PROC_ORDER,
    p.DBT_MODEL_NAME,
    p.DBT_TAG
    
FROM DCF_T_PRCS_INST pi
LEFT JOIN DCF_T_PROC p 
    ON pi.PRCS_NAME = p.PROC_NAME
ORDER BY pi.PRCS_START_TS DESC;

-- ============================================================================
-- SECTION 5: INDEXES FOR PERFORMANCE
-- ============================================================================

-- Note: Snowflake uses automatic clustering, but we can define clustering keys for large tables

-- Clustering for large log table
-- ALTER TABLE DCF_T_EXEC_LOG 
--   CLUSTER BY (STRM_NAME, LOG_TS);

-- Clustering for header control table
-- ALTER TABLE DCF_T_IGSN_FRMW_HDR_CTRL 
--   CLUSTER BY (FEED_NM, EXTRACTED_PROCESSING_DT, PROCESSING_STATUS);

-- ============================================================================
-- SECTION 6: GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to SYSADMIN role (adjust as needed for your environment)
GRANT ALL PRIVILEGES ON TABLE DCF_T_STRM TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE DCF_T_STRM_BUS_DT TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE DCF_T_STRM_INST TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE DCF_T_PROC TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE DCF_T_PRCS_INST TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE DCF_T_EXEC_LOG TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE DCF_T_IGSN_FRMW_HDR_CTRL TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON TABLE XFM_ERR_DTL TO ROLE SYSADMIN;

GRANT SELECT ON VIEW VW_XFM_ERR_DTL_FLAT TO ROLE SYSADMIN;
GRANT SELECT ON VIEW V_DCF_STREAM_STATUS TO ROLE SYSADMIN;
GRANT SELECT ON VIEW V_DCF_PROCESS_INSTANCES TO ROLE SYSADMIN;

GRANT USAGE ON FILE FORMAT FF_JSON_HEADERS TO ROLE SYSADMIN;

-- ============================================================================
-- SECTION 7: VERIFICATION QUERIES
-- ============================================================================

-- Check all created objects
SELECT 
    'TABLE' as OBJECT_TYPE,
    TABLE_NAME as OBJECT_NAME,
    'CREATED' as STATUS
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '{{ var("dcf_schema") | upper }}'
  AND TABLE_NAME IN (
    'DCF_T_STRM', 'DCF_T_STRM_BUS_DT', 'DCF_T_STRM_INST', 'DCF_T_PROC', 'DCF_T_PRCS_INST',
    'DCF_T_EXEC_LOG', 'DCF_T_IGSN_FRMW_HDR_CTRL', 'XFM_ERR_DTL'
  )

UNION ALL

SELECT 
    'VIEW' as OBJECT_TYPE,
    TABLE_NAME as OBJECT_NAME,
    'CREATED' as STATUS
FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = '{{ var("dcf_schema") | upper }}'
  AND TABLE_NAME IN (
    'VW_XFM_ERR_DTL_FLAT', 'V_DCF_STREAM_STATUS', 'V_DCF_PROCESS_INSTANCES'
  )

UNION ALL

SELECT 
    'FILE_FORMAT' as OBJECT_TYPE,
    FILE_FORMAT_NAME as OBJECT_NAME,
    'CREATED' as STATUS
FROM INFORMATION_SCHEMA.FILE_FORMATS 
WHERE FILE_FORMAT_SCHEMA = '{{ var("dcf_schema") | upper }}'
  AND FILE_FORMAT_NAME = 'FF_JSON_HEADERS'

ORDER BY OBJECT_TYPE, OBJECT_NAME;

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================
SELECT 
    '✅ DCF Framework deployed successfully!' as DEPLOYMENT_STATUS,
    'All tables, views, and supporting objects created' as DETAILS,
    'Ready for stream configuration and dbt model execution' as NEXT_STEPS;

-- ============================================================================
-- END OF DCF DDL SCRIPT
-- ============================================================================
