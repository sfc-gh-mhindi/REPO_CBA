-- ============================================================================
-- Test Script for Generic Header Validation Macro
-- ============================================================================
-- Purpose: Test the validate_header macro with real data
-- Usage: Execute via snow sql CLI to verify macro functionality

-- ============================================================================
-- 1. CHECK DCF BUSINESS DATE TABLE
-- ============================================================================
SELECT 
    '=== DCF BUSINESS DATE STATUS ===' AS section,
    STRM_NAME,
    BUS_DT,
    STREAM_STATUS,
    INST_TS,
    UPDT_TS
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM_BUS_DT
WHERE STRM_NAME LIKE '%BCFINSG%'
ORDER BY STRM_NAME, BUS_DT DESC;

-- ============================================================================
-- 2. CHECK CURRENT HEADER TRACKER STATUS
-- ============================================================================
SELECT 
    '=== HEADER TRACKER STATUS ===' AS section,
    FEED_NM,
    PROCESSING_STATUS,
    STREAM_NAME,
    COUNT(*) AS file_count,
    MIN(FILE_LOAD_TS) AS earliest_file,
    MAX(FILE_LOAD_TS) AS latest_file
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
GROUP BY FEED_NM, PROCESSING_STATUS, STREAM_NAME
ORDER BY FEED_NM, PROCESSING_STATUS;

-- ============================================================================
-- 3. EXAMINE HEADER METADATA FOR TESTING
-- ============================================================================
SELECT 
    '=== HEADER METADATA DETAILS ===' AS section,
    HEADER_TRACKER_ID,
    FEED_NM,
    HEADER_FILE_NM,
    PROCESSING_STATUS,
    STREAM_NAME,
    
    -- Extract date fields from JSON
    HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING AS bcf_dt_curr_proc,
    HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_NEXT_PROC']::STRING AS bcf_dt_next_proc,
    HEADER_METADATA['file_metadata']['header_records'][0]['BCF_ACCOUNT_NO_0']::STRING AS control_record_id,
    HEADER_METADATA['file_metadata']['total_header_records']::NUMBER AS total_header_records,
    
    FILE_LOAD_TS,
    CREATED_TIMESTAMP
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
WHERE PROCESSING_STATUS = 'DISCOVERED'
ORDER BY FILE_LOAD_TS DESC
LIMIT 5;

-- ============================================================================
-- 4. SETUP TEST DATA (if needed)
-- ============================================================================
-- Insert test business date if not exists
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM_BUS_DT (
    STRM_ID,
    STRM_NAME,
    BUS_DT,
    PREV_BUS_DT,
    NEXT_BUS_DT,
    BUSINESS_DATE_CYCLE_NUM,
    BUSINESS_DATE_CYCLE_START_TS,
    PROCESSING_FLAG,
    STREAM_STATUS,
    CREATED_BY
)
SELECT 
    1490 AS STRM_ID,
    'BCFINSG_PLAN_BALN_SEGM_LOAD' AS STRM_NAME,
    CURRENT_DATE() AS BUS_DT,
    CURRENT_DATE() - INTERVAL '1 day' AS PREV_BUS_DT,
    CURRENT_DATE() + INTERVAL '1 day' AS NEXT_BUS_DT,
    1 AS BUSINESS_DATE_CYCLE_NUM,
    CURRENT_TIMESTAMP() AS BUSINESS_DATE_CYCLE_START_TS,
    1 AS PROCESSING_FLAG,
    'ACTIVE' AS STREAM_STATUS,
    CURRENT_USER() AS CREATED_BY
WHERE NOT EXISTS (
    SELECT 1 FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM_BUS_DT 
    WHERE STRM_NAME = 'BCFINSG_PLAN_BALN_SEGM_LOAD' 
      AND STREAM_STATUS = 'ACTIVE'
);

-- ============================================================================
-- 5. TEST INDIVIDUAL HEADER FILE VALIDATION
-- ============================================================================
-- Get first available header for testing
WITH test_header AS (
    SELECT 
        HEADER_TRACKER_ID,
        FEED_NM,
        HEADER_FILE_NM,
        STREAM_NAME,
        HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING AS file_date,
        TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') AS current_date_str
    FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
    WHERE PROCESSING_STATUS = 'DISCOVERED'
      AND FEED_NM = 'BCFINSG'
    LIMIT 1
)
SELECT 
    '=== TEST HEADER VALIDATION SETUP ===' AS section,
    HEADER_TRACKER_ID,
    HEADER_FILE_NM,
    STREAM_NAME,
    file_date AS extracted_date,
    current_date_str AS expected_date,
    CASE 
        WHEN file_date = current_date_str THEN 'DATES_MATCH'
        WHEN file_date IS NULL THEN 'DATE_MISSING'
        ELSE 'DATE_MISMATCH'
    END AS validation_prediction,
    'dbt run-operation validate_header --args ''{"stream_name": "BCFINSG_PLAN_BALN_SEGM_LOAD", "batch_name": "' || STREAM_NAME || '", "header_date_field": "BCF_DT_CURR_PROC"}''' AS test_command
FROM test_header;

-- ============================================================================
-- 6. VALIDATION READINESS CHECK
-- ============================================================================
SELECT 
    '=== VALIDATION READINESS SUMMARY ===' AS section,
    COUNT(*) AS total_discovered_files,
    COUNT(CASE WHEN STREAM_NAME IS NOT NULL THEN 1 END) AS files_with_batch_name,
    COUNT(CASE WHEN HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC'] IS NOT NULL THEN 1 END) AS files_with_date,
    COUNT(CASE WHEN HEADER_METADATA['file_metadata']['header_records'][0]['BCF_ACCOUNT_NO_0']::STRING = '0000000000000000' THEN 1 END) AS files_with_valid_control_id,
    LISTAGG(DISTINCT STREAM_NAME, ', ') AS available_batch_names
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
WHERE PROCESSING_STATUS = 'DISCOVERED'
  AND FEED_NM = 'BCFINSG';

-- ============================================================================
-- 7. BUSINESS DATE VALIDATION CHECK
-- ============================================================================
WITH business_date_check AS (
    SELECT 
        BUS_DT,
        TO_CHAR(BUS_DT, 'YYYYMMDD') AS bus_dt_str,
        STREAM_STATUS
    FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM_BUS_DT
    WHERE STRM_NAME = 'BCFINSG_PLAN_BALN_SEGM_LOAD'
      AND STREAM_STATUS = 'ACTIVE'
    ORDER BY BUS_DT DESC
    LIMIT 1
),
header_dates AS (
    SELECT 
        HEADER_METADATA['file_metadata']['header_records'][0]['BCF_DT_CURR_PROC']::STRING AS header_date,
        COUNT(*) AS file_count
    FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_IGSN_FRMW_HDR_CTRL
    WHERE PROCESSING_STATUS = 'DISCOVERED'
      AND FEED_NM = 'BCFINSG'
    GROUP BY header_date
)
SELECT 
    '=== DATE MATCHING ANALYSIS ===' AS section,
    b.bus_dt_str AS active_business_date,
    h.header_date AS file_header_date,
    h.file_count AS files_with_this_date,
    CASE 
        WHEN b.bus_dt_str = h.header_date THEN 'MATCH - WILL VALIDATE'
        WHEN h.header_date IS NULL THEN 'ERROR - MISSING DATE'
        ELSE 'MISMATCH - WILL FAIL'
    END AS validation_prediction
FROM business_date_check b
CROSS JOIN header_dates h
ORDER BY h.file_count DESC;

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================
SELECT '✅ Header validation test setup completed!' AS test_status,
       'Ready to execute validate_header macro' AS next_steps,
       'Use the test_command from above results' AS instruction;