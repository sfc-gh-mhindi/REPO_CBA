-- ============================================================================
-- GDW1 STREAM CONFIGURATION ADDITION
-- ============================================================================
-- Purpose: Add GDW1 BCFINSG stream to existing GDW2 DCF infrastructure
-- Reuses: GDW2 DCF control tables and framework
-- Stream: BCFINSG (Stream ID: 1490)

-- NOTE: This script assumes GDW2 DCF tables already exist
-- Run GDW2 dcf_schema_init.sql first if DCF tables don't exist

-- ============================================================================
-- CLEANUP EXISTING ENTRIES (IDEMPOTENT SETUP)
-- ============================================================================

-- Delete existing CSEL4_CPL_BUS_APP processes first (due to FK constraints)
DELETE FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC 
WHERE STRM_ID IN (
    SELECT STRM_ID FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM 
    WHERE STRM_NAME = 'CSEL4_CPL_BUS_APP'
);

-- Delete existing BCFINSG processes first (due to FK constraints)
DELETE FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC 
WHERE STRM_ID IN (
    SELECT STRM_ID FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM 
    WHERE STRM_NAME = 'BCFINSG'
);

-- Delete existing streams
DELETE FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM 
WHERE STRM_NAME IN ('BCFINSG', 'CSEL4_CPL_BUS_APP');

-- Key column configurations not used - skipping cleanup

-- ============================================================================
-- INSERT GDW1 STREAM CONFIGURATION
-- ============================================================================

-- BCFINSG Plan Balance Segment Master Stream (1490)
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM (
    STRM_ID,
    STRM_NAME,
    STRM_DESC,
    STRM_TYPE,
    CYCLE_FREQ_CODE,
    MAX_CYCLES_PER_DAY,
    ALLOW_MULTIPLE_CYCLES,
    BUSINESS_DOMAIN,
    TARGET_SCHEMA,
    TARGET_TABLE,
    DBT_TAG,
    CTL_ID,
    STRM_STATUS
) VALUES (
    1490,
    'BCFINSG',
    'BCFINSG Plan Balance Segment Master Load - GDW1 Migration',
    'DAILY',
    1,  -- 1 = Daily
    1,  -- Max 1 cycle per day
    FALSE,
    'FINANCE',
    'PDSRCCS',
    'PLAN_BALN_SEGM_MSTR',
    'tag:stream_bcfinsg',
    149,  -- Control ID for BCFINSG stream
    'ACTIVE'
);

-- ============================================================================
-- INSERT PROCESS CONFIGURATIONS FOR BCFINSG
-- ============================================================================

-- Process 1: Error Validation Process
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
    PROC_ID,
    STRM_ID,
    PROC_NAME,
    PROC_DESC,
    PROC_TYPE,
    PROC_ORDER,
    PROC_STATUS,
    DBT_MODEL_NAME,
    DBT_TAG
) VALUES (
    14901,
    1490,
    'BCFINSG_ERROR_VALIDATION',
    'BCFINSG Error Detection and Business Validation Process',
    'VALIDATION',
    1,
    'ACTIVE',
    'int_bcfinsg_error_check',
    'tag:process_int_bcfinsg_error_check'
);

-- Process 2: Fact Table Load Process
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
    PROC_ID,
    STRM_ID,
    PROC_NAME,
    PROC_DESC,
    PROC_TYPE,
    PROC_ORDER,
    PROC_STATUS,
    DBT_MODEL_NAME,
    DBT_TAG
) VALUES (
    14902,
    1490,
    'BCFINSG_FACT_LOAD',
    'BCFINSG Plan Balance Segment Master Fact Table Load Process',
    'LOAD',
    2,
    'ACTIVE',
    'fct_plan_baln_segm_mstr',
    'tag:process_fct_plan_baln_segm_mstr'
);

-- ============================================================================
-- KEY COLUMN CONFIGURATIONS - NOT ACTIVELY USED
-- ============================================================================
-- Note: Key column configurations removed as they are not actively used in current implementation

-- ============================================================================
-- VERIFICATION QUERIES - Check GDW1 stream and processes were added successfully
-- ============================================================================

-- Show the BCFINSG stream configuration
SELECT * FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM WHERE STRM_NAME = 'BCFINSG';

-- Show process configurations for BCFINSG
SELECT 
    p.PROC_ID,
    p.PROC_NAME,
    p.PROC_DESC,
    p.PROC_TYPE,
    p.PROC_ORDER,
    p.DBT_MODEL_NAME,
    p.DBT_TAG,
    s.STRM_NAME
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC p
JOIN PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM s ON p.STRM_ID = s.STRM_ID
WHERE s.STRM_NAME = 'BCFINSG'
ORDER BY p.PROC_ORDER;

-- Key column configurations not used - skipping verification

-- Count all active streams (should include GDW1 + GDW2)
SELECT 
    BUSINESS_DOMAIN,
    COUNT(*) as stream_count,
    LISTAGG(STRM_NAME, ', ') as stream_names
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM 
WHERE STRM_STATUS = 'ACTIVE'
GROUP BY BUSINESS_DOMAIN;

-- ============================================================================
-- INSERT CSEL4_CPL_BUS_APP STREAM CONFIGURATION
-- ============================================================================

-- CSEL4_CPL_BUS_APP Stream (1491)
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM (
    STRM_ID,
    STRM_NAME,
    STRM_DESC,
    STRM_TYPE,
    CYCLE_FREQ_CODE,
    MAX_CYCLES_PER_DAY,
    ALLOW_MULTIPLE_CYCLES,
    BUSINESS_DOMAIN,
    TARGET_SCHEMA,
    TARGET_TABLE,
    DBT_TAG,
    CTL_ID,
    STRM_STATUS
) VALUES (
    1491,
    'CSEL4_CPL_BUS_APP',
    'CSEL4 CSE CPL Business Application Data Load - GDW1 Migration',
    'DAILY',
    1,  -- 1 = Daily
    1,  -- Max 1 cycle per day
    FALSE,
    'APPLICATIONS',
    'STARCADPRODDATA',
    'APPT_DEPT,APPT_PDCT,DEPT_APPT',
    'tag:stream_csel4',
    150,  -- Control ID for CSEL4 stream
    'ACTIVE'
);

-- ============================================================================
-- INSERT PROCESS CONFIGURATIONS FOR CSEL4_CPL_BUS_APP
-- ============================================================================

-- Process 1: Data Validation Process
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
    PROC_ID,
    STRM_ID,
    PROC_NAME,
    PROC_DESC,
    PROC_TYPE,
    PROC_ORDER,
    PROC_STATUS,
    DBT_MODEL_NAME,
    DBT_TAG
) VALUES (
    15001,
    1491,
    'CSEL4_CSE_CPL_BUS_APP_VALIDATE',
    'CSEL4 CSE CPL Business Application Data Validation Process',
    'VALIDATION',
    1,
    'ACTIVE',
    'int_validate_cse_cpl_bus_app',
    'tag:process_csel4_cse_cpl_bus_app_validate'
);

-- Process 2: APPT_DEPT Fact Table Load Process
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
    PROC_ID,
    STRM_ID,
    PROC_NAME,
    PROC_DESC,
    PROC_TYPE,
    PROC_ORDER,
    PROC_STATUS,
    DBT_MODEL_NAME,
    DBT_TAG
) VALUES (
    15002,
    1491,
    'CSEL4_APPT_DEPT_TRANSFORM',
    'CSEL4 Application Department Relationship Fact Load Process',
    'LOAD',
    2,
    'ACTIVE',
    'appt_dept',
    'tag:process_csel4_appt_dept_transform'
);

-- Process 3: APPT_PDCT Fact Table Load Process
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
    PROC_ID,
    STRM_ID,
    PROC_NAME,
    PROC_DESC,
    PROC_TYPE,
    PROC_ORDER,
    PROC_STATUS,
    DBT_MODEL_NAME,
    DBT_TAG
) VALUES (
    15003,
    1491,
    'CSEL4_APPT_PDCT_TRANSFORM',
    'CSEL4 Application Product Relationship Fact Load Process',
    'LOAD',
    3,
    'ACTIVE',
    'appt_pdct',
    'tag:process_csel4_appt_pdct_transform'
);

-- Process 4: DEPT_APPT Fact Table Load Process
INSERT INTO PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
    PROC_ID,
    STRM_ID,
    PROC_NAME,
    PROC_DESC,
    PROC_TYPE,
    PROC_ORDER,
    PROC_STATUS,
    DBT_MODEL_NAME,
    DBT_TAG
) VALUES (
    15004,
    1491,
    'CSEL4_DEPT_APPT_TRANSFORM',
    'CSEL4 Department Application Relationship Fact Load Process',
    'LOAD',
    4,
    'ACTIVE',
    'dept_appt',
    'tag:process_csel4_dept_appt_transform'
);

-- ============================================================================
-- KEY COLUMN CONFIGURATIONS FOR CSEL4_CPL_BUS_APP - NOT ACTIVELY USED
-- ============================================================================
-- Note: Key column configurations removed as they are not actively used in current implementation

-- ============================================================================
-- VERIFICATION QUERIES - Check CSEL4 stream and processes were added successfully
-- ============================================================================

-- Show the CSEL4_CPL_BUS_APP stream configuration
SELECT * FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM WHERE STRM_NAME = 'CSEL4_CPL_BUS_APP';

-- Show process configurations for CSEL4_CPL_BUS_APP
SELECT 
    p.PROC_ID,
    p.PROC_NAME,
    p.PROC_DESC,
    p.PROC_TYPE,
    p.PROC_ORDER,
    p.DBT_MODEL_NAME,
    p.DBT_TAG,
    s.STRM_NAME
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC p
JOIN PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM s ON p.STRM_ID = s.STRM_ID
WHERE s.STRM_NAME = 'CSEL4_CPL_BUS_APP'
ORDER BY p.PROC_ORDER;

-- Key column configurations not used - skipping verification

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================
SELECT 'GDW1 BCFINSG and CSEL4_CPL_BUS_APP streams and process configurations added to DCF successfully!' AS status_message;