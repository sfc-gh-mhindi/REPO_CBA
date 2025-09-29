-- ============================================================================
-- Transformation Error Detail Table (Modern Design)
-- ============================================================================
-- Table: XFM_ERR_DTL
-- Purpose: Capture transformation errors with one row per source record
-- Design: JSON-based error capture with flattening view
-- ============================================================================

-- ============================================================================
-- Main Error Table: XFM_ERR_DTL
-- ============================================================================

CREATE OR REPLACE TABLE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.XFM_ERR_DTL (
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
    PRCS_INST_ID                 VARCHAR(100),               -- Batch identifier
    REC_INS_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    INS_USR_NM              VARCHAR(100) DEFAULT CURRENT_USER()
);

-- ============================================================================
-- Error Flattening View: VW_XFM_ERR_DTL_FLAT
-- ============================================================================

CREATE OR REPLACE VIEW PSUND_MIGR_DCF.P_D_DCF_001_STD_0.VW_XFM_ERR_DTL_FLAT AS
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
    
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.XFM_ERR_DTL e,
     LATERAL FLATTEN(input => e.ERR_DTLS_JSON) f;

-- ============================================================================
-- Example JSON Structure for ERR_DTLS_JSON
-- ============================================================================

/*
ERR_DTLS_JSON example:
[
  {
    "column_name": "BCF_DT_FIRST_TRANS",
    "error_type": "DATE_CONVERSION_ERROR",
    "error_message": "Invalid date: cannot convert EBCDIC integer to valid date",
    "original_value": "20241399",
    "attempted_value": null,
    "validation_rule": "fn_is_valid_dt"
  },
  {
    "column_name": "BCF_DT_LAST_PAYMENT", 
    "error_type": "DATE_CONVERSION_ERROR",
    "error_message": "Invalid date: EBCDIC integer too short",
    "original_value": "999",
    "attempted_value": null,
    "validation_rule": "fn_is_valid_dt"
  }
]
*/

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT SELECT, INSERT, UPDATE ON PSUND_MIGR_DCF.P_D_DCF_001_STD_0.XFM_ERR_DTL TO ROLE SYSADMIN;
GRANT SELECT ON PSUND_MIGR_DCF.P_D_DCF_001_STD_0.VW_XFM_ERR_DTL_FLAT TO ROLE SYSADMIN;

-- ============================================================================
-- Sample error analysis queries
-- ============================================================================

-- Daily error summary
/*
SELECT 
    PRCS_DT,
    COUNT(DISTINCT SRCE_KEY_NM) as failed_records,
    COUNT(*) as total_error_records,
    COUNT(DISTINCT SRCE_FILE_NM) as affected_files
FROM XFM_ERR_DTL 
WHERE PRCS_DT >= CURRENT_DATE - 7
GROUP BY PRCS_DT
ORDER BY PRCS_DT DESC;
*/

-- Error by column (using flattened view)
/*
SELECT 
    ERR_COLM_NM,
    ERR_TYPE_NM,
    COUNT(*) as error_count,
    COUNT(DISTINCT SRCE_KEY_NM) as affected_records
FROM VW_XFM_ERR_DTL_FLAT
WHERE PRCS_DT >= CURRENT_DATE - 7
GROUP BY ERR_COLM_NM, ERR_TYPE_NM
ORDER BY error_count DESC;
*/