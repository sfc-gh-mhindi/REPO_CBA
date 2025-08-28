-- ============================================================================
-- DCF PROCESS TABLES CREATION
-- ============================================================================
-- Purpose: Create DCF_T_PROC and DCF_T_PRCS_INST tables for process management
-- DCF_T_PROC: Process configurations for each stream (definitions)
-- DCF_T_PRCS_INST: Process instances for tracking actual executions

-- ============================================================================
-- DCF_T_PROC: Process Configuration Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC (
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
    
    -- Unique constraint on stream + process name
    CONSTRAINT UQ_DCF_T_PROC_STRM_NAME UNIQUE (STRM_ID, PROC_NAME)
);

-- ============================================================================
-- DCF_T_PRCS_INST: Process Instance Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PRCS_INST (
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
    -- CONSTRAINT FK_DCF_T_PRCS_INST_STRM FOREIGN KEY (STRM_ID) REFERENCES PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_STRM(STRM_ID),
    
    -- Unique constraint on stream + process + business date (prevent duplicate runs)
    CONSTRAINT UQ_DCF_T_PRCS_INST_UNIQUE_RUN UNIQUE (STRM_ID, PRCS_NAME, PRCS_BUS_DT)
);

-- ============================================================================
-- INSERT DEFAULT PROCESS TYPES
-- ============================================================================

-- Note: Process types are defined in the table structure
-- Common types: VALIDATION, LOAD, TRANSFORM, EXTRACT, CLEANSE

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Show the created table structures
DESCRIBE TABLE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC;
DESCRIBE TABLE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PRCS_INST;

-- Show any existing data (should be empty initially)
SELECT * FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PROC;
SELECT * FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.DCF_T_PRCS_INST;

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================
SELECT 'DCF_T_PROC and DCF_T_PRCS_INST tables created successfully!' AS status_message; 