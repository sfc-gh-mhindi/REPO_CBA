-- ============================================================================
-- Utility Functions for Data Processing (Snowflake UDFs)
-- ============================================================================
-- File: util_funcs.sql
-- Purpose: Common utility functions for EBCDIC data processing and validation
-- 
-- Functions:
--   - fn_ebcdic_to_dt: Convert EBCDIC integer dates to DATE values
--   - fn_is_valid_dt: Validate EBCDIC integer dates
-- ============================================================================

-- ============================================================================
-- EBCDIC Date Conversion Function
-- ============================================================================
-- Purpose: Convert EBCDIC integer dates to proper DATE values
-- Usage: SELECT fn_ebcdic_to_dt(20241201) 
-- Returns: DATE or NULL (NULL for invalid dates)
-- Input: INTEGER (EBCDIC COMP-3 packed decimal converted to integer)

CREATE OR REPLACE FUNCTION PSUND_MIGR_DCF.P_D_DCF_001_STD_0.fn_ebcdic_to_dt(INPUT_DATE_INT NUMBER)
RETURNS DATE
LANGUAGE SQL
AS
$$
  CASE 
    -- DataStage null/empty patterns (EBCDIC packed decimal null values)
    WHEN INPUT_DATE_INT IS NULL OR INPUT_DATE_INT = 0 THEN NULL
    
    -- DataStage format validation for EBCDIC integer dates
    WHEN INPUT_DATE_INT < 10000000 OR INPUT_DATE_INT > 99999999 THEN NULL  -- Must be 8-digit YYYYMMDD
    
    -- Convert integer to string for date parsing (DataStage does this internally)
    WHEN TRY_TO_DATE(LPAD(INPUT_DATE_INT::VARCHAR, 8, '0'), 'YYYYMMDD') IS NULL THEN NULL
    
    -- Valid date - return converted date
    ELSE TRY_TO_DATE(LPAD(INPUT_DATE_INT::VARCHAR, 8, '0'), 'YYYYMMDD')
  END
$$;

-- ============================================================================
-- Date Validity Check Function
-- ============================================================================
-- Purpose: Check if EBCDIC integer represents a valid date
-- Usage: SELECT fn_is_valid_dt(20241201)
-- Returns: TRUE if date is valid, FALSE if invalid
-- Input: INTEGER (EBCDIC COMP-3 packed decimal converted to integer)

CREATE OR REPLACE FUNCTION PSUND_MIGR_DCF.P_D_DCF_001_STD_0.fn_is_valid_dt(INPUT_DATE_INT NUMBER)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
  CASE 
    -- Null/zero values are considered valid (null dates)
    WHEN INPUT_DATE_INT IS NULL OR INPUT_DATE_INT = 0 THEN TRUE
    
    -- Invalid range - not a valid date
    WHEN INPUT_DATE_INT < 10000000 OR INPUT_DATE_INT > 99999999 THEN FALSE  
    WHEN TRY_TO_DATE(LPAD(INPUT_DATE_INT::VARCHAR, 8, '0'), 'YYYYMMDD') IS NULL THEN FALSE
    
    -- Valid date
    ELSE TRUE
  END
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON FUNCTION PSUND_MIGR_DCF.P_D_DCF_001_STD_0.fn_ebcdic_to_dt(NUMBER) TO ROLE SYSADMIN;
GRANT USAGE ON FUNCTION PSUND_MIGR_DCF.P_D_DCF_001_STD_0.fn_is_valid_dt(NUMBER) TO ROLE SYSADMIN;

-- ============================================================================
-- Test the functions
-- ============================================================================
SELECT 
    'Testing EBCDIC Date UDFs with INTEGER dates' as test_name,
    fn_ebcdic_to_dt(20241201) as valid_date,              -- Valid YYYYMMDD integer
    fn_ebcdic_to_dt(0) as null_date,                      -- EBCDIC null (zero)
    fn_ebcdic_to_dt(999) as invalid_short_date,           -- Too short
    fn_ebcdic_to_dt(20241399) as invalid_date,            -- Invalid month
    fn_is_valid_dt(20241201) as valid_check,              -- Should be TRUE
    fn_is_valid_dt(999) as invalid_check,                 -- Should be FALSE
    fn_is_valid_dt(0) as null_check;                      -- Should be TRUE (null is valid)