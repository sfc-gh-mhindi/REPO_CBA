-- =====================================================================
-- Account Balance Monthly Average Calculation Procedure (Converted from BTEQ)
-- Original BTEQ: ACCT_BALN_BKDT_AVG_CALL_PROC.sql
-- =====================================================================

-- Version History:
-- Ver  Date       Modified By            Description
-- ---- ---------- ---------------------- -----------------------------------
-- 1.0  2011-10-05 Suresh Vajapeyajula    Initial Version
-- 2.0  2025-01-XX Cursor AI              Converted to Snowflake

-- Description: Process to calculate the Monthly Average balance.  
-- This is sourcing from ACCT BALN BKDT.

CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_AVG_CALL_PROC(
    DATABASE_NAME STRING DEFAULT 'GDW_DATABASE',
    SCHEMA_NAME STRING DEFAULT 'CAD_PROD_MACRO'
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Calculate Monthly Average balance from ACCT BALN BKDT'
AS
$$
DECLARE
    result_msg STRING;
    error_msg STRING;
    prev_month_date STRING;
BEGIN
    -- Set session context (equivalent to BTEQ login)
    EXECUTE IMMEDIATE 'USE DATABASE ' || DATABASE_NAME;
    EXECUTE IMMEDIATE 'USE SCHEMA ' || SCHEMA_NAME;
    
    -- Calculate previous month in YYYYMMDD format
    -- Snowflake equivalent of CAST(ADD_MONTHS(CURRENT_DATE, -1) AS DATE FORMAT 'YYYYMMDD')
    SELECT TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYYMMDD')
    INTO prev_month_date;
    
    -- Call the stored procedure (equivalent to BTEQ CALL statement)
    -- Note: Procedure name will need to be adjusted to actual Snowflake procedure name
    CALL SP_CALC_AVRG_DAY_BALN_BKDT(prev_month_date);
    
    result_msg := 'Successfully calculated monthly average balance for period: ' || prev_month_date;
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        error_msg := 'Error in ACCT_BALN_BKDT_AVG_CALL_PROC: ' || SQLERRM;
        RETURN error_msg;
END;
$$;

-- Alternative: Simple SQL call without wrapper procedure
-- CALL SP_CALC_AVRG_DAY_BALN_BKDT(TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYYMMDD'));

-- Usage example:
-- CALL ACCT_BALN_BKDT_AVG_CALL_PROC('YOUR_DATABASE', 'YOUR_SCHEMA'); 