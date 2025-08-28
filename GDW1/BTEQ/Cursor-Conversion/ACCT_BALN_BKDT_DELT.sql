-- =====================================================================
-- Account Balance Delete Procedure (Converted from BTEQ)
-- Original BTEQ: ACCT_BALN_BKDT_DELT.sql
-- =====================================================================

-- Version History:
-- Ver  Date       Modified By            Description
-- ---- ---------- ---------------------- -----------------------------------
-- 1.0  2011-10-05 Suresh Vajapeyajula    Initial Version
-- 2.0  2025-01-XX Cursor AI              Converted to Snowflake

-- Description: Delete Accounts from ACCT BALN so that the modified data 
--              can be inserted in next step

CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_DELETE_PROC(
    PROD_DATABASE STRING DEFAULT 'CAD_PROD_DATA',
    STAGING_DATABASE STRING DEFAULT 'DDSTG',
    SCHEMA_NAME STRING DEFAULT 'PUBLIC'
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Delete modified records from ACCT_BALN_BKDT table before reinsertion'
AS
$$
DECLARE
    rows_deleted INTEGER;
    result_msg STRING;
    error_msg STRING;
BEGIN
    -- Set session context
    EXECUTE IMMEDIATE 'USE SCHEMA ' || SCHEMA_NAME;
    
    -- Delete records that will be reinserted from STG2
    -- Deleting the records from the ACCT_BALN_BKDT table. These records are modified 
    -- as a result of applying adjustment and so will be reinserted from STG2 at next step
    DELETE FROM IDENTIFIER(PROD_DATABASE || '.ACCT_BALN_BKDT') BAL
    USING IDENTIFIER(STAGING_DATABASE || '.ACCT_BALN_BKDT_STG1') STG1
    WHERE STG1.ACCT_I = BAL.ACCT_I    
      AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C                    
      AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C                   
      AND STG1.TIME_PERD_C = BAL.TIME_PERD_C                   
      AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D                        
      AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D                        
      AND STG1.BALN_A = BAL.BALN_A                        
      AND STG1.CALC_F = BAL.CALC_F                        
      AND COALESCE(STG1.PROS_KEY_EFFT_I, 0) = COALESCE(BAL.PROS_KEY_EFFT_I, 0)
      AND COALESCE(STG1.PROS_KEY_EXPY_I, 0) = COALESCE(BAL.PROS_KEY_EXPY_I, 0);
    
    -- Get count of deleted rows
    rows_deleted := SQLROWCOUNT;
    
    result_msg := 'Successfully deleted ' || rows_deleted || ' records from ACCT_BALN_BKDT';
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        error_msg := 'Error in ACCT_BALN_BKDT_DELETE_PROC: ' || SQLERRM;
        RETURN error_msg;
END;
$$;

-- Alternative: Direct SQL execution (without stored procedure wrapper)
/*
DELETE FROM <PROD_DATABASE>.ACCT_BALN_BKDT BAL
USING <STAGING_DATABASE>.ACCT_BALN_BKDT_STG1 STG1
WHERE STG1.ACCT_I = BAL.ACCT_I    
  AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C                    
  AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C                   
  AND STG1.TIME_PERD_C = BAL.TIME_PERD_C                   
  AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D                        
  AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D                        
  AND STG1.BALN_A = BAL.BALN_A                        
  AND STG1.CALC_F = BAL.CALC_F                        
  AND COALESCE(STG1.PROS_KEY_EFFT_I, 0) = COALESCE(BAL.PROS_KEY_EFFT_I, 0)
  AND COALESCE(STG1.PROS_KEY_EXPY_I, 0) = COALESCE(BAL.PROS_KEY_EXPY_I, 0);
*/

-- Usage example:
-- CALL ACCT_BALN_BKDT_DELETE_PROC('CAD_PROD_DATA', 'DDSTG', 'PUBLIC'); 