CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_ISRT
(
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
  )
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
-- =============================================================================
-- Procedure: ACCT_BALN_BKDT_ISRT
-- Generated: 2025-08-22 16:03:10
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
-- .RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
-- .IF ERRORCODE <> 0 THEN .GOTO EXITERR
-- 
-- .SET QUIET OFF
-- .SET ECHOREQ ON
-- .SET FORMAT OFF
-- .SET WIDTH 120
-- ----------------------------------------------------------------------
-- -- $LastChangedBy: vajapes $
-- -- $LastChangedDate: 2012-02-28 09:09:17 +1100 (Tue, 28 Feb 2012) $
-- =============================================================================

  -- Variable declarations
  LET error_code INTEGER DEFAULT 0;
  LET sql_state STRING DEFAULT '00000';
  LET error_message STRING DEFAULT '';
  LET row_count INTEGER DEFAULT 0;
  LET current_step STRING DEFAULT 'INIT';

  -- Label tracking variables
  LET goto_exiterr BOOLEAN DEFAULT FALSE;

  -- Exception handling setup
  DECLARE
    general_exception EXCEPTION (-20001, 'General procedure error');
  BEGIN
    -- Main procedure logic starts here

    -- Line 1: .RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
    -- RUN statement: Execute accumulated SQL
    -- (SQL execution handled inline in Snowflake)

    -- Line 2: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- SQL Block (lines 23-57)
    current_step := 'EXECUTING_SQL';
    BEGIN
      INSERT INTO STAR_CAD_PROD_DATA.ACCT_BALN_BKDT
      (
      ACCT_I,                        
      BALN_TYPE_C,                   
      CALC_FUNC_C,                   
      TIME_PERD_C,                   
      BALN_A,                        
      CALC_F,                        
      SRCE_SYST_C,                   
      ORIG_SRCE_SYST_C,              
      LOAD_D,                        
      BKDT_EFFT_D,                   
      BKDT_EXPY_D,                  
      PROS_KEY_EFFT_I,               
      PROS_KEY_EXPY_I,               
      BKDT_PROS_KEY_I
      )
      SELECT 
      ACCT_I,                        
      BALN_TYPE_C,                   
      CALC_FUNC_C,                   
      TIME_PERD_C,                   
      BALN_A,                        
      CALC_F,                        
      SRCE_SYST_C,                   
      ORIG_SRCE_SYST_C,              
      LOAD_D,                        
      BKDT_EFFT_D,                   
      BKDT_EXPY_D,                  
      PROS_KEY_EFFT_I,               
      PROS_KEY_EXPY_I,               
      BKDT_PROS_KEY_I
      FROM
      PDDSTG.ACCT_BALN_BKDT_STG2;
      ;
      -- Get row count and check for errors
      row_count := SQLROWCOUNT;
      IF (SQLCODE <> 0) THEN
        error_code := SQLCODE;
        error_message := SQLERRM;
        GOTO error_exit;
      END IF;
    EXCEPTION
      WHEN OTHER THEN
        error_code := SQLCODE;
        error_message := SQLERRM;
        GOTO error_exit;
    END;

    -- Line 58: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 61: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Line 64: .LABEL EXITERR
    exiterr:

    -- Line 66: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Success path
    RETURN 'SUCCESS: ' || current_step || ' completed. Rows processed: ' || row_count;

    -- Error handling
    error_exit:
      -- Log error to error table if available
      INSERT INTO IDENTIFIER(:ERROR_TABLE) (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP)
      VALUES (:PROCESS_KEY, error_code, error_message, CURRENT_TIMESTAMP());
      
      RETURN 'ERROR: ' || error_message || ' (Code: ' || error_code || ')';

  EXCEPTION
    WHEN OTHER THEN
      RETURN 'FATAL ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;