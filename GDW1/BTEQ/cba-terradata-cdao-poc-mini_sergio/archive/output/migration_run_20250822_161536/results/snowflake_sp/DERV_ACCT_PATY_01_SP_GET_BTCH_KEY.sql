CREATE OR REPLACE PROCEDURE DERV_ACCT_PATY_01_SP_GET_BTCH_KEY
(
    OUTPUT_PATH STRING DEFAULT '/tmp/output',
    OUTPUT_PATH STRING DEFAULT '/tmp/output',
    INPUT_PATH STRING DEFAULT '/tmp/input',
    OUTPUT_PATH STRING DEFAULT '/tmp/output',
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
-- Procedure: DERV_ACCT_PATY_01_SP_GET_BTCH_KEY
-- Generated: 2025-08-22 16:15:53
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
-- .RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
-- .IF ERRORCODE <> 0    THEN .GOTO EXITERR
-- 
-- .SET QUIET OFF
-- .SET ECHOREQ ON
-- .SET FORMAT OFF
-- .SET WIDTH 120
-- 
-- ------------------------------------------------------------------------------
-- -- Object Name             :  DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql
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

    -- Line 2: .IF ERRORCODE <> 0    THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 28: .EXPORT RESET
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 31: .OS rm /cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt
    -- OS Command: Not supported in Snowflake stored procedures
    -- Consider using external functions or stages for file operations

    -- Line 33: .EXPORT DATA FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 40: .IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_extr_date.txt
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 43: CALL STAR_CAD_PROD_MACRO.SP_GET_BTCH_KEY(     
  'DERIVED_ACCOUNT_PARTY'
  ,CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8))
    CALL STAR_CAD_PROD_MACRO.SP_GET_BTCH_KEY(     
  'DERIVED_ACCOUNT_PARTY'
  ,CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8));
    -- Check for errors after stored procedure call
    IF (SQLCODE <> 0) THEN
      error_code := SQLCODE;
      error_message := SQLERRM;
      GOTO error_exit;
    END IF;

    -- Line 52: .EXPORT RESET
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 54: .IF ERRORCODE <> 0    THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 57: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Line 59: .LABEL EXITERR
    exiterr:

    -- Line 61: .LOGOFF
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