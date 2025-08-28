CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_AVG_CALL_PROC
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
-- Procedure: ACCT_BALN_BKDT_AVG_CALL_PROC
-- Generated: 2025-08-22 16:15:53
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
-- .RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
-- .IF ERRORCODE <> 0 THEN .GOTO EXITERR
-- 
-- 
-- .SET QUIET OFF
-- .SET ECHOREQ ON
-- .SET FORMAT OFF
-- .SET WIDTH 120
-- ----------------------------------------------------------------------
-- -- $LastChangedBy: 
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

    -- Line 24: CALL %%CAD_PROD_MACRO%%.SP_CALC_AVRG_DAY_BALN_BKDT (
CAST(ADD_MONTHS(CURRENT_DATE, -1) AS DATE FORMAT 'YYYYMMDD'));
    CALL %%CAD_PROD_MACRO%%.SP_CALC_AVRG_DAY_BALN_BKDT (
CAST(ADD_MONTHS(CURRENT_DATE, -1) AS DATE FORMAT 'YYYYMMDD'));;
    -- Check for errors after stored procedure call
    IF (SQLCODE <> 0) THEN
      error_code := SQLCODE;
      error_message := SQLERRM;
      GOTO error_exit;
    END IF;

    -- Line 27: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 30: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Line 32: .LABEL EXITERR
    exiterr:

    -- Line 34: .LOGOFF
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