CREATE OR REPLACE PROCEDURE SP_GET_BTCH_KEY
(
    OUTPUT_PATH STRING DEFAULT '/tmp/output',
    OUTPUT_PATH STRING DEFAULT '/tmp/output',
    OUTPUT_PATH STRING DEFAULT '/tmp/output',
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
-- Procedure: SP_GET_BTCH_KEY
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
-- --
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

    -- Line 23: .EXPORT RESET
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 26: .OS rm /cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt
    -- OS Command: Not supported in Snowflake stored procedures
    -- Consider using external functions or stages for file operations

    -- Line 28: .EXPORT DATA FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 32: CALL STAR_CAD_PROD_MACRO.SP_GET_BTCH_KEY(     
  'DERIVED_ACCOUNT_PARTY'
  ,CAST(%%INDATE%% as date format'yyyymmdd')
    CALL STAR_CAD_PROD_MACRO.SP_GET_BTCH_KEY(     
  'DERIVED_ACCOUNT_PARTY'
  ,CAST(%%INDATE%% as date format'yyyymmdd');
    -- Check for errors after stored procedure call
    IF (SQLCODE <> 0) THEN
      error_code := SQLCODE;
      error_message := SQLERRM;
      GOTO error_exit;
    END IF;

    -- Line 41: .EXPORT RESET
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- Line 44: .OS rm /cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_DATE.txt
    -- OS Command: Not supported in Snowflake stored procedures
    -- Consider using external functions or stages for file operations

    -- Line 46: .EXPORT DATA FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_DATE.txt
    -- File I/O: Consider using Snowflake stages and COPY commands

    -- SQL Block (lines 47-52)
    current_step := 'EXECUTING_SQL';
    BEGIN
      Select
        (CAST(%%INDATE%% as date format'yyyymmdd'))(CHAR(8))
      ;
      .SET FORMAT OFF
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

    -- Line 53: .IF ERRORCODE <> 0    THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 56: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Line 58: .LABEL EXITERR
    exiterr:

    -- Line 60: .LOGOFF
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