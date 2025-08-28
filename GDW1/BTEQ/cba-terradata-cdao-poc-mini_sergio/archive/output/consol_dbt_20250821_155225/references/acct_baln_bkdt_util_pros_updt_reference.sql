CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_UTIL_PROS_UPDT
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
-- Procedure: ACCT_BALN_BKDT_UTIL_PROS_UPDT
-- Generated: 2025-08-21 15:51:05
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
--  .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
-- .IF ERRORCODE <> 0 THEN .GOTO EXITERR
-- 
-- .SET QUIET OFF
-- .SET ECHOREQ ON
-- .SET FORMAT OFF
-- .SET WIDTH 120
-- ----------------------------------------------------------------------
-- -- $LastChangedBy: vajapes $
-- -- $LastChangedDate: 2012-02-28 09:09:54 +1100 (Tue, 28 Feb 2012) $
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

    -- Line 1:  .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
    -- RUN statement: Execute accumulated SQL
    -- (SQL execution handled inline in Snowflake)

    -- Line 2: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- SQL Block (lines 22-38)
    current_step := 'EXECUTING_SQL';
    BEGIN
      UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
      FROM
      (SELECT COUNT(*) FROM 
      PDDSTG.ACCT_BALN_BKDT_STG2)A(INS_CNT),
      (SELECT COUNT(*) FROM 
      PDDSTG.ACCT_BALN_BKDT_STG1)B(DEL_CNT)
      SET  
              COMT_F = 'Y',
      	SUCC_F = 'Y',
      	COMT_S = CURRENT_TIMESTAMP(0),
      	SYST_INS_Q = A.INS_CNT,
      	SYST_DEL_Q = B.DEL_CNT
      WHERE 
      CONV_M='CAD_X01_ACCT_BALN_BKDT'
      AND PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM PVTECH.UTIL_PROS_ISAC 
      WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT');
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

    -- Line 39: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 42: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Line 45: .LABEL EXITERR
    exiterr:

    -- Line 47: .LOGOFF
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