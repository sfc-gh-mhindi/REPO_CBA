CREATE OR REPLACE PROCEDURE DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808
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
-- Procedure: DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808
-- Generated: 2025-08-22 16:15:53
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
-- .RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
-- 
-- .IF ERRORLEVEL <> 0 THEN .GOTO EXITERR
-- 
-- .SET QUIET OFF
-- .SET ECHOREQ ON
-- .SET FORMAT OFF
-- .SET WIDTH 120
-- 
-- ------------------------------------------------------------------------------
-- =============================================================================

  -- Variable declarations
  LET error_code INTEGER DEFAULT 0;
  LET sql_state STRING DEFAULT '00000';
  LET error_message STRING DEFAULT '';
  LET row_count INTEGER DEFAULT 0;
  LET current_step STRING DEFAULT 'INIT';

  -- Label tracking variables
  LET goto_next1 BOOLEAN DEFAULT FALSE;
  LET goto_next2 BOOLEAN DEFAULT FALSE;
  LET goto_next3 BOOLEAN DEFAULT FALSE;
  LET goto_next4 BOOLEAN DEFAULT FALSE;
  LET goto_next5 BOOLEAN DEFAULT FALSE;
  LET goto_next6 BOOLEAN DEFAULT FALSE;
  LET goto_next7 BOOLEAN DEFAULT FALSE;
  LET goto_next8 BOOLEAN DEFAULT FALSE;
  LET goto_next9 BOOLEAN DEFAULT FALSE;
  LET goto_next10 BOOLEAN DEFAULT FALSE;
  LET goto_next11 BOOLEAN DEFAULT FALSE;
  LET goto_next12 BOOLEAN DEFAULT FALSE;
  LET goto_next13 BOOLEAN DEFAULT FALSE;
  LET goto_next14 BOOLEAN DEFAULT FALSE;
  LET goto_next15 BOOLEAN DEFAULT FALSE;
  LET goto_next16 BOOLEAN DEFAULT FALSE;
  LET goto_next17 BOOLEAN DEFAULT FALSE;
  LET goto_next18 BOOLEAN DEFAULT FALSE;
  LET goto_next19 BOOLEAN DEFAULT FALSE;
  LET goto_next20 BOOLEAN DEFAULT FALSE;
  LET goto_exitok BOOLEAN DEFAULT FALSE;
  LET goto_exiterr BOOLEAN DEFAULT FALSE;

  -- Exception handling setup
  DECLARE
    general_exception EXCEPTION (-20001, 'General procedure error');
  BEGIN
    -- Main procedure logic starts here

    -- Line 1: .RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
    -- RUN statement: Execute accumulated SQL
    -- (SQL execution handled inline in Snowflake)

    -- Line 37: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 46: .IF ERRORCODE = 3807 THEN .GOTO NEXT1
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 47: .IF ERRORCODE = 0    THEN .GOTO NEXT1
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 50: .LABEL NEXT1
    next1:

    -- Line 54: .IF ERRORCODE = 3807 THEN .GOTO NEXT2
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 55: .IF ERRORCODE = 0    THEN .GOTO NEXT2
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 58: .LABEL NEXT2
    next2:

    -- Line 64: .IF ERRORCODE = 3807 THEN .GOTO NEXT3
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 65: .IF ERRORCODE = 0    THEN .GOTO NEXT3
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 68: .LABEL NEXT3
    next3:

    -- Line 71: .IF ERRORCODE = 3807 THEN .GOTO NEXT4
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 72: .IF ERRORCODE = 0    THEN .GOTO NEXT4
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 75: .LABEL NEXT4
    next4:

    -- Line 78: .IF ERRORCODE = 3807 THEN .GOTO NEXT5
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 79: .IF ERRORCODE = 0    THEN .GOTO NEXT5
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 82: .LABEL NEXT5
    next5:

    -- Line 85: .IF ERRORCODE = 3807 THEN .GOTO NEXT6
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 86: .IF ERRORCODE = 0    THEN .GOTO NEXT6
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 89: .LABEL NEXT6
    next6:

    -- Line 93: .IF ERRORCODE = 3807 THEN .GOTO NEXT7
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 94: .IF ERRORCODE = 0    THEN .GOTO NEXT7
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 97: .LABEL NEXT7
    next7:

    -- Line 102: .IF ERRORCODE = 3807 THEN .GOTO NEXT8
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 103: .IF ERRORCODE = 0    THEN .GOTO NEXT8
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 106: .LABEL NEXT8
    next8:

    -- Line 109: .IF ERRORCODE = 3807 THEN .GOTO NEXT9
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 110: .IF ERRORCODE = 0    THEN .GOTO NEXT9
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 113: .LABEL NEXT9
    next9:

    -- Line 116: .IF ERRORCODE = 3807 THEN .GOTO NEXT10
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 117: .IF ERRORCODE = 0    THEN .GOTO NEXT10
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 120: .LABEL NEXT10
    next10:

    -- Line 124: .IF ERRORCODE = 3807 THEN .GOTO NEXT11
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 125: .IF ERRORCODE = 0    THEN .GOTO NEXT11
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 128: .LABEL NEXT11
    next11:

    -- Line 132: .IF ERRORCODE = 3807 THEN .GOTO NEXT12
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 133: .IF ERRORCODE = 0    THEN .GOTO NEXT12
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 136: .LABEL NEXT12
    next12:

    -- Line 142: .IF ERRORCODE = 3807 THEN .GOTO NEXT13
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 143: .IF ERRORCODE = 0    THEN .GOTO NEXT13
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 146: .LABEL NEXT13
    next13:

    -- Line 149: .IF ERRORCODE = 3807 THEN .GOTO NEXT14
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 150: .IF ERRORCODE = 0    THEN .GOTO NEXT14
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 153: .LABEL NEXT14
    next14:

    -- Line 156: .IF ERRORCODE = 3807 THEN .GOTO NEXT15
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 157: .IF ERRORCODE = 0    THEN .GOTO NEXT15
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 160: .LABEL NEXT15
    next15:

    -- Line 163: .IF ERRORCODE = 3807 THEN .GOTO NEXT16
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 164: .IF ERRORCODE = 0    THEN .GOTO NEXT16
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 167: .LABEL NEXT16
    next16:

    -- Line 172: .IF ERRORCODE = 3807 THEN .GOTO NEXT17
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 173: .IF ERRORCODE = 0    THEN .GOTO NEXT17
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 176: .LABEL NEXT17
    next17:

    -- Line 180: .IF ERRORCODE = 3807 THEN .GOTO NEXT18
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 181: .IF ERRORCODE = 0    THEN .GOTO NEXT18
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 184: .LABEL NEXT18
    next18:

    -- Line 187: .IF ERRORCODE = 3807 THEN .GOTO NEXT19
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 188: .IF ERRORCODE = 0    THEN .GOTO NEXT19
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 191: .LABEL NEXT19
    next19:

    -- Line 194: .IF ERRORCODE = 3807 THEN .GOTO NEXT20
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 195: .IF ERRORCODE = 0    THEN .GOTO NEXT20
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 198: .LABEL NEXT20
    next20:

    -- Line 200: .LABEL EXITOK
    exitok:

    -- Line 207: .LABEL EXITERR
    exiterr:

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