CREATE OR REPLACE PROCEDURE DERV_ACCT_PATY_99_DROP_WORK_TABL
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
-- Procedure: DERV_ACCT_PATY_99_DROP_WORK_TABL
-- Generated: 2025-08-22 15:51:38
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
-- .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
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
  LET goto_next21 BOOLEAN DEFAULT FALSE;
  LET goto_next22 BOOLEAN DEFAULT FALSE;
  LET goto_exitok BOOLEAN DEFAULT FALSE;
  LET goto_exiterr BOOLEAN DEFAULT FALSE;

  -- Exception handling setup
  DECLARE
    general_exception EXCEPTION (-20001, 'General procedure error');
  BEGIN
    -- Main procedure logic starts here

    -- Line 1: .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
    -- RUN statement: Execute accumulated SQL
    -- (SQL execution handled inline in Snowflake)

    -- Line 39: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 48: .IF ERRORCODE = 3807 THEN .GOTO NEXT1
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 49: .IF ERRORCODE = 0    THEN .GOTO NEXT1
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 52: .LABEL NEXT1
    next1:

    -- Line 56: .IF ERRORCODE = 3807 THEN .GOTO NEXT2
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 57: .IF ERRORCODE = 0    THEN .GOTO NEXT2
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 60: .LABEL NEXT2
    next2:

    -- Line 66: .IF ERRORCODE = 3807 THEN .GOTO NEXT3
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 67: .IF ERRORCODE = 0    THEN .GOTO NEXT3
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 70: .LABEL NEXT3
    next3:

    -- Line 73: .IF ERRORCODE = 3807 THEN .GOTO NEXT4
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 74: .IF ERRORCODE = 0    THEN .GOTO NEXT4
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 77: .LABEL NEXT4
    next4:

    -- Line 80: .IF ERRORCODE = 3807 THEN .GOTO NEXT5
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 81: .IF ERRORCODE = 0    THEN .GOTO NEXT5
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 84: .LABEL NEXT5
    next5:

    -- Line 87: .IF ERRORCODE = 3807 THEN .GOTO NEXT6
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 88: .IF ERRORCODE = 0    THEN .GOTO NEXT6
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 91: .LABEL NEXT6
    next6:

    -- Line 95: .IF ERRORCODE = 3807 THEN .GOTO NEXT7
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 96: .IF ERRORCODE = 0    THEN .GOTO NEXT7
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 99: .LABEL NEXT7
    next7:

    -- Line 104: .IF ERRORCODE = 3807 THEN .GOTO NEXT8
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 105: .IF ERRORCODE = 0    THEN .GOTO NEXT8
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 108: .LABEL NEXT8
    next8:

    -- Line 111: .IF ERRORCODE = 3807 THEN .GOTO NEXT9
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 112: .IF ERRORCODE = 0    THEN .GOTO NEXT9
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 115: .LABEL NEXT9
    next9:

    -- Line 118: .IF ERRORCODE = 3807 THEN .GOTO NEXT10
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 119: .IF ERRORCODE = 0    THEN .GOTO NEXT10
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 122: .LABEL NEXT10
    next10:

    -- Line 126: .IF ERRORCODE = 3807 THEN .GOTO NEXT11
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 127: .IF ERRORCODE = 0    THEN .GOTO NEXT11
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 130: .LABEL NEXT11
    next11:

    -- Line 134: .IF ERRORCODE = 3807 THEN .GOTO NEXT12
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 135: .IF ERRORCODE = 0    THEN .GOTO NEXT12
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 138: .LABEL NEXT12
    next12:

    -- Line 144: .IF ERRORCODE = 3807 THEN .GOTO NEXT13
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 145: .IF ERRORCODE = 0    THEN .GOTO NEXT13
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 148: .LABEL NEXT13
    next13:

    -- Line 151: .IF ERRORCODE = 3807 THEN .GOTO NEXT14
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 152: .IF ERRORCODE = 0    THEN .GOTO NEXT14
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 155: .LABEL NEXT14
    next14:

    -- Line 158: .IF ERRORCODE = 3807 THEN .GOTO NEXT15
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 159: .IF ERRORCODE = 0    THEN .GOTO NEXT15
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 162: .LABEL NEXT15
    next15:

    -- Line 165: .IF ERRORCODE = 3807 THEN .GOTO NEXT16
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 166: .IF ERRORCODE = 0    THEN .GOTO NEXT16
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 169: .LABEL NEXT16
    next16:

    -- Line 174: .IF ERRORCODE = 3807 THEN .GOTO NEXT17
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 175: .IF ERRORCODE = 0    THEN .GOTO NEXT17
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 178: .LABEL NEXT17
    next17:

    -- Line 182: .IF ERRORCODE = 3807 THEN .GOTO NEXT18
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 183: .IF ERRORCODE = 0    THEN .GOTO NEXT18
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 186: .LABEL NEXT18
    next18:

    -- Line 189: .IF ERRORCODE = 3807 THEN .GOTO NEXT19
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 190: .IF ERRORCODE = 0    THEN .GOTO NEXT19
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 193: .LABEL NEXT19
    next19:

    -- Line 196: .IF ERRORCODE = 3807 THEN .GOTO NEXT20
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 197: .IF ERRORCODE = 0    THEN .GOTO NEXT20
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 200: .LABEL NEXT20
    next20:

    -- Line 204: .IF ERRORCODE = 3807 THEN .GOTO NEXT21
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 205: .IF ERRORCODE = 0    THEN .GOTO NEXT21
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 208: .LABEL NEXT21
    next21:

    -- Line 212: .IF ERRORCODE = 3807 THEN .GOTO NEXT22
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 213: .IF ERRORCODE = 0    THEN .GOTO NEXT22
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 216: .LABEL NEXT22
    next22:

    -- Line 218: .LABEL EXITOK
    exitok:

    -- Line 225: .LABEL EXITERR
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