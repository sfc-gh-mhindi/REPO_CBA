CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_ADJ_RULE_ISRT
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
-- Procedure: ACCT_BALN_BKDT_ADJ_RULE_ISRT
-- Generated: 2025-08-22 14:13:23
-- Source: Converted from Teradata BTEQ script
-- Generator: SnowflakeSPGenerator v1.0
-- =============================================================================
-- Original BTEQ Preview:
-- .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
-- .IF ERRORCODE <> 0 THEN .GOTO EXITERR
-- 
-- .SET QUIET OFF
-- .SET ECHOREQ ON
-- .SET FORMAT OFF
-- .SET WIDTH 120
-- 
-- ------------------------------------------------------------------------------
-- --  SCRIPT NAME: 90_ISRT_ACCT_BALN_BKDT_ADJ_RULE
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

    -- Line 1: .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
    -- RUN statement: Execute accumulated SQL
    -- (SQL execution handled inline in Snowflake)

    -- Line 2: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- SQL Block (lines 25-26)
    current_step := 'EXECUTING_SQL';
    BEGIN
      DELETE %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE;
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

    -- Line 27: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- SQL Block (lines 29-155)
    current_step := 'EXECUTING_SQL';
    BEGIN
      INSERT INTO %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE
      (
      ACCT_I, 
      SRCE_SYST_C,
      BALN_TYPE_C,
      CALC_FUNC_C,
      TIME_PERD_C,
      ADJ_FROM_D,
      BKDT_ADJ_FROM_D,
      ADJ_TO_D,
      ADJ_A,
      EFFT_D,
      Gl_RECN_F,
      PROS_KEY_EFFT_I               
      )
      SELECT 
      DT1.ACCT_I,
      DT1.SRCE_SYST_C, 
      DT1.BALN_TYPE_C,
      DT1.CALC_FUNC_C,
      DT1.TIME_PERD_C,
      DT1.ADJ_FROM_D,
      CASE WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))  = 0 
      THEN DT1.ADJ_FROM_D 
      /*Backdated logic calculation when diffrence of months is 1 
      and DT1.EFFT_D is between Business day 1 and Biz day 4*/
      WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))  = 1 
      AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D THEN DT1.ADJ_FROM_D
      /*Backdated logic calculation when diffrence of months is 1 
      and DT1.EFFT_D is NOT between Business day 1 and Biz day 4*/
      WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4)) = 1 
      AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  THEN  DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1)  
      /*Backdated logic calculation when diffrence of months is greater than 1 
      and DT1.EFFT_D is between Business day 1 and Biz day 4*/
      WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))> 1 
      AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
      THEN DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1)-INTERVAL '1' MONTH
      /*Backdated logic calculation when diffrence of months is greater than 1 
      and DT1.EFFT_D is  NOT between Business day 1 and Biz day 4*/
      WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))  > 1 
      AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  THEN DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1) 
      END AS BKDT_ADJ_FROM_D,
      DT1.ADJ_TO_D,
      /*Similar adjustments for the same period are added */
      SUM(DT1.ADJ_A) AS ADJ_A,
      DT1.EFFT_D,
      DT1.Gl_RECN_F,
      DT1.PROS_KEY_EFFT_I
      FROM
      (
      SELECT	
      ADJ.ACCT_I AS ACCT_I,
      ADJ.SRCE_SYST_C AS SRCE_SYST_C, 
      ADJ.BALN_TYPE_C AS BALN_TYPE_C,
      ADJ.CALC_FUNC_C AS CALC_FUNC_C,
      ADJ.TIME_PERD_C AS TIME_PERD_C,
      ADJ.ADJ_FROM_D AS ADJ_FROM_D,
      ADJ.ADJ_TO_D,
      /*Adjustments impacting the current record need to be loaded on the next day to avoid changing the open balances
      */
      (CASE WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D THEN EFFT_D+1
      ELSE EFFT_D END) AS EFFT_D,
      ADJ.Gl_RECN_F,
      ADJ_A,
      PROS_KEY_EFFT_I
      FROM
      %%VTECH%%.ACCT_BALN_ADJ  ADJ
      WHERE	
      SRCE_SYST_C = 'SAP'
      AND BALN_TYPE_C='BALN'
      AND CALC_FUNC_C='SPOT' 
      AND TIME_PERD_C = 'E' 
      /*Excluding the adjustments  with $0 in value as this brings no change to the 
      $value in tha ACCT BALN and had a negative impact on the last records in 
      ACCT BALN, so considerably important to eliminate*/
      AND ADJ.ADJ_A <> 0 
      /* Capturing delta adjustments*/
      AND ADJ.EFFT_D >= 
      	(SELECT MAX(BTCH_RUN_D) 
      	FROM %%VTECH%%.UTIL_PROS_ISAC 
      	WHERE    TRGT_M='ACCT_BALN_BKDT' AND SRCE_SYST_M='GDW'
      	AND COMT_F = 'Y'  	AND SUCC_F='Y')
      )DT1
      INNER JOIN
      (
      /*Calulation of Business day 4 Logic*/
      SELECT	
      CALR_YEAR_N,
      CALR_MNTH_N,
      CALR_CALR_D
      FROM	
      %%VTECH%%.GRD_RPRT_CALR_CLYR
      WHERE	
      CALR_WEEK_DAY_N NOT IN (1,7) 
      AND CALR_NON_WORK_DAY_F = 'N'
      AND CALR_CALR_D BETWEEN  ADD_MONTHS(CURRENT_DATE,-13) AND ADD_MONTHS(CURRENT_DATE,+1)
      QUALIFY	ROW_NUMBER() OVER (PARTITION BY CALR_YEAR_N, CALR_MNTH_N 
      ORDER	BY CALR_CALR_D) = 4
      )BSDY_4
      ON EXTRACT (YEAR 
      FROM DT1.EFFT_D)=EXTRACT (YEAR FROM BSDY_4.CALR_CALR_D)
      AND EXTRACT (MONTH FROM DT1.EFFT_D)=EXTRACT (MONTH FROM BSDY_4.CALR_CALR_D)
      WHERE
      /*Including the adjustments that are excluded  in the previous run  for open record*/
      DT1.EFFT_D <= (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
      FROM %%VTECH%%.UTIL_PROS_ISAC
      WHERE    TRGT_M='ACCT_BALN_ADJ' AND SRCE_SYST_M='SAP'
      AND COMT_F = 'Y'  AND SUCC_F='Y')
      /*To avoid any records that are processed in the previous runs */
      AND  DT1.EFFT_D > (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
      FROM %%VTECH%%.UTIL_PROS_ISAC
      WHERE    
      TRGT_M='ACCT_BALN_BKDT' AND SRCE_SYST_M='GDW'
      AND COMT_F = 'Y'  AND SUCC_F='Y')
      /*To exclude any adjustments that fall in the period where the GL is closed*/
      AND BKDT_ADJ_FROM_D <= ADJ_TO_D
      GROUP BY ACCT_I,SRCE_SYST_C, BALN_TYPE_C ,CALC_FUNC_C,TIME_PERD_C,ADJ_FROM_D,
      BKDT_ADJ_FROM_D,ADJ_TO_D,EFFT_D,Gl_RECN_F, PROS_KEY_EFFT_I;
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

    -- Line 156: .IF ERRORCODE <> 0 THEN .GOTO EXITERR
    IF (error_code <> 0) THEN
      GOTO error_exit;
    END IF;

    -- Line 159: .LOGOFF
    -- LOGOFF: Connection cleanup handled by Snowflake
    current_step := 'LOGOFF_COMPLETED';

    -- Line 161: .LABEL EXITERR
    exiterr:

    -- Line 163: .LOGOFF
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