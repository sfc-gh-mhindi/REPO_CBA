```sql
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
    error_code INTEGER DEFAULT 0;
    sql_state STRING DEFAULT '00000';
    error_message STRING DEFAULT '';
    row_count INTEGER DEFAULT 0;
    current_step STRING DEFAULT 'INIT';

    -- Label tracking variables
    goto_exiterr BOOLEAN DEFAULT FALSE;

    -- Exception handling setup
BEGIN
    -- Main procedure logic starts here

    -- SQL Block (lines 25-26)
    current_step := 'EXECUTING_DELETE';
    BEGIN
        DELETE FROM %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE;
        row_count := SQLROWCOUNT;
        IF (SQLCODE <> 0) THEN
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT INTO :ERROR_TABLE (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP)
            VALUES (:PROCESS_KEY, error_code, error_message, CURRENT_TIMESTAMP());
            RETURN 'ERROR: ' || error_message || ' (Code: ' || error_code || ')';
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT INTO :ERROR_TABLE (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP)
            VALUES (:PROCESS_KEY, error_code, error_message, CURRENT_TIMESTAMP());
            RETURN 'FATAL ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
    END;

    -- SQL Block (lines 29-155)
    current_step := 'EXECUTING_INSERT';
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
            CASE 
                WHEN (DATE_PART('month', DT1.EFFT_D) - DATE_PART('month', DT1.ADJ_FROM_D)) = 0 
                THEN DT1.ADJ_FROM_D 
                WHEN (DATE_PART('month', DT1.EFFT_D) - DATE_PART('month', DT1.ADJ_FROM_D)) = 1 
                AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D THEN DT1.ADJ_FROM_D
                WHEN (DATE_PART('month', DT1.EFFT_D) - DATE_PART('month', DT1.ADJ_FROM_D)) = 1 
                AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  THEN  DT1.EFFT_D - (DAY(DT1.EFFT_D) - 1)  
                WHEN (DATE_PART('month', DT1.EFFT_D) - DATE_PART('month', DT1.ADJ_FROM_D)) > 1 
                AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
                THEN DT1.EFFT_D - (DAY(DT1.EFFT_D) - 1) - INTERVAL '1 month'
                WHEN (DATE_PART('month', DT1.EFFT_D) - DATE_PART('month', DT1.ADJ_FROM_D)) > 1 
                AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  THEN DT1.EFFT_D - (DAY(DT1.EFFT_D) - 1) 
            END AS BKDT_ADJ_FROM_D,
            DT1.ADJ_TO_D,
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
                CASE WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D THEN EFFT_D+1
                ELSE EFFT_D END AS EFFT_D,
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
                AND ADJ.ADJ_A <> 0 
                AND ADJ.EFFT_D >= 
                    (SELECT MAX(BTCH_RUN_D) 
                    FROM %%VTECH%%.UTIL_PROS_ISAC 
                    WHERE    TRGT_M='ACCT_BALN_BKDT' AND SRCE_SYST_M='GDW'
                    AND COMT_F = 'Y'  	AND SUCC_F='Y')
        )DT1
        INNER JOIN
        (
            SELECT	
                CALR_YEAR_N,
                CALR_MNTH_N,
                CALR_CALR_D
            FROM	
            %%VTECH%%.GRD_RPRT_CALYR
            WHERE	
                CALR_WEEK_DAY_N NOT IN (1,7) 
                AND CALR_NON_WORK_DAY_F = 'N'
                AND CALR_CALR_D BETWEEN DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '13 month' AND DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CALR_YEAR_N, CALR_MNTH_N ORDER BY CALR_CALR_D) = 4
        )BSDY_4
        ON EXTRACT(YEAR FROM DT1.EFFT_D)=EXTRACT(YEAR FROM BSDY_4.CALR_CALR_D)
        AND EXTRACT(MONTH FROM DT1.EFFT_D)=EXTRACT(MONTH FROM BSDY_4.CALR_CALR_D)
        WHERE
        DT1.EFFT_D <= (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
        FROM %%VTECH%%.UTIL_PROS_ISAC
        WHERE    TRGT_M='ACCT_BALN_ADJ' AND SRCE_SYST_M='SAP'
        AND COMT_F = 'Y' 	AND SUCC_F='Y')
        AND DT1.EFFT_D > (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
        FROM %%VTECH%%.UTIL_PROS_ISAC
        WHERE    
        TRGT_M='ACCT_BALN_BKDT' AND SRCE_SYST_M='GDW'
        AND COMT_F = 'Y' 	AND SUCC_F='Y')
        AND BKDT_ADJ_FROM_D <= ADJ_TO_D
        GROUP BY 
            ACCT_I, 
            SRCE_SYST_C, 
            BALN_TYPE_C, 
            CALC_FUNC_C, 
            TIME_PERD_C, 
            ADJ_FROM_D, 
            BKDT_ADJ_FROM_D, 
            ADJ_TO_D, 
            EFFT_D, 
            Gl_RECN_F, 
            PROS_KEY_EFFT_I;
        row_count := SQLROWCOUNT;
        IF (SQLCODE <> 0) THEN
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT INTO :ERROR_TABLE (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP)
            VALUES (:PROCESS_KEY, error_code, error_message, CURRENT_TIMESTAMP());
            RETURN 'ERROR: ' || error_message || ' (Code: ' || error_code || ')';
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            error_code := SQLCODE;
            error_message := SQLERRM;
            INSERT INTO :ERROR_TABLE (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP)
            VALUES (:PROCESS_KEY, error_code, error_message, CURRENT_TIMESTAMP());
            RETURN 'FATAL ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
    END;

    RETURN 'SUCCESS: ' || current_step || ' completed. Rows processed: ' || row_count;
END;
$$;
```