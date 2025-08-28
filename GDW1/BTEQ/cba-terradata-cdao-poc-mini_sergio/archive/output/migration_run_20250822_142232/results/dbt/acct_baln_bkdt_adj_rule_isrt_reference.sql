```sql
CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_ADJ_RULE_ISRT(
    P_DDSTG_SCHEMA STRING DEFAULT 'DDSTG',
    P_VTECH_SCHEMA STRING DEFAULT 'VTECH',
    P_DEBUG_MODE BOOLEAN DEFAULT FALSE,
    P_DRY_RUN BOOLEAN DEFAULT FALSE,
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    PROCESS_KEY STRING DEFAULT 'ACCT_BALN_BKDT_ADJ_RULE_ISRT'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
-- =============================================================================
-- Procedure: ACCT_BALN_BKDT_ADJ_RULE_ISRT
-- Purpose: Calculate backdated adjustments from ACCT_BALN_ADJ and apply to ACCT_BALN
-- 
-- Description: This procedure processes account balance adjustments by calculating
--              backdated adjustment periods based on business day rules and applies
--              them to the account balance backdated adjustment rule table.
--
-- Parameters:
--   P_DDSTG_SCHEMA  - Target schema for staging tables (default: 'DDSTG')
--   P_VTECH_SCHEMA  - Source schema for base tables (default: 'VTECH')
--   P_DEBUG_MODE    - Enable debug logging (default: FALSE)
--   P_DRY_RUN       - Execute in dry-run mode without commits (default: FALSE)
--   ERROR_TABLE     - Error logging table (default: 'PROCESS_ERROR_LOG')
--   PROCESS_KEY     - Process identifier for logging (default: procedure name)
--
-- Returns: SUCCESS/ERROR message with processing details
--
-- Migration Notes:
--   - Converted from Teradata BTEQ script 90_ISRT_ACCT_BALN_BKDT_ADJ_RULE
--   - QUALIFY clause preserved (supported in Snowflake)
--   - Teradata interval arithmetic converted to Snowflake DATEDIFF/DATEADD
--   - ADD_MONTHS function converted to DATEADD
--   - Complex CASE logic for backdated calculations preserved
--
-- Version History:
--   1.0  2011-07-22  Suresh Vajapeyajula  Initial Teradata version
--   2.0  2025-01-XX  Migration Team       Snowflake conversion
-- =============================================================================

    -- Control variables
    v_error_code INTEGER DEFAULT 0;
    v_error_message STRING DEFAULT '';
    v_rows_deleted INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_current_step STRING DEFAULT 'INITIALIZATION';
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    
    -- Business logic variables
    v_last_batch_run_date DATE;
    v_adj_batch_run_date DATE;
    
    -- SQL statement variables
    v_delete_sql STRING;
    v_insert_sql STRING;
    
    -- Cursor for validation
    v_validation_count INTEGER DEFAULT 0;

BEGIN
    -- Initialize logging
    IF (P_DEBUG_MODE) THEN
        CALL SYSTEM$LOG('INFO', 'Starting ACCT_BALN_BKDT_ADJ_RULE_ISRT procedure');
    END IF;

    -- Parameter validation
    v_current_step := 'PARAMETER_VALIDATION';
    
    IF (P_DDSTG_SCHEMA IS NULL OR P_DDSTG_SCHEMA = '') THEN
        v_error_code := -20001;
        v_error_message := 'P_DDSTG_SCHEMA parameter cannot be null or empty';
        GOTO error_handler;
    END IF;
    
    IF (P_VTECH_SCHEMA IS NULL OR P_VTECH_SCHEMA = '') THEN
        v_error_code := -20002;
        v_error_message := 'P_VTECH_SCHEMA parameter cannot be null or empty';
        GOTO error_handler;
    END IF;

    -- Validate schema existence and access
    v_current_step := 'SCHEMA_VALIDATION';
    
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ''' || P_DDSTG_SCHEMA || '''';
        IF (SQLROWCOUNT = 0) THEN
            v_error_code := -20003;
            v_error_message := 'Target schema ' || P_DDSTG_SCHEMA || ' does not exist';
            GOTO error_handler;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Error validating target schema: ' || SQLERRM;
            GOTO error_handler;
    END;

    -- Get control dates for delta processing
    v_current_step := 'CONTROL_DATE_RETRIEVAL';
    
    BEGIN
        -- Get last successful batch run date for ACCT_BALN_BKDT
        SELECT MAX(BTCH_RUN_D) 
        INTO v_last_batch_run_date
        FROM IDENTIFIER(P_VTECH_SCHEMA || '.UTIL_PROS_ISAC')
        WHERE TRGT_M = 'ACCT_BALN_BKDT' 
          AND SRCE_SYST_M = 'GDW'
          AND COMT_F = 'Y' 
          AND SUCC_F = 'Y';
          
        -- Get last successful batch run date for ACCT_BALN_ADJ
        SELECT MAX(BTCH_RUN_D)
        INTO v_adj_batch_run_date
        FROM IDENTIFIER(P_VTECH_SCHEMA || '.UTIL_PROS_ISAC')
        WHERE TRGT_M = 'ACCT_BALN_ADJ' 
          AND SRCE_SYST_M = 'SAP'
          AND COMT_F = 'Y' 
          AND SUCC_F = 'Y';
          
        IF (P_DEBUG_MODE) THEN
            CALL SYSTEM$LOG('INFO', 'Last BKDT batch date: ' || COALESCE(v_last_batch_run_date::STRING, 'NULL'));
            CALL SYSTEM$LOG('INFO', 'Last ADJ batch date: ' || COALESCE(v_adj_batch_run_date::STRING, 'NULL'));
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Error retrieving control dates: ' || SQLERRM;
            GOTO error_handler;
    END;

    -- Begin transaction for data processing
    BEGIN TRANSACTION;

    -- Step 1: Delete existing records from target table
    v_current_step := 'DELETE_EXISTING_RECORDS';
    
    v_delete_sql := 'DELETE FROM ' || P_DDSTG_SCHEMA || '.ACCT_BALN_BKDT_ADJ_RULE';
    
    IF (P_DRY_RUN) THEN
        IF (P_DEBUG_MODE) THEN
            CALL SYSTEM$LOG('INFO', 'DRY RUN - Would execute: ' || v_delete_sql);
        END IF;
        v_rows_deleted := 0;
    ELSE
        BEGIN
            EXECUTE IMMEDIATE v_delete_sql;
            v_rows_deleted := SQLROWCOUNT;
            
            IF (P_DEBUG_MODE) THEN
                CALL SYSTEM$LOG('INFO', 'Deleted ' || v_rows_deleted || ' existing records');
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_code := SQLCODE;
                v_error_message := 'Error deleting existing records: ' || SQLERRM;
                ROLLBACK;
                GOTO error_handler;
        END;
    END IF;

    -- Step 2: Insert new backdated adjustment rules
    v_current_step := 'INSERT_BACKDATED_RULES';
    
    v_insert_sql := '
    INSERT INTO ' || P_DDSTG_SCHEMA || '.ACCT_BALN_BKDT_ADJ_RULE (
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
        GL_RECN_F,
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
            -- Same month: no backdating needed
            WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 0 
            THEN DT1.ADJ_FROM_D 
            
            -- One month difference and within business day 4: use original date
            WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                 AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
            THEN DT1.ADJ_FROM_D

            -- One month difference and after business day 4: use first of current month
            WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                 AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
            THEN DATE_TRUNC(''MONTH'', DT1.EFFT_D)

            -- More than one month and within business day 4: use first of previous month
            WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                 AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
            THEN DATEADD(MONTH, -1, DATE_TRUNC(''MONTH'', DT1.EFFT_D))

            -- More than one month and after business day 4: use first of current month
            WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                 AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
            THEN DATE_TRUNC(''MONTH'', DT1.EFFT_D)
            
            ELSE DT1.ADJ_FROM_D
        END AS BKDT_ADJ_FROM_D,
        DT1.ADJ_TO_D,
        SUM(DT1.ADJ_A) AS ADJ_A,  -- Aggregate similar adjustments for same period
        DT1.EFFT_D,
        DT1.GL_RECN_F,
        DT1.PROS_KEY_EFFT_I
    FROM (
        SELECT	
            ADJ.ACCT_I,
            ADJ.SRCE_SYST_C, 
            ADJ.BALN_TYPE_C,
            ADJ.CALC_FUNC_C,
            ADJ.TIME_PERD_C,
            ADJ.ADJ_FROM_D,
            ADJ.ADJ_TO_D,
            -- Adjustments impacting current record loaded next day to avoid changing open balances
            CASE 
                WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D 
                THEN DATEADD(DAY, 1, ADJ.EFFT_D)
                ELSE ADJ.EFFT_D 
            END AS EFFT_D,
            ADJ.GL_RECN_F,
            ADJ.ADJ_A,
            ADJ.PROS_KEY_EFFT_I
        FROM ' || P_VTECH_SCHEMA || '.ACCT_BALN_ADJ ADJ
        WHERE ADJ.SRCE_SYST_C = ''SAP''
          AND ADJ.BALN_TYPE_C = ''BALN''
          AND ADJ.CALC_FUNC_C = ''SPOT'' 
          AND ADJ.TIME_PERD_C = ''E'' 
          AND ADJ.ADJ_A <> 0  -- Exclude zero-value adjustments
          AND ADJ.EFFT_D >= COALESCE(''' || COALESCE(v_last_batch_run_date::STRING, '1900-01-01') || '''::DATE, ''1900-01-01''::DATE)
    ) DT1
    INNER JOIN (
        -- Calculate 4th business day of each month
        SELECT	
            YEAR(CALR_CALR_D) AS CALR_YEAR_N,
            MONTH(CALR_CALR_D) AS CALR_MNTH_N,
            CALR_CALR_D
        FROM ' || P_VTECH_SCHEMA || '.GRD_RPRT_CALR_CLYR
        WHERE DAYOFWEEK(CALR_CALR_D) NOT IN (1, 7)  -- Exclude weekends (Sunday=1, Saturday=7)
          AND CALR_NON_WORK_DAY_F = ''N''
          AND CALR_CALR_D BETWEEN DATEADD(MONTH, -13, CURRENT_DATE()) 
                              AND DATEADD(MONTH, 1, CURRENT_DATE())
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY YEAR(CALR_CALR_D), MONTH(CALR_CALR_D) 
            ORDER BY CALR_CALR_D
        ) = 4
    ) BSDY_4
    ON YEAR(DT1.EFFT_D) = BSDY_4.CALR_YEAR_N
   AND MONTH(DT1.EFFT_D) = BSDY_4.CALR_MNTH_N
    WHERE DT1.EFFT_D <= COALESCE(''' || COALESCE(v_adj_batch_run_date::STRING, CURRENT_DATE()::STRING) || '''::DATE, CURRENT_DATE())
      AND DT1.EFFT_D > COALESCE(''' || COALESCE(v_last_batch_run_date::STRING, '1900-01-01') || '''::DATE, ''1900-01-01''::DATE)
    GROUP BY 
        DT1.ACCT_I, DT1.SRCE_SYST_C, DT1.BALN_TYPE_C, DT1.CALC_FUNC_C, 
        DT1.TIME_PERD_C, DT1.ADJ_FROM_D, BKDT_ADJ_FROM_D, DT1.ADJ_TO_D, 
        DT1.EFFT_D, DT1.GL_RECN_F, DT1.PROS_KEY_EFFT_I
    HAVING BKDT_ADJ_FROM_D <= DT1.ADJ_TO_D  -- Exclude adjustments in closed GL periods
    ';

    IF (P_DRY_RUN) THEN
        IF (P_DEBUG_MODE) THEN
            CALL SYSTEM$LOG('INFO', 'DRY RUN - Would execute insert statement');
        END IF;
        v_rows_inserted := 0;
    ELSE
        BEGIN
            EXECUTE IMMEDIATE v_insert_sql;
            v_rows_inserted := SQLROWCOUNT;
            
            IF (P_DEBUG_MODE) THEN
                CALL SYSTEM$LOG('INFO', 'Inserted ' || v_rows_inserted || ' new records');
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_code := SQLCODE;
                v_error_message := 'Error inserting backdated rules: ' || SQLERRM;
                ROLLBACK;
                GOTO error_handler;
        END;
    END IF;

    -- Data validation
    v_current_step := 'DATA_VALIDATION';
    
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || P_DDSTG_SCHEMA || '.ACCT_BALN_BKDT_ADJ_RULE WHERE ADJ_A = 0';
        v_validation_count := SQLROWCOUNT;
        
        IF (v_validation_count > 0) THEN
            v_error_code := -20004;
            v_error_message := 'Data validation failed: Found ' || v_validation_count || ' records with zero adjustment amounts';
            ROLLBACK;
            GOTO error_handler;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Error during data validation: ' || SQLERRM;
            ROLLBACK;
            GOTO error_handler;
    END;

    -- Commit transaction
    IF (NOT P_DRY_RUN) THEN
        COMMIT;
    END IF;

    v_current_step := 'COMPLETED_SUCCESSFULLY';
    
    RETURN 'SUCCESS: Process completed successfully. ' ||
           'Deleted: ' || v_rows_deleted || ' records, ' ||
           'Inserted: ' || v_rows_inserted || ' records. ' ||
           'Duration: ' || DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP()) || ' seconds.';

    -- Error handling
    error_handler:
    
    BEGIN
        -- Log error to error table if it exists
        EXECUTE IMMEDIATE '
        INSERT INTO ' || ERROR_TABLE || ' (
            PROCESS_KEY, 
            ERROR_CODE, 
            ERROR_MESSAGE, 
            ERROR_STEP,
            ERROR_TIMESTAMP
        ) VALUES (
            ''' || PROCESS_KEY || ''', 
            ' || v_error_code || ', 
            ''' || v_error_message || ''',
            ''' || v_current_step || ''',
            CURRENT_TIMESTAMP()
        )';
    EXCEPTION
        WHEN OTHER THEN
            -- If error logging fails, continue with return
            NULL;
    END;
    
    RETURN 'ERROR in step ' || v_current_step || ': ' || v_error_message || ' (Code: ' || v_error_code || ')';

EXCEPTION
    WHEN OTHER THEN
        -- Rollback any open transaction
        ROLLBACK;
        
        RETURN 'FATAL ERROR in step ' || v_current_step || ': ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
```