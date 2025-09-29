```sql
CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_ADJ_RULE_ISRT(
    P_DDSTG_SCHEMA STRING DEFAULT 'DDSTG',
    P_VTECH_SCHEMA STRING DEFAULT 'VTECH',
    P_ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    P_PROCESS_KEY STRING DEFAULT 'ACCT_BALN_BKDT_ADJ_RULE_ISRT',
    P_DEBUG_MODE BOOLEAN DEFAULT FALSE
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
-- Original: Migrated from Teradata BTEQ script 90_ISRT_ACCT_BALN_BKDT_ADJ_RULE
-- Author: Suresh Vajapeyajula (Original), Enhanced for Snowflake
-- Version: 2.0 - Snowflake Migration
-- Date: 2025-01-XX
--
-- Parameters:
--   P_DDSTG_SCHEMA   - Target schema for staging tables (default: DDSTG)
--   P_VTECH_SCHEMA   - Source schema for base tables (default: VTECH)
--   P_ERROR_TABLE    - Error logging table name (default: PROCESS_ERROR_LOG)
--   P_PROCESS_KEY    - Process identifier for logging (default: procedure name)
--   P_DEBUG_MODE     - Enable debug logging (default: FALSE)
--
-- Returns: SUCCESS/ERROR message with processing details
--
-- Business Logic:
--   1. Clears existing backdated adjustment rules
--   2. Calculates backdated periods based on business day 4 logic
--   3. Applies month difference calculations for adjustment periods
--   4. Excludes zero-value adjustments and processed records
--   5. Groups similar adjustments for the same period
-- =============================================================================

    -- Control variables
    v_error_code INTEGER DEFAULT 0;
    v_error_message STRING DEFAULT '';
    v_current_step STRING DEFAULT 'INITIALIZATION';
    v_rows_deleted INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    
    -- Dynamic SQL variables
    v_delete_sql STRING;
    v_insert_sql STRING;
    v_target_table STRING;
    v_source_table_adj STRING;
    v_source_table_isac STRING;
    v_source_table_calendar STRING;
    
    -- Process tracking
    v_last_batch_run_date DATE;
    v_max_batch_run_date DATE;

BEGIN
    -- Initialize procedure
    v_current_step := 'PARAMETER_VALIDATION';
    
    -- Validate input parameters
    IF (P_DDSTG_SCHEMA IS NULL OR TRIM(P_DDSTG_SCHEMA) = '') THEN
        RAISE EXCEPTION 'P_DDSTG_SCHEMA cannot be null or empty';
    END IF;
    
    IF (P_VTECH_SCHEMA IS NULL OR TRIM(P_VTECH_SCHEMA) = '') THEN
        RAISE EXCEPTION 'P_VTECH_SCHEMA cannot be null or empty';
    END IF;
    
    -- Construct table references
    v_target_table := P_DDSTG_SCHEMA || '.ACCT_BALN_BKDT_ADJ_RULE';
    v_source_table_adj := P_VTECH_SCHEMA || '.ACCT_BALN_ADJ';
    v_source_table_isac := P_VTECH_SCHEMA || '.UTIL_PROS_ISAC';
    v_source_table_calendar := P_VTECH_SCHEMA || '.GRD_RPRT_CALR_CLYR';
    
    IF (P_DEBUG_MODE) THEN
        CALL SYSTEM$LOG('INFO', 'Starting ' || P_PROCESS_KEY || ' at ' || v_start_time::STRING);
        CALL SYSTEM$LOG('INFO', 'Target table: ' || v_target_table);
    END IF;

    -- Step 1: Get process control dates
    v_current_step := 'FETCH_CONTROL_DATES';
    
    BEGIN
        -- Get last successful batch run date for ACCT_BALN_BKDT
        SELECT MAX(BTCH_RUN_D)
        INTO v_last_batch_run_date
        FROM IDENTIFIER(:v_source_table_isac)
        WHERE TRGT_M = 'ACCT_BALN_BKDT' 
          AND SRCE_SYST_M = 'GDW'
          AND COMT_F = 'Y' 
          AND SUCC_F = 'Y';
          
        -- Get max batch run date for ACCT_BALN_ADJ
        SELECT MAX(BTCH_RUN_D)
        INTO v_max_batch_run_date
        FROM IDENTIFIER(:v_source_table_isac)
        WHERE TRGT_M = 'ACCT_BALN_ADJ' 
          AND SRCE_SYST_M = 'SAP'
          AND COMT_F = 'Y' 
          AND SUCC_F = 'Y';
          
        IF (P_DEBUG_MODE) THEN
            CALL SYSTEM$LOG('INFO', 'Last batch run date (BKDT): ' || COALESCE(v_last_batch_run_date::STRING, 'NULL'));
            CALL SYSTEM$LOG('INFO', 'Max batch run date (ADJ): ' || COALESCE(v_max_batch_run_date::STRING, 'NULL'));
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Error fetching control dates: ' || SQLERRM;
            GOTO error_handler;
    END;

    -- Step 2: Clear existing data
    v_current_step := 'DELETE_EXISTING_DATA';
    
    BEGIN
        v_delete_sql := 'DELETE FROM ' || v_target_table;
        
        EXECUTE IMMEDIATE v_delete_sql;
        v_rows_deleted := SQLROWCOUNT;
        
        IF (P_DEBUG_MODE) THEN
            CALL SYSTEM$LOG('INFO', 'Deleted ' || v_rows_deleted || ' existing records');
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Error deleting existing data: ' || SQLERRM;
            GOTO error_handler;
    END;

    -- Step 3: Insert backdated adjustment rules
    v_current_step := 'INSERT_ADJUSTMENT_RULES';
    
    BEGIN
        v_insert_sql := '
        INSERT INTO ' || v_target_table || '
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
            GL_RECN_F,
            PROS_KEY_EFFT_I               
        )
        WITH business_day_4 AS (
            -- Calculate 4th business day of each month
            SELECT 
                YEAR(CALR_CALR_D) AS CALR_YEAR_N,
                MONTH(CALR_CALR_D) AS CALR_MNTH_N,
                CALR_CALR_D
            FROM ' || v_source_table_calendar || '
            WHERE CALR_WEEK_DAY_N NOT IN (1, 7)  -- Exclude weekends
              AND CALR_NON_WORK_DAY_F = ''N''     -- Exclude holidays
              AND CALR_CALR_D BETWEEN DATEADD(MONTH, -13, CURRENT_DATE()) 
                                  AND DATEADD(MONTH, 1, CURRENT_DATE())
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY YEAR(CALR_CALR_D), MONTH(CALR_CALR_D) 
                ORDER BY CALR_CALR_D
            ) = 4
        ),
        adjustment_data AS (
            SELECT
                ADJ.ACCT_I,
                ADJ.SRCE_SYST_C, 
                ADJ.BALN_TYPE_C,
                ADJ.CALC_FUNC_C,
                ADJ.TIME_PERD_C,
                ADJ.ADJ_FROM_D,
                ADJ.ADJ_TO_D,
                -- Adjust effective date for current period impacts
                CASE 
                    WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D THEN DATEADD(DAY, 1, ADJ.EFFT_D)
                    ELSE ADJ.EFFT_D 
                END AS EFFT_D,
                ADJ.GL_RECN_F,
                ADJ.ADJ_A,
                ADJ.PROS_KEY_EFFT_I
            FROM ' || v_source_table_adj || ' ADJ
            WHERE ADJ.SRCE_SYST_C = ''SAP''
              AND ADJ.BALN_TYPE_C = ''BALN''
              AND ADJ.CALC_FUNC_C = ''SPOT'' 
              AND ADJ.TIME_PERD_C = ''E''
              AND ADJ.ADJ_A <> 0  -- Exclude zero adjustments
              AND ADJ.EFFT_D >= COALESCE(''' || COALESCE(v_last_batch_run_date::STRING, '1900-01-01') || '''::DATE, ''1900-01-01''::DATE)
        )
        SELECT 
            DT1.ACCT_I,
            DT1.SRCE_SYST_C, 
            DT1.BALN_TYPE_C,
            DT1.CALC_FUNC_C,
            DT1.TIME_PERD_C,
            DT1.ADJ_FROM_D,
            -- Complex backdated adjustment calculation
            CASE 
                -- Same month: no backdating needed
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 0 
                THEN DT1.ADJ_FROM_D
                
                -- 1 month difference, within business day 4: use original from date
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                 AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
                THEN DT1.ADJ_FROM_D
                
                -- 1 month difference, after business day 4: use first of effective month
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                 AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
                THEN DATE_TRUNC(''MONTH'', DT1.EFFT_D)
                
                -- >1 month difference, within business day 4: use previous month first
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                 AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
                THEN DATEADD(MONTH, -1, DATE_TRUNC(''MONTH'', DT1.EFFT_D))
                
                -- >1 month difference, after business day 4: use current month first
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                 AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
                THEN DATE_TRUNC(''MONTH'', DT1.EFFT_D)
                
                ELSE DT1.ADJ_FROM_D
            END AS BKDT_ADJ_FROM_D,
            DT1.ADJ_TO_D,
            SUM(DT1.ADJ_A) AS ADJ_A,  -- Aggregate similar adjustments
            DT1.EFFT_D,
            DT1.GL_RECN_F,
            DT1.PROS_KEY_EFFT_I
        FROM adjustment_data DT1
        INNER JOIN business_day_4 BSDY_4
            ON YEAR(DT1.EFFT_D) = BSDY_4.CALR_YEAR_N
           AND MONTH(DT1.EFFT_D) = BSDY_4.CALR_MNTH_N
        WHERE DT1.EFFT_D <= COALESCE(''' || COALESCE(v_max_batch_run_date::STRING, '2099-12-31') || '''::DATE, ''2099-12-31''::DATE)
          AND DT1.EFFT_D > COALESCE(''' || COALESCE(v_last_batch_run_date::STRING, '1900-01-01') || '''::DATE, ''1900-01-01''::DATE)
        GROUP BY 
            DT1.ACCT_I, DT1.SRCE_SYST_C, DT1.BALN_TYPE_C, DT1.CALC_FUNC_C,
            DT1.TIME_PERD_C, DT1.ADJ_FROM_D, 7, DT1.ADJ_TO_D, DT1.EFFT_D,
            DT1.GL_RECN_F, DT1.PROS_KEY_EFFT_I
        HAVING 7 <= DT1.ADJ_TO_D  -- Ensure backdated from date is valid
        ';
        
        EXECUTE IMMEDIATE v_insert_sql;
        v_rows_inserted := SQLROWCOUNT;
        
        IF (P_DEBUG_MODE) THEN
            CALL SYSTEM$LOG('INFO', 'Inserted ' || v_rows_inserted || ' adjustment records');
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Error inserting adjustment rules: ' || SQLERRM;
            GOTO error_handler;
    END;

    -- Success completion
    v_current_step := 'COMPLETION';
    
    IF (P_DEBUG_MODE) THEN
        CALL SYSTEM$LOG('INFO', 'Process completed successfully in ' || 
                       DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP()) || ' seconds');
    END IF;
    
    RETURN 'SUCCESS: ' || P_PROCESS_KEY || ' completed. ' ||
           'Deleted: ' || v_rows_deleted || ', Inserted: ' || v_rows_inserted || ' records. ' ||
           'Duration: ' || DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP()) || ' seconds.';

    -- Error handling section
    error_handler:
    
    BEGIN
        -- Log error to error table if it exists
        EXECUTE IMMEDIATE 
        'INSERT INTO ' || P_ERROR_TABLE || ' 
         (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_STEP, ERROR_TIMESTAMP, DURATION_SECONDS)
         VALUES (?, ?, ?, ?, ?, ?)'
        USING (P_PROCESS_KEY, v_error_code, v_error_message, v_current_step, 
               CURRENT_TIMESTAMP(), DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP()));
    EXCEPTION
        WHEN OTHER THEN
            -- If error logging fails, continue with return message
            NULL;
    END;
    
    -- Rollback any partial changes
    ROLLBACK;
    
    RETURN 'ERROR in ' || v_current_step || ': ' || v_error_message || 
           ' (Code: ' || v_error_code || ')';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FATAL ERROR in ' || v_current_step || ': ' || SQLERRM || 
               ' (Code: ' || SQLCODE || ')';
END;
$$;
```