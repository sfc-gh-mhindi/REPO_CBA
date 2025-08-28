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
-- Parameters:
--   P_DDSTG_SCHEMA   - Target schema for staging tables (default: 'DDSTG')
--   P_VTECH_SCHEMA   - Source schema for base tables (default: 'VTECH')
--   P_ERROR_TABLE    - Error logging table name (default: 'PROCESS_ERROR_LOG')
--   P_PROCESS_KEY    - Process identifier for logging (default: procedure name)
--   P_DEBUG_MODE     - Enable debug logging (default: FALSE)
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
--   1.0  22/07/2011 Suresh Vajapeyajula - Initial Teradata version
--   2.0  2025-01-XX Migration Team      - Snowflake conversion
-- =============================================================================

    -- Process control variables
    v_error_code INTEGER DEFAULT 0;
    v_error_message STRING DEFAULT '';
    v_rows_deleted INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_current_step STRING DEFAULT 'INITIALIZATION';
    v_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    
    -- Dynamic SQL variables
    v_target_table STRING;
    v_source_schema STRING;
    v_sql_statement STRING;
    
    -- Business logic variables
    v_last_batch_run_date DATE;
    v_max_adj_batch_date DATE;
    
    -- Exception handling
    validation_error EXCEPTION (-20001, 'Parameter validation failed');
    business_logic_error EXCEPTION (-20002, 'Business logic validation failed');
    data_processing_error EXCEPTION (-20003, 'Data processing error');

BEGIN
    -- =============================================================================
    -- INITIALIZATION AND VALIDATION
    -- =============================================================================
    
    v_current_step := 'PARAMETER_VALIDATION';
    
    -- Validate input parameters
    IF (P_DDSTG_SCHEMA IS NULL OR TRIM(P_DDSTG_SCHEMA) = '') THEN
        RAISE validation_error;
    END IF;
    
    IF (P_VTECH_SCHEMA IS NULL OR TRIM(P_VTECH_SCHEMA) = '') THEN
        RAISE validation_error;
    END IF;
    
    -- Construct fully qualified table names
    v_target_table := P_DDSTG_SCHEMA || '.ACCT_BALN_BKDT_ADJ_RULE';
    v_source_schema := P_VTECH_SCHEMA;
    
    -- Debug logging
    IF (P_DEBUG_MODE) THEN
        INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
        ) VALUES (
            P_PROCESS_KEY, 0, 
            'Starting process with target: ' || v_target_table || ', source: ' || v_source_schema,
            CURRENT_TIMESTAMP(), v_current_step
        );
    END IF;
    
    -- =============================================================================
    -- BUSINESS LOGIC VALIDATION
    -- =============================================================================
    
    v_current_step := 'BUSINESS_VALIDATION';
    
    -- Validate that required source tables exist and have recent data
    v_sql_statement := 'SELECT MAX(BTCH_RUN_D) FROM ' || v_source_schema || '.UTIL_PROS_ISAC 
                       WHERE TRGT_M = ''ACCT_BALN_BKDT'' AND SRCE_SYST_M = ''GDW'' 
                       AND COMT_F = ''Y'' AND SUCC_F = ''Y''';
    
    EXECUTE IMMEDIATE v_sql_statement INTO v_last_batch_run_date;
    
    IF (v_last_batch_run_date IS NULL) THEN
        v_error_message := 'No successful batch runs found for ACCT_BALN_BKDT process';
        RAISE business_logic_error;
    END IF;
    
    -- Validate adjustment batch date
    v_sql_statement := 'SELECT MAX(BTCH_RUN_D) FROM ' || v_source_schema || '.UTIL_PROS_ISAC 
                       WHERE TRGT_M = ''ACCT_BALN_ADJ'' AND SRCE_SYST_M = ''SAP'' 
                       AND COMT_F = ''Y'' AND SUCC_F = ''Y''';
    
    EXECUTE IMMEDIATE v_sql_statement INTO v_max_adj_batch_date;
    
    IF (v_max_adj_batch_date IS NULL OR v_max_adj_batch_date <= v_last_batch_run_date) THEN
        v_error_message := 'No new adjustment data available for processing';
        RAISE business_logic_error;
    END IF;
    
    -- =============================================================================
    -- DATA PROCESSING - DELETE EXISTING RECORDS
    -- =============================================================================
    
    v_current_step := 'DELETE_EXISTING_DATA';
    
    BEGIN
        v_sql_statement := 'DELETE FROM ' || v_target_table;
        EXECUTE IMMEDIATE v_sql_statement;
        v_rows_deleted := SQLROWCOUNT;
        
        IF (P_DEBUG_MODE) THEN
            INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
                PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
            ) VALUES (
                P_PROCESS_KEY, 0, 
                'Deleted ' || v_rows_deleted || ' existing records from target table',
                CURRENT_TIMESTAMP(), v_current_step
            );
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Failed to delete existing data: ' || SQLERRM;
            RAISE data_processing_error;
    END;
    
    -- =============================================================================
    -- DATA PROCESSING - INSERT NEW RECORDS WITH BACKDATED LOGIC
    -- =============================================================================
    
    v_current_step := 'INSERT_BACKDATED_ADJUSTMENTS';
    
    BEGIN
        v_sql_statement := '
        INSERT INTO ' || v_target_table || ' (
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
            -- Complex backdated logic calculation using Snowflake date functions
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
            -- Aggregate similar adjustments for the same period
            SUM(DT1.ADJ_A) AS ADJ_A,
            DT1.EFFT_D,
            DT1.GL_RECN_F,
            DT1.PROS_KEY_EFFT_I
        FROM (
            -- Source adjustment data with effective date logic
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
            FROM ' || v_source_schema || '.ACCT_BALN_ADJ ADJ
            WHERE	
                ADJ.SRCE_SYST_C = ''SAP''
                AND ADJ.BALN_TYPE_C = ''BALN''
                AND ADJ.CALC_FUNC_C = ''SPOT'' 
                AND ADJ.TIME_PERD_C = ''E'' 
                -- Exclude zero-value adjustments to avoid negative impact on last records
                AND ADJ.ADJ_A <> 0 
                -- Capture delta adjustments since last successful run
                AND ADJ.EFFT_D >= (
                    SELECT MAX(BTCH_RUN_D) 
                    FROM ' || v_source_schema || '.UTIL_PROS_ISAC 
                    WHERE TRGT_M = ''ACCT_BALN_BKDT'' 
                      AND SRCE_SYST_M = ''GDW''
                      AND COMT_F = ''Y'' 
                      AND SUCC_F = ''Y''
                )
        ) DT1
        INNER JOIN (
            -- Business day 4 calculation logic
            SELECT	
                YEAR(CALR_CALR_D) AS CALR_YEAR_N,
                MONTH(CALR_CALR_D) AS CALR_MNTH_N,
                CALR_CALR_D
            FROM ' || v_source_schema || '.GRD_RPRT_CALR_CLYR
            WHERE	
                DAYOFWEEK(CALR_CALR_D) NOT IN (1, 7)  -- Exclude weekends
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
        WHERE
            -- Include adjustments excluded in previous run for open records
            DT1.EFFT_D <= (
                SELECT MAX(BTCH_RUN_D) 
                FROM ' || v_source_schema || '.UTIL_PROS_ISAC
                WHERE TRGT_M = ''ACCT_BALN_ADJ'' 
                  AND SRCE_SYST_M = ''SAP''
                  AND COMT_F = ''Y'' 
                  AND SUCC_F = ''Y''
            )
            -- Avoid records processed in previous runs
            AND DT1.EFFT_D > (
                SELECT MAX(BTCH_RUN_D) 
                FROM ' || v_source_schema || '.UTIL_PROS_ISAC
                WHERE TRGT_M = ''ACCT_BALN_BKDT'' 
                  AND SRCE_SYST_M = ''GDW''
                  AND COMT_F = ''Y'' 
                  AND SUCC_F = ''Y''
            )
        GROUP BY 
            DT1.ACCT_I, DT1.SRCE_SYST_C, DT1.BALN_TYPE_C, DT1.CALC_FUNC_C,
            DT1.TIME_PERD_C, DT1.ADJ_FROM_D, DT1.ADJ_TO_D, DT1.EFFT_D,
            DT1.GL_RECN_F, DT1.PROS_KEY_EFFT_I, BSDY_4.CALR_CALR_D
        HAVING 
            -- Exclude adjustments that fall in closed GL periods
            CASE 
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 0 
                THEN DT1.ADJ_FROM_D 
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                     AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
                THEN DT1.ADJ_FROM_D
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                     AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
                THEN DATE_TRUNC(''MONTH'', DT1.EFFT_D)
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                     AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
                THEN DATEADD(MONTH, -1, DATE_TRUNC(''MONTH'', DT1.EFFT_D))
                WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                     AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
                THEN DATE_TRUNC(''MONTH'', DT1.EFFT_D)
                ELSE DT1.ADJ_FROM_D
            END <= DT1.ADJ_TO_D';
        
        EXECUTE IMMEDIATE v_sql_statement;
        v_rows_inserted := SQLROWCOUNT;
        
        IF (P_DEBUG_MODE) THEN
            INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
                PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
            ) VALUES (
                P_PROCESS_KEY, 0, 
                'Inserted ' || v_rows_inserted || ' backdated adjustment records',
                CURRENT_TIMESTAMP(), v_current_step
            );
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_code := SQLCODE;
            v_error_message := 'Failed to insert backdated adjustments: ' || SQLERRM;
            RAISE data_processing_error;
    END;
    
    -- =============================================================================
    -- COMPLETION AND SUCCESS LOGGING
    -- =============================================================================
    
    v_current_step := 'COMPLETION';
    
    -- Log successful completion
    INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
        PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
    ) VALUES (
        P_PROCESS_KEY, 0, 
        'Process completed successfully. Deleted: ' || v_rows_deleted || 
        ', Inserted: ' || v_rows_inserted || 
        ', Duration: ' || DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP()) || ' seconds',
        CURRENT_TIMESTAMP(), v_current_step
    );
    
    RETURN 'SUCCESS: ACCT_BALN_BKDT_ADJ_RULE processing completed. ' ||
           'Records deleted: ' || v_rows_deleted || ', ' ||
           'Records inserted: ' || v_rows_inserted || ', ' ||
           'Processing time: ' || DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP()) || ' seconds';

-- =============================================================================
-- EXCEPTION HANDLING
-- =============================================================================

EXCEPTION
    WHEN validation_error THEN
        v_error_message := 'Parameter validation failed: ' || COALESCE(v_error_message, 'Invalid input parameters');
        INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
        ) VALUES (
            P_PROCESS_KEY, -20001, v_error_message, CURRENT_TIMESTAMP(), v_current_step
        );
        RETURN 'ERROR: ' || v_error_message;
        
    WHEN business_logic_error THEN
        INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
        ) VALUES (
            P_PROCESS_KEY, -20002, v_error_message, CURRENT_TIMESTAMP(), v_current_step
        );
        RETURN 'ERROR: ' || v_error_message;
        
    WHEN data_processing_error THEN
        -- Attempt rollback of any partial changes
        BEGIN
            ROLLBACK;
        EXCEPTION
            WHEN OTHER THEN
                NULL; -- Ignore rollback errors
        END;
        
        INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
        ) VALUES (
            P_PROCESS_KEY, -20003, v_error_message, CURRENT_TIMESTAMP(), v_current_step
        );
        RETURN 'ERROR: ' || v_error_message;
        
    WHEN OTHER THEN
        v_error_code := SQLCODE;
        v_error_message := 'Unexpected error in step ' || v_current_step || ': ' || SQLERRM;
        
        -- Attempt rollback of any partial changes
        BEGIN
            ROLLBACK;
        EXCEPTION
            WHEN OTHER THEN
                NULL; -- Ignore rollback errors
        END;
        
        INSERT INTO IDENTIFIER(P_ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP, STEP_NAME
        ) VALUES (
            P_PROCESS_KEY, v_error_code, v_error_message, CURRENT_TIMESTAMP(), v_current_step
        );
        RETURN 'FATAL ERROR: ' || v_error_message || ' (Code: ' || v_error_code || ')';
        
END;
$$;
```