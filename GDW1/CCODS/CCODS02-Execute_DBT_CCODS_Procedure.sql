CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CCODS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    step_success BOOLEAN DEFAULT FALSE;
    step_counter INTEGER DEFAULT 0;
    total_steps INTEGER DEFAULT 2;
 
    dbt_sql STRING;
    dbt_stdout_value STRING;
    dbt_exception_value STRING;
   
    -- JSON accumulator for all results
    results_json OBJECT DEFAULT OBJECT_CONSTRUCT();
    steps_array ARRAY DEFAULT ARRAY_CONSTRUCT();
   
    -- Process tracking variables
    process_name STRING DEFAULT 'P_EXECUTE_DBT_CCODS';
    stream_name STRING DEFAULT 'CCODS_DBT_EXECUTION';
BEGIN
 
    -- Log process start
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
        PRCS_NAME,
        STRM_NAME,
        STRM_ID,
        BUSINESS_DATE,
        STEP_STATUS,
        MESSAGE_TYPE,
        MESSAGE_TEXT,
        SQL_TEXT,
        CREATED_BY,
        CREATED_TS,
        SESSION_ID, 
        WAREHOUSE_NAME
    )
    SELECT
        :process_name,
        :stream_name,
        99,
        CURRENT_DATE(),
        'RUNNING',
        10, -- Info
        'Starting CCODS DBT procedure execution with 2 sequential model groups',
        'CCODS PLAN_BALN_SEGM_MSTR_NPW pipeline execution including transformation and load phases',
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE();
 
    -- Step 0: Control table update for CCODS
    EXECUTE IMMEDIATE 'UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL SET RUN_STRM_ABRT_F = ''N'', RUN_STRM_ACTV_F = ''I'' WHERE RUN_STRM_C IN (''BCFINSG'') AND SYST_C = ''CCODS''';
 
    -- 1. Execute CCODS 40_transform models (xfmplanbalnsegmmstrfrombcfinsg)
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/ccods/40_transform/xfmplanbalnsegmmstrfrombcfinsg/''';
    LET rs1 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur1 CURSOR FOR rs1;
    OPEN cur1;
    FETCH cur1 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur1;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 
        'step_name', 'ccods_40_transform_xfmplanbalnsegmmstrfrombcfinsg',
        'success_flag', step_success, 
        'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 
        'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        -- Log step failure
        INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
            PRCS_NAME, STRM_NAME, STRM_ID, BUSINESS_DATE, STEP_STATUS, MESSAGE_TYPE,
            MESSAGE_TEXT, SQL_TEXT, CREATED_BY, CREATED_TS, SESSION_ID, WAREHOUSE_NAME
        )
        SELECT
            :process_name,
            :stream_name,
            99,
            CURRENT_DATE(),
            'FAILED',
            11, -- Error
            'CCODS DBT procedure execution failed at step ' || :step_counter || ': 40_transform xfmplanbalnsegmmstrfrombcfinsg',
            'Transformation step failed - BCFINSG data transformation and validation failed: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;

    -- Log successful completion of transformation step
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
        PRCS_NAME, STRM_NAME, STRM_ID, BUSINESS_DATE, STEP_STATUS, MESSAGE_TYPE,
        MESSAGE_TEXT, SQL_TEXT, CREATED_BY, CREATED_TS, SESSION_ID, WAREHOUSE_NAME
    )
    SELECT
        :process_name,
        :stream_name,
        1,
        CURRENT_DATE(),
        'COMPLETED',
        10, -- Info
        'CCODS transformation step completed successfully',
        'All BCFINSG transformation models executed: before, bcfinsg, xfmtotable, plan_baln_segm_mstr_i transformation',
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE();
 
    -- 2. Execute CCODS 60_load_gdw models (ldbcfinsgplanbalnsegmmstr)
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/ccods/60_load_gdw/ldbcfinsgplanbalnsegmmstr/''';
    LET rs2 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur2 CURSOR FOR rs2;
    OPEN cur2;
    FETCH cur2 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur2;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 
        'step_name', 'ccods_60_load_gdw_ldbcfinsgplanbalnsegmmstr',
        'success_flag', step_success, 
        'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 
        'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        -- Log step failure
        INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
            PRCS_NAME, STRM_NAME, STRM_ID, BUSINESS_DATE, STEP_STATUS, MESSAGE_TYPE,
            MESSAGE_TEXT, SQL_TEXT, CREATED_BY, CREATED_TS, SESSION_ID, WAREHOUSE_NAME
        )
        SELECT
            :process_name,
            :stream_name,
            99,
            CURRENT_DATE(),
            'FAILED',
            11, -- Error
            'CCODS DBT procedure execution failed at step ' || :step_counter || ': 60_load_gdw ldbcfinsgplanbalnsegmmstr',
            'GDW load step failed - PLAN_BALN_SEGM_MSTR_NPW table population failed: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;

    -- Log successful completion of load step
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
        PRCS_NAME, STRM_NAME, STRM_ID, BUSINESS_DATE, STEP_STATUS, MESSAGE_TYPE,
        MESSAGE_TEXT, SQL_TEXT, CREATED_BY, CREATED_TS, SESSION_ID, WAREHOUSE_NAME
    )
    SELECT
        :process_name,
        :stream_name,
        2,
        CURRENT_DATE(),
        'COMPLETED',
        10, -- Info
        'CCODS GDW load step completed successfully',
        'PLAN_BALN_SEGM_MSTR_NPW table populated successfully via plan_baln_segm_mstr_i and plan_baln_segm_mstr models',
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE();
 
    -- SUCCESS - All steps completed
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
        PRCS_NAME, STRM_NAME, STRM_ID, BUSINESS_DATE, STEP_STATUS, MESSAGE_TYPE,
        MESSAGE_TEXT, SQL_TEXT, CREATED_BY, CREATED_TS, SESSION_ID, WAREHOUSE_NAME
    )
    SELECT
        :process_name,
        :stream_name,
        99,
        CURRENT_DATE(),
        'COMPLETED',
        10, -- Info
        'CCODS DBT procedure execution completed successfully - all ' || :step_counter || ' model groups executed',
        'PLAN_BALN_SEGM_MSTR_NPW pipeline completed: transformation and GDW load phases successful',
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE();
 
    results_json := OBJECT_CONSTRUCT(
        'total_steps', step_counter,
        'final_status', 'SUCCESS',
        'completed_at', CURRENT_TIMESTAMP()::STRING,
        'target_table', 'PLAN_BALN_SEGM_MSTR_NPW',
        'process_summary', 'BCFINSG data transformed and loaded to GDW successfully',
        'steps', steps_array
    );
   
    RETURN results_json::STRING;
 
EXCEPTION
    WHEN OTHER THEN
        -- Log exception/failure
        INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG (
            PRCS_NAME, STRM_NAME, STRM_ID, BUSINESS_DATE, STEP_STATUS, MESSAGE_TYPE,
            MESSAGE_TEXT, SQL_TEXT, CREATED_BY, CREATED_TS, SESSION_ID, WAREHOUSE_NAME
        )
        SELECT
            :process_name,
            :stream_name,
            99,
            CURRENT_DATE(),
            'FAILED',
            11, -- Error
            'CCODS DBT procedure execution failed with exception',
            'Failed at step ' || :step_counter || ' - Exception: ' || SQLERRM,
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        results_json := OBJECT_CONSTRUCT(
            'total_steps', step_counter,
            'final_status', 'EXCEPTION',
            'error_message', SQLERRM,
            'failed_at_step', step_counter,
            'steps', steps_array
        );
        RETURN results_json::STRING;
END;
$$;

-- Test execution (commented out - uncomment to test)
-- CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CCODS(); 