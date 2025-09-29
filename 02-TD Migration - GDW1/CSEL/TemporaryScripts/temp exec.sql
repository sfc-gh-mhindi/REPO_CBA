DECLARE
    dbt_sql STRING;
    -- rs RESULTSET;
    -- cur CURSOR FOR rs;
    success_flag BOOLEAN;
    stdout_value STRING;
    exception_value STRING;
  
    -- JSON accumulator for all results
    results_json OBJECT DEFAULT OBJECT_CONSTRUCT();
    steps_array ARRAY DEFAULT ARRAY_CONSTRUCT();
    step_counter INT DEFAULT 0;
  
BEGIN
    -- Initialize the results object
    results_json := OBJECT_CONSTRUCT('total_steps', 0, 'steps', ARRAY_CONSTRUCT());
 
    EXECUTE IMMEDIATE 'UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL SET RUN_STRM_ABRT_F = ''N'', RUN_STRM_ACTV_F = ''I'' WHERE RUN_STRM_C IN (''CSE_L4_PRE_PROC'',''CSE_CPL_BUS_APP'') AND SYST_C = ''CSEL4''';
 
    ------------------------------------------------------------------------------------------------------------
    -- STEP 1
    ------------------------------------------------------------------------------------------------------------
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamstatuscheck/''';
 
    -- Execute and capture result
    LET rs RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur CURSOR FOR rs;
  
    OPEN cur;
    FETCH cur INTO success_flag, exception_value, stdout_value;
    CLOSE cur;
  
    -- Add step result to the steps array
    steps_array := ARRAY_APPEND(
        steps_array,
        OBJECT_CONSTRUCT(
            'step_number', step_counter,
            'step_name', 'processrunstreamstatuscheck_1',
            'success_flag', success_flag,
            'exception_value', exception_value,
            'stdout_value', stdout_value,
            'timestamp', CURRENT_TIMESTAMP()::STRING
        )
    );
 
    IF (NVL(LOWER(success_flag::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT(
            'total_steps', step_counter,
            'final_status', 'FAILED',
            'failed_at_step', step_counter,
            'steps', steps_array
        );
        RETURN results_json::STRING;
    END IF;
 
    ------------------------------------------------------------------------------------------------------------
    -- STEP 2
    ------------------------------------------------------------------------------------------------------------
    EXECUTE IMMEDIATE 'UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL SET RUN_STRM_ABRT_F = ''N'', RUN_STRM_ACTV_F = ''I'' WHERE RUN_STRM_C IN (''CSE_L4_PRE_PROC'',''CSE_CPL_BUS_APP'') AND SYST_C = ''CSEL4''';
   
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamstatuscheck/''';
  
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    -- LET cur CURSOR FOR rs;
  
    OPEN cur;
    FETCH cur INTO success_flag, exception_value, stdout_value;
    CLOSE cur;
  
    -- Add step result to the steps array
    steps_array := ARRAY_APPEND(
        steps_array,
        OBJECT_CONSTRUCT(
            'step_number', step_counter,
            'step_name', 'processrunstreamstatuscheck_2',
            'success_flag', success_flag,
            'exception_value', exception_value,
            'stdout_value', stdout_value,
            'timestamp', CURRENT_TIMESTAMP()::STRING
        )
    );
 
    IF (NVL(LOWER(success_flag::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT(
            'total_steps', step_counter,
            'final_status', 'FAILED',
            'failed_at_step', step_counter,
            'steps', steps_array
        );
        RETURN results_json::STRING;
    END IF;
 
    ------------------------------------------------------------------------------------------------------------
    -- SUCCESS - All steps completed
    ------------------------------------------------------------------------------------------------------------
    results_json := OBJECT_CONSTRUCT(
        'total_steps', step_counter,
        'final_status', 'SUCCESS',
        'completed_at', CURRENT_TIMESTAMP()::STRING,
        'steps', steps_array
    );
  
    RETURN results_json::STRING;
END;