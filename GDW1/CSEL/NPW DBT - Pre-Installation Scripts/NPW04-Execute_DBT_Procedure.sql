CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    step_success BOOLEAN DEFAULT FALSE;
    step_counter INTEGER DEFAULT 0;
    total_steps INTEGER DEFAULT 18;
 
    dbt_sql STRING;
    dbt_stdout_value STRING;
    dbt_exception_value STRING;
   
    -- JSON accumulator for all results
    results_json OBJECT DEFAULT OBJECT_CONSTRUCT();
    steps_array ARRAY DEFAULT ARRAY_CONSTRUCT();
   
    -- Process tracking variables
    process_name STRING DEFAULT 'P_EXECUTE_DBT_CSEL';
    stream_name STRING DEFAULT 'CSEL_DBT_EXECUTION';
BEGIN
 
    -- Log process start and register process instance
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
        SESSION_ID, WAREHOUSE_NAME
    )
    SELECT
        :process_name,
        :stream_name,
        99,
        CURRENT_DATE(),
        'RUNNING',
        10, -- Info
        'Starting CSEL DBT procedure execution with 18 sequential steps',
        'Full CSEL DBT pipeline execution including all appointment product and department processing',
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE();
 
    -- -- Register process instance
    -- INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE (
    --     PROCESS_NAME, STREAM_NAME, MODEL_NAME, PROCESS_STATUS, START_TIMESTAMP,
    --     STATUS_MESSAGE, CREATED_BY, CREATED_TS
    -- )
    -- SELECT
    --     :process_name,
    --     :stream_name,
    --     :process_name,
    --     'RUNNING',
    --     CURRENT_TIMESTAMP(),
    --     'CSEL DBT procedure execution started',
    --     CURRENT_USER(),
    --     CURRENT_TIMESTAMP();
 
    -- Step 0: Control table update
    EXECUTE IMMEDIATE 'UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL SET RUN_STRM_ABRT_F = ''N'', RUN_STRM_ACTV_F = ''I'' WHERE RUN_STRM_C IN (''CSE_L4_PRE_PROC'',''CSE_CPL_BUS_APP'') AND SYST_C = ''CSEL4''';
 
    -- 1. processrunstreamstatuscheck
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamstatuscheck/''';
    LET rs1 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur1 CURSOR FOR rs1;
    OPEN cur1;
    FETCH cur1 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur1;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'processrunstreamstatuscheck',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        -- Log step failure (pattern should be applied to all step failure checks)
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
                         'CSEL DBT procedure execution failed at step ' || :step_counter || ': processrunstreamstatuscheck',
            'Step execution returned failure status: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --                  STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter || ': processrunstreamstatuscheck',
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 2. utilprosisacprevloadcheck
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/utilprosisacprevloadcheck/''';
    LET rs2 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur2 CURSOR FOR rs2;
    OPEN cur2;
    FETCH cur2 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur2;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'utilprosisacprevloadcheck',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
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
            'CSEL DBT procedure execution failed at step ' || :step_counter || ': utilprosisacprevloadcheck',
            'Step execution returned failure status: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --     STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter || ': utilprosisacprevloadcheck',
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 3. loadgdwproskeyseq
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/02processkey/loadgdwproskeyseq/''';
    LET rs3 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur3 CURSOR FOR rs3;
    OPEN cur3;
    FETCH cur3 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur3;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'loadgdwproskeyseq',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
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
            'CSEL DBT procedure execution failed at step ' || :step_counter || ': loadgdwproskeyseq',
            'Step execution returned failure status: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --     STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter || ': loadgdwproskeyseq',
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 4. ldmap_cse_pack_pdct_pllkp
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/04MappingLookupSets/ldmap_cse_pack_pdct_pllkp/''';
    LET rs4 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur4 CURSOR FOR rs4;
    OPEN cur4;
    FETCH cur4 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur4;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldmap_cse_pack_pdct_pllkp',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
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
            'CSEL DBT procedure execution failed at step ' || :step_counter || ': ldmap_cse_pack_pdct_pllkp',
            'Step execution returned failure status: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --     STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter || ': ldmap_cse_pack_pdct_pllkp',
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 5. processrunstreamfinishingpoint
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamfinishingpoint/''';
    LET rs5 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur5 CURSOR FOR rs5;
    OPEN cur5;
    FETCH cur5 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur5;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'processrunstreamfinishingpoint',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
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
            'CSEL DBT procedure execution failed at step ' || :step_counter || ': processrunstreamfinishingpoint',
            'Step execution returned failure status: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --     STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter || ': processrunstreamfinishingpoint',
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 6. processrunstreamstatuscheck with run_stream var
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamstatuscheck/  --vars ''''{"run_stream": "CSE_CPL_BUS_APP"}''''''';
    LET rs6 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur6 CURSOR FOR rs6;
    OPEN cur6;
    FETCH cur6 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur6;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'processrunstreamstatuscheck_with_var',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
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
            'CSEL DBT procedure execution failed at step ' || :step_counter || ': processrunstreamstatuscheck_with_var',
            'Step execution returned failure status: ' || COALESCE(:dbt_exception_value, 'No exception details'),
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --     STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter || ': processrunstreamstatuscheck_with_var',
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 7. extpl_app
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/08extraction/cplbusapp/extpl_app/''';
    LET rs7 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur7 CURSOR FOR rs7;
    OPEN cur7;
    FETCH cur7 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur7;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'extpl_app',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 8. xfmpl_appfrmext
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/12MappingTransformation/CplBusApp/xfmpl_appfrmext/''';
    LET rs8 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur8 CURSOR FOR rs8;
    OPEN cur8;
    FETCH cur8 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur8;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'xfmpl_appfrmext',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 9. ldtmp_appt_deptrmxfm
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/14loadtotemp/ldtmp_appt_deptrmxfm/''';
    LET rs9 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur9 CURSOR FOR rs9;
    OPEN cur9;
    FETCH cur9 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur9;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldtmp_appt_deptrmxfm',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 10. dltappt_deptfrmtmp_appt_dept
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/16transformdelta/dltappt_deptfrmtmp_appt_dept/''';
    LET rs10 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur10 CURSOR FOR rs10;
    OPEN cur10;
    FETCH cur10 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur10;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'dltappt_deptfrmtmp_appt_dept',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 11. ldapptdeptupd
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptupd/''';
    LET rs11 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur11 CURSOR FOR rs11;
    OPEN cur11;
    FETCH cur11 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur11;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldapptdeptupd',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 12. ldapptdeptins
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptins/''';
    LET rs12 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur12 CURSOR FOR rs12;
    OPEN cur12;
    FETCH cur12 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur12;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldapptdeptins',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 13. ldtmp_appt_pdctfrmxfm
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/appt_pdct/ldtmp_appt_pdctfrmxfm/''';
    LET rs13 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur13 CURSOR FOR rs13;
    OPEN cur13;
    FETCH cur13 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur13;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldtmp_appt_pdctfrmxfm',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 14. dltappt_pdctfrmtmp_appt_pdct
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/appt_pdct/dltappt_pdctfrmtmp_appt_pdct/''';
    LET rs14 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur14 CURSOR FOR rs14;
    OPEN cur14;
    FETCH cur14 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur14;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'dltappt_pdctfrmtmp_appt_pdct',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 15. ldapptpdctupd
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_pdct/ldapptpdctupd/''';
    LET rs15 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur15 CURSOR FOR rs15;
    OPEN cur15;
    FETCH cur15 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur15;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldapptpdctupd',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 16. ldapptpdctins
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/appt_pdct/ldapptpdctins/''';
    LET rs16 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur16 CURSOR FOR rs16;
    OPEN cur16;
    FETCH cur16 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur16;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldapptpdctins',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 17. ldapptdeptupd with tgt_table=dept_appt
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptupd/ --vars ''''{"tgt_table": "dept_appt"}''''''';
    LET rs17 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur17 CURSOR FOR rs17;
    OPEN cur17;
    FETCH cur17 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur17;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldapptdeptupd_dept_appt',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- 18. ldapptdeptins with tgt_table=dept_appt
    step_counter := step_counter + 1;
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptins/ --vars ''''{"tgt_table": "dept_appt"}''''''';
    LET rs18 RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur18 CURSOR FOR rs18;
    OPEN cur18;
    FETCH cur18 INTO step_success, dbt_exception_value, dbt_stdout_value;
    CLOSE cur18;
 
    steps_array := ARRAY_APPEND(steps_array, OBJECT_CONSTRUCT(
        'step_number', step_counter, 'step_name', 'ldapptdeptins_dept_appt',
        'success_flag', step_success, 'exception_value', dbt_exception_value,
        'stdout_value', dbt_stdout_value, 'timestamp', CURRENT_TIMESTAMP()::STRING));
 
    IF (NVL(LOWER(step_success::VARCHAR), 'false') != 'true') THEN
        results_json := OBJECT_CONSTRUCT('total_steps', step_counter, 'final_status', 'FAILED', 'failed_at_step', step_counter, 'steps', steps_array);
        RETURN results_json::STRING;
    END IF;
 
    -- SUCCESS - All steps completed
   
    -- Log successful completion
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
        'CSEL DBT procedure execution completed successfully - all ' || :step_counter || ' steps executed',
        'All 18 DBT model steps completed without errors',
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE();
 
    -- -- Update process instance status to completed
    -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
    -- SET
    --     PROCESS_STATUS = 'COMPLETED',
    --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
    --     STATUS_MESSAGE = 'CSEL DBT procedure execution completed successfully - all ' || :step_counter || ' steps executed',
    --     UPDATED_BY = CURRENT_USER(),
    --     UPDATED_TS = CURRENT_TIMESTAMP()
    -- WHERE PROCESS_NAME = :process_name
    --   AND STREAM_NAME = :stream_name
    --   AND PROCESS_STATUS = 'RUNNING';
 
    results_json := OBJECT_CONSTRUCT(
        'total_steps', step_counter,
        'final_status', 'SUCCESS',
        'completed_at', CURRENT_TIMESTAMP()::STRING,
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
            'CSEL DBT procedure execution failed with exception',
            'Failed at step ' || :step_counter || ' - Exception occurred during execution',
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_SESSION(),
            CURRENT_WAREHOUSE();
 
        -- -- Update process instance status to failed
        -- UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.NPW_T_PROCESS_INSTANCE
        -- SET
        --     PROCESS_STATUS = 'FAILED',
        --     END_TIMESTAMP = CURRENT_TIMESTAMP(),
        --     STATUS_MESSAGE = 'CSEL DBT procedure execution failed at step ' || :step_counter,
        --     UPDATED_BY = CURRENT_USER(),
        --     UPDATED_TS = CURRENT_TIMESTAMP()
        -- WHERE PROCESS_NAME = :process_name
        --   AND STREAM_NAME = :stream_name
        --   AND PROCESS_STATUS = 'RUNNING';
 
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
 
CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL();