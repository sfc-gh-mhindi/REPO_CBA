use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;
 
 
CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    result_message STRING DEFAULT '';
    step_success BOOLEAN DEFAULT FALSE;
    step_counter INTEGER DEFAULT 0;
    total_steps INTEGER DEFAULT 16;
 
    dbt_sql STRING;
    -- rs RESULTSET;
    -- cur CURSOR FOR rs;
BEGIN
 
    result_message := '[' || CURRENT_TIMESTAMP()::STRING || '] Starting NPW DBT Script execution (procedure)...\n';
 
    -- Step 0: Control table update
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step 0: Resetting control flags...\n';
    EXECUTE IMMEDIATE 'UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL SET RUN_STRM_ABRT_F = ''N'', RUN_STRM_ACTV_F = ''I'' WHERE RUN_STRM_C IN (''CSE_L4_PRE_PROC'',''CSE_CPL_BUS_APP'') AND SYST_C = ''CSEL4''';
 
    /* Helper block to run one DBT command and check SUCCESS */
    -- Usage: set dbt_sql; execute; fetch SUCCESS; if false, return
 
    -- 1. processrunstreamstatuscheck
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': processrunstreamstatuscheck...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamstatuscheck/''';
    LET rs RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    LET cur CURSOR FOR rs;
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
   
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: processrunstreamstatuscheck failed\n';
        RETURN result_message;
    END IF;
 
    -- 2. utilprosisacprevloadcheck
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': utilprosisacprevloadcheck...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/utilprosisacprevloadcheck/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: utilprosisacprevloadcheck failed\n';
        RETURN result_message;
    END IF;
 
    -- 3. loadgdwproskeyseq
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': loadgdwproskeyseq...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/02processkey/loadgdwproskeyseq/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: loadgdwproskeyseq failed\n';
        RETURN result_message;
    END IF;
 
    -- 4. ldmap_cse_pack_pdct_pllkp
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldmap_cse_pack_pdct_pllkp...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/04MappingLookupSets/ldmap_cse_pack_pdct_pllkp/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldmap_cse_pack_pdct_pllkp failed\n';
        RETURN result_message;
    END IF;
 
    -- 5. processrunstreamfinishingpoint
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': processrunstreamfinishingpoint...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamfinishingpoint/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: processrunstreamfinishingpoint failed\n';
        RETURN result_message;
    END IF;
 
    -- 6. processrunstreamstatuscheck with run_stream var
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': processrunstreamstatuscheck (CSE_CPL_BUS_APP)...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/24processmetadata/processrunstreamstatuscheck/  --vars ''''{"run_stream": "CSE_CPL_BUS_APP"}''''''';
    -- result_message := result_message || 'ERROR: Failing Statement: ' || dbt_sql || '\n';
    -- RETURN result_message;
    -- LET rs RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    -- LET cur CURSOR FOR rs;
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: processrunstreamstatuscheck (with var) failed\n';
        RETURN result_message;
    END IF;
 
    -- 7. extpl_app
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': extpl_app...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/08extraction/cplbusapp/extpl_app/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: extpl_app failed\n';
        RETURN result_message;
    END IF;
 
    -- 8. xfmpl_appfrmext
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': xfmpl_appfrmext...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/12MappingTransformation/CplBusApp/xfmpl_appfrmext/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: xfmpl_appfrmext failed\n';
        RETURN result_message;
    END IF;
 
    -- 9. ldtmp_appt_deptrmxfm
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldtmp_appt_deptrmxfm...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/14loadtotemp/ldtmp_appt_deptrmxfm/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldtmp_appt_deptrmxfm failed\n';
        RETURN result_message;
    END IF;
 
    -- 10. dltappt_deptfrmtmp_appt_dept
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': dltappt_deptfrmtmp_appt_dept...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/16transformdelta/dltappt_deptfrmtmp_appt_dept/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: dltappt_deptfrmtmp_appt_dept failed\n';
        RETURN result_message;
    END IF;
 
    -- 11. ldapptdeptupd
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldapptdeptupd...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptupd/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldapptdeptupd failed\n';
        RETURN result_message;
    END IF;
 
    -- 12. ldapptdeptins
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldapptdeptins...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptins/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldapptdeptins failed\n';
        RETURN result_message;
    END IF;
 
    -- 13. ldtmp_appt_pdctfrmxfm
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldtmp_appt_pdctfrmxfm...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/appt_pdct/ldtmp_appt_pdctfrmxfm/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldtmp_appt_pdctfrmxfm failed\n';
        RETURN result_message;
    END IF;
 
    -- 14. dltappt_pdctfrmtmp_appt_pdct
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': dltappt_pdctfrmtmp_appt_pdct...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/appt_pdct/dltappt_pdctfrmtmp_appt_pdct/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: dltappt_pdctfrmtmp_appt_pdct failed\n';
        RETURN result_message;
    END IF;
 
    -- 15. ldapptpdctupd
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldapptpdctupd...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_pdct/ldapptpdctupd/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldapptpdctupd failed\n';
        RETURN result_message;
    END IF;
 
    -- 16. ldapptpdctins
    step_counter := step_counter + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldapptpdctins...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/appt_pdct/ldapptpdctins/''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldapptpdctins failed\n';
        RETURN result_message;
    END IF;
 
    -- 17. ldapptdeptupd with tgt_table=dept_appt
    step_counter := step_counter + 1;
    total_steps := total_steps + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldapptdeptupd (tgt_table=dept_appt)...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptupd/ --vars ''''{"tgt_table": "dept_appt"}''''''';
    -- LET rs RESULTSET := (EXECUTE IMMEDIATE :dbt_sql);
    -- LET cur CURSOR FOR rs;
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldapptdeptupd (tgt_table=dept_appt) failed\n';
        RETURN result_message;
    END IF;
 
    -- 18. ldapptdeptins with tgt_table=dept_appt
    step_counter := step_counter + 1;
    total_steps := total_steps + 1;
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Step ' || step_counter || ': ldapptdeptins (tgt_table=dept_appt)...\n';
    dbt_sql := 'EXECUTE DBT PROJECT NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT args=''run --select models/cse_dataload/18loadtogdw/appt_dept/ldapptdeptins/ --vars ''''{"tgt_table": "dept_appt"}''''''';
    rs := (EXECUTE IMMEDIATE :dbt_sql);
    OPEN cur;
    FETCH cur INTO step_success;
    CLOSE cur;
    IF (step_success != TRUE) THEN
        result_message := result_message || 'ERROR: ldapptdeptins (tgt_table=dept_appt) failed\n';
        RETURN result_message;
    END IF;
 
    -- Completed
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] ============================================\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] DBT SCRIPT EXECUTION COMPLETED SUCCESSFULLY\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] Steps executed: ' || step_counter || ' of ' || total_steps || '\n';
    result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] ============================================\n';
    RETURN result_message;
 
EXCEPTION
    WHEN OTHER THEN
        result_message := result_message || '[' || CURRENT_TIMESTAMP()::STRING || '] ERROR: ' || SQLERRM || '\n';
        RETURN result_message;
END;
$$;
 
-- CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL();