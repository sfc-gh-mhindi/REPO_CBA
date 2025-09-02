-- =====================================================================================
-- DATA FLOW DEBUGGING SCRIPT: CSE_CPL_BUS_APP → DEPT_APPT
-- =====================================================================================
-- PURPOSE: Trace a specific record through the entire DBT pipeline
-- USAGE: Replace 'YOUR_PL_APP_ID_HERE' with an actual PL_APP_ID from your source data
-- NOTE: APPT_I format is 'CSEPL' + PL_APP_ID (e.g., if PL_APP_ID='12345', then APPT_I='CSEPL12345')
-- =====================================================================================

-- CONFIGURATION: Set your PL_APP_ID here
SET PL_APP_ID_TO_TRACE = 'YOUR_PL_APP_ID_HERE';

-- =====================================================================================
-- STEP 1: SOURCE TABLE (CSE_CPL_BUS_APP)
-- =====================================================================================
SELECT 
    1 as step_number,
    'CSE_CPL_BUS_APP_SOURCE' as model_name,
    'Raw source data from ingested table' as description,
    COUNT(1) as record_count,
    $PL_APP_ID_TO_TRACE as traced_pl_app_id
FROM NPD_D12_DMN_GDWMIG_IBRG.PDSSTG.CSE_CPL_BUS_APP
WHERE PL_APP_ID = $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 2: INITIAL EXTRACTION (srcplappseq__extpl_app)
-- =====================================================================================
SELECT 
    2 as step_number,
    'srcplappseq__extpl_app' as model_name,
    'Initial extraction with all fields (including MOD_TIMESTAMP)' as description,
    COUNT(1) as record_count,
    $PL_APP_ID_TO_TRACE as traced_pl_app_id
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__srcplappseq__extpl_app
WHERE PL_APP_ID = $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 3: DEDUPLICATION (cpyplappseq__extpl_app) ⚠️ CRITICAL STEP
-- =====================================================================================
SELECT 
    3 as step_number,
    'cpyplappseq__extpl_app' as model_name,
    '🚨 DEDUPLICATION by PL_APP_ID (drops MOD_TIMESTAMP)' as description,
    COUNT(1) as record_count,
    $PL_APP_ID_TO_TRACE as traced_pl_app_id
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__cpyplappseq__extpl_app
WHERE PL_APP_ID = $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 4: BUSINESS RULES (xfmbusinessrules__xfmpl_appfrmext)
-- =====================================================================================
SELECT 
    4 as step_number,
    'xfmbusinessrules__xfmpl_appfrmext' as model_name,
    'Creates svLoadApptDept flag based on NOMINATED_BRANCH_ID' as description,
    COUNT(1) as record_count,
    $PL_APP_ID_TO_TRACE as traced_pl_app_id
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__xfmbusinessrules__xfmpl_appfrmext
WHERE PL_APP_ID = $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 5: BUSINESS FILTER (tmpapptdeptds__xfmpl_appfrmext) ⚠️ CRITICAL FILTER
-- =====================================================================================
SELECT 
    5 as step_number,
    'tmpapptdeptds__xfmpl_appfrmext' as model_name,
    '🚨 FILTERS WHERE svLoadApptDept = Y and creates APPT_I' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__tmpapptdeptds__xfmpl_appfrmext
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 6: DATASET TABLE (Post-hook from tmpapptdeptds__xfmpl_appfrmext)
-- =====================================================================================
SELECT 
    6 as step_number,
    'DATASET_Tmp_CSE_CPL_BUS_APP_APPT_DEPT__DS' as model_name,
    'Intermediate dataset storage via post_hook' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__dataset__Tmp_CSE_CPL_BUS_APP_APPT_DEPT__DS
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 7: LOAD TO TEMP SOURCE (srcapptdeptds__ldtmp_appt_deptrmxfm)
-- =====================================================================================
SELECT 
    7 as step_number,
    'srcapptdeptds__ldtmp_appt_deptrmxfm' as model_name,
    'Source for loading to temp table' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__srcapptdeptds__ldtmp_appt_deptrmxfm
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 8: TEMP TABLE TARGET (tgtapptdepttera__ldtmp_appt_deptrmxfm)
-- =====================================================================================
SELECT 
    8 as step_number,
    'tgtapptdepttera__ldtmp_appt_deptrmxfm' as model_name,
    'Loads to temp table via MERGE operation' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__tgtapptdepttera__ldtmp_appt_deptrmxfm
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 9: PHYSICAL TEMP TABLE (TMP_APPT_DEPT)
-- =====================================================================================
SELECT 
    9 as step_number,
    'TMP_APPT_DEPT' as model_name,
    'Physical temp table storage (post_hook target)' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.GDWSTAG.TMP_APPT_DEPT
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 10: DELTA TRANSFORMATION (xfmcheckdeltaaction__dltappt_deptfrmtmp_appt_dept)
-- =====================================================================================
SELECT 
    10 as step_number,
    'xfmcheckdeltaaction__dltappt_deptfrmtmp_appt_dept' as model_name,
    'Delta processing logic for change detection' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__xfmcheckdeltaaction__dltappt_deptfrmtmp_appt_dept
WHERE NEW_APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 11: FINAL TARGET PREPARATION (tgtdeptapptinsertds__dltappt_deptfrmtmp_appt_dept)
-- =====================================================================================
SELECT 
    11 as step_number,
    'tgtdeptapptinsertds__dltappt_deptfrmtmp_appt_dept' as model_name,
    'Prepares final DEPT_APPT records for insertion' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__tgtdeptapptinsertds__dltappt_deptfrmtmp_appt_dept
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 12: FINAL DATASET (Post-hook from tgtdeptapptinsertds__dltappt_deptfrmtmp_appt_dept)
-- =====================================================================================
SELECT 
    12 as step_number,
    'DATASET_DEPT_APPT_I_CSE_CPL_BUS_APP_20250807__DS' as model_name,
    'Final dataset before target table (post_hook)' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.cba_app__cse_dataload__cse_dataload_dev__dataset__DEPT_APPT_I_CSE_CPL_BUS_APP_20250807__DS
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- STEP 13: FINAL TARGET TABLE (DEPT_APPT) 🎯
-- =====================================================================================
SELECT 
    13 as step_number,
    'DEPT_APPT_FINAL_TARGET' as model_name,
    '🎯 FINAL TARGET TABLE - This is your end result' as description,
    COUNT(1) as record_count,
    'CSEPL' || $PL_APP_ID_TO_TRACE as traced_appt_i
FROM NPD_D12_DMN_GDWMIG_IBRG.GDWSTAG.DEPT_APPT
WHERE APPT_I = 'CSEPL' || $PL_APP_ID_TO_TRACE;

-- =====================================================================================
-- SUMMARY: ALL STEPS COMBINED
-- =====================================================================================
SELECT 
    'SUMMARY' as step_number,
    'ALL_STEPS_COMBINED' as model_name,
    'Summary of record count at each step' as description,
    'Execute individual queries above for detailed analysis' as record_count,
    'Look for steps where count drops to 0' as traced_appt_i;

-- =====================================================================================
-- DEBUGGING TIPS:
-- =====================================================================================
-- 1. If Step 3 count < Step 2 count: You have duplicate PL_APP_ID values
-- 2. If Step 5 count = 0: Your record has NULL/empty NOMINATED_BRANCH_ID
-- 3. If Step 10 count = 0: Delta processing filtered out your record
-- 4. If Step 13 count = 0: Final load failed or record was filtered
-- 
-- ADDITIONAL CHECKS:
-- - Check for duplicate PL_APP_ID: SELECT PL_APP_ID, COUNT(*) FROM source GROUP BY PL_APP_ID HAVING COUNT(*) > 1
-- - Check NOMINATED_BRANCH_ID: SELECT PL_APP_ID, NOMINATED_BRANCH_ID FROM source WHERE PL_APP_ID = 'YOUR_ID'
-- - Check svLoadApptDept flag: SELECT PL_APP_ID, svLoadApptDept FROM business_rules WHERE PL_APP_ID = 'YOUR_ID'
-- ===================================================================================== 