-- =====================================================================================
-- DERV_ACCT_PATY_CHG TROUBLESHOOTING QUERIES
-- =====================================================================================
-- PURPOSE: Debug why 8 records are missing in Snowflake version vs Teradata
-- ISSUE: acct_i = 'S1' on efft_d = '2025-03-19' exists in TD but not in SF
-- =====================================================================================

-- CONFIGURATION: Set your test values
SET acct_test = 'S1';
SET efft_test = '2025-03-19';
SET extract_date = '2025-07-10'; -- Replace with actual extract date used

-- =====================================================================================
-- STEP 1: CHECK SOURCE DATA DIFFERENCES
-- =====================================================================================

-- 1.1 Check if the record exists in the source DERV_ACCT_PATY_FLAG table (Snowflake)
SELECT 'SF_DERV_ACCT_PATY_FLAG' as source_table, COUNT(*) as record_count
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
WHERE T1.ACCT_I = $acct_test
  AND T1.EFFT_D = $efft_test
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D;

-- 1.2 Check all records for this account in FLAG table
SELECT 'SF_FLAG_ALL_RECORDS' as check_type, 
       ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D, 
       ASSC_ACCT_I, PRFR_PATY_F, SRCE_SYST_C
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG
WHERE ACCT_I = $acct_test
ORDER BY EFFT_D, PATY_I, PATY_ACCT_REL_C;

-- 1.3 Check if record exists in target DERV_ACCT_PATY table (what we're comparing against)
SELECT 'SF_TARGET_DERV_ACCT_PATY' as source_table, COUNT(*) as record_count,
       MIN(EFFT_D) as min_efft_d, MAX(EXPY_D) as max_expy_d
FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
WHERE T2.ACCT_I = $acct_test
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D;

-- =====================================================================================
-- STEP 2: CHECK JOIN CONDITIONS FOR MISSING RECORD
-- =====================================================================================

-- 2.1 Test the exact JOIN condition that creates CHG records
SELECT 'JOIN_TEST' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.PATY_ACCT_REL_C, T1.EFFT_D as FLAG_EFFT_D,
       T2.ACCT_I as TARGET_ACCT_I, T2.EFFT_D as TARGET_EFFT_D, T2.EXPY_D as TARGET_EXPY_D,
       -- Show the comparison fields
       T1.ASSC_ACCT_I as FLAG_ASSC_ACCT_I, T2.ASSC_ACCT_I as TARGET_ASSC_ACCT_I,
       T1.PRFR_PATY_F as FLAG_PRFR_PATY_F, T2.PRFR_PATY_F as TARGET_PRFR_PATY_F,
       T1.SRCE_SYST_C as FLAG_SRCE_SYST_C, T2.SRCE_SYST_C as TARGET_SRCE_SYST_C,
       -- Show the actual comparison results
       CASE WHEN TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I)) THEN 'DIFF_ASSC_ACCT' ELSE 'SAME_ASSC_ACCT' END as ASSC_ACCT_COMPARISON,
       CASE WHEN TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F)) THEN 'DIFF_PRFR_PATY' ELSE 'SAME_PRFR_PATY' END as PRFR_PATY_COMPARISON,
       CASE WHEN TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C)) THEN 'DIFF_SRCE_SYST' ELSE 'SAME_SRCE_SYST' END as SRCE_SYST_COMPARISON
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)
WHERE T1.ACCT_I = $acct_test
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D;

-- 2.2 Check if the WHERE condition filters out our record
SELECT 'WHERE_CONDITION_TEST' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.PATY_ACCT_REL_C,
       -- Check each part of the WHERE condition
       (TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))) as ASSC_ACCT_DIFFERS,
       (TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))) as PRFR_PATY_DIFFERS,
       (TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C))) as SRCE_SYST_DIFFERS,
       -- Overall condition
       ((TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))
         OR TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))
         OR TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C)))) as SHOULD_BE_IN_CHG
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)
WHERE T1.ACCT_I = $acct_test
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D;

-- =====================================================================================
-- STEP 3: CHECK TRIM vs NO-TRIM DIFFERENCES (KEY DIFFERENCE BETWEEN TD AND SF)
-- =====================================================================================

-- 3.1 Compare Teradata logic (no TRIM on comparison) vs Snowflake logic (with TRIM)
SELECT 'TRIM_COMPARISON_TEST' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.PATY_ACCT_REL_C,
       -- Teradata style comparison (no TRIM on field comparison)
       (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I) as TD_ASSC_ACCT_DIFFERS,
       (T1.PRFR_PATY_F <> T2.PRFR_PATY_F) as TD_PRFR_PATY_DIFFERS,
       (T1.SRCE_SYST_C <> T2.SRCE_SYST_C) as TD_SRCE_SYST_DIFFERS,
       -- Snowflake style comparison (with TRIM/UPPER)
       (TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))) as SF_ASSC_ACCT_DIFFERS,
       (TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))) as SF_PRFR_PATY_DIFFERS,
       (TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C))) as SF_SRCE_SYST_DIFFERS,
       -- Show actual values for comparison
       '[' || T1.ASSC_ACCT_I || ']' as FLAG_ASSC_ACCT_RAW,
       '[' || T2.ASSC_ACCT_I || ']' as TARGET_ASSC_ACCT_RAW,
       '[' || T1.PRFR_PATY_F || ']' as FLAG_PRFR_PATY_RAW,
       '[' || T2.PRFR_PATY_F || ']' as TARGET_PRFR_PATY_RAW
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)
WHERE T1.ACCT_I = $acct_test
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D;

-- =====================================================================================
-- STEP 4: CHECK ACTUAL CHG TABLE CONTENTS
-- =====================================================================================

-- 4.1 Check what's actually in the CHG table
SELECT 'CHG_TABLE_CONTENTS' as check_type, 
       ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D,
       ASSC_ACCT_I, PRFR_PATY_F, SRCE_SYST_C
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
WHERE ACCT_I = $acct_test
ORDER BY EFFT_D, PATY_I, PATY_ACCT_REL_C;

-- 4.2 Check if any records for this account exist in CHG table
SELECT 'CHG_TABLE_ALL_RECORDS' as check_type, COUNT(*) as total_records
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
WHERE ACCT_I = $acct_test;

-- =====================================================================================
-- STEP 5: CHECK DATA PIPELINE DEPENDENCIES
-- =====================================================================================

-- 5.1 Check if DERV_ACCT_PATY_FLAG was populated correctly
SELECT 'FLAG_TABLE_RECORD_COUNT' as check_type, COUNT(*) as total_records,
       MIN(EFFT_D) as min_efft_d, MAX(EXPY_D) as max_expy_d
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG
WHERE ACCT_I = $acct_test;

-- 5.2 Check if the source ACCT_PATY table has the record
SELECT 'SOURCE_ACCT_PATY' as check_type, COUNT(*) as record_count,
       ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D
FROM NPD_D12_DMN_GDWMIG_IBRG_V.pvtech.ACCT_PATY
WHERE ACCT_I = $acct_test
  AND $extract_date BETWEEN EFFT_D AND EXPY_D
GROUP BY ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D
ORDER BY EFFT_D;

-- =====================================================================================
-- STEP 6: REPRODUCE THE EXACT TERADATA LOGIC IN SNOWFLAKE
-- =====================================================================================

-- 6.1 Run the exact Teradata logic to see what should be in CHG table
SELECT 'SIMULATE_TD_LOGIC' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C,
       T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, T1.ROW_SECU_ACCS_C
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C  -- Note: No TRIM here like TD
WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I        -- Note: No TRIM/UPPER here like TD
    OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
    OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C)
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  AND T1.ACCT_I = $acct_test;

-- =====================================================================================
-- STEP 7: CHECK FOR EXECUTION ISSUES
-- =====================================================================================

-- 7.1 Check if the stored procedure was executed with correct parameters
-- This requires checking the execution logs or process audit tables
SELECT 'PROC_EXECUTION_CHECK' as check_type,
       'Check NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG for derv_acct_paty_07_crat_deltas execution' as instruction;

-- 7.2 Check the process execution date parameter
SELECT 'EXTRACT_DATE_CHECK' as check_type,
       $extract_date as configured_extract_date,
       'Verify this matches the actual extract date used in the process' as instruction;

-- =====================================================================================
-- DEBUGGING SUMMARY AND RECOMMENDATIONS:
-- =====================================================================================

-- Summary query to show all key checks
SELECT 'DEBUGGING_SUMMARY' as section,
       'Execute the above queries in sequence to identify the root cause' as next_steps,
       'Key areas to check:' as focus_areas,
       '1. Source data differences between FLAG and target tables' as check_1,
       '2. JOIN condition matching (especially TRIM differences)' as check_2,
       '3. WHERE condition evaluation (TD vs SF logic)' as check_3,
       '4. Data type and encoding differences' as check_4,
       '5. Process execution parameters and timing' as check_5;

-- =====================================================================================
-- EXPECTED ROOT CAUSES:
-- =====================================================================================
-- 1. TRIM/UPPER differences: SF uses TRIM(UPPER()) while TD uses direct comparison
-- 2. JOIN condition differences: SF uses TRIM on PATY_ACCT_REL_C, TD doesn't
-- 3. Data encoding/spacing issues: Fields may have trailing spaces in TD but not SF
-- 4. Extract date parameter: Different extract dates between TD and SF runs
-- 5. Source data timing: FLAG table populated at different times or with different data
-- ===================================================================================== 