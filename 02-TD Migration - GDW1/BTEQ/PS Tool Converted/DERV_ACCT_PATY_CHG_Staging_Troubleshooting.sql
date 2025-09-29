-- =====================================================================================
-- DERV_ACCT_PATY_CHG STAGING TABLE TROUBLESHOOTING QUERIES
-- =====================================================================================
-- PURPOSE: Debug why 8 records are missing in Snowflake PDDSTG.DERV_ACCT_PATY_CHG vs Teradata
-- ISSUE: acct_i = 'S1' on efft_d = '2025-03-19' exists in TD staging but not in SF staging
-- FOCUS: Comparing PDDSTG.DERV_ACCT_PATY_CHG between TD and SF (not PVTECH)
-- =====================================================================================

-- CONFIGURATION: Set your test values
SET acct_test = 'S1';
SET efft_test = '2025-03-19';
SET extract_date = '2025-07-10'; -- Replace with actual extract date used in the process

-- =====================================================================================
-- STEP 1: COMPARE STAGING TABLE CONTENTS DIRECTLY
-- =====================================================================================

-- 1.1 Check what's in Snowflake staging CHG table for the test account
SELECT 'SF_STAGING_CHG_TABLE' as source_table,
       ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D,
       ASSC_ACCT_I, PRFR_PATY_F, SRCE_SYST_C, ROW_SECU_ACCS_C
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
WHERE ACCT_I = $acct_test
ORDER BY EFFT_D, PATY_I, PATY_ACCT_REL_C;

-- 1.2 Count total records in SF staging CHG table for this account
SELECT 'SF_STAGING_CHG_COUNT' as check_type, COUNT(*) as record_count
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
WHERE ACCT_I = $acct_test;

-- 1.3 Check if the specific missing record should exist based on effective date
SELECT 'SF_STAGING_CHG_SPECIFIC_DATE' as check_type, COUNT(*) as record_count
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
WHERE ACCT_I = $acct_test
  AND EFFT_D = $efft_test;

-- =====================================================================================
-- STEP 2: TRACE THE STAGING CHG CREATION PROCESS
-- =====================================================================================

-- 2.1 Check source FLAG table that feeds into CHG table
SELECT 'SF_FLAG_TABLE_CONTENTS' as source_table,
       ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D,
       ASSC_ACCT_I, PRFR_PATY_F, SRCE_SYST_C, ROW_SECU_ACCS_C
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG
WHERE ACCT_I = $acct_test
ORDER BY EFFT_D, PATY_I, PATY_ACCT_REL_C;

-- 2.2 Check if the FLAG table has the missing record
SELECT 'SF_FLAG_SPECIFIC_DATE' as check_type, COUNT(*) as record_count
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG
WHERE ACCT_I = $acct_test
  AND EFFT_D = $efft_test
  AND $extract_date BETWEEN EFFT_D AND EXPY_D;

-- =====================================================================================
-- STEP 3: REPRODUCE THE CHG CREATION LOGIC (EXACT SF vs TD COMPARISON)
-- =====================================================================================

-- 3.1 Simulate the Snowflake CHG creation logic
-- This is the exact logic from derv_acct_paty_07_crat_deltas_proc.sql
SELECT 'SF_CHG_CREATION_SIMULATION' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C,
       T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, T1.ROW_SECU_ACCS_C,
       -- Show what the comparison fields look like
       T1.ASSC_ACCT_I as FLAG_ASSC_ACCT_I, T2.ASSC_ACCT_I as TARGET_ASSC_ACCT_I,
       T1.PRFR_PATY_F as FLAG_PRFR_PATY_F, T2.PRFR_PATY_F as TARGET_PRFR_PATY_F,
       T1.SRCE_SYST_C as FLAG_SRCE_SYST_C, T2.SRCE_SYST_C as TARGET_SRCE_SYST_C,
       -- Show actual comparison results
       (TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))) as ASSC_DIFFERS,
       (TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))) as PRFR_DIFFERS,
       (TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C))) as SRCE_DIFFERS
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)
WHERE (TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))
    OR TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))
    OR TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C)))
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  AND T1.ACCT_I = $acct_test;

-- 3.2 Simulate the Teradata CHG creation logic (no TRIM/UPPER)
-- This is the exact logic from DERV_ACCT_PATY_07_CRAT_DELTAS.sql
SELECT 'TD_CHG_CREATION_SIMULATION' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C,
       T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, T1.ROW_SECU_ACCS_C,
       -- Show what the comparison fields look like
       T1.ASSC_ACCT_I as FLAG_ASSC_ACCT_I, T2.ASSC_ACCT_I as TARGET_ASSC_ACCT_I,
       T1.PRFR_PATY_F as FLAG_PRFR_PATY_F, T2.PRFR_PATY_F as TARGET_PRFR_PATY_F,
       T1.SRCE_SYST_C as FLAG_SRCE_SYST_C, T2.SRCE_SYST_C as TARGET_SRCE_SYST_C,
       -- Show actual comparison results (TD style - no TRIM/UPPER)
       (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I) as ASSC_DIFFERS,
       (T1.PRFR_PATY_F <> T2.PRFR_PATY_F) as PRFR_DIFFERS,
       (T1.SRCE_SYST_C <> T2.SRCE_SYST_C) as SRCE_DIFFERS
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C  -- TD style: no TRIM
WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I        -- TD style: no TRIM/UPPER
    OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
    OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C)
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  AND T1.ACCT_I = $acct_test;

-- =====================================================================================
-- STEP 4: CHECK DATA QUALITY AND ENCODING ISSUES
-- =====================================================================================

-- 4.1 Check for data encoding differences (spaces, case, etc.)
SELECT 'DATA_ENCODING_CHECK' as test_type,
       T1.ACCT_I, T1.PATY_I, T1.PATY_ACCT_REL_C,
       -- Show raw values with length and brackets to reveal hidden characters
       LENGTH(T1.ASSC_ACCT_I) as FLAG_ASSC_LEN, '[' || T1.ASSC_ACCT_I || ']' as FLAG_ASSC_RAW,
       LENGTH(T2.ASSC_ACCT_I) as TARGET_ASSC_LEN, '[' || T2.ASSC_ACCT_I || ']' as TARGET_ASSC_RAW,
       LENGTH(T1.PRFR_PATY_F) as FLAG_PRFR_LEN, '[' || T1.PRFR_PATY_F || ']' as FLAG_PRFR_RAW,
       LENGTH(T2.PRFR_PATY_F) as TARGET_PRFR_LEN, '[' || T2.PRFR_PATY_F || ']' as TARGET_PRFR_RAW,
       LENGTH(T1.SRCE_SYST_C) as FLAG_SRCE_LEN, '[' || T1.SRCE_SYST_C || ']' as FLAG_SRCE_RAW,
       LENGTH(T2.SRCE_SYST_C) as TARGET_SRCE_LEN, '[' || T2.SRCE_SYST_C || ']' as TARGET_SRCE_RAW,
       -- Check if TRIM makes them equal
       (TRIM(T1.ASSC_ACCT_I) = TRIM(T2.ASSC_ACCT_I)) as ASSC_EQUAL_AFTER_TRIM,
       (UPPER(T1.PRFR_PATY_F) = UPPER(T2.PRFR_PATY_F)) as PRFR_EQUAL_AFTER_UPPER
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
WHERE T1.ACCT_I = $acct_test
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D;

-- =====================================================================================
-- STEP 5: CHECK PROCESS EXECUTION AND TIMING
-- =====================================================================================

-- 5.1 Check when the CHG table was last populated
SELECT 'CHG_TABLE_METADATA' as check_type,
       'Check table metadata for last updated time' as instruction;

-- 5.2 Check the source ACCT_PATY data for completeness
SELECT 'SOURCE_ACCT_PATY_CHECK' as check_type,
       COUNT(*) as total_records,
       COUNT(DISTINCT ACCT_I) as distinct_accounts,
       MIN(EFFT_D) as min_efft_d, MAX(EXPY_D) as max_expy_d
FROM NPD_D12_DMN_GDWMIG_IBRG_V.pvtech.ACCT_PATY
WHERE ACCT_I = $acct_test;

-- 5.3 Check if the process execution parameter matches
SELECT 'EXTRACT_DATE_VALIDATION' as check_type,
       $extract_date as configured_extract_date,
       'Verify this matches the date used in Teradata process' as note;

-- =====================================================================================
-- STEP 6: ROOT CAUSE ANALYSIS SUMMARY
-- =====================================================================================

-- 6.1 Compare the difference between SF and TD logic side by side
SELECT 'LOGIC_COMPARISON_SUMMARY' as test_type,
       COUNT(*) as sf_would_create_records,
       'SF Logic: Uses TRIM(UPPER()) comparisons' as sf_logic
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)
WHERE (TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))
    OR TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))
    OR TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C)))
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  AND T1.ACCT_I = $acct_test

UNION ALL

SELECT 'LOGIC_COMPARISON_SUMMARY' as test_type,
       COUNT(*) as td_would_create_records,
       'TD Logic: Direct string comparisons (no TRIM/UPPER)' as td_logic
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
    OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
    OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C)
  AND $extract_date BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND $extract_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  AND T1.ACCT_I = $acct_test;

-- =====================================================================================
-- DEBUGGING RECOMMENDATIONS:
-- =====================================================================================

SELECT 'DEBUGGING_RECOMMENDATIONS' as section,
       'Expected root causes for missing staging CHG records:' as analysis,
       '1. TRIM/UPPER logic differences between TD and SF' as cause_1,
       '2. JOIN condition differences (TRIM on PATY_ACCT_REL_C)' as cause_2,
       '3. Source data timing or extract date differences' as cause_3,
       '4. Data encoding issues (trailing spaces, case)' as cause_4,
       '5. Process execution sequence or parameter differences' as cause_5;

-- =====================================================================================
-- EXPECTED RESULTS:
-- =====================================================================================
-- If TD_CHG_CREATION_SIMULATION returns more records than SF_CHG_CREATION_SIMULATION,
-- then the TRIM/UPPER logic is masking differences that Teradata detects.
-- 
-- If DATA_ENCODING_CHECK shows different lengths or hidden characters,
-- then data encoding is the issue.
-- 
-- If both simulations return 0 records, then the issue is in the source FLAG table
-- or the extract date parameter.
-- ===================================================================================== 