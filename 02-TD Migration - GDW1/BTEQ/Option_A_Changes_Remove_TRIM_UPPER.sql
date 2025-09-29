-- =====================================================================================
-- OPTION A: EXACT CHANGES TO MAKE SNOWFLAKE MATCH TERADATA LOGIC
-- =====================================================================================
-- PURPOSE: Remove TRIM/UPPER functions from Snowflake procedures to match TD exactly
-- IMPACT: Snowflake will detect the same data differences as Teradata
-- =====================================================================================

-- =====================================================================================
-- FILE 1: derv_acct_paty_07_crat_deltas_proc.sql
-- =====================================================================================

-- CHANGE 1.1: Remove TRIM from JOIN condition (Line 32)
-- CURRENT (Snowflake):
   AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)

-- CHANGE TO (Match Teradata):
   AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C

-- CHANGE 1.2: Remove TRIM from JOIN condition (Line 58) 
-- CURRENT (Snowflake):
   AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)

-- CHANGE TO (Match Teradata):
   AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C

-- CHANGE 1.3: Remove TRIM/UPPER from WHERE condition (Lines 59-61)
-- CURRENT (Snowflake):
  WHERE (TRIM(UPPER(T1.ASSC_ACCT_I)) <> TRIM(UPPER(T2.ASSC_ACCT_I))
      OR TRIM(UPPER(T1.PRFR_PATY_F)) <> TRIM(UPPER(T2.PRFR_PATY_F))
      OR TRIM(UPPER(T1.SRCE_SYST_C)) <> TRIM(UPPER(T2.SRCE_SYST_C)))

-- CHANGE TO (Match Teradata):
  WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
      OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
      OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C)

-- =====================================================================================
-- FILE 2: derv_acct_paty_08_apply_deltas_proc.sql
-- =====================================================================================

-- CHANGE 2.1: Remove TRIM from INSERT statement (Line 75)
-- CURRENT (Snowflake):
        TRIM(T1.PATY_ACCT_REL_C),

-- CHANGE TO (Match Teradata):
        T1.PATY_ACCT_REL_C,

-- CHANGE 2.2: Remove TRIM from NOT EXISTS condition (Line 90)
-- CURRENT (Snowflake):
        AND TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)

-- CHANGE TO (Match Teradata):
        AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C

-- CHANGE 2.3: Remove TRIM/UPPER from UPDATE WHERE condition (Lines 60-62)
-- CURRENT (Snowflake):
      AND (DERV_ACCT_PATY.ASSC_ACCT_I <> T2.ASSC_ACCT_I
        OR DERV_ACCT_PATY.PRFR_PATY_F <> T2.PRFR_PATY_F
        OR DERV_ACCT_PATY.SRCE_SYST_C <> T2.SRCE_SYST_C)

-- NO CHANGE NEEDED - This one already matches Teradata (no TRIM/UPPER)

-- =====================================================================================
-- FILE 3: derv_acct_paty_05_set_prtf_prfr_flag_proc.sql
-- =====================================================================================

-- CHANGE 3.1: Remove TRIM from WHERE condition (Line 51)
-- CURRENT (Snowflake):
      AND TRIM(PIG.REL_C) = 'RLMT'

-- CHANGE TO (Match Teradata):
      AND PIG.REL_C = 'RLMT'

-- CHANGE 3.2: Remove TRIM from WHERE condition (Line 63)
-- CURRENT (Snowflake):
        WHERE TRIM(RULE_CODE) = 'RMPOC'

-- CHANGE TO (Match Teradata):
        WHERE RULE_CODE = 'RMPOC'

-- CHANGE 3.3: Remove TRIM from WHERE condition (Line 76)
-- CURRENT (Snowflake):
    AND TRIM(DERV_PRTF_ACCT_PATY_PSST.PATY_ACCT_REL_C) = 'ZINTE0';

-- CHANGE TO (Match Teradata):
    AND DERV_PRTF_ACCT_PATY_PSST.PATY_ACCT_REL_C = 'ZINTE0';

-- CHANGE 3.4: Remove TRIM from CASE condition (Line 89)
-- CURRENT (Snowflake):
      WHEN DAPP.RANK_I <> 99 AND TRIM(DAPP.PATY_ACCT_REL_C) = 'ZINTE0' THEN DAPP.RANK_I

-- CHANGE TO (Match Teradata):
      WHEN DAPP.RANK_I <> 99 AND DAPP.PATY_ACCT_REL_C = 'ZINTE0' THEN DAPP.RANK_I

-- CHANGE 3.5: Remove TRIM from CASE condition (Line 90)
-- CURRENT (Snowflake):
      WHEN TRIM(DAPP.PATY_ACCT_REL_C) = 'ZINTE0' THEN 98

-- CHANGE TO (Match Teradata):
      WHEN DAPP.PATY_ACCT_REL_C = 'ZINTE0' THEN 98

-- CHANGE 3.6: Remove TRIM from JOIN condition (Line 96)
-- CURRENT (Snowflake):
    AND (DPPS.PRTF_CODE_X = DAPP.ACCT_PRTF_C OR TRIM(DAPP.PATY_ACCT_REL_C) = 'ZINTE0')

-- CHANGE TO (Match Teradata):
    AND (DPPS.PRTF_CODE_X = DAPP.ACCT_PRTF_C OR DAPP.PATY_ACCT_REL_C = 'ZINTE0')

-- CHANGE 3.7: Remove TRIM from WHERE condition (Line 97)
-- CURRENT (Snowflake):
    AND TRIM(DPPS.PRTF_CODE_X) <> 'NA';

-- CHANGE TO (Match Teradata):
    AND DPPS.PRTF_CODE_X <> 'NA';

-- CHANGE 3.8: Remove TRIM from JOIN condition (Line 129)
-- CURRENT (Snowflake):
    ON TRIM(DAP.ASSC_ACCT_I) = T1.ACCT_I

-- CHANGE TO (Match Teradata):
    ON DAP.ASSC_ACCT_I = T1.ACCT_I

-- =====================================================================================
-- SUMMARY OF FILES TO MODIFY:
-- =====================================================================================

-- 1. CBA/REPO_CBA/GDW1/BTEQ/PS Tool Converted/derv_pty/sf_derv_acct_paty/derv_acct_paty_07_crat_deltas_proc.sql
--    - 3 changes (2 JOIN conditions, 1 WHERE condition)

-- 2. CBA/REPO_CBA/GDW1/BTEQ/PS Tool Converted/derv_pty/sf_derv_acct_paty/derv_acct_paty_08_apply_deltas_proc.sql
--    - 2 changes (1 INSERT field, 1 NOT EXISTS condition)

-- 3. CBA/REPO_CBA/GDW1/BTEQ/PS Tool Converted/derv_pty/sf_derv_acct_paty/derv_acct_paty_05_set_prtf_prfr_flag_proc.sql
--    - 8 changes (various WHERE, CASE, and JOIN conditions)

-- =====================================================================================
-- CRITICAL NOTES:
-- =====================================================================================

-- 1. BACKUP FIRST: Make sure to backup the original procedures before making changes

-- 2. TESTING REQUIRED: After changes, run your comparison queries to verify the 8 missing records now appear

-- 3. DATA IMPACT: These changes will make Snowflake more sensitive to:
--    - Trailing spaces in string fields
--    - Case differences (uppercase vs lowercase)
--    - Leading spaces in string fields

-- 4. PERFORMANCE IMPACT: Removing TRIM/UPPER may slightly improve performance

-- 5. CONSISTENCY: Consider if other processes also need similar changes for consistency

-- =====================================================================================
-- VALIDATION QUERIES TO RUN AFTER CHANGES:
-- =====================================================================================

-- Test 1: Compare staging CHG table record counts
SELECT 'AFTER_CHANGES_CHG_COUNT' as test, COUNT(*) as record_count
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
WHERE ACCT_I = 'S1';

-- Test 2: Simulate new logic
SELECT 'NEW_TD_LOGIC_SIMULATION' as test, COUNT(*) as would_create_records
FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
  ON T1.ACCT_I = T2.ACCT_I
 AND T1.PATY_I = T2.PATY_I
 AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C  -- No TRIM
WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I        -- No TRIM/UPPER
    OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
    OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C)
  AND '2025-07-10' BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND '2025-07-10' BETWEEN T2.EFFT_D AND T2.EXPY_D
  AND T1.ACCT_I = 'S1';

-- =====================================================================================
-- EXPECTED OUTCOME:
-- =====================================================================================
-- After these changes, Snowflake should detect the same 8 records that Teradata does,
-- assuming the extract dates are the same between both systems.
-- ===================================================================================== 