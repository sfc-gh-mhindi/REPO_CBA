USE ROLE r_dev_npd_d12_gdwmig;
CREATE OR REPLACE PROCEDURE npd_d12_dmn_gdwmig.lcl.sp_bteq_derv_acct_paty_06_set_max_prfr_flag_chg0379808(
  EXTR_D STRING DEFAULT '2024-01-01'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  total_rows INTEGER DEFAULT 0;
BEGIN
  -- Object Name: DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql
  -- Description: Set preferred party flag for non-RM accounts
  -- Use MAX(PATY_I) for this purpose and remove holder rows
  -- that have effective date prior to the effective date of the flagged row
  
  -- Step 1: Clear and populate non-RM accounts table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM
  SELECT 
    DAP.ACCT_I,
    DAP.PATY_I,
    DAP.ASSC_ACCT_I,
    DAP.PATY_ACCT_REL_C,
    DAP.PRFR_PATY_F,
    DAP.SRCE_SYST_C,
    DAP.EFFT_D,
    DAP.EXPY_D,
    DAP.ROW_SECU_ACCS_C,
    99 AS RANK_I -- Fund Holder Project - Adding RANK_I for Non RM Account
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR DAP
  LEFT JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG T1
    ON DAP.ASSC_ACCT_I = T1.ACCT_I
  WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
    AND T1.ACCT_I IS NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Step 2: Update priority ranking for Fund Holder relationships
  MERGE INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM AS target USING (
  SELECT
    DISTINCT PIG.PATY_I,
    FIRST_VALUE(GDFVC.PRTY) OVER (
      PARTITION BY PIG.PATY_I
      ORDER BY
        GDFVC.PRTY ASC
    ) AS PRTY
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.PATY_INT_GRUP_CURR AS PIG
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.INT_GRUP_DEPT_CURR AS IGD ON IGD.INT_GRUP_I = PIG.INT_GRUP_I
    AND TRIM(PIG.REL_C) = 'RLMT'
    INNER JOIN (
      SELECT
        GFC.DEPT_LEAF_NODE_C AS DEPT_I,
        ORU.PRTY
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_DEPT_FLAT_CURR AS GFC
        LEFT JOIN (
          SELECT
            COALESCE(PRTY, 9999) AS PRTY,
            LKUP1_TEXT
          FROM
            NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ODS_RULE
          WHERE
            TRIM(RULE_CODE) = 'RMPOC'
            AND :EXTR_D BETWEEN VALD_FROM
            AND VALD_TO QUALIFY ROW_NUMBER() OVER (
              PARTITION BY LKUP1_TEXT,
              PRTY
              ORDER BY
                COALESCE(UPDT_DTTS, CRAT_DTTS) DESC
            ) = 1
        ) AS ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
      WHERE
        PRTY <> 9999
    ) AS GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I
  WHERE
    PIG.PATY_I IN (
      SELECT
        DISTINCT PATY_I
      FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
    )
) AS source ON target.PATY_I = source.PATY_I
AND TRIM(target.PATY_ACCT_REL_C) = 'ZINTE0'
WHEN MATCHED THEN
UPDATE
SET
  target.RANK_I = source.PRTY;
  
  -- Step 3: Set final ranking based on relationship type
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM
  SET RANK_I = CASE 
    WHEN RANK_I <> 99 AND TRIM(PATY_ACCT_REL_C) = 'ZINTE0' THEN RANK_I -- Fund Holder with Rule Priority gets First Priority
    WHEN TRIM(PATY_ACCT_REL_C) = 'ZINTE0' THEN 98 -- Fund Holder gets Second Priority
    ELSE 99 -- All others get Least Priority
  END;
  
  -- Step 4: Set preferred party flag based on ranking and MAX(PATY_I)
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM
  SET PRFR_PATY_F = 'Y'
  WHERE EXISTS (
    SELECT 1 
    FROM (
      SELECT 
        T1.ACCT_I,
        T1.PATY_I,
        T1.PATY_ACCT_REL_C,
        T1.EFFT_D,
        T1.EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM T1
      JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD T2  
        ON TRIM(T1.PATY_ACCT_REL_C) = TRIM(T2.PATY_ACCT_REL_C)
      WHERE UPPER(TRIM(T1.ACCT_I)) <> LOWER(TRIM(T1.ACCT_I))
        AND T1.EFFT_D <= T1.EXPY_D     
        AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
      QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.RANK_I ASC, T1.PATY_I DESC, T1.PATY_ACCT_REL_C) = 1
    ) T3
    WHERE DERV_ACCT_PATY_NON_RM.ACCT_I = T3.ACCT_I
      AND DERV_ACCT_PATY_NON_RM.PATY_I = T3.PATY_I
      AND DERV_ACCT_PATY_NON_RM.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C
      AND DERV_ACCT_PATY_NON_RM.EFFT_D = T3.EFFT_D
      AND DERV_ACCT_PATY_NON_RM.EXPY_D = T3.EXPY_D
  );
  
  -- Step 5: Clear and populate final flag table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG;
  
  -- Insert RM accounts
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG
  SELECT 
    ACCT_I,
    PATY_I,
    ASSC_ACCT_I,
    PATY_ACCT_REL_C,
    PRFR_PATY_F,
    SRCE_SYST_C,
    EFFT_D,
    EXPY_D,
    ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Insert non-RM accounts
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG
  SELECT 
    ACCT_I,
    PATY_I,
    ASSC_ACCT_I,
    PATY_ACCT_REL_C,
    PRFR_PATY_F,
    SRCE_SYST_C,
    EFFT_D,
    EXPY_D,
    ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Processed ' || :total_rows || ' total records. Preferred party flags set for non-RM accounts.';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;