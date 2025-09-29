CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808_PROC(
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
  DELETE FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM;
  
  INSERT INTO PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM
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
  FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_CURR DAP
  LEFT JOIN PS_CLD_RW.PDDSTG.DERV_PRTF_ACCT_PATY_STAG T1
    ON DAP.ASSC_ACCT_I = T1.ACCT_I
  WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
    AND T1.ACCT_I IS NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Step 2: Update priority ranking for Fund Holder relationships
  UPDATE PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM
  SET RANK_I = (
    SELECT DRVD.PRTY
    FROM (
      SELECT DISTINCT 
        PIG.PATY_I,
        GDFVC.PRTY
      FROM ps_gdw1_bteq.PVDATA.PATY_INT_GRUP_CURR PIG 
      INNER JOIN ps_gdw1_bteq.PVDATA.INT_GRUP_DEPT_CURR IGD 
        ON IGD.INT_GRUP_I = PIG.INT_GRUP_I
        AND PIG.REL_C = 'RLMT'
      INNER JOIN (
        SELECT 
          GFC.DEPT_LEAF_NODE_C AS DEPT_I,
          ORU.PRTY AS PRTY 
        FROM ps_gdw1_bteq.PVTECH.GRD_DEPT_FLAT_CURR GFC 
        LEFT JOIN (
          SELECT 
            COALESCE(PRTY, 9999) AS PRTY,
            LKUP1_TEXT,
            COALESCE(UPDT_DTTS, CRAT_DTTS) AS LoadTimeStamp
          FROM ps_gdw1_bteq.PVTECH.ODS_RULE
          WHERE RULE_CODE = 'RMPOC'
            AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
          QUALIFY ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT, PRTY ORDER BY LoadTimeStamp DESC) = 1
        ) ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
        WHERE PRTY <> 9999
      ) GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I
      WHERE PIG.PATY_I IN (
        SELECT DISTINCT PATY_I 
        FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_CURR
      )
      QUALIFY ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC) = 1
    ) DRVD
    WHERE DERV_ACCT_PATY_NON_RM.PATY_I = DRVD.PATY_I
  )
  WHERE PATY_ACCT_REL_C = 'ZINTE0'
    AND EXISTS (
      SELECT 1 FROM (
        SELECT DISTINCT 
          PIG.PATY_I,
          GDFVC.PRTY
        FROM ps_gdw1_bteq.PVDATA.PATY_INT_GRUP_CURR PIG 
        INNER JOIN ps_gdw1_bteq.PVDATA.INT_GRUP_DEPT_CURR IGD 
          ON IGD.INT_GRUP_I = PIG.INT_GRUP_I
          AND PIG.REL_C = 'RLMT'
        INNER JOIN (
          SELECT 
            GFC.DEPT_LEAF_NODE_C AS DEPT_I,
            ORU.PRTY AS PRTY 
          FROM ps_gdw1_bteq.PVTECH.GRD_DEPT_FLAT_CURR GFC 
          LEFT JOIN (
            SELECT 
              COALESCE(PRTY, 9999) AS PRTY,
              LKUP1_TEXT,
              COALESCE(UPDT_DTTS, CRAT_DTTS) AS LoadTimeStamp
            FROM ps_gdw1_bteq.PVTECH.ODS_RULE
            WHERE RULE_CODE = 'RMPOC'
              AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT, PRTY ORDER BY LoadTimeStamp DESC) = 1
          ) ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
          WHERE PRTY <> 9999
        ) GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I
        WHERE PIG.PATY_I IN (
          SELECT DISTINCT PATY_I 
          FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_CURR
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC) = 1
      ) DRVD
      WHERE DERV_ACCT_PATY_NON_RM.PATY_I = DRVD.PATY_I
    );
  
  -- Step 3: Set final ranking based on relationship type
  UPDATE PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM
  SET RANK_I = CASE 
    WHEN RANK_I <> 99 AND PATY_ACCT_REL_C = 'ZINTE0' THEN RANK_I -- Fund Holder with Rule Priority gets First Priority
    WHEN PATY_ACCT_REL_C = 'ZINTE0' THEN 98 -- Fund Holder gets Second Priority
    ELSE 99 -- All others get Least Priority
  END;
  
  -- Step 4: Set preferred party flag based on ranking and MAX(PATY_I)
  UPDATE PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM
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
      FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM T1
      JOIN PS_CLD_RW.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD T2  
        ON T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
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
  DELETE FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_FLAG;
  
  -- Insert RM accounts
  INSERT INTO PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_FLAG
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
  FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_RM
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Insert non-RM accounts
  INSERT INTO PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_FLAG
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
  FROM PS_CLD_RW.PDDSTG.DERV_ACCT_PATY_NON_RM
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Processed ' || :total_rows || ' total records. Preferred party flags set for non-RM accounts.';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;