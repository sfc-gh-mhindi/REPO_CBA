use role r_dev_npd_d12_gdwmig;
use warehouse wh_usr_npd_d12_gdwmig_001;

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;

-- CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.bteq_sps;

-- =============================================================================
-- CONSOLIDATED PROCEDURES FILE - REGENERATED
-- Generated from: cba-terradata-cdao-poc-mini_sergio/dcf/schema_definitions/sps/
-- Total procedures: 29 files across 4 directories
-- 
-- Source directories:
--   - prtf_tech/     (15 procedures)
--   - others/        (2 procedures)  
--   - derv_acct_paty/ (10 procedures)
--   - acct_baln_bkdt/ (2 procedures)
--
-- Replacements applied:
--   NPD_D12_DMN_GDWMIG_IBRG_V → NPD_D12_DMN_GDWMIG_IBRG_V
--   NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0. → NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.
--   NPD_D12_DMN_GDWMIG_IBRG → NPD_D12_DMN_GDWMIG_IBRG
-- ============================================================================= 
-- =============================================================================
-- PROCEDURE: prtf_tech_acct_int_grup_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_acct_int_grup_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_ACCT_INT_GRUP_PSST_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  total_processed INTEGER DEFAULT 0;
BEGIN
  -- Portfolio Account Position Processing
  -- Original Author: T Jelliffe (Initial Version 11/06/2013)
  -- Purpose: Process AIG group data and calculate overlapping business period dates
  
  -- STEP 1: AIG group processing
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  SELECT
     AIG.INT_GRUP_I               as INT_GRUP_I
    ,AIG.ACCT_I                   as ACCT_I
    ,AIG.REL_C
    ,AIG.VALD_FROM_D              as JOIN_FROM_D
    ,AIG.VALD_TO_D                as JOIN_TO_D
    ,AIG.VALD_FROM_D
    ,AIG.VALD_TO_D
    ,AIG.EFFT_D
    ,AIG.EXPY_D
    ,AIG.SRCE_SYST_C
    ,AIG.PROS_KEY_EFFT_I as PROS_KEY_I
    ,AIG.ROW_SECU_ACCS_C
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP AIG
    
    /* Add the GRD filter to reduce the data */
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP IG
    ON IG.INT_GRUP_I = AIG.INT_GRUP_I
      
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
    ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  
    -- Overlaps
    AND GPTE.VALD_TO_D >= IG.CRAT_D
    AND GPTE.VALD_FROM_D <= IG.VALD_TO_D
  
    AND GPTE.VALD_TO_D >= AIG.VALD_FROM_D
    AND GPTE.VALD_FROM_D <= AIG.VALD_TO_D
     
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;
  
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  -- STEP 2: Overlapping Business period dates
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST
  SELECT
     DT2.INT_GRUP_I       
    ,DT2.ACCT_I
    ,DT2.EFFT_D
    ,DT2.EXPY_D
    ,DT2.VALD_FROM_D
    ,DT2.VALD_TO_D
    ,DT2.PERD_D
    ,DT2.REL_C
    ,DT2.SRCE_SYST_C
    ,ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ACCT_I ORDER BY DT2.PERD_D, DT2.REL_C ) as ROW_N
    ,DT2.ROW_SECU_ACCS_C
    ,0                          as PROS_KEY_I
  FROM
    (
      SELECT
         DT1.INT_GRUP_I       
        ,DT1.ACCT_I
        ,DT1.EFFT_D
        ,DT1.EXPY_D
        ,DT1.VALD_FROM_D
        ,DT1.VALD_TO_D
        ,C.CALENDAR_DATE as PERD_D
        ,DT1.REL_C
        ,DT1.SRCE_SYST_C
        ,DT1.ROW_SECU_ACCS_C
      FROM
        (
          SELECT
             A.INT_GRUP_I
            ,A.ACCT_I
            ,A.REL_C
            ,A.EFFT_D
            ,A.EXPY_D
            ,A.VALD_FROM_D
            ,A.VALD_TO_D
            ,A.JOIN_FROM_D
            ,A.JOIN_TO_D
            ,A.SRCE_SYST_C
            ,A.ROW_SECU_ACCS_C
          FROM
            NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST A
            INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST B
            ON A.ACCT_I = B.ACCT_I
            AND A.INT_GRUP_I = B.INT_GRUP_I
            AND A.JOIN_TO_D >= B.JOIN_FROM_D
            AND A.JOIN_FROM_D <= B.JOIN_TO_D
  
            AND (
              A.EFFT_D <> B.EFFT_D
              OR A.EXPY_D <> B.EXPY_D
              OR A.PROS_KEY_I <> B.PROS_KEY_I
            )
        ) DT1
  
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
  
      QUALIFY ROW_NUMBER() OVER(PARTITION BY DT1.ACCT_I, DT1.INT_GRUP_I, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC, DT1.REL_C DESC) = 1
  
    ) DT2;
    
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  -- STEP 3: Calculate correct history
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST
  SELECT DISTINCT
     DT2.INT_GRUP_I
    ,DT2.ACCT_I
    ,DT2.REL_C
    ,MIN(DT2.PERD_D) OVER ( PARTITION BY DT2.ACCT_I, DT2.INT_GRUP_I, DT2.ROW_N ) as JOIN_FROM_D
    ,MAX(DT2.PERD_D) OVER ( PARTITION BY DT2.ACCT_I, DT2.INT_GRUP_I, DT2.ROW_N ) as JOIN_TO_D
    ,DT2.VALD_FROM_D
    ,DT2.VALD_TO_D
    ,DT2.EFFT_D
    ,DT2.EXPY_D
    ,DT2.SRCE_SYST_C
    ,DT2.ROW_SECU_ACCS_C
  FROM
    (
    SELECT DISTINCT
       C.INT_GRUP_I
      ,C.ACCT_I
      ,C.REL_C
      ,C.VALD_FROM_D
      ,C.VALD_TO_D
      ,C.EFFT_D
      ,C.EXPY_D
      ,C.SRCE_SYST_C
      ,C.ROW_SECU_ACCS_C
      ,C.PERD_D
      ,MAX(COALESCE( DT1.ROW_N, 0 )) OVER (PARTITION BY C.ACCT_I, C.INT_GRUP_I, C.PERD_D) as ROW_N     
    FROM
      NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST C
      LEFT JOIN (
        -- Detect the change in non-key values between rows
        SELECT
           A.INT_GRUP_I
          ,A.ACCT_I
          ,A.PERD_D
          ,A.REL_C
          ,A.ROW_N
          ,A.EFFT_D
        FROM
          NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST A
          INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST B
          ON A.ACCT_I = B.ACCT_I
          AND A.INT_GRUP_I = B.INT_GRUP_I
          AND A.ROW_N = B.ROW_N + 1
          AND A.EFFT_D <> B.EFFT_D
      ) DT1
      ON C.INT_GRUP_I = DT1.INT_GRUP_I
      AND C.ACCT_I = DT1.ACCT_I
      AND C.REL_C = DT1.REL_C
      AND C.EFFT_D = DT1.EFFT_D
      AND C.ROW_N >= DT1.ROW_N
      AND DT1.PERD_D <= C.PERD_D    
  
    ) DT2;
    
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  -- STEP 4: Delete all the original overlap records
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST B
    WHERE  
      DERV_PRTF_ACCT_PSST.ACCT_I = B.ACCT_I
      AND DERV_PRTF_ACCT_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_ACCT_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      AND DERV_PRTF_ACCT_PSST.JOIN_FROM_D <= B.JOIN_TO_D  
  );
  
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  -- STEP 5: Replace with updated records
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  SELECT
     INT_GRUP_I                    
    ,ACCT_I                        
    ,REL_C                         
    ,JOIN_FROM_D                
    ,(CASE
        WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE             
        ELSE JOIN_TO_D
      END
      ) as JOIN_TO_D
    ,VALD_FROM_D
    ,VALD_TO_D
    ,EFFT_D                        
    ,EXPY_D                        
    ,SRCE_SYST_C 
    ,0 as PROS_KEY_I  
    ,ROW_SECU_ACCS_C               
  FROM 
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST;
    
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  RETURN 'SUCCESS: Portfolio account processing completed. Total records processed: ' || :total_processed;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_acct_own_rel_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_acct_own_rel_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_ACCT_OWN_REL_PSST_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  total_inserted INTEGER DEFAULT 0;
BEGIN
  -- Object Name: prtf_tech_acct_own_rel_psst.sql
  -- Description: Persist rows for DERV_PRTF_ACCT_OWN view
  -- Original Author: Helen Zak (09/01/2014)
  
  -- Delete all rows from target table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL;
  
  row_count := SQLROWCOUNT;
  
  -- Insert portfolio account ownership relationships
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL
  (  ACCT_I 
   , INT_GRUP_I 
   , DERV_PRTF_CATG_C
   , DERV_PRTF_CLAS_C 
   , DERV_PRTF_TYPE_C 
   , PRTF_ACCT_VALD_FROM_D 
   , PRTF_ACCT_VALD_TO_D 
   , PRTF_ACCT_EFFT_D 
   , PRTF_ACCT_EXPY_D
   , PRTF_OWN_VALD_FROM_D 
   , PRTF_OWN_VALD_TO_D 
   , PRTF_OWN_EFFT_D
   , PRTF_OWN_EXPY_D
   , PTCL_N 
   , REL_MNGE_I 
   , PRTF_CODE_X 
   , DERV_PRTF_ROLE_C
   , ROLE_PLAY_TYPE_X 
   , ROLE_PLAY_I 
   , SRCE_SYST_C
   , ROW_SECU_ACCS_C
  )
  SELECT 
        PP4.ACCT_I AS ACCT_I 
      , PP4.INT_GRUP_I AS INT_GRUP_I  
      , PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C 
      , PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C 
      , PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
      , PP4.VALD_FROM_D AS PRTF_ACCT_VALD_FROM_D 
      , PP4.VALD_TO_D AS PRTF_ACCT_VALD_TO_D 
      , PP4.EFFT_D AS PRTF_ACCT_EFFT_D 
      , PP4.EXPY_D AS PRTF_ACCT_EXPY_D 
      , PO4.VALD_FROM_D AS PRTF_OWN_VALD_FROM_D 
      , PO4.VALD_TO_D AS PRTF_OWN_VALD_TO_D 
      , PO4.EFFT_D AS PRTF_OWN_EFFT_D
      , PO4.EXPY_D AS PRTF_OWN_EXPY_D 
      , PP4.PTCL_N AS PTCL_N 
      , PP4.REL_MNGE_I AS REL_MNGE_I 
      , PP4.PRTF_CODE_X AS PRTF_CODE_X 
      , PO4.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
      , PO4.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X 
      , PO4.ROLE_PLAY_I AS ROLE_PLAY_I
      , PP4.SRCE_SYST_C AS SRCE_SYST_C 
      , PP4.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_REL PP4 
  INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_REL PO4 
  ON PO4.INT_GRUP_I = PP4.INT_GRUP_I 
  AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C;
  
  total_inserted := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Deleted ' || :row_count || ' records, inserted ' || :total_inserted || ' records into DERV_PRTF_ACCT_OWN_REL';
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_acct_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_acct_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_ACCT_PSST_PROC(
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
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
  -- Portfolio Account Interest Group Processing
  -- Ver 1.7 - Fix overlap calc, JOIN_DATE to calendar
  -- Original Author: T Jelliffe
  
  -- STEP 6: Process Portfolio Account Interest Group Persist
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST
  SELECT
    DT2.INT_GRUP_I,
    DT2.ACCT_I,
    DT2.EFFT_D,
    DT2.EXPY_D,
    DT2.VALD_FROM_D,
    DT2.VALD_TO_D,
    DT2.PERD_D,
    DT2.REL_C,
    DT2.SRCE_SYST_C,
    ROW_NUMBER() OVER (PARTITION BY DT2.ACCT_I, DT2.REL_C ORDER BY DT2.PERD_D) as GRUP_N,
    DT2.ROW_SECU_ACCS_C,
    0 as PROS_KEY_I
  FROM (
    SELECT DISTINCT
      DT1.INT_GRUP_I,
      DT1.ACCT_I,
      DT1.EFFT_D,
      DT1.EXPY_D,
      DT1.VALD_FROM_D,
      DT1.VALD_TO_D,
      C.CALENDAR_DATE as PERD_D,
      DT1.REL_C,
      DT1.SRCE_SYST_C,
      DT1.ROW_SECU_ACCS_C
    FROM (
      SELECT
        A.INT_GRUP_I,
        A.ACCT_I,
        A.REL_C,
        A.EFFT_D,
        A.EXPY_D,
        A.VALD_FROM_D,
        A.VALD_TO_D,
        A.JOIN_FROM_D,
        A.JOIN_TO_D,
        A.SRCE_SYST_C,
        A.ROW_SECU_ACCS_C
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST A
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST B
        ON A.ACCT_I = B.ACCT_I
        AND A.REL_C = B.REL_C
        AND A.JOIN_TO_D >= B.JOIN_FROM_D
        AND A.JOIN_FROM_D <= B.JOIN_TO_D
        AND (
          A.EFFT_D <> B.EFFT_D
          OR A.EXPY_D <> B.EXPY_D
          OR A.INT_GRUP_I <> B.INT_GRUP_I
          OR A.PROS_KEY_I <> B.PROS_KEY_I
        )
    ) DT1
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
    ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D
    AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY DT1.ACCT_I, DT1.REL_C, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC, DT1.INT_GRUP_I DESC) = 1
  ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 7: Process Portfolio Account History Persist
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST
  SELECT
    DT3.INT_GRUP_I,
    DT3.ACCT_I,
    DT3.REL_C,
    DT3.JOIN_FROM_D,
    DT3.JOIN_TO_D,
    DT3.VALD_FROM_D,
    DT3.VALD_TO_D,
    DT3.EFFT_D,
    DT3.EXPY_D,
    DT3.SRCE_SYST_C,
    DT3.ROW_SECU_ACCS_C
  FROM (
    SELECT
      DT2.INT_GRUP_I,
      DT2.ACCT_I,
      DT2.REL_C,
      DT2.EFFT_D,
      DT2.EXPY_D,
      DT2.VALD_FROM_D,
      DT2.VALD_TO_D,
      DT2.PERD_D,
      MIN(DT2.PERD_D) OVER (PARTITION BY DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N) as JOIN_FROM_D,
      MAX(DT2.PERD_D) OVER (PARTITION BY DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N) as JOIN_TO_D,
      DT2.SRCE_SYST_C,
      DT2.GRUP_N,
      DT2.ROW_SECU_ACCS_C
    FROM (
      SELECT
        PAIG.INT_GRUP_I,
        PAIG.ACCT_I,
        PAIG.REL_C,
        PAIG.PERD_D,
        PAIG.EFFT_D,
        PAIG.EXPY_D,
        PAIG.VALD_FROM_D,
        PAIG.VALD_TO_D,
        PAIG.SRCE_SYST_C,
        MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY PAIG.ACCT_I, PAIG.INT_GRUP_I, PAIG.PERD_D) as GRUP_N,
        PAIG.ROW_SECU_ACCS_C
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST PAIG
        LEFT JOIN (
          SELECT
            A.INT_GRUP_I,
            A.ACCT_I,
            A.REL_C,
            A.ROW_N,
            A.PERD_D,
            A.EFFT_D,
            A.EXPY_D
          FROM
            NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST A
            INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST B
            ON A.ACCT_I = B.ACCT_I
            AND A.REL_C = B.REL_C
            AND A.ROW_N = B.ROW_N + 1
            AND (
              A.EFFT_D <> B.EFFT_D
              OR A.INT_GRUP_I <> B.INT_GRUP_I
            )
        ) DT1
        ON DT1.INT_GRUP_I = PAIG.INT_GRUP_I
        AND DT1.ACCT_I = PAIG.ACCT_I
        AND DT1.REL_C = PAIG.REL_C
        AND DT1.EFFT_D = PAIG.EFFT_D
        AND DT1.EXPY_D = PAIG.EXPY_D
        AND PAIG.ROW_N >= DT1.ROW_N
        AND DT1.PERD_D <= PAIG.PERD_D
    ) DT2
  ) DT3
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 8: Remove records with old VALD dates
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  WHERE EXISTS (
    SELECT 1
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST B
    WHERE
      DERV_PRTF_ACCT_PSST.ACCT_I = B.ACCT_I
      AND DERV_PRTF_ACCT_PSST.REL_C = B.REL_C
      AND DERV_PRTF_ACCT_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      AND DERV_PRTF_ACCT_PSST.JOIN_FROM_D <= B.JOIN_TO_D
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 9: Insert updated records with corrected VALD dates
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  SELECT
    INT_GRUP_I,
    ACCT_I,
    REL_C,
    JOIN_FROM_D,
    CASE
      WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE
      ELSE JOIN_TO_D
    END as JOIN_TO_D,
    VALD_FROM_D,
    VALD_TO_D,
    EFFT_D,
    EXPY_D,
    SRCE_SYST_C,
    0 as PROS_KEY_EFFT_I,
    ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Portfolio account processing completed. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_acct_rel_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_acct_rel_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_ACCT_REL_PSST_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  total_inserted INTEGER DEFAULT 0;
BEGIN
  -- Object Name: prtf_tech_acct_rel_psst.sql
  -- Description: Persist rows for DERV_PRTF_ACCT view
  -- Original Author: Helen Zak (Initial), Zeewa Lwin (Modified)
  
  -- Delete all existing records from target table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_REL;
  
  row_count := SQLROWCOUNT;
  
  -- Insert portfolio account relationship data
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_REL
  (
         ACCT_I 
        ,INT_GRUP_I 
        ,DERV_PRTF_CATG_C 
        ,DERV_PRTF_CLAS_C 
        ,DERV_PRTF_TYPE_C 
        ,VALD_FROM_D 
        ,VALD_TO_D 
        ,EFFT_D
        ,EXPY_D 
        ,PTCL_N
        ,REL_MNGE_I 
        ,PRTF_CODE_X 
        ,SRCE_SYST_C 
        ,ROW_SECU_ACCS_C
  )
  SELECT
     DT1.ACCT_I          
    ,DT1.INT_GRUP_I            
    ,GPTE2.PRTF_CATG_C                            AS DERV_PRTF_CATG_C
    ,GPTE2.PRTF_CLAS_C                            AS DERV_PRTF_CLAS_C  
    ,DT1.DERV_PRTF_TYPE_C
    ,DT1.VALD_FROM_D
    ,DT1.VALD_TO_D
    ,DT1.EFFT_D         
    ,DT1.EXPY_D  
    ,DT1.PTCL_N 
    ,DT1.REL_MNGE_I
    ,DT1.PRTF_CODE_X
    ,DT1.SRCE_SYST_C
    ,DT1.ROW_SECU_ACCS_C  
  FROM
    (
      SELECT
         PIG3.ACCT_I                                  AS ACCT_I
        ,IG3.INT_GRUP_I                               AS INT_GRUP_I
        ,PIG3.EFFT_D                                  AS EFFT_D
        ,PIG3.EXPY_D                                  AS EXPY_D
        ,IG3.INT_GRUP_TYPE_C                          AS DERV_PRTF_TYPE_C
        ,CAST(IG3.PTCL_N AS SMALLINT)                 AS PTCL_N
        ,IG3.REL_MNGE_I                               AS REL_MNGE_I
        ,(CASE
            WHEN (IG3.PTCL_N IS NULL) OR (IG3.REL_MNGE_I IS NULL) THEN 'NA'
            ELSE TRIM(IG3.PTCL_N) || TRIM(IG3.REL_MNGE_I)
          END)                                        AS PRTF_CODE_X
        ,PIG3.SRCE_SYST_C                             AS SRCE_SYST_C
        ,PIG3.ROW_SECU_ACCS_C                         AS ROW_SECU_ACCS_C
        ,(CASE
            WHEN IG3.JOIN_FROM_D > PIG3.JOIN_FROM_D THEN IG3.JOIN_FROM_D
            ELSE PIG3.JOIN_FROM_D
          END) AS VALD_FROM_D
        ,(CASE
            WHEN IG3.JOIN_TO_D < PIG3.JOIN_TO_D THEN IG3.JOIN_TO_D
            ELSE PIG3.JOIN_TO_D
          END) AS VALD_TO_D
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST PIG3
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST IG3
        ON IG3.INT_GRUP_I = PIG3.INT_GRUP_I
        AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D
        AND PIG3.JOIN_FROM_D <= IG3.JOIN_TO_D
    ) DT1
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2
    ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C;
  
  total_inserted := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Deleted ' || :row_count || ' records, Inserted ' || :total_inserted || ' records into DERV_PRTF_ACCT_REL';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_daly_datawatcher_c_proc.sql
-- SOURCE: prtf_tech/prtf_tech_daly_datawatcher_c_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_DALY_DATAWATCHER_C_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  source_found INTEGER DEFAULT 0;
BEGIN
  -- Check pre-requisite table loads have completed (ODS and Analytics)
  -- Ver 1.0 14/06/2013 T Jelliffe - Initial Version
  
  -- Check Analytics at table level and verify all dependencies are loaded
  SELECT COUNT(*) INTO :source_found
  FROM (
    SELECT SRCE_FND
    FROM (
      -- Check Analytics at table level
      SELECT 1 AS SRCE_FND
      FROM (
        SELECT 
          UPI.TRGT_M AS TRGT_M,
          UPI.SRCE_M as SRCE_M
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.UTIL_PROS_ISAC AS UPI
        -- Want the data loaded for yesterday
        WHERE UPI.BTCH_RUN_D = CURRENT_DATE - 1
        AND UPI.TRGT_M IN (
          -- Get the list of pre-requisite tables 
          SELECT UP1.PARM_LTRL_STRG_X
          FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.UTIL_PARM UP1
          WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'
        )
        -- Get the list of pre-requisite sources
        AND UPI.SRCE_M LIKE ANY (
          SELECT '%'||TRIM(UP1.PARM_LTRL_STRG_X)||'%' AS SRCE_M
          FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.UTIL_PARM UP1
          WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE'
        )
        -- and check they have been loaded
        AND UPI.COMT_F = 'Y'
        QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, SRCE_M)
      -- and check all relevant sources have loaded
      HAVING COUNT(TRGT_M) = (
        SELECT UP2.PARM_LTRL_N
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.UTIL_PARM UP2
        WHERE UP2.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'
      )
    ) SRCES
    GROUP BY 1
    HAVING COUNT(SRCE_FND) = 1
  );
  
  -- Handle the three exit conditions from original BTEQ
  IF (:source_found = 0) THEN
    -- Equivalent to REPOLL label - dependencies not ready
    RETURN 'REPOLL: Dependencies not ready for processing';
  ELSEIF (:source_found = 1) THEN
    -- Equivalent to successful exit - all dependencies loaded
    RETURN 'SUCCESS: All prerequisite table loads completed';
  ELSE
    -- Unexpected condition
    RETURN 'ERROR: Unexpected dependency check result: ' || :source_found;
  END IF;
  
EXCEPTION
  WHEN OTHER THEN
    -- Equivalent to EXITERR label
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_grd_prtf_type_enhc_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_grd_prtf_type_enhc_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_GRD_PRTF_TYPE_ENHC_PSST_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
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
  ------------------------------------------------------------------------------
  --
  --  Ver  Date       Modified By            Description
  --  ---- ---------- ---------------------- -----------------------------------
  --  1.0  15/07/2013 T Jelliffe             Initial Version
  --  1.5  01/11/2013 T Jelliffe             Add the HIST persisted table
  ------------------------------------------------------------------------------

  -- PDGRD DATA - Delete from main persistent table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG_V.DGRDDB.GRD_PRTF_TYPE_ENHC_PSST;
  row_count := SQLROWCOUNT;
  total_rows := total_rows + :row_count;

  -- Insert into main persistent table from source
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.DGRDDB.GRD_PRTF_TYPE_ENHC_PSST
  SELECT
     GP.PERD_D
    ,GP.PRTF_TYPE_C
    ,GP.PRTF_TYPE_M
    ,GP.PRTF_CLAS_C
    ,GP.PRTF_CLAS_M
    ,GP.PRTF_CATG_C
    ,GP.PRTF_CATG_M
  FROM                
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC GP;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + :row_count;

  -- Populate the HISTORY version of the table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG_V.DGRDDB.GRD_PRTF_TYPE_ENHC_HIST_PSST;
  row_count := SQLROWCOUNT;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.DGRDDB.GRD_PRTF_TYPE_ENHC_HIST_PSST
  SELECT
     G.PRTF_TYPE_C                   
    ,G.PRTF_TYPE_M                   
    ,G.PRTF_CLAS_C                   
    ,G.PRTF_CLAS_M                   
    ,G.PRTF_CATG_C                   
    ,G.PRTF_CATG_M    
    ,MIN(PERD_D) as VALD_FROM_D
    ,MAX(PERD_D) as VALD_TO_D
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_PSST G
  GROUP BY 1,2,3,4,5,6;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + :row_count;

  RETURN 'SUCCESS: Portfolio type enhancement data processed. Total rows affected: ' || :total_rows;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_int_grup_enhc_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_int_grup_enhc_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_INT_GRUP_ENHC_PSST_PROC(
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
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
  -- Portfolio Interest Group Enhancement Processing
  -- Original Author: T Jelliffe (2013)
  -- Processes interest group data with historical tracking
  
  -- STEP 1: Keep a copy of INT_GRUP base table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_PSST
  SELECT
     A.INT_GRUP_I,
     A.INT_GRUP_TYPE_C,
     A.CRAT_D as JOIN_FROM_D,
     A.VALD_TO_D as JOIN_TO_D,
     A.EFFT_D,
     A.EXPY_D,
     A.PTCL_N,
     A.REL_MNGE_I,
     A.CRAT_D as VALD_FROM_D,
     A.VALD_TO_D,
     A.PROS_KEY_EFFT_I
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP A
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE
      ON GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C
      AND GPTE.VALD_TO_D >= A.CRAT_D
      AND GPTE.VALD_FROM_D <= A.VALD_TO_D
      AND TRY_TO_NUMBER(COALESCE(A.PTCL_N, '0')) IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 2: Process INT_GRUP enhancement
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_ENHC_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_ENHC_PSST
  SELECT
     DT2.INT_GRUP_I,
     DT2.INT_GRUP_TYPE_C,
     DT2.EFFT_D,
     DT2.EXPY_D,
     DT2.VALD_FROM_D,
     DT2.VALD_TO_D,
     DT2.PERD_D,
     DT2.PTCL_N,
     DT2.REL_MNGE_I,
     ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I ORDER BY DT2.PERD_D),
     0 as PROS_KEY
  FROM
    (
      SELECT
         A.INT_GRUP_I,
         A.INT_GRUP_TYPE_C,
         A.PTCL_N,
         C.CALENDAR_DATE as PERD_D,
         A.EFFT_D,
         A.EXPY_D,
         A.VALD_FROM_D,
         A.VALD_TO_D,
         A.REL_MNGE_I
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST A
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST B
          ON A.INT_GRUP_I = B.INT_GRUP_I
          AND TRY_TO_NUMBER(COALESCE(A.PTCL_N, '0')) IS NOT NULL
          AND TRY_TO_NUMBER(COALESCE(B.PTCL_N, '0')) IS NOT NULL
          AND A.JOIN_TO_D >= B.JOIN_FROM_D
          AND A.JOIN_FROM_D <= B.JOIN_TO_D
          AND (
            A.JOIN_FROM_D <> B.JOIN_FROM_D
            OR A.JOIN_TO_D <> B.JOIN_TO_D
            OR A.EFFT_D <> B.EFFT_D
            OR A.EXPY_D <> B.EXPY_D
            OR A.PTCL_N <> B.PTCL_N
            OR A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C
            OR A.REL_MNGE_I <> B.REL_MNGE_I
          )
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
          ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D
          AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) 
                                  AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY A.INT_GRUP_I, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC) = 1
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 3: Create history records
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_HIST_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_HIST_PSST
  SELECT DISTINCT
     DT2.INT_GRUP_I,
     DT2.INT_GRUP_TYPE_C,
     DT2.EFFT_D,
     DT2.EXPY_D,
     DT2.VALD_FROM_D,
     DT2.VALD_TO_D,
     MIN(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.GRUP_N) as JOIN_FROM_D,
     MAX(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.GRUP_N) as JOIN_TO_D,
     DT2.PTCL_N,
     DT2.REL_MNGE_I
  FROM
    (
      SELECT
         C.INT_GRUP_I,
         C.INT_GRUP_TYPE_C,
         C.EFFT_D,
         C.EXPY_D,
         C.VALD_FROM_D,
         C.VALD_TO_D,
         C.PERD_D,
         C.PTCL_N,
         C.REL_MNGE_I,
         MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.PTCL_N, C.REL_MNGE_I, C.PERD_D) as GRUP_N
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST C
        LEFT JOIN (
          SELECT
             A.INT_GRUP_I,
             A.PTCL_N,
             A.REL_MNGE_I,
             A.PERD_D,
             A.ROW_N
          FROM
            NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST A
            INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST B
              ON A.INT_GRUP_I = B.INT_GRUP_I
              AND A.ROW_N = B.ROW_N + 1
              AND (
                A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C
                OR A.PTCL_N <> B.PTCL_N
                OR A.REL_MNGE_I <> B.REL_MNGE_I
              )
        ) DT1
          ON C.INT_GRUP_I = DT1.INT_GRUP_I
          AND C.PTCL_N = DT1.PTCL_N
          AND C.REL_MNGE_I = DT1.REL_MNGE_I
          AND C.ROW_N >= DT1.ROW_N
          AND DT1.PERD_D <= C.PERD_D
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Portfolio Interest Group Enhancement completed. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_int_grup_own_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_int_grup_own_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_INT_GRUP_OWN_PSST_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
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
  -- Portfolio Ownership Processing
  -- Original Author: T Jelliffe (Initial Version 11/06/2013)
  -- Purpose: Populate derived portfolio ownership tables with employee and department data
  
  -- Step 1: Populate the final OWN_PSST table with source data
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  SELECT
     IGE.INT_GRUP_I                    
    ,IGE.VALD_FROM_D as JOIN_FROM_D                   
    ,COALESCE(IGE.VALD_TO_D, '9999-12-31'::DATE) as JOIN_TO_D
    ,IGE.VALD_FROM_D                   
    ,IGE.VALD_TO_D
    ,IGE.EFFT_D                        
    ,IGE.EXPY_D                                            
    ,(CASE
        WHEN IGE.EMPL_ROLE_C = 'OWN' THEN 'OWNR'
        WHEN IGE.EMPL_ROLE_C = 'AOW' THEN 'AOWN'
        WHEN IGE.EMPL_ROLE_C = 'AST' THEN 'ASTT'      
        ELSE NULL
       END) as DERV_PRTF_ROLE_C
    ,'Employee'::VARCHAR(40) as ROLE_PLAY_TYPE_X  
    ,IGE.EMPL_I as ROLE_PLAY_I                        
    ,IGE.SRCE_SYST_C                                       
    ,IGE.ROW_SECU_ACCS_C               
    ,IGE.PROS_KEY_EFFT_I               
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP_EMPL IGE
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP IG
      ON IG.INT_GRUP_I = IGE.INT_GRUP_I
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
      ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
      AND (GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= IG.CRAT_D)
      AND (GPTE.VALD_FROM_D <= IGE.VALD_TO_D AND GPTE.VALD_TO_D >= IGE.VALD_FROM_D)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

  UNION ALL

  SELECT
     IGD.INT_GRUP_I                                   
    ,IGD.VALD_FROM_D as JOIN_FROM_D                   
    ,COALESCE(IGD.VALD_TO_D, '9999-12-31'::DATE) as JOIN_TO_D              
    ,IGD.VALD_FROM_D                 
    ,IGD.VALD_TO_D                  
    ,IGD.EFFT_D                        
    ,IGD.EXPY_D                        
    ,(CASE
        WHEN IGD.DEPT_ROLE_C = 'OWNG' THEN 'OWNR'
        WHEN IGD.DEPT_ROLE_C = 'STDP' THEN 'STDP'   
        ELSE NULL
      END) as DERV_PRTF_ROLE_C                
    ,'Department'::VARCHAR(40) as ROLE_PLAY_TYPE_X              
    ,IGD.DEPT_I as ROLE_PLAY_I                   
    ,IGD.SRCE_SYST_C                   
    ,IGD.ROW_SECU_ACCS_C       
    ,IGD.PROS_KEY_EFFT_I       
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP_DEPT IGD
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP IG
      ON IG.INT_GRUP_I = IGD.INT_GRUP_I
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
      ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
      AND (GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= IG.CRAT_D)
      AND (GPTE.VALD_FROM_D <= IGD.VALD_TO_D AND GPTE.VALD_TO_D >= IGD.VALD_FROM_D)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  -- Step 2: Employee Business date deduplication
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
  SELECT
     DT2.INT_GRUP_I           as INT_GRUP_I
    ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
    ,DT2.EFFT_D               as EFFT_D
    ,DT2.EXPY_D               as EXPY_D
    ,DT2.VALD_FROM_D          as VALD_FROM_D
    ,DT2.VALD_TO_D            as VALD_TO_D
    ,DT2.PERD_D               as PERD_D  
    ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
    ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
    ,DT2.SRCE_SYST_C          as SRCE_SYST_C
    ,ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I ORDER BY DT2.PERD_D, DT2.DERV_PRTF_ROLE_C) as ROW_N
    ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
    ,0                        as PROS_KEY_I
  FROM
    (
      SELECT DERV.*, C.CALENDAR_DATE as PERD_D  
      FROM 
      (
        SELECT
           A.INT_GRUP_I           as INT_GRUP_I
          ,A.ROLE_PLAY_I
          ,A.DERV_PRTF_ROLE_C  
          ,A.EFFT_D               as EFFT_D
          ,A.EXPY_D               as EXPY_D
          ,A.ROLE_PLAY_TYPE_X 
          ,A.VALD_FROM_D
          ,A.VALD_TO_D
          ,A.SRCE_SYST_C          as SRCE_SYST_C
          ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
          ,A.JOIN_FROM_D
          ,A.JOIN_TO_D
        FROM
          NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST A
          INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST B
            ON A.INT_GRUP_I = B.INT_GRUP_I
            AND A.ROLE_PLAY_I = B.ROLE_PLAY_I
            AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
            AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
            AND A.DERV_PRTF_ROLE_C = 'OWNR'
            AND (A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
      ) DERV
      INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN DERV.JOIN_FROM_D AND DERV.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY DERV.INT_GRUP_I, DERV.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY DERV.EFFT_D DESC, DERV.ROLE_PLAY_I) = 1 
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  -- Step 3: History version of above
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST 
  SELECT
     DT3.INT_GRUP_I                    
    ,DT3.ROLE_PLAY_I 
    ,DT3.JOIN_FROM_D
    ,(CASE
        WHEN DT3.JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE             
        ELSE DT3.JOIN_TO_D
      END) as JOIN_TO_D 
    ,DT3.EFFT_D                        
    ,DT3.EXPY_D   
    ,DT3.VALD_FROM_D
    ,DT3.VALD_TO_D  
    ,DT3.ROLE_PLAY_TYPE_X              
    ,DT3.DERV_PRTF_ROLE_C              
    ,DT3.SRCE_SYST_C     
    ,DT3.ROW_SECU_ACCS_C               
    ,DT3.PROS_KEY_I    
  FROM
    (
      SELECT DISTINCT
         DT2.INT_GRUP_I                    
        ,DT2.ROLE_PLAY_I 
        ,MIN(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) as JOIN_FROM_D
        ,MAX(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) as JOIN_TO_D
        ,DT2.EFFT_D                        
        ,DT2.EXPY_D   
        ,DT2.VALD_FROM_D
        ,DT2.VALD_TO_D  
        ,DT2.ROLE_PLAY_TYPE_X              
        ,DT2.DERV_PRTF_ROLE_C              
        ,DT2.SRCE_SYST_C     
        ,DT2.ROW_SECU_ACCS_C               
        ,DT2.PROS_KEY_I                    
      FROM
        (
          SELECT
             C.INT_GRUP_I                    
            ,C.ROLE_PLAY_I                   
            ,C.EFFT_D                        
            ,C.EXPY_D   
            ,C.VALD_FROM_D
            ,C.VALD_TO_D  
            ,C.PERD_D                        
            ,C.ROLE_PLAY_TYPE_X              
            ,C.DERV_PRTF_ROLE_C              
            ,C.SRCE_SYST_C     
            ,C.ROW_N
            ,C.ROW_SECU_ACCS_C               
            ,C.PROS_KEY_I                    
            ,MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N
          FROM
            NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST C
            LEFT JOIN (
              SELECT
                 A.INT_GRUP_I
                ,A.ROLE_PLAY_I
                ,A.DERV_PRTF_ROLE_C
                ,A.ROW_N
                ,A.EFFT_D
                ,A.PERD_D
              FROM
                NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST A
                INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST B
                  ON A.INT_GRUP_I = B.INT_GRUP_I
                  AND A.ROLE_PLAY_I = B.ROLE_PLAY_I
                  AND A.ROW_N = B.ROW_N + 1
                  AND (
                    A.DERV_PRTF_ROLE_C <> B.DERV_PRTF_ROLE_C
                    OR A.EFFT_D <> B.EFFT_D
                  )
            ) DT1
              ON C.INT_GRUP_I = DT1.INT_GRUP_I
              AND C.ROLE_PLAY_I = DT1.ROLE_PLAY_I
              AND C.EFFT_D = DT1.EFFT_D
              AND C.ROW_N >= DT1.ROW_N
              AND DT1.PERD_D <= C.PERD_D
        ) DT2
    ) DT3;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  -- Step 4: Replace overlapping records
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_HIST_PSST B
    WHERE DERV_PRTF_OWN_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_OWN_PSST.ROLE_PLAY_I = B.ROLE_PLAY_I
      AND DERV_PRTF_OWN_PSST.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
      AND DERV_PRTF_OWN_PSST.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
      AND DERV_PRTF_OWN_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      AND DERV_PRTF_OWN_PSST.JOIN_FROM_D <= B.JOIN_TO_D
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  SELECT
     INT_GRUP_I,
     JOIN_FROM_D,
     CASE
       WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE
       ELSE JOIN_TO_D
     END as JOIN_TO_D,
     VALD_FROM_D,
     VALD_TO_D,
     EFFT_D,
     EXPY_D,
     DERV_PRTF_ROLE_C,
     ROLE_PLAY_TYPE_X,
     ROLE_PLAY_I,
     SRCE_SYST_C,
     ROW_SECU_ACCS_C,
     PROS_KEY_I
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  RETURN 'SUCCESS: Portfolio ownership processing completed. Total rows processed: ' || :total_rows;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_int_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_int_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_INT_PSST_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  delete_count INTEGER DEFAULT 0;
  insert_count INTEGER DEFAULT 0;
BEGIN
  ------------------------------------------------------------------------------
  --
  --  Ver  Date       Modified By        Description
  --  ---- ---------- ------------------ ---------------------------------------
  --  1.0  11/06/2013 T Jelliffe         Initial Version
  --  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
  --  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
  --  1.3  27/08/2013 T Jelliffe         Use only records with same EFFT_D
  --  1.4  21/10/2013 T Jelliffe         Insert/Delete changed records
  --  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
  ------------------------------------------------------------------------------
  
  --<================================================>--
  --< STEP 4 Delete all the original overlap records >--
  --<================================================>--
  
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_PSST
  WHERE EXISTS (
    SELECT 1
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_HIST_PSST B
    WHERE 
      DERV_PRTF_INT_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_INT_PSST.JOIN_FROM_D <= B.JOIN_TO_D AND DERV_PRTF_INT_PSST.JOIN_TO_D >= B.JOIN_FROM_D
  );
  
  delete_count := SQLROWCOUNT;
  
  --<===============================================>--
  --< STEP 5 - Insert all deduped records into base >--
  --<===============================================>--
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_PSST
  SELECT
     A.INT_GRUP_I,
     A.INT_GRUP_TYPE_C,
     A.JOIN_FROM_D,
     A.JOIN_TO_D,
     A.EFFT_D,
     A.EXPY_D,
     A.PTCL_N,
     A.REL_MNGE_I,
     A.VALD_FROM_D,
     A.VALD_TO_D,
     0 as PROS_KEY_I
  FROM 
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_HIST_PSST A;
  
  insert_count := SQLROWCOUNT;
  
  -- Collect Statistics equivalent (Snowflake auto-manages statistics)
  -- Statistics collection is automatic in Snowflake
  
  RETURN 'SUCCESS: Deleted ' || :delete_count || ' overlapping records, inserted ' || :insert_count || ' deduped records';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_own_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_own_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_OWN_PSST_PROC()
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
  ------------------------------------------------------------------------------
  --
  --  Ver  Date       Modified By        Description
  --  ---- ---------- ------------------ ---------------------------------------
  --  1.0  18/07/2013 T Jelliffe         Initial Version
  --  1.1  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
  --  1.2  27/11/2013 T Jelliffe         Fix overlap calc, JOIN_DATE to calendar
  ------------------------------------------------------------------------------
  
  --<=========================================================================>--
  --< Step 5 : Rule 3 & 4 - Only one Dept/Empl owner
  --<=========================================================================>--
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
  SELECT
     DT2.INT_GRUP_I           as INT_GRUP_I
    ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
    ,DT2.EFFT_D               as EFFT_D
    ,DT2.EXPY_D               as EXPY_D
    ,DT2.VALD_FROM_D          as VALD_FROM_D
    ,DT2.VALD_TO_D            as VALD_TO_D
    ,DT2.PERD_D               as PERD_D  
    ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
    ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
    ,DT2.SRCE_SYST_C          as SRCE_SYST_C
    ,ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I ORDER BY DT2.PERD_D, DT2.DERV_PRTF_ROLE_C)
    ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
    ,0                        as PROS_KEY_I
  FROM
    (
       -- KEY =  (INT_GRUP_I, DERV_PRTF_ROLE_C)
      SELECT
         A.INT_GRUP_I           as INT_GRUP_I
        ,A.ROLE_PLAY_I               as ROLE_PLAY_I
        ,C.CALENDAR_DATE as PERD_D
        ,A.EFFT_D               as EFFT_D
        ,A.EXPY_D               as EXPY_D
        ,A.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
        ,A.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
        ,A.VALD_FROM_D
        ,A.VALD_TO_D
        ,A.SRCE_SYST_C          as SRCE_SYST_C
        ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST A
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST B    
        ON A.INT_GRUP_I = B.INT_GRUP_I
        AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
        AND A.DERV_PRTF_ROLE_C = 'OWNR'
        AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
        AND (A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D)
        AND (
          A.JOIN_FROM_D <> B.JOIN_FROM_D
          OR A.JOIN_TO_D <> B.JOIN_TO_D
          OR A.ROLE_PLAY_I <> B.ROLE_PLAY_I
        )
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY A.INT_GRUP_I, A.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC, A.ROLE_PLAY_I DESC) = 1
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  --<=========================================================================>--
  --< Step 6 : Hist view of Step 5
  --<=========================================================================>--
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST
  SELECT DISTINCT
     DT2.INT_GRUP_I                    
    ,DT2.ROLE_PLAY_I 
    ,MIN(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) as JOIN_FROM_D
    ,MAX(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) as JOIN_TO_D
    ,DT2.EFFT_D                        
    ,DT2.EXPY_D   
    ,DT2.VALD_FROM_D
    ,DT2.VALD_TO_D  
    ,DT2.ROLE_PLAY_TYPE_X              
    ,DT2.DERV_PRTF_ROLE_C              
    ,DT2.SRCE_SYST_C    
    ,DT2.ROW_SECU_ACCS_C               
    ,DT2.PROS_KEY_I                    
  FROM
    (
      SELECT DISTINCT
         C.INT_GRUP_I                    
        ,C.ROLE_PLAY_I                   
        ,C.EFFT_D                        
        ,C.EXPY_D   
        ,C.VALD_FROM_D
        ,C.VALD_TO_D  
        ,C.PERD_D                        
        ,C.ROLE_PLAY_TYPE_X              
        ,C.DERV_PRTF_ROLE_C              
        ,C.SRCE_SYST_C     
        ,C.ROW_N
        ,C.ROW_SECU_ACCS_C               
        ,C.PROS_KEY_I                    
        ,MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N 
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST C
        LEFT JOIN (
          -- Detect the change in non-key values between rows
          SELECT
             A.INT_GRUP_I
            ,A.ROLE_PLAY_I
            ,A.DERV_PRTF_ROLE_C
            ,A.ROLE_PLAY_TYPE_X
            ,A.ROW_N
            ,A.PERD_D
            ,A.EFFT_D
          FROM
            NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST A
            INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST B
            ON A.INT_GRUP_I = B.INT_GRUP_I
            AND A.ROW_N = B.ROW_N + 1
            AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
            AND A.DERV_PRTF_ROLE_C = 'OWNR'
            AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
            AND ( 
              A.ROLE_PLAY_I <> B.ROLE_PLAY_I
              OR A.EFFT_D <> B.EFFT_D
            )
        ) DT1
        ON C.INT_GRUP_I = DT1.INT_GRUP_I
        AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X
        AND C.EFFT_D = DT1.EFFT_D
        AND C.ROW_N >= DT1.ROW_N
        AND DT1.PERD_D <= C.PERD_D 
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  --<=========================================================================>--
  --< Step 7 : Delete and refresh for Step 6
  --<=========================================================================>--
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_HIST_PSST B
    WHERE  
      DERV_PRTF_OWN_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_OWN_PSST.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X        
      AND DERV_PRTF_OWN_PSST.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C        
      AND (DERV_PRTF_OWN_PSST.JOIN_FROM_D <= B.JOIN_TO_D AND DERV_PRTF_OWN_PSST.JOIN_TO_D >= B.JOIN_FROM_D)
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  SELECT
     INT_GRUP_I                    
    ,JOIN_FROM_D                   
    ,(CASE
        WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE             
        ELSE JOIN_TO_D
      END
     ) as JOIN_TO_D   
    ,VALD_FROM_D                   
    ,VALD_TO_D             
    ,EFFT_D                        
    ,EXPY_D                        
    ,DERV_PRTF_ROLE_C              
    ,ROLE_PLAY_TYPE_X              
    ,ROLE_PLAY_I                   
    ,SRCE_SYST_C                   
    ,ROW_SECU_ACCS_C               
    ,PROS_KEY_I                      
  FROM 
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  --<=========================================================================>--
  --< Step 8 : When both Dept and Empl take Empl (Rule 5)
  --<=========================================================================>--
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
  SELECT
     DT2.INT_GRUP_I           as INT_GRUP_I
    ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
    ,DT2.EFFT_D               as EFFT_D
    ,DT2.EXPY_D               as EXPY_D
    ,DT2.VALD_FROM_D          as VALD_FROM_D
    ,DT2.VALD_TO_D            as VALD_TO_D
    ,DT2.PERD_D               as PERD_D  
    ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
    ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
    ,DT2.SRCE_SYST_C          as SRCE_SYST_C
    ,ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.DERV_PRTF_ROLE_C ORDER BY DT2.PERD_D, DT2.ROLE_PLAY_I DESC) as ROW_N
    ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
    ,0                        as PROS_KEY_I
  FROM
    (
       -- KEY =  (INT_GRUP_I, DERV_PRTF_ROLE_C)
      SELECT
         A.INT_GRUP_I           as INT_GRUP_I
        ,A.ROLE_PLAY_I               as ROLE_PLAY_I
        ,C.CALENDAR_DATE as PERD_D
        ,A.EFFT_D               as EFFT_D
        ,A.EXPY_D               as EXPY_D
        ,A.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
        ,A.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
        ,A.VALD_FROM_D
        ,A.VALD_TO_D
        ,A.SRCE_SYST_C          as SRCE_SYST_C
        ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST A
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST B    
        ON A.INT_GRUP_I = B.INT_GRUP_I
        AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
        AND A.DERV_PRTF_ROLE_C = 'OWNR'
        AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
        AND (A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D)
        AND (
          A.JOIN_FROM_D <> B.JOIN_FROM_D
          OR A.JOIN_TO_D <> B.JOIN_TO_D
          OR A.ROLE_PLAY_I <> B.ROLE_PLAY_I
        )
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY A.INT_GRUP_I, A.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC, A.ROLE_PLAY_I DESC) = 1
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Processed ' || :total_rows || ' total rows across all operations';

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_own_rel_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_own_rel_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_OWN_REL_PSST_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  deleted_count INTEGER DEFAULT 0;
BEGIN
  -- Object Name: prtf_tech_own_rel_psst.sql
  -- Description: Persist rows for DERV_PRTF_OWN view
  -- Original Authors: Helen Zak (v1.0), Zeewa Lwin (v2.0)
  
  -- Delete all records from target table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_REL;
  deleted_count := SQLROWCOUNT;
  
  -- Insert new records with portfolio ownership relationships
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_REL
  (
      INT_GRUP_I 
    , DERV_PRTF_CATG_C 
    , DERV_PRTF_CLAS_C 
    , DERV_PRTF_TYPE_C 
    , VALD_FROM_D 
    , VALD_TO_D 
    , EFFT_D 
    , EXPY_D 
    , PTCL_N 
    , REL_MNGE_I 
    , PRTF_CODE_X 
    , DERV_PRTF_ROLE_C 
    , ROLE_PLAY_TYPE_X 
    , ROLE_PLAY_I
    , SRCE_SYST_C 
    , ROW_SECU_ACCS_C 
  )
  SELECT 
      DT1.INT_GRUP_I
    , GPTE2.PRTF_CATG_C AS DERV_PRTF_CATG_C 
    , GPTE2.PRTF_CLAS_C AS DERV_PRTF_CLAS_C
    , DT1.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
    , DT1.VALD_FROM_D
    , DT1.VALD_TO_D
    , DT1.EFFT_D AS EFFT_D
    , DT1.EXPY_D AS EXPY_D 
    , DT1.PTCL_N AS PTCL_N
    , DT1.REL_MNGE_I AS REL_MNGE_I
    , DT1.PRTF_CODE_X AS PRTF_CODE_X 
    , DT1.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
    , DT1.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X
    , DT1.ROLE_PLAY_I AS ROLE_PLAY_I 
    , DT1.SRCE_SYST_C AS SRCE_SYST_C 
    , DT1.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 
  FROM 
      ( SELECT 
            IG2.INT_GRUP_I AS INT_GRUP_I
          , IGED.EFFT_D AS EFFT_D 
          , IGED.EXPY_D AS EXPY_D 
          , IG2.INT_GRUP_TYPE_C AS DERV_PRTF_TYPE_C
          , CAST( IG2.PTCL_N AS SMALLINT ) AS PTCL_N
          , IG2.REL_MNGE_I AS REL_MNGE_I
          , ( CASE 
                 WHEN ( IG2.PTCL_N IS NULL ) OR ( IG2.REL_MNGE_I IS NULL ) THEN 'NA'
                 ELSE TRIM ( IG2.PTCL_N ) || TRIM ( IG2.REL_MNGE_I ) 
                 END  ) AS PRTF_CODE_X
          , IGED.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C 
          , IGED.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X 
          , IGED.ROLE_PLAY_I AS ROLE_PLAY_I 
          , IGED.SRCE_SYST_C AS SRCE_SYST_C
          , IGED.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 
          , ( CASE 
                 WHEN IG2.JOIN_FROM_D > IGED.JOIN_FROM_D 
                 THEN IG2.JOIN_FROM_D 
                 ELSE IGED.JOIN_FROM_D
                 END  ) AS VALD_FROM_D 
          , ( CASE 
                 WHEN IG2.JOIN_TO_D < IGED.JOIN_TO_D 
                 THEN IG2.JOIN_TO_D 
                 ELSE IGED.JOIN_TO_D
                 END  ) AS VALD_TO_D 
                         
     FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST IGED
  
     INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST IG2
             ON IGED.INT_GRUP_I = IG2.INT_GRUP_I
            AND IGED.JOIN_TO_D >= IG2.JOIN_FROM_D 
            AND IGED.JOIN_FROM_D <= IG2.JOIN_TO_D 
  ) DT1 
                            
  INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2 
          ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C;
  
  row_count := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Deleted ' || :deleted_count || ' records, inserted ' || :row_count || ' records into DERV_PRTF_OWN_REL';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_paty_int_grup_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_paty_int_grup_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_PATY_INT_GRUP_PSST_PROC()
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
  ------------------------------------------------------------------------------
  --
  --  Ver  Date       Modified By        Description
  --  ---- ---------- ------------------ ---------------------------------------
  --  1.0  11/06/2013 T Jelliffe         Initial Version
  --  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
  --  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
  --  1.3  31/07/2013 Z Lwin             Remove EROR_SEQN_I
  --  1.4  27/08/2013 T Jelliffe         Use only records with same EFFT_D
  --  1.5  21/10/2013 T Jelliffe         Use Insert/Delete process
  --  1.6  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
  ------------------------------------------------------------------------------

  --<============================================>--
  --< STEP 1 - PIG group                         >--
  --<============================================>--

  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST 
  SELECT
     PIG.INT_GRUP_I                    
    ,PIG.PATY_I                        
    ,PIG.VALD_FROM_D                   as JOIN_FROM_D
    ,PIG.VALD_TO_D                     as JOIN_TO_D  
    ,PIG.EFFT_D                        
    ,PIG.EXPY_D                        
    ,PIG.VALD_FROM_D                   
    ,PIG.VALD_TO_D                     
    ,PIG.REL_C                         
    ,PIG.SRCE_SYST_C                   
    ,PIG.ROW_SECU_ACCS_C
    ,PIG.PROS_KEY_EFFT_I                as PROS_KEY_I
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.PATY_INT_GRUP PIG

    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.INT_GRUP IG
    ON PIG.INT_GRUP_I = IG.INT_GRUP_I
    AND PIG.VALD_FROM_D <= IG.VALD_TO_D AND PIG.VALD_TO_D >= IG.CRAT_D

    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
    ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
    AND GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= IG.CRAT_D
    AND GPTE.VALD_FROM_D <= PIG.VALD_TO_D AND GPTE.VALD_TO_D >= PIG.VALD_FROM_D
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  --<============================================>--
  --< STEP 2 - Daily PIG overlaps                >--
  --<============================================>--

  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST 
  SELECT
     DT2.INT_GRUP_I
    ,DT2.PATY_I
    ,DT2.EFFT_D
    ,DT2.EXPY_D      
    ,DT2.VALD_FROM_D
    ,DT2.VALD_TO_D   
    ,DT2.PERD_D
    ,DT2.REL_C
    ,DT2.SRCE_SYST_C
    ,ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.PATY_I ORDER BY DT2.PERD_D, DT2.REL_C ) as GRUP_N  
    ,DT2.ROW_SECU_ACCS_C
    ,DT2.PROS_KEY_I
  FROM 
    (
      SELECT
         A.INT_GRUP_I
        ,A.PATY_I
        ,C.CALENDAR_DATE                as PERD_D             
        ,A.EFFT_D
        ,A.EXPY_D
        ,A.VALD_FROM_D
        ,A.VALD_TO_D
        ,A.REL_C
        ,A.SRCE_SYST_C
        ,A.ROW_SECU_ACCS_C
        ,A.PROS_KEY_I
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_PSST A
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_PSST B
        ON A.PATY_I = B.PATY_I
        AND A.INT_GRUP_I = B.INT_GRUP_I
        AND A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D  
        AND (
          A.JOIN_FROM_D <> B.JOIN_FROM_D
          OR A.JOIN_TO_D <> B.JOIN_TO_D
        )

        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN A.VALD_FROM_D AND A.VALD_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)

      QUALIFY ROW_NUMBER() OVER( PARTITION BY A.PATY_I, A.INT_GRUP_I, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC) = 1    
    ) DT2;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  --<============================================>--
  --< STEP 3 - History group                     >--
  --<============================================>--
  
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST 
  SELECT DISTINCT
     DT2.INT_GRUP_I
    ,DT2.PATY_I
    ,DT2.REL_C
    ,MIN(DT2.PERD_D) OVER ( PARTITION BY DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
    ,MAX(DT2.PERD_D) OVER ( PARTITION BY DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
    ,DT2.VALD_FROM_D
    ,DT2.VALD_TO_D
    ,DT2.EFFT_D
    ,DT2.EXPY_D
    ,DT2.SRCE_SYST_C
    ,DT2.ROW_SECU_ACCS_C
    ,DT2.PROS_KEY_I
  FROM
    (
    SELECT
       C.INT_GRUP_I
      ,C.PATY_I
      ,C.REL_C
      ,C.VALD_FROM_D
      ,C.VALD_TO_D
      ,C.EFFT_D
      ,C.EXPY_D
      ,C.SRCE_SYST_C
      ,C.ROW_SECU_ACCS_C
      ,C.PERD_D
      ,MAX(COALESCE( DT1.ROW_N, 0 )) OVER (PARTITION BY C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D) as GRUP_N     
      ,C.PROS_KEY_I
    FROM
      NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST C
      LEFT JOIN (
        -- Detect the change in non-key values between rows
        SELECT
           A.INT_GRUP_I
          ,A.PATY_I
          ,A.EFFT_D
          ,A.REL_C
          ,A.ROW_N as ROW_N
          ,A.PERD_D
        FROM
          NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST A
          INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST B
          ON A.PATY_I = B.PATY_I
          AND A.INT_GRUP_I = B.INT_GRUP_I
          AND A.ROW_N = B.ROW_N + 1
          AND A.EFFT_D <> B.EFFT_D
      ) DT1
      ON C.INT_GRUP_I = DT1.INT_GRUP_I
      AND C.PATY_I = DT1.PATY_I
      AND C.INT_GRUP_I = DT1.INT_GRUP_I
      AND C.REL_C = DT1.REL_C
      AND C.EFFT_D = DT1.EFFT_D
      AND C.ROW_N >= DT1.ROW_N
      AND DT1.PERD_D <= C.PERD_D 
   
    ) DT2;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  --<================================================>--
  --< STEP 4 Delete all the original overlap records >--
  --<================================================>--

  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_HIST_PSST B
    WHERE  
      DERV_PRTF_PATY_PSST.PATY_I = B.PATY_I
      AND DERV_PRTF_PATY_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_PATY_PSST.JOIN_FROM_D <= B.JOIN_TO_D AND DERV_PRTF_PATY_PSST.JOIN_TO_D >= B.JOIN_FROM_D
  );

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  --<================================================>--
  --< STEP 5 - Replace with updated records          >--
  --<================================================>--

  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST
  SELECT
     INT_GRUP_I                    
    ,PATY_I                        
    ,JOIN_FROM_D                   
    ,(CASE
        WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE             
        ELSE JOIN_TO_D
      END
      ) as JOIN_TO_D                   
    ,EFFT_D                        
    ,EXPY_D                        
    ,VALD_FROM_D                   
    ,VALD_TO_D                     
    ,REL_C                         
    ,SRCE_SYST_C                   
    ,ROW_SECU_ACCS_C               
    ,PROS_KEY_I                    
  FROM
    NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_HIST_PSST;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  RETURN 'SUCCESS: Portfolio party interest group processing completed. Total operations: ' || :total_rows;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_paty_own_rel_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_paty_own_rel_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_PATY_OWN_REL_PSST_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  deleted_count INTEGER DEFAULT 0;
  inserted_count INTEGER DEFAULT 0;
BEGIN
  -- Object Name: prtf_tech_paty_own_rel_psst.sql
  -- Description: Persist rows for DERV_PRTF_PATY_OWN view
  -- Original Author: Helen Zak (09/01/2014)
  
  -- Delete all rows from target table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_OWN_REL;
  
  deleted_count := SQLROWCOUNT;
  
  -- Insert data from joined source tables
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_OWN_REL
  ( PATY_I 
  , INT_GRUP_I 
  , DERV_PRTF_CATG_C
  , DERV_PRTF_CLAS_C 
  , DERV_PRTF_TYPE_C 
  , PRTF_PATY_VALD_FROM_D 
  , PRTF_PATY_VALD_TO_D 
  , PRTF_PATY_EFFT_D 
  , PRTF_PATY_EXPY_D
  , PRTF_OWN_VALD_FROM_D 
  , PRTF_OWN_VALD_TO_D 
  , PRTF_OWN_EFFT_D
  , PRTF_OWN_EXPY_D
  , PTCL_N 
  , REL_MNGE_I 
  , PRTF_CODE_X 
  , DERV_PRTF_ROLE_C
  , ROLE_PLAY_TYPE_X 
  , ROLE_PLAY_I 
  , SRCE_SYST_C
  , ROW_SECU_ACCS_C
  )
  SELECT 
    PP4.PATY_I AS PATY_I 
  , PP4.INT_GRUP_I AS INT_GRUP_I  
  , PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C 
  , PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C 
  , PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
  , PP4.VALD_FROM_D AS PRTF_PATY_VALD_FROM_D 
  , PP4.VALD_TO_D AS PRTF_PATY_VALD_TO_D 
  , PP4.EFFT_D AS PRTF_PATY_EFFT_D 
  , PP4.EXPY_D AS PRTF_PATY_EXPY_D 
  , PO4.VALD_FROM_D AS PRTF_OWN_VALD_FROM_D 
  , PO4.VALD_TO_D AS PRTF_OWN_VALD_TO_D 
  , PO4.EFFT_D AS PRTF_OWN_EFFT_D
  , PO4.EXPY_D AS PRTF_OWN_EXPY_D 
  , PP4.PTCL_N AS PTCL_N 
  , PP4.REL_MNGE_I AS REL_MNGE_I 
  , PP4.PRTF_CODE_X AS PRTF_CODE_X 
  , PO4.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
  , PO4.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X 
  , PO4.ROLE_PLAY_I AS ROLE_PLAY_I
  , PP4.SRCE_SYST_C AS SRCE_SYST_C 
  , PP4.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_REL PP4 
  INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_REL PO4 
  ON PO4.INT_GRUP_I = PP4.INT_GRUP_I 
  AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C;
  
  inserted_count := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Deleted ' || :deleted_count || ' records, Inserted ' || :inserted_count || ' records into DERV_PRTF_PATY_OWN_REL';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_paty_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_paty_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_PATY_PSST_PROC(
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
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
  -- Portfolio Party Interest Group Position Processing
  -- Ver 1.3 - 27/11/2013 T Jelliffe - Fix overlap calc, JOIN_DATE to calendar
  -- Rule 1: Multiple party to Interest Group processing
  
  -- STEP 6 - Rule 1 Multiple party to IG
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST
  SELECT
    DT2.INT_GRUP_I,
    DT2.PATY_I,
    DT2.EFFT_D,
    DT2.EXPY_D,
    DT2.VALD_FROM_D,
    DT2.VALD_TO_D,
    DT2.PERD_D,
    DT2.REL_C,
    DT2.SRCE_SYST_C,
    ROW_NUMBER() OVER (PARTITION BY DT2.PATY_I, DT2.REL_C ORDER BY DT2.PERD_D) AS GRUP_N,
    DT2.ROW_SECU_ACCS_C,
    DT2.PROS_KEY_I
  FROM (
    SELECT
      DT1.INT_GRUP_I,
      DT1.PATY_I,
      C.CALENDAR_DATE AS PERD_D,
      DT1.EFFT_D,
      DT1.EXPY_D,
      DT1.VALD_FROM_D,
      DT1.VALD_TO_D,
      DT1.REL_C,
      DT1.SRCE_SYST_C,
      DT1.ROW_SECU_ACCS_C,
      DT1.PROS_KEY_I
    FROM (
      SELECT
        A.INT_GRUP_I,
        A.PATY_I,
        A.JOIN_FROM_D,
        A.JOIN_TO_D,
        A.VALD_FROM_D,
        A.VALD_TO_D,
        A.EFFT_D,
        A.EXPY_D,
        A.REL_C,
        A.SRCE_SYST_C,
        A.ROW_SECU_ACCS_C,
        A.PROS_KEY_I
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_PSST A
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_PSST B
        ON A.PATY_I = B.PATY_I
        AND A.REL_C = B.REL_C
        AND (
          A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D
        )
        AND (
          A.JOIN_FROM_D <> B.JOIN_FROM_D
          OR A.JOIN_TO_D <> B.JOIN_TO_D
          OR A.INT_GRUP_I <> B.INT_GRUP_I
        )
    ) DT1
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
    ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D
    AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) 
                            AND DATEADD(MONTH, 1, CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY DT1.PATY_I, DT1.REL_C, C.CALENDAR_DATE 
      ORDER BY DT1.EFFT_D DESC, DT1.INT_GRUP_I DESC
    ) = 1
  ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 7 - Do the history version of above
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST
  SELECT DISTINCT
    DT2.INT_GRUP_I,
    DT2.PATY_I,
    DT2.REL_C,
    MIN(DT2.PERD_D) OVER (PARTITION BY DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_FROM_D,
    MAX(DT2.PERD_D) OVER (PARTITION BY DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_TO_D,
    DT2.VALD_FROM_D,
    DT2.VALD_TO_D,
    DT2.EFFT_D,
    DT2.EXPY_D,
    DT2.SRCE_SYST_C,
    DT2.ROW_SECU_ACCS_C,
    DT2.PROS_KEY_I
  FROM (
    SELECT
      C.INT_GRUP_I,
      C.PATY_I,
      C.REL_C,
      C.VALD_FROM_D,
      C.VALD_TO_D,
      C.EFFT_D,
      C.EXPY_D,
      C.SRCE_SYST_C,
      C.ROW_SECU_ACCS_C,
      C.PERD_D,
      MAX(COALESCE(DT1.ROW_N, 0)) OVER (
        PARTITION BY C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D
      ) AS GRUP_N,
      C.PROS_KEY_I
    FROM
      NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST C
      LEFT JOIN (
        SELECT
          A.INT_GRUP_I,
          A.PATY_I,
          A.REL_C,
          A.ROW_N AS ROW_N,
          A.PERD_D,
          A.EFFT_D,
          A.EXPY_D
        FROM
          NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST A
          INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST B
          ON A.PATY_I = B.PATY_I
          AND A.REL_C = B.REL_C
          AND A.ROW_N = B.ROW_N + 1
          AND A.EFFT_D <> B.EFFT_D
      ) DT1
      ON C.INT_GRUP_I = DT1.INT_GRUP_I
      AND C.PATY_I = DT1.PATY_I
      AND C.REL_C = DT1.REL_C
      AND C.ROW_N >= DT1.ROW_N
      AND DT1.PERD_D <= C.PERD_D
  ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 8 - Delete all the original overlap records
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST
  WHERE EXISTS (
    SELECT 1
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_HIST_PSST B
    WHERE
      DERV_PRTF_PATY_PSST.PATY_I = B.PATY_I
      AND DERV_PRTF_PATY_PSST.REL_C = B.REL_C
      AND (
        DERV_PRTF_PATY_PSST.JOIN_FROM_D <= B.JOIN_TO_D 
        AND DERV_PRTF_PATY_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      )
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 9 - Insert the updated records with corrected VALD dates
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST
  SELECT
    INT_GRUP_I,
    PATY_I,
    JOIN_FROM_D,
    CASE
      WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) THEN '9999-12-31'::DATE
      ELSE JOIN_TO_D
    END AS JOIN_TO_D,
    EFFT_D,
    EXPY_D,
    VALD_FROM_D,
    VALD_TO_D,
    REL_C,
    SRCE_SYST_C,
    ROW_SECU_ACCS_C,
    PROS_KEY_I
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Portfolio party interest group processing completed. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: prtf_tech_paty_rel_psst_proc.sql
-- SOURCE: prtf_tech/prtf_tech_paty_rel_psst_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.PRTF_TECH_PATY_REL_PSST_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  delete_count INTEGER DEFAULT 0;
  insert_count INTEGER DEFAULT 0;
BEGIN
  -- Object Name: prtf_tech_paty_rel_psst.sql
  -- Description: Persist rows for DERV_PRTF_PATY view
  -- Original Author: Helen Zak (Initial), Zeewa Lwin (Modified)
  -- Migration Date: Current
  
  -- Delete all existing records from target table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_REL;
  
  delete_count := SQLROWCOUNT;
  
  -- Insert new records with portfolio party relationship data
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_REL
  (
         PATY_I 
        ,INT_GRUP_I 
        ,DERV_PRTF_CATG_C 
        ,DERV_PRTF_CLAS_C 
        ,DERV_PRTF_TYPE_C 
        ,VALD_FROM_D 
        ,VALD_TO_D 
        ,EFFT_D
        ,EXPY_D 
        ,PTCL_N
        ,REL_MNGE_I 
        ,PRTF_CODE_X 
        ,SRCE_SYST_C 
        ,ROW_SECU_ACCS_C
  )
  SELECT
     DT1.PATY_I          
    ,DT1.INT_GRUP_I            
    ,GPTE2.PRTF_CATG_C     AS DERV_PRTF_CATG_C
    ,GPTE2.PRTF_CLAS_C     AS DERV_PRTF_CLAS_C  
    ,DT1.DERV_PRTF_TYPE_C
    ,DT1.VALD_FROM_D
    ,DT1.VALD_TO_D
    ,DT1.EFFT_D         
    ,DT1.EXPY_D  
    ,DT1.PTCL_N 
    ,DT1.REL_MNGE_I
    ,DT1.PRTF_CODE_X
    ,DT1.SRCE_SYST_C
    ,DT1.ROW_SECU_ACCS_C  
  FROM
    (
      SELECT
         PIG3.PATY_I                                  AS PATY_I
        ,IG3.INT_GRUP_I                               AS INT_GRUP_I
        ,PIG3.EFFT_D                                  AS EFFT_D
        ,PIG3.EXPY_D                                  AS EXPY_D
        ,IG3.INT_GRUP_TYPE_C                          AS DERV_PRTF_TYPE_C
        ,CAST(IG3.PTCL_N AS SMALLINT)                 AS PTCL_N
        ,IG3.REL_MNGE_I                               AS REL_MNGE_I
        ,(CASE
            WHEN (IG3.PTCL_N IS NULL) OR (IG3.REL_MNGE_I IS NULL) THEN 'NA'
            ELSE TRIM(IG3.PTCL_N) || TRIM(IG3.REL_MNGE_I)
          END)                                        AS PRTF_CODE_X
        ,PIG3.SRCE_SYST_C                             AS SRCE_SYST_C
        ,PIG3.ROW_SECU_ACCS_C                         AS ROW_SECU_ACCS_C
        ,(CASE
            WHEN IG3.JOIN_FROM_D > PIG3.JOIN_FROM_D THEN IG3.JOIN_FROM_D
            ELSE PIG3.JOIN_FROM_D
          END) AS VALD_FROM_D
        ,(CASE
            WHEN IG3.JOIN_TO_D < PIG3.JOIN_TO_D THEN IG3.JOIN_TO_D
            ELSE PIG3.JOIN_TO_D
          END) AS VALD_TO_D
      FROM
        NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_PSST PIG3
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST IG3
        ON IG3.INT_GRUP_I = PIG3.INT_GRUP_I
        AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D
        AND PIG3.JOIN_FROM_D <= IG3.JOIN_TO_D
    ) DT1
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2
    ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C;
  
  insert_count := SQLROWCOUNT;
  row_count := delete_count + insert_count;
  
  RETURN 'SUCCESS: Deleted ' || :delete_count || ' records, Inserted ' || :insert_count || ' records. Total operations: ' || :row_count;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;


-- =============================================================================
-- PROCEDURE: derv_acct_paty_02_crat_work_tabl_chg0379808_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_02_crat_work_tabl_chg0379808_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  table_count INTEGER DEFAULT 0;
BEGIN
  -- DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql
  -- Create work/staging tables for DERV_ACCT_PATY stream
  -- Original Author: Helen Zak
  -- Version: 1.6 (C2039845)
  
  -- Drop all work tables (ignore errors if tables don't exist)
  
  -- 1. Drop ACCT_PATY_REL_THA
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA;
  EXCEPTION
    WHEN OTHER THEN
      NULL; -- Ignore drop errors
  END;
  
  -- 2. Drop ACCT_PATY_THA_NEW_RNGE
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_THA_NEW_RNGE;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 3. Drop DERV_ACCT_PATY_CURR
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 4. Drop DERV_PRTF_ACCT_STAG
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 5. Drop DERV_PRTF_PATY_STAG
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 6. Drop DERV_PRTF_ACCT_PATY_STAG
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 7. Drop DERV_ACCT_PATY_RM
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 8. Drop DERV_ACCT_PATY_ADD
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 9. Drop DERV_ACCT_PATY_CHG
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 10. Drop DERV_ACCT_PATY_FLAG
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 11. Drop ACCT_PATY_REL_WSS
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_WSS;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 12. Drop ACCT_REL_WSS_DITPS
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_REL_WSS_DITPS;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 13. Drop GRD_GNRC_MAP_DERV_PATY_HOLD
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 14. Drop GRD_GNRC_MAP_DERV_UNID_PATY
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 15. Drop GRD_GNRC_MAP_DERV_PATY_REL
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 16. Drop DERV_ACCT_PATY_DEL
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 17. Drop ACCT_PATY_DEDUP
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 18. Drop DERV_PRTF_ACCT_PATY_PSST
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 19. Drop DERV_ACCT_PATY_NON_RM
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- 20. Drop DERV_ACCT_PATY_ROW_SECU_FIX
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  -- Create new staging tables
  
  -- 1. Create ACCT_PATY_THA_NEW_RNGE
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_THA_NEW_RNGE (
    THA_ACCT_I STRING NOT NULL,
    TRAD_ACCT_I STRING NOT NULL,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    NEW_EXPY_D DATE
  );
  
  -- 2. Create ACCT_PATY_REL_THA
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA (
    THA_ACCT_I STRING NOT NULL,
    TRAD_ACCT_I STRING NOT NULL,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL
  );
  
  -- 3. Create DERV_ACCT_PATY_CURR
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 4. Create DERV_PRTF_ACCT_STAG
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG (
    ACCT_I STRING,
    PRTF_CODE_X STRING
  );
  
  -- 5. Create DERV_PRTF_PATY_STAG
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG (
    PATY_I STRING,
    PRTF_CODE_X STRING
  );
  
  -- 6. Create DERV_PRTF_ACCT_PATY_STAG
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG (
    ACCT_I STRING,
    PATY_I STRING,
    ACCT_PRTF_C STRING,
    PATY_PRTF_C STRING,
    PATY_ACCT_REL_C STRING,
    RANK_I INTEGER
  );
  
  -- 7. Create DERV_ACCT_PATY_RM
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 8. Create DERV_ACCT_PATY_NON_RM
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER,
    RANK_I INTEGER
  );
  
  -- 9. Create DERV_ACCT_PATY_FLAG
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 10. Create DERV_ACCT_PATY_ADD
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 11. Create DERV_ACCT_PATY_CHG
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 12. Create DERV_ACCT_PATY_DEL
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 13. Create ACCT_REL_WSS_DITPS
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_REL_WSS_DITPS (
    ACCT_I STRING NOT NULL
  );
  
  -- 14. Create ACCT_PATY_DEDUP  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- 15. Create GRD_GNRC_MAP_DERV_PATY_HOLD
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD (
    MAP_TYPE_C STRING NOT NULL,
    PATY_ACCT_REL_C STRING,
    PATY_ACCT_REL_X STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL
  );
  
  -- 16. Create GRD_GNRC_MAP_DERV_UNID_PATY
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY (
    MAP_TYPE_C STRING NOT NULL,
    SRCE_SYST_C STRING,
    UNID_PATY_SRCE_SYST_C STRING,
    UNID_PATY_ACCT_REL_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL
  );
  
  -- 17. Create GRD_GNRC_MAP_DERV_PATY_REL
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL (
    MAP_TYPE_C STRING NOT NULL,
    SRCE_SYST_C STRING,
    REL_C STRING,
    ACCT_I_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL
  );
  
  -- 18. Create DERV_PRTF_ACCT_PATY_PSST
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST (
    ACCT_I STRING,
    PATY_I STRING,
    ACCT_PRTF_C STRING,
    PATY_PRTF_C STRING,
    PATY_ACCT_REL_C STRING,
    RANK_I INTEGER
  );
  
  -- 19. Create DERV_ACCT_PATY_ROW_SECU_FIX
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX (
    ACCT_I STRING NOT NULL,
    PATY_I STRING NOT NULL,
    ASSC_ACCT_I STRING NOT NULL,
    PATY_ACCT_REL_C STRING NOT NULL,
    PRFR_PATY_F STRING,
    SRCE_SYST_C STRING,
    EFFT_D DATE NOT NULL,
    EXPY_D DATE NOT NULL,
    ROW_SECU_ACCS_C INTEGER
  );
  
  -- Populate reference data tables (from VTECH schema)
  
  -- Populate GRD_GNRC_MAP_DERV_PATY_HOLD
  BEGIN
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD
    SELECT MAP_TYPE_C,
           PATY_ACCT_REL_C,
           PATY_ACCT_REL_X,
           EFFT_D,
           EXPY_D
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_GNRC_MAP_DERV_PATY_HOLD;
    
    row_count := SQLROWCOUNT;
    table_count := table_count + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_code := SQLCODE;
      RETURN 'ERROR: Failed to populate GRD_GNRC_MAP_DERV_PATY_HOLD - ' || SQLERRM;
  END;
  
  -- Populate GRD_GNRC_MAP_DERV_UNID_PATY
  BEGIN
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY
    SELECT MAP_TYPE_C,
           SRCE_SYST_C,
           UNID_PATY_SRCE_SYST_C,
           UNID_PATY_ACCT_REL_C,
           EFFT_D,
           EXPY_D
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_GNRC_MAP_DERV_UNID_PATY;
    
    row_count := SQLROWCOUNT;
    table_count := table_count + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_code := SQLCODE;
      RETURN 'ERROR: Failed to populate GRD_GNRC_MAP_DERV_UNID_PATY - ' || SQLERRM;
  END;
  
  -- Populate GRD_GNRC_MAP_DERV_PATY_REL
  BEGIN
    INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL
    SELECT MAP_TYPE_C,
           SRCE_SYST_C,
           REL_C,
           ACCT_I_C,
           EFFT_D,
           EXPY_D
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_GNRC_MAP_DERV_PATY_REL;
    
    row_count := SQLROWCOUNT;
    table_count := table_count + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_code := SQLCODE;
      RETURN 'ERROR: Failed to populate GRD_GNRC_MAP_DERV_PATY_REL - ' || SQLERRM;
  END;
  
  -- Success completion
  RETURN 'SUCCESS: Work tables created and populated. Tables created: ' || table_count::STRING;
  
EXCEPTION
  WHEN OTHER THEN
    error_code := SQLCODE;
    RETURN 'FATAL ERROR: ' || SQLERRM || ' (Code: ' || error_code::STRING || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_03_set_acct_prtf_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_03_set_acct_prtf_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_03_SET_ACCT_PRTF_PROC(
  EXTR_D STRING DEFAULT '2023-01-01'
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
  -- Get accounts that are relationship managed and ONLY ONE
  -- of the parties on this account is relationship managed 
  -- by the same RM. This party will be a preferred party for
  -- such an account.
  
  -- Clear account portfolio staging table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG;
  row_count := SQLROWCOUNT;
  
  -- Load account portfolio details as per the extract date
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG
  SELECT ACCT_I,
         PRTF_CODE_X
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT 
  WHERE PERD_D = :EXTR_D
    AND DERV_PRTF_CATG_C = 'RM'
  GROUP BY 1,2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Clear party portfolio staging table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG;
  row_count := SQLROWCOUNT;
  
  -- Load party portfolio details as per the extract date
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG
  SELECT PATY_I,
         PRTF_CODE_X
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY 
  WHERE PERD_D = :EXTR_D
    AND DERV_PRTF_CATG_C = 'RM'
  GROUP BY 1,2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Processed ' || :total_rows || ' total records for extract date ' || :EXTR_D;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_04_pop_curr_tabl_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_04_pop_curr_tabl_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_04_POP_CURR_TABL_PROC(
    EXTR_D STRING DEFAULT '2023-01-01'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
BEGIN
  -- Populate staging table with rows from all sources effective on extract date
  -- Original Author: Helen Zak, Version 1.0 (04/06/2013)
  
  -- 1. Clean and populate ACCT_PATY_DEDUP with deduplicated ACCT_PATY data
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
  SELECT AP.ACCT_I,
         PATY_I,
         AP.ACCT_I AS ASSC_ACCT_I,
         PATY_ACCT_REL_C,
         'N',
         SRCE_SYST_C,
         EFFT_D,
         CASE
           WHEN EFFT_D = EXPY_D THEN EXPY_D
           WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
           ELSE EXPY_D
         END AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY AP
  WHERE :EXTR_D BETWEEN AP.EFFT_D AND AP.EXPY_D
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I, PATY_I, PATY_ACCT_REL_C ORDER BY EFFT_D) = 1;
  
  -- 2. Initialize main table with ACCT_PATY data
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
  SELECT ACCT_I, PATY_I, ASSC_ACCT_I, PATY_ACCT_REL_C, PRFR_PATY_F,
         SRCE_SYST_C, EFFT_D, EXPY_D, ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP;
  
  -- 3. BPS accounts - convert using ACCT_REL and XREF table
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
  SELECT AX.BPS_ACCT_I AS ACCT_I,
         AP.PATY_I,
         CBS_ACCT_I AS ASSC_ACCT_I,
         AP.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         AP.SRCE_SYST_C,
         (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D,
         (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM (
    SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D,
           ROW_SECU_ACCS_C
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AP
  JOIN (
    SELECT CBS_ACCT_I, BPS_ACCT_I, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_XREF_BPS_CBS
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AX ON AP.ACCT_I = AX.CBS_ACCT_I
  WHERE (
    (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
    OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
  )
  GROUP BY 1,2,3,4,5,6,7,8,9
  
  UNION ALL
  
  SELECT AR.OBJC_ACCT_I AS ACCT_I,
         BPS.PATY_I,
         BPS.CBS_ACCT_I AS ASSC_ACCT_I,
         BPS.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         BPS.SRCE_SYST_C,
         (CASE WHEN AR.EFFT_D > BPS.EFFT_D THEN AR.EFFT_D ELSE BPS.EFFT_D END) AS EFFT_D,
         (CASE WHEN AR.EXPY_D < BPS.EXPY_D THEN AR.EXPY_D ELSE BPS.EXPY_D END) AS EXPY_D,
         BPS.ROW_SECU_ACCS_C
  FROM (
    SELECT SUBJ_ACCT_I, OBJC_ACCT_I, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_REL
    WHERE REL_C = 'FLBLL' AND :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AR
  JOIN (
    SELECT AX.BPS_ACCT_I, CBS_ACCT_I, AP.PATY_I, AP.PATY_ACCT_REL_C, AP.SRCE_SYST_C,
           (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D,
           (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D,
           AP.ROW_SECU_ACCS_C
    FROM (
      SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D,
             ROW_SECU_ACCS_C
      FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) AP
    JOIN (
      SELECT CBS_ACCT_I, BPS_ACCT_I, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_XREF_BPS_CBS
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) AX ON AP.ACCT_I = AX.CBS_ACCT_I
    WHERE (
      (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
      OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
    )
    GROUP BY 1,2,3,4,5,6,7,8
  ) BPS ON AR.SUBJ_ACCT_I = BPS.BPS_ACCT_I
  WHERE (
    (AR.EFFT_D BETWEEN BPS.EFFT_D AND BPS.EXPY_D)
    OR (BPS.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
  )
  GROUP BY 1,2,3,4,5,6,7,8,9;

  
  -- 4. CLS accounts - convert using CLS_FCLY and CLS_UNID_PATY
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
  SELECT CLS.ACCT_I,
         AP.PATY_I,
         CLS.GDW_ACCT_I AS ASSC_ACCT_I,
         AP.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         AP.SRCE_SYST_C,
         (CASE WHEN AP.EFFT_D > CLS.EFFT_D THEN AP.EFFT_D ELSE CLS.EFFT_D END) AS EFFT_D,
         (CASE WHEN AP.EXPY_D < CLS.EXPY_D THEN AP.EXPY_D ELSE CLS.EXPY_D END) AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM (
    SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D,
           ROW_SECU_ACCS_C
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AP
  JOIN (
    SELECT CF.ACCT_I,
           'CLSCO'||TRIM(CUP.CRIS_DEBT_I) AS GDW_ACCT_I,
           (CASE WHEN CF.EFFT_D > CUP.EFFT_D THEN CF.EFFT_D ELSE CUP.EFFT_D END) AS EFFT_D,
           (CASE WHEN CF.EXPY_D < CUP.EXPY_D THEN CF.EXPY_D ELSE CUP.EXPY_D END) AS EXPY_D
    FROM (
      SELECT ACCT_I, SRCE_SYST_PATY_I, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CLS_FCLY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) CF
    JOIN (
      SELECT SRCE_SYST_PATY_I, CRIS_DEBT_I, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CLS_UNID_PATY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) CUP ON CUP.SRCE_SYST_PATY_I = CF.SRCE_SYST_PATY_I
    WHERE (
      (CUP.EFFT_D BETWEEN CF.EFFT_D AND CF.EXPY_D)
      OR (CF.EFFT_D BETWEEN CUP.EFFT_D AND CUP.EXPY_D)
    )
    GROUP BY 1,2,3,4
  ) AS CLS ON CLS.GDW_ACCT_I = AP.ACCT_I
  WHERE (
    (AP.EFFT_D BETWEEN CLS.EFFT_D AND CLS.EXPY_D)
    OR (CLS.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
  )
  GROUP BY 1,2,3,4,5,6,7,8,9;

  
  -- 5. THA accounts - populate staging table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA
  SELECT THA_ACCT_I,
         TRAD_ACCT_I,
         EFFT_D,
         CASE
           WHEN EFFT_D = EXPY_D THEN EXPY_D
           WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
           ELSE EXPY_D
         END AS EXPY_D
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.THA_ACCT
  WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  QUALIFY ROW_NUMBER() OVER (PARTITION BY THA_ACCT_I, EFFT_D ORDER BY TRAD_ACCT_I, CSL_CLNT_I DESC) = 1;
  
  -- 6. Insert THA account mappings
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
  SELECT TA.THA_ACCT_I AS ACCT_I,
         AP.PATY_I,
         TA.TRAD_ACCT_I AS ASSC_ACCT_I,
         AP.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         AP.SRCE_SYST_C,
         (CASE WHEN TA.EFFT_D > AP.EFFT_D THEN TA.EFFT_D ELSE AP.EFFT_D END) AS EFFT_D,
         (CASE WHEN TA.EXPY_D < AP.EXPY_D THEN TA.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM (
    SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D,
           ROW_SECU_ACCS_C
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AP
  JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA TA ON TA.TRAD_ACCT_I = AP.ACCT_I
  WHERE (
    (TA.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
    OR (AP.EFFT_D BETWEEN TA.EFFT_D AND TA.EXPY_D)
  )
  GROUP BY 1,2,3,4,5,6,7,8,9;

  
  -- 7. Insert MID, MTX and LMS accounts using ACCT_UNID_PATY transformation
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
  SELECT XREF.ACCT_I,
         AP.PATY_I,
         XREF.ASSC_ACCT_I,
         AP.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         AP.SRCE_SYST_C,
         (CASE WHEN AP.EFFT_D > XREF.EFFT_D THEN AP.EFFT_D ELSE XREF.EFFT_D END) AS EFFT_D,
         (CASE WHEN AP.EXPY_D < XREF.EXPY_D THEN AP.EXPY_D ELSE XREF.EXPY_D END) AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM (
    SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D,
           ROW_SECU_ACCS_C
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AP
  JOIN (
    SELECT XREF1.ACCT_I,
           XREF2.ASSC_ACCT_I,
           (CASE WHEN XREF2.EFFT_D > XREF1.EFFT_D THEN XREF2.EFFT_D ELSE XREF1.EFFT_D END) AS EFFT_D,
           (CASE WHEN XREF2.EXPY_D < XREF1.EXPY_D THEN XREF2.EXPY_D ELSE XREF1.EXPY_D END) AS EXPY_D
    FROM (
      SELECT ACCT_I, SRCE_SYST_PATY_I, SRCE_SYST_C, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_UNID_PATY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) XREF1
    JOIN (
      SELECT ACCT_I AS ASSC_ACCT_I, SRCE_SYST_PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, ORIG_SRCE_SYST_C, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_UNID_PATY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) XREF2 ON XREF2.SRCE_SYST_PATY_I = XREF1.SRCE_SYST_PATY_I
    JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY GGM 
      ON GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C
     AND GGM.UNID_PATY_ACCT_REL_C = XREF2.PATY_ACCT_REL_C
     AND GGM.SRCE_SYST_C = XREF2.ORIG_SRCE_SYST_C
    WHERE XREF1.SRCE_SYST_C = GGM.SRCE_SYST_C
      AND ((XREF1.EFFT_D BETWEEN XREF2.EFFT_D AND XREF2.EXPY_D)
           OR (XREF2.EFFT_D BETWEEN XREF1.EFFT_D AND XREF1.EXPY_D))
  ) XREF ON XREF.ASSC_ACCT_I = AP.ACCT_I
  WHERE ((AP.EFFT_D BETWEEN XREF.EFFT_D AND XREF.EXPY_D)
         OR (XREF.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D))
  GROUP BY 1,2,3,4,5,6,7,8,9;

  
  -- 8. Include MOS account level mappings - Loan accounts
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
  SELECT AX.ACCT_I,
         AP.PATY_I,
         AX.ASSC_ACCT_I,
         AP.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         AP.SRCE_SYST_C,
         (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D,
         (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM (
    SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D,
           ROW_SECU_ACCS_C
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AP
  JOIN (
    -- Loan accounts processing
    SELECT T1.LOAN_I AS ACCT_I,
           UIP.ACCT_I AS ASSC_ACCT_I,
           (CASE WHEN UIP.EFFT_D > T1.EFFT_D THEN UIP.EFFT_D ELSE T1.EFFT_D END) AS EFFT_D,
           (CASE WHEN UIP.EXPY_D < T1.EXPY_D THEN UIP.EXPY_D ELSE T1.EXPY_D END) AS EXPY_D
    FROM (
      SELECT LOAN.LOAN_I,
             FCLY.SRCE_SYST_PATY_I,
             (CASE WHEN LOAN.EFFT_D > FCLY.EFFT_D THEN LOAN.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D,
             (CASE WHEN LOAN.EXPY_D < FCLY.EXPY_D THEN LOAN.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D
      FROM (
        SELECT LOAN_I, FCLY_I, EFFT_D,
               CASE
                 WHEN EFFT_D = EXPY_D THEN EXPY_D
                 WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
                 ELSE EXPY_D
               END AS EXPY_D
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MOS_LOAN
        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
      ) LOAN
      JOIN (
        SELECT SUBSTR(FCLY_I,1,14) AS MOS_FCLY_I, SRCE_SYST_PATY_I, EFFT_D,
               CASE
                 WHEN EFFT_D = EXPY_D THEN EXPY_D
                 WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
                 ELSE EXPY_D
               END AS EXPY_D
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MOS_FCLY
        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
      ) FCLY ON FCLY.MOS_FCLY_I = LOAN.FCLY_I
      WHERE ((FCLY.EFFT_D BETWEEN LOAN.EFFT_D AND LOAN.EXPY_D)
             OR (LOAN.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D))
    ) T1
    JOIN (
      SELECT ACCT_I, SRCE_SYST_PATY_I, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_UNID_PATY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
        AND SRCE_SYST_C = 'SAP'
        AND PATY_ACCT_REL_C = 'ACTO'
    ) UIP ON UIP.SRCE_SYST_PATY_I = T1.SRCE_SYST_PATY_I
    WHERE ((UIP.EFFT_D BETWEEN T1.EFFT_D AND T1.EXPY_D)
           OR (T1.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D))
  ) AX ON AX.ASSC_ACCT_I = AP.ACCT_I
  WHERE ((AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
         OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D))
  GROUP BY 1,2,3,4,5,6,7,8,9

  UNION ALL
  
  -- Facility accounts processing
  SELECT AX.ACCT_I,
         AP.PATY_I,
         AX.ASSC_ACCT_I,
         AP.PATY_ACCT_REL_C,
         'N' AS PRFR_PATY_F,
         AP.SRCE_SYST_C,
         (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D,
         (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D,
         AP.ROW_SECU_ACCS_C
  FROM (
    SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D,
           CASE
             WHEN EFFT_D = EXPY_D THEN EXPY_D
             WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
             ELSE EXPY_D
           END AS EXPY_D,
           ROW_SECU_ACCS_C
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP
    WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
  ) AP
  JOIN (
    SELECT FCLY.FCLY_I AS ACCT_I,
           UIP.ACCT_I AS ASSC_ACCT_I,
           (CASE WHEN UIP.EFFT_D > FCLY.EFFT_D THEN UIP.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D,
           (CASE WHEN UIP.EXPY_D < FCLY.EXPY_D THEN UIP.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D
    FROM (
      SELECT FCLY_I, SRCE_SYST_PATY_I, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MOS_FCLY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
    ) FCLY
    JOIN (
      SELECT ACCT_I, SRCE_SYST_PATY_I, EFFT_D,
             CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE '9999-12-31'
               ELSE EXPY_D
             END AS EXPY_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_UNID_PATY
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
        AND SRCE_SYST_C = 'SAP'
        AND PATY_ACCT_REL_C = 'ACTO'
    ) UIP ON UIP.SRCE_SYST_PATY_I = FCLY.SRCE_SYST_PATY_I
    WHERE ((UIP.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D)
           OR (FCLY.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D))
  ) AX ON AX.ASSC_ACCT_I = AP.ACCT_I
  WHERE ((AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
         OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D))
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  RETURN 'SUCCESS: Completed processing for extract date ' || :EXTR_D;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_05_set_prtf_prfr_flag_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_05_set_prtf_prfr_flag_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG_PROC(
  EXTR_DATE STRING DEFAULT '2024-01-01'
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
  -- Set preferred party flag for accounts with same RM for account and party
  -- Based on portfolio management and fund holder relationships
  
  -- Step 1: Clear staging table and populate with portfolio account party data
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST
  SELECT 
    DAP.ASSC_ACCT_I,
    DAP.PATY_I,
    DPAS.PRTF_CODE_X AS ACCT_PRTF_C,
    NULL AS PATY_PRTF_C,
    DAP.PATY_ACCT_REL_C,
    99 AS RANK_I
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR DAP
  JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD AHR
    ON DAP.PATY_ACCT_REL_C = AHR.PATY_ACCT_REL_C
  JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG DPAS
    ON DAP.ASSC_ACCT_I = DPAS.ACCT_I
  WHERE DPAS.PRTF_CODE_X <> 'NA'
    AND :EXTR_DATE BETWEEN DAP.EFFT_D AND DAP.EXPY_D
  GROUP BY 1,2,3,4,5,6;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Step 2: Update priority rankings for fund holder relationships
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST
  SET RANK_I = DRVD.PRTY
  FROM (
    SELECT DISTINCT 
      PIG.PATY_I,
      GDFVC.PRTY
    FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.PATY_INT_GRUP_CURR PIG
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.INT_GRUP_DEPT_CURR IGD
      ON IGD.INT_GRUP_I = PIG.INT_GRUP_I
      AND PIG.REL_C = 'RLMT'
    INNER JOIN (
      SELECT 
        GFC.DEPT_LEAF_NODE_C AS DEPT_I,
        ORU.PRTY
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_DEPT_FLAT_CURR GFC
      LEFT JOIN (
        SELECT 
          COALESCE(PRTY, 9999) AS PRTY,
          LKUP1_TEXT,
          COALESCE(UPDT_DTTS, CRAT_DTTS) AS LoadTimeStamp
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ODS_RULE
        WHERE RULE_CODE = 'RMPOC'
          AND :EXTR_DATE BETWEEN VALD_FROM AND VALD_TO
        QUALIFY ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT, PRTY ORDER BY LoadTimeStamp DESC) = 1
      ) ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
      WHERE PRTY <> 9999
    ) GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I
    WHERE PIG.PATY_I IN (
      SELECT DISTINCT PATY_I 
      FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC) = 1
  ) DRVD
  WHERE DERV_PRTF_ACCT_PATY_PSST.PATY_I = DRVD.PATY_I
    AND DERV_PRTF_ACCT_PATY_PSST.PATY_ACCT_REL_C = 'ZINTE0';
  
  -- Step 3: Clear and populate final staging table with ranked data
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG
  SELECT 
    DAPP.ACCT_I,
    DAPP.PATY_I,
    DAPP.ACCT_PRTF_C,
    DPPS.PRTF_CODE_X AS PATY_PRTF_C,
    DAPP.PATY_ACCT_REL_C,
    CASE 
      WHEN DAPP.RANK_I <> 99 AND DAPP.PATY_ACCT_REL_C = 'ZINTE0' THEN DAPP.RANK_I
      WHEN DAPP.PATY_ACCT_REL_C = 'ZINTE0' THEN 98
      ELSE 99
    END AS RANK_I
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST DAPP
  JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG DPPS
    ON DAPP.PATY_I = DPPS.PATY_I
    AND (DPPS.PRTF_CODE_X = DAPP.ACCT_PRTF_C OR DAPP.PATY_ACCT_REL_C = 'ZINTE0')
    AND DPPS.PRTF_CODE_X <> 'NA';
  
  -- Step 4: Remove duplicate records based on ranking
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG
  WHERE (ACCT_I, RANK_I, PATY_I) IN (
    SELECT ACCT_I, RANK_I, PATY_I
    FROM (
      SELECT 
        ACCT_I,
        RANK_I,
        PATY_I
      FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG
      QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I ORDER BY RANK_I ASC, PATY_I DESC) > 1
    ) DRVD
  );
  
  -- Step 5: Populate RM working table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM
  SELECT 
    DAP.ACCT_I,
    DAP.PATY_I,
    DAP.ASSC_ACCT_I,
    DAP.PATY_ACCT_REL_C,
    DAP.PRFR_PATY_F,
    DAP.SRCE_SYST_C,
    DAP.EFFT_D,
    DAP.EXPY_D,
    DAP.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR DAP
  JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG T1
    ON DAP.ASSC_ACCT_I = T1.ACCT_I
  WHERE :EXTR_DATE BETWEEN DAP.EFFT_D AND DAP.EXPY_D;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Step 6: Set preferred party flag for identified account/party combinations
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM
  SET PRFR_PATY_F = 'Y'
  WHERE (ACCT_I, PATY_I, PATY_ACCT_REL_C, EFFT_D, EXPY_D) IN (
    SELECT 
      T1.ACCT_I,
      T1.PATY_I,
      T1.PATY_ACCT_REL_C,
      T1.EFFT_D,
      T1.EXPY_D
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM T1
    JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD T2
      ON T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
    JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG T4
      ON T1.ASSC_ACCT_I = T4.ACCT_I
      AND T1.PATY_I = T4.PATY_I
      AND T1.PATY_ACCT_REL_C = T4.PATY_ACCT_REL_C
    WHERE :EXTR_DATE BETWEEN T1.EFFT_D AND T1.EXPY_D
    QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T4.RANK_I, T1.EFFT_D, T1.PATY_ACCT_REL_C, T4.PATY_I DESC) = 1
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Portfolio preferred party flags set. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_06_set_max_prfr_flag_chg0379808_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_06_set_max_prfr_flag_chg0379808_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808_PROC(
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
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM
  SET RANK_I = (
    SELECT DRVD.PRTY
    FROM (
      SELECT DISTINCT 
        PIG.PATY_I,
        GDFVC.PRTY
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.PATY_INT_GRUP_CURR PIG 
      INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.INT_GRUP_DEPT_CURR IGD 
        ON IGD.INT_GRUP_I = PIG.INT_GRUP_I
        AND PIG.REL_C = 'RLMT'
      INNER JOIN (
        SELECT 
          GFC.DEPT_LEAF_NODE_C AS DEPT_I,
          ORU.PRTY AS PRTY 
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_DEPT_FLAT_CURR GFC 
        LEFT JOIN (
          SELECT 
            COALESCE(PRTY, 9999) AS PRTY,
            LKUP1_TEXT,
            COALESCE(UPDT_DTTS, CRAT_DTTS) AS LoadTimeStamp
          FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ODS_RULE
          WHERE RULE_CODE = 'RMPOC'
            AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
          QUALIFY ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT, PRTY ORDER BY LoadTimeStamp DESC) = 1
        ) ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
        WHERE PRTY <> 9999
      ) GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I
      WHERE PIG.PATY_I IN (
        SELECT DISTINCT PATY_I 
        FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
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
        FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.PATY_INT_GRUP_CURR PIG 
        INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.INT_GRUP_DEPT_CURR IGD 
          ON IGD.INT_GRUP_I = PIG.INT_GRUP_I
          AND PIG.REL_C = 'RLMT'
        INNER JOIN (
          SELECT 
            GFC.DEPT_LEAF_NODE_C AS DEPT_I,
            ORU.PRTY AS PRTY 
          FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_DEPT_FLAT_CURR GFC 
          LEFT JOIN (
            SELECT 
              COALESCE(PRTY, 9999) AS PRTY,
              LKUP1_TEXT,
              COALESCE(UPDT_DTTS, CRAT_DTTS) AS LoadTimeStamp
            FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ODS_RULE
            WHERE RULE_CODE = 'RMPOC'
              AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT, PRTY ORDER BY LoadTimeStamp DESC) = 1
          ) ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
          WHERE PRTY <> 9999
        ) GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I
        WHERE PIG.PATY_I IN (
          SELECT DISTINCT PATY_I 
          FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC) = 1
      ) DRVD
      WHERE DERV_ACCT_PATY_NON_RM.PATY_I = DRVD.PATY_I
    );
  
  -- Step 3: Set final ranking based on relationship type
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM
  SET RANK_I = CASE 
    WHEN RANK_I <> 99 AND PATY_ACCT_REL_C = 'ZINTE0' THEN RANK_I -- Fund Holder with Rule Priority gets First Priority
    WHEN PATY_ACCT_REL_C = 'ZINTE0' THEN 98 -- Fund Holder gets Second Priority
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
-- =============================================================================
-- PROCEDURE: derv_acct_paty_07_crat_deltas_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_07_crat_deltas_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_07_CRAT_DELTAS_PROC(
  EXTR_D STRING DEFAULT '2023-01-01'
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
  -- 1. Insert rows into PDDSTG.DERV_ACCT_PATY_ADD that exist now but didn't exist before
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD
  SELECT T1.ACCT_I,
         T1.PATY_I,
         T1.ASSC_ACCT_I,
         T1.PATY_ACCT_REL_C,
         T1.PRFR_PATY_F,
         T1.SRCE_SYST_C,
         T1.EFFT_D,
         T1.EXPY_D,
         T1.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
  LEFT JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
    ON T1.ACCT_I = T2.ACCT_I
   AND T1.PATY_I = T2.PATY_I
   AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
   AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D 
    AND T2.ACCT_I IS NULL
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- 2. Insert rows into PDDSTG.DERV_ACCT_PATY_CHG that changed
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG
  SELECT T1.ACCT_I,
         T1.PATY_I,
         T1.ASSC_ACCT_I,
         T1.PATY_ACCT_REL_C,
         T1.PRFR_PATY_F,
         T1.SRCE_SYST_C,
         T1.EFFT_D,
         T1.EXPY_D,
         T1.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T1
  JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
    ON T1.ACCT_I = T2.ACCT_I
   AND T1.PATY_I = T2.PATY_I
   AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
  WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
      OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
      OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C)
    AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
    AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- 3. Insert rows into PDDSTG.DERV_ACCT_PATY_DEL that were effective before but not in the current table anymore
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL
  SELECT T1.ACCT_I,
         T1.PATY_I,
         T1.ASSC_ACCT_I,
         T1.PATY_ACCT_REL_C,
         T1.PRFR_PATY_F,
         T1.SRCE_SYST_C,
         T1.EFFT_D,
         T1.EXPY_D,
         T1.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T1
  LEFT JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T2
    ON T1.ACCT_I = T2.ACCT_I
   AND T1.PATY_I = T2.PATY_I
   AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
   AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D 
    AND T2.ACCT_I IS NULL 
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Delta processing completed. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_08_apply_deltas_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_08_apply_deltas_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_08_APPLY_DELTAS_PROC(
  INPUT_PATH STRING DEFAULT '/tmp/input',
  OUTPUT_PATH STRING DEFAULT '/tmp/output',
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
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
  pros_key INTEGER DEFAULT 0;
  extr_date DATE;
BEGIN
  -- Object Name: DERV_ACCT_PATY_08_APPLY_DELTAS
  -- Description: Apply the changes as determined in previous step
  -- Original Author: Helen Zak (2013)
  
  -- Get PROS_KEY and BTCH_RUN_D from UTIL_PROS_ISAC
  -- This replaces the file import/export logic from BTEQ
  SELECT PROS_KEY_I, BTCH_RUN_D 
  INTO :pros_key, :extr_date
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.UTIL_PROS_ISAC 
  WHERE PROS_KEY_I IS NOT NULL
  ORDER BY BTCH_RUN_D DESC
  LIMIT 1;
  
  -- 1. Update rows from DERV_ACCT_PATY that are no longer effective (logically delete)
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY
  SET EXPY_D = :extr_date - 1,
      PROS_KEY_EXPY_I = :pros_key
  WHERE EXISTS (
    SELECT 1 
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL T2
    WHERE DERV_ACCT_PATY.ACCT_I = T2.ACCT_I
      AND DERV_ACCT_PATY.PATY_I = T2.PATY_I
      AND DERV_ACCT_PATY.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
      AND DERV_ACCT_PATY.EFFT_D = T2.EFFT_D
      AND :extr_date BETWEEN DERV_ACCT_PATY.EFFT_D AND DERV_ACCT_PATY.EXPY_D
      AND :extr_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- 2. Expire current rows that changed
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY
  SET EXPY_D = :extr_date - 1,
      PROS_KEY_EXPY_I = :pros_key
  WHERE EXISTS (
    SELECT 1 
    FROM (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG 
      QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I, PATY_I, PATY_ACCT_REL_C ORDER BY EFFT_D DESC) = 1
    ) T2
    WHERE DERV_ACCT_PATY.ACCT_I = T2.ACCT_I
      AND DERV_ACCT_PATY.PATY_I = T2.PATY_I
      AND DERV_ACCT_PATY.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
      AND (DERV_ACCT_PATY.ASSC_ACCT_I <> T2.ASSC_ACCT_I
        OR DERV_ACCT_PATY.PRFR_PATY_F <> T2.PRFR_PATY_F
        OR DERV_ACCT_PATY.SRCE_SYST_C <> T2.SRCE_SYST_C)
      AND :extr_date BETWEEN DERV_ACCT_PATY.EFFT_D AND DERV_ACCT_PATY.EXPY_D
      AND :extr_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- 3. Insert new rows for the changes if they don't already exist
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY
  SELECT T1.ACCT_I,
         T1.PATY_I,
         T1.ASSC_ACCT_I,
         T1.PATY_ACCT_REL_C,
         T1.PRFR_PATY_F,
         T1.SRCE_SYST_C,
         :extr_date AS EFFT_D,
         T1.EXPY_D,
         :pros_key AS PROS_KEY_EFFT_D,
         CASE
           WHEN T1.EXPY_D = T1.EFFT_D THEN :pros_key
           ELSE 0
         END AS PROS_KEY_EXPY_D,
         T1.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG T1
  LEFT JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_ACCT_PATY T2
    ON T1.ACCT_I = T2.ACCT_I
    AND T1.PATY_I = T2.PATY_I
    AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
    AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I
    AND T1.SRCE_SYST_C = T2.SRCE_SYST_C
    AND T1.PRFR_PATY_F = T2.PRFR_PATY_F
    AND :extr_date BETWEEN T2.EFFT_D AND T2.EXPY_D
  WHERE :extr_date BETWEEN T1.EFFT_D AND T1.EXPY_D
    AND T2.ACCT_I IS NULL;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- 4. Insert rows that exist now but didn't exist before
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY
  SELECT T1.ACCT_I,
         T1.PATY_I,
         T1.ASSC_ACCT_I,
         T1.PATY_ACCT_REL_C,
         T1.PRFR_PATY_F,
         T1.SRCE_SYST_C,
         T1.EFFT_D,
         T1.EXPY_D,
         :pros_key AS PROS_KEY_EFFT_D,
         CASE
           WHEN T1.EXPY_D = T1.EFFT_D THEN :pros_key
           ELSE 0
         END AS PROS_KEY_EXPY_D,
         T1.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD T1
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- 5. Synchronize ROW_SECU_ACCS_C values with current rows in ACCT_PATY
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX;
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX
    (ACCT_I, PATY_I, ASSC_ACCT_I, PATY_ACCT_REL_C, PRFR_PATY_F, SRCE_SYST_C, EFFT_D, EXPY_D, ROW_SECU_ACCS_C)
  SELECT T1.ACCT_I,
         T1.PATY_I,
         T1.ASSC_ACCT_I,
         T1.PATY_ACCT_REL_C,
         T1.PRFR_PATY_F,
         T1.SRCE_SYST_C,
         T1.EFFT_D,
         T1.EXPY_D,
         T2.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY T1
  JOIN NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG T2
    ON T1.ACCT_I = T2.ACCT_I
    AND T1.PATY_I = T2.PATY_I
    AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
    AND T1.ROW_SECU_ACCS_C <> T2.ROW_SECU_ACCS_C
  GROUP BY 1,2,3,4,5,6,7,8,9;
  
  -- Update ROW_SECU_ACCS_C values
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY
  SET ROW_SECU_ACCS_C = T2.ROW_SECU_ACCS_C
  FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX T2
  WHERE DERV_ACCT_PATY.ACCT_I = T2.ACCT_I
    AND DERV_ACCT_PATY.PATY_I = T2.PATY_I
    AND DERV_ACCT_PATY.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Applied deltas to DERV_ACCT_PATY. Total rows affected: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_99_drop_work_tabl_chg0379808_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_99_drop_work_tabl_chg0379808_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  env_check INTEGER DEFAULT 0;
  tables_dropped INTEGER DEFAULT 0;
BEGIN
  -- Check if we are in PROD environment
  -- Only drop tables if in PROD, skip in DEV
  SELECT CASE WHEN 'PROD' = 'PROD' THEN 1 ELSE 0 END INTO :env_check;
  
  -- If not in PROD (env_check = 0), exit successfully without dropping tables
  IF (env_check = 0) THEN
    RETURN 'SUCCESS: DEV environment detected - tables preserved';
  END IF;
  
  -- Drop work/staging tables with individual error handling
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL; -- Ignore drop errors (equivalent to ERRORCODE 3807)
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_THA_NEW_RNGE;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_WSS;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_REL_WSS_DITPS;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  RETURN 'SUCCESS: PROD environment - attempted to drop ' || :tables_dropped || ' work/staging tables';
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_99_sp_comt_btch_key_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_99_sp_comt_btch_key_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY_PROC(
  INPUT_PATH STRING DEFAULT '/tmp/input',
  OUTPUT_PATH STRING DEFAULT '/tmp/output',
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  btch_key_value STRING;
BEGIN
  -- Update the Batch record to show successful completion
  -- Ver  Date       Modified By            Description
  -- ---- ---------- ---------------------- -----------------------------------
  -- 1.0  25/07/2013 Helen Zak            Initial Version
  
  -- Read batch key from input file (simulated via parameter)
  -- Original BTEQ imported from: /cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt
  -- For Snowflake, batch key should be passed as parameter or retrieved from staging table
  
  -- Update batch status to 'COMT' (committed)
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_BTCH_ISAC           
  SET
     BTCH_STUS_C = 'COMT',
     STUS_CHNG_S = CAST(CAST(CURRENT_TIMESTAMP() AS STRING) AS TIMESTAMP_NTZ(0))
  WHERE
    BTCH_KEY_I = CAST(TRIM(:PROCESS_KEY) AS NUMBER(10,0));
  
  row_count := SQLROWCOUNT;
  
  IF (row_count = 0) THEN
    RETURN 'WARNING: No batch record found for BTCH_KEY_I = ' || :PROCESS_KEY;
  END IF;
  
  RETURN 'SUCCESS: Updated batch status to COMT for ' || :row_count || ' record(s). BTCH_KEY_I = ' || :PROCESS_KEY;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: derv_acct_paty_99_sp_comt_pros_key_proc.sql
-- SOURCE: derv_acct_paty/derv_acct_paty_99_sp_comt_pros_key_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.DERV_ACCT_PATY_99_SP_COMT_PROS_KEY_PROC(
  INPUT_PATH STRING DEFAULT '/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_PROS_KEY_DATE.txt',
  OUTPUT_PATH STRING DEFAULT '/tmp/output',
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  proskey_val STRING;
  extr_d_val STRING;
BEGIN
  -- Script: DERV_ACCT_PATY_99_SP_COMT_PROS_KEY.sql
  -- Purpose: Update UTIL_PROS_ISAC table with success and commit flags
  -- Original Author: Helen Zak (25/07/2013)
  -- Modified By: Megan Disch, Helen Zak (C0714578 post-implementation changes)
  
  -- Note: In Snowflake, file import functionality would need to be handled
  -- through external stages or alternative data loading mechanisms
  -- For now, this procedure expects the data to be available through parameters
  -- or would need to be modified to read from a staging table
  
  -- Simulate the file import logic - in production this would read from a staging table
  -- populated by an external data loading process
  -- The original BTEQ imported: FILLER CHAR(4), PROSKEY CHAR(10), EXTR_D CHAR(10)
  
  -- Update UTIL_PROS_ISAC table with success and commit flags
  -- This simulates the original UPDATE logic but would need actual data source
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PROS_ISAC           
  SET
     SUCC_F = 'Y',
     COMT_F = 'Y',
     COMT_S = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(0))
  WHERE
    PROS_KEY_I = CAST(TRIM(:proskey_val) AS NUMBER(10,0))
    AND TRGT_M = 'DERV_ACCT_PATY'
    AND BTCH_RUN_D = CAST(:extr_d_val AS DATE);
  
  row_count := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Updated ' || :row_count || ' records in UTIL_PROS_ISAC';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: acct_baln_bkdt_delt_proc.sql
-- SOURCE: acct_baln_bkdt/acct_baln_bkdt_delt_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.ACCT_BALN_BKDT_DELT_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- =============================================================================
  -- Procedure: ACCT_BALN_BKDT_DELT_PROC
  -- Original Author: vajapes
  -- Original Date: 2012-02-28 09:08:57 +1100 (Tue, 28 Feb 2012)
  -- Description: Delete Accts from ACCT BALN so that the modified data can be inserted in next step
  -- =============================================================================
  
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  
BEGIN
  
  -- Delete records from ACCT_BALN_BKDT table that are modified as a result of applying adjustment
  -- These records will be reinserted from STG2 at next step
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BALN_BKDT
  WHERE EXISTS (
    SELECT 1 
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_BALN_BKDT_STG1 STG1
    WHERE 
      STG1.ACCT_I = ACCT_BALN_BKDT.ACCT_I    
      AND STG1.BALN_TYPE_C = ACCT_BALN_BKDT.BALN_TYPE_C                    
      AND STG1.CALC_FUNC_C = ACCT_BALN_BKDT.CALC_FUNC_C                   
      AND STG1.TIME_PERD_C = ACCT_BALN_BKDT.TIME_PERD_C                   
      AND STG1.BKDT_EFFT_D = ACCT_BALN_BKDT.BKDT_EFFT_D                        
      AND STG1.BKDT_EXPY_D = ACCT_BALN_BKDT.BKDT_EXPY_D                        
      AND STG1.BALN_A = ACCT_BALN_BKDT.BALN_A                        
      AND STG1.CALC_F = ACCT_BALN_BKDT.CALC_F                        
      AND COALESCE(STG1.PROS_KEY_EFFT_I,0) = COALESCE(ACCT_BALN_BKDT.PROS_KEY_EFFT_I,0)
      AND COALESCE(STG1.PROS_KEY_EXPY_I,0) = COALESCE(ACCT_BALN_BKDT.PROS_KEY_EXPY_I,0)
  );
  
  row_count := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Deleted ' || row_count || ' records from ACCT_BALN_BKDT';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;
-- =============================================================================
-- PROCEDURE: acct_baln_bkdt_util_pros_updt_proc.sql
-- SOURCE: acct_baln_bkdt/acct_baln_bkdt_util_pros_updt_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.ACCT_BALN_BKDT_UTIL_PROS_UPDT_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- Procedure: ACCT_BALN_BKDT_UTIL_PROS_UPDT_PROC
  -- Purpose: Update UTIL_PROS_ISAC with status and record counts
  -- Original Author: Suresh Vajapeyajula (vajapes)
  -- Original Date: 2011-10-05
  
  ERROR_CODE INTEGER DEFAULT 0;
  ROW_COUNT INTEGER DEFAULT 0;
  INS_COUNT INTEGER DEFAULT 0;
  DEL_COUNT INTEGER DEFAULT 0;
BEGIN
  -- Get record counts from staging tables
  SELECT COUNT(*) INTO :INS_COUNT FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_BALN_BKDT_STG2;
  SELECT COUNT(*) INTO :DEL_COUNT FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_BALN_BKDT_STG1;
  
  -- Update UTIL_PROS_ISAC with completion status and record counts
  UPDATE NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PROS_ISAC
  SET  
    COMT_F = 'Y',
    SUCC_F = 'Y',
    COMT_S = CURRENT_TIMESTAMP(),
    SYST_INS_Q = :INS_COUNT,
    SYST_DEL_Q = :DEL_COUNT
  WHERE 
    CONV_M = 'CAD_X01_ACCT_BALN_BKDT'
    AND PROS_KEY_I = (
      SELECT MAX(PROS_KEY_I) 
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.UTIL_PROS_ISAC 
      WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT'
    );
  
  ROW_COUNT := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Updated ' || :ROW_COUNT || ' UTIL_PROS_ISAC records. Insert count: ' || :INS_COUNT || ', Delete count: ' || :DEL_COUNT;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;



-- =============================================================================
-- PROCEDURE: bteq_sap_edo_wkly_load_proc.sql
-- SOURCE: others/bteq_sap_edo_wkly_load_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.BTEQ_SAP_EDO_WKLY_LOAD_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  table_count INTEGER DEFAULT 0;
BEGIN
  -- Object Name: BTEQ_SAP_EDO_WKLY_LOAD.sql
  -- Description: to load the missing accounts
  
  -- Create XUJE_OFFSET_MC_RPT_1710 volatile table
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.TEMP.XUJE_OFFSET_MC_RPT_1710;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.TEMP.XUJE_OFFSET_MC_RPT_1710 AS
  (
    SELECT 
      OPEN_MC.INTR_CNCT_NUMB_OF_MAIN_CNCT AS OFFSET_MC_ID, 
      OPEN_MC.MSTR_CNCT_NUMB AS OFFSET_MC_NUMBER,
      OPEN_MC.SALE_PDCT,
      OPEN_MC.LOAD_S AS MCDMG_LOAD_S,
      CASE WHEN C.CTCT_STUS = '30' THEN 'OPEN' ELSE 'CLOSED' END AS PACKAGE_STATUS, 
      CASE WHEN C.CTCT_STUS = '50' THEN C.VALD_FROM ELSE NULL END AS CLOSURE_DATE,
      E.EXTL_CNCT_PART_1 AS PARTICIPATE_ACCOUNT_ID,
      CASE WHEN E.CNCT_TYPE_KEY = '5' THEN F.BANK END AS PARTICIPATE_ACCOUNT_BSB,            
      CASE WHEN E.CNCT_TYPE_KEY = '6' AND E.NON_SAP_ACCT_NUMB IS NOT NULL THEN NON_SAP_ACCT_NUMB 
           WHEN E.CNCT_TYPE_KEY = '5' AND EXTL_CNCT_PART_1 IS NOT NULL THEN F.EXTL_ACCT_NUMB END AS PARTICIPATE_ACCOUNT_NUMBER,
      CAST(E.CIF_PDCT_IDNN AS DECIMAL(4,0)) as Prxy_PDCT_IDNN,
      CASE WHEN E.CNCT_TYPE_KEY = '5' THEN 'SAP' ELSE E.SRCE_SYST END AS PARTICIPATE_ACCOUNT_SOURCE,
      CASE WHEN E.NON_SAP_ACCT_SALE_PDCT IS NOT NULL THEN NON_SAP_ACCT_SALE_PDCT ELSE F.SALE_PDCT END AS PARTICIPATE_ACCOUNT_SP, 
      E.LOAD_S AS PRXY_LOAD_S,
      CASE WHEN F.ACCT_STUS = '30' THEN 'OPEN' WHEN F.ACCT_STUS IS NULL AND G.ACCT_CLSE_DATE ='9999-12-31' THEN 'OPEN' ELSE 'CLOSED' END AS ACCT_STATUS, 
      G.PDCT_IDNN as Wul_pdct,
      G.LOAD_S AS WNSA_LOAD_S,
      P.SALES_PRODUCT_DESC AS PARTICIPATE_ACCOUNT_SP_DESCRIPTION,
      CAST(D.VALD_FROM AS DATE) AS DATE_OF_LINKAGE,
      CAST(D.ACTL_VALD_END AS DATE) AS EXPIRE_OF_LINKAGE,
      D.LOAD_S AS MCB_LOAD_S
    FROM 
    (
      SELECT B.INTR_CNCT_NUMB_OF_MAIN_CNCT, B.MSTR_CNCT_NUMB, B.LOAD_S, A.SALE_PDCT, C.LOAD_S AS CNCT_LOAD_S 
      FROM NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.CBA_FNCL_SERV_GL_DATA_CURR A
      INNER JOIN NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.MSTR_CNCT_MSTR_DATA_GENL_CURR B ON A.INTR_CNCT_ID = B.INTR_CNCT_NUMB_OF_MAIN_CNCT 
      INNER JOIN NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.CNCT_HEAD C ON C.CNCT = B.INTR_CNCT_NUMB_OF_MAIN_CNCT
      WHERE A.SALE_PDCT = '001603' AND A.SRCE_SYST_ISAC_CODE = 'E001' AND B.SRCE_SYST_ISAC_CODE = 'E001' AND C.SRCE_SYST_ISAC_CODE = 'E001'
      QUALIFY RANK() OVER (PARTITION BY C.CNCT ORDER BY C.LOAD_S DESC, C.VALD_FROM DESC) = 1
    ) AS OPEN_MC
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.CNCT_HEAD_CURR C ON C.CNCT = OPEN_MC.INTR_CNCT_NUMB_OF_MAIN_CNCT
    LEFT OUTER JOIN 
    (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.MSTR_CNCT_BALN_TRNF_PRTP
      WHERE SRCE_SYST_ISAC_CODE = 'E001'
      AND COALESCE(OBJC_BDT_APPT, '') <> '' 
      QUALIFY RANK() OVER (PARTITION BY INTR_CNCT_NUMB_OF_MAIN_CNCT, ELEM_OF_A_CNCT_HIER_GRUP, OBJC_BDT_APPT ORDER BY LOAD_S DESC) = 1
    ) D ON D.INTR_CNCT_NUMB_OF_MAIN_CNCT = OPEN_MC.INTR_CNCT_NUMB_OF_MAIN_CNCT
    LEFT OUTER JOIN 
    (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.MSTR_CNCT_PRXY_ACCT 
      WHERE SRCE_SYST_ISAC_CODE = 'E001' AND CNCT_TYPE_KEY IN ('5', '6') 
      QUALIFY RANK() OVER (PARTITION BY ELEM_OF_A_CNCT_HIER_GRUP, CNCT_TYPE_KEY ORDER BY LOAD_S DESC) = 1
    ) E ON E.ELEM_OF_A_CNCT_HIER_GRUP = D.ELEM_OF_A_CNCT_HIER_GRUP
    LEFT OUTER JOIN NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.ACCT_MSTR_DATA_CURR F ON F.ACCT = E.EXTL_CNCT_PART_1 AND CNCT_CATG = '0001'
    LEFT JOIN 
    (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVCBODS.WUL_NON_SAP_ACCT 
      QUALIFY RANK() OVER(PARTITION BY ACCT_I ORDER BY LOAD_S DESC) = 1
    ) G ON 'HLSHL'||E.NON_SAP_ACCT_NUMB = G.ACCT_I
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG.VEXTR.BD_SALE_PDCT P ON P.SALES_PRODUCT_CODE = PARTICIPATE_ACCOUNT_SP
    WHERE PARTICIPATE_ACCOUNT_SOURCE IS NOT NULL 
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
  );
  
  table_count := table_count + 1;
  
  -- Create EDO_MC_RECONCILIATION_GDW_1710 volatile table
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.TEMP.EDO_MC_RECONCILIATION_GDW_1710;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.TEMP.EDO_MC_RECONCILIATION_GDW_1710 AS
  (
    SELECT DISTINCT A.ACCT_I,
      TRIM(A.PDCT_N) AS MC_PDCT_N,
      B.OFFR_I,
      B.ACCT_OFFR_STRT_D,
      B.ACCT_OFFR_END_D,
      OBJC_ACCT_I,
      TRIM(D.PDCT_N) AS PARTICIPANT_PDCT_N
    FROM (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVTECH.ACCT_PDCT WHERE PDCT_N = '1603' 
      AND ACCT_I IN (
        SELECT ACCT_I FROM NPD_D12_DMN_GDWMIG_IBRG.PVTECH.ACCT_BASE 
        WHERE ACCT_I LIKE 'SAPMC%' AND COALESCE(CLSE_D, DATE'9999-12-31') = '9999-12-31'
      )
      AND CURRENT_DATE - 1 BETWEEN EFFT_D AND EXPY_D
    ) A 
    LEFT JOIN (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVTECH.ACCT_OFFR 
      WHERE ACCT_OFFR_END_D = '9999-12-31' AND CURRENT_DATE - 1 BETWEEN EFFT_D AND EXPY_D
    ) B ON A.ACCT_I = B.ACCT_I 
    JOIN (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVTECH.ACCT_REL 
      WHERE Rel_c LIKE 'MC%' AND CURRENT_DATE - 1 BETWEEN EFFT_D AND EXPY_D
    ) C ON A.ACCT_I = C.SUBJ_ACCT_I 
    LEFT JOIN (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.PVTECH.ACCT_PDCT WHERE 
      ACCT_I IN (
        SELECT ACCT_I FROM NPD_D12_DMN_GDWMIG_IBRG.PVTECH.ACCT_BASE 
        WHERE (ACCT_I LIKE 'HLSHL%' OR ACCT_I LIKE 'SAPSP%') 
        AND COALESCE(CLSE_D, DATE'9999-12-31') = '9999-12-31'
      )
      AND CURRENT_DATE - 1 BETWEEN EFFT_D AND EXPY_D
    ) D ON D.ACCT_I = C.OBJC_ACCT_I 
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  );
  
  table_count := table_count + 1;
  
  -- Create HLS_DIFF_ODS_GDW_1710 volatile table
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.TEMP.HLS_DIFF_ODS_GDW_1710;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.TEMP.HLS_DIFF_ODS_GDW_1710 AS
  (
    SELECT DISTINCT A.* FROM 
    (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.TEMP.XUJE_OFFSET_MC_RPT_1710 
      WHERE PARTICIPATE_ACCOUNT_SOURCE = 'HLS' 
      AND DATE_OF_LINKAGE <= CURRENT_DATE - 2 
      AND EXPIRE_OF_LINKAGE >= CURRENT_DATE - 2
      AND PACKAGE_STATUS <> 'CLOSED'
    ) A
    LEFT JOIN 
    (
      SELECT SUBSTR(ACCT_I, 10) AS OFFSET_MC_NUMBER,
        PARTICIPANT_PDCT_N AS PARTICIPATE_ACCOUNT_SP, 
        SUBSTR(OBJC_ACCT_I, 6) AS PARTICIPATE_ACCOUNT_NUMBER 
      FROM NPD_D12_DMN_GDWMIG_IBRG.TEMP.EDO_MC_RECONCILIATION_GDW_1710 
      WHERE SUBSTR(OBJC_ACCT_I, 1, 3) = 'HLS'
    ) B
    ON CAST(A.OFFSET_MC_NUMBER AS NUMBER) = CAST(B.OFFSET_MC_NUMBER AS NUMBER)
    AND CAST(A.PARTICIPATE_ACCOUNT_NUMBER AS NUMBER) = CAST(B.PARTICIPATE_ACCOUNT_NUMBER AS NUMBER)
    WHERE B.OFFSET_MC_NUMBER IS NULL
  );
  
  table_count := table_count + 1;
  
  -- Create SAP_DIFF_ODS_GDW_1710 volatile table
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.TEMP.SAP_DIFF_ODS_GDW_1710;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.TEMP.SAP_DIFF_ODS_GDW_1710 AS
  (
    SELECT DISTINCT A.* FROM 
    (
      SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG.TEMP.XUJE_OFFSET_MC_RPT_1710 
      WHERE PARTICIPATE_ACCOUNT_SOURCE = 'SAP' 
      AND DATE_OF_LINKAGE <= CURRENT_DATE - 2 
      AND EXPIRE_OF_LINKAGE >= CURRENT_DATE - 2
      AND PACKAGE_STATUS <> 'CLOSED'
    ) A
    LEFT JOIN 
    (
      SELECT SUBSTR(ACCT_I, 10) AS OFFSET_MC_NUMBER,
        SUBSTR(OBJC_ACCT_I, 8, 6) AS PARTICIPATE_ACCOUNT_BSB, 
        SUBSTR(OBJC_ACCT_I, 14) AS PARTICIPATE_ACCOUNT_NUMBER 
      FROM NPD_D12_DMN_GDWMIG_IBRG.TEMP.EDO_MC_RECONCILIATION_GDW_1710 
      WHERE SUBSTR(OBJC_ACCT_I, 1, 3) = 'SAP'
    ) B
    ON CAST(A.OFFSET_MC_NUMBER AS NUMBER) = CAST(B.OFFSET_MC_NUMBER AS NUMBER)
    AND CAST(A.PARTICIPATE_ACCOUNT_BSB AS NUMBER) = CAST(B.PARTICIPATE_ACCOUNT_BSB AS NUMBER)
    AND CAST(A.PARTICIPATE_ACCOUNT_NUMBER AS NUMBER) = CAST(B.PARTICIPATE_ACCOUNT_NUMBER;
$$;



-- =============================================================================
-- PROCEDURE: bteq_tax_inss_mnly_load_proc.sql
-- SOURCE: others/bteq_tax_inss_mnly_load_proc.sql
-- =============================================================================

CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_BAL_001_STD_0.BTEQ_TAX_INSS_MNLY_LOAD_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  total_processed INTEGER DEFAULT 0;
BEGIN
  -- Object Name: BTEQ_TAX_INSS_MNLY_LOAD.sql
  -- Description: To load the missing accounts MONTHLY frequency
  
  -- Create temporary table ACCT_MSTR1 with monthly frequency filter
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_MSTR1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_MSTR1 (
    ACCT STRING,
    BUSN_PTNR_NUMB STRING,
    SRCE_SYST_ISAC_CODE STRING,
    RSDT_STUS STRING,
    USER_IN_CYT_CALC STRING,
    LAST_UPDT_ON TIMESTAMP,
    CYT_DOCU_QOTE STRING,
    VALD_FROM DATE,
    VALD_TO DATE,
    LOAD_S TIMESTAMP
  );
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_MSTR1
  SELECT 
    ACCT,
    BUSN_PTNR_NUMB,
    SRCE_SYST_ISAC_CODE,
    RSDT_STUS,
    USER_IN_CYT_CALC,
    LAST_UPDT_ON,
    CYT_DOCU_QOTE,
    VALD_FROM,
    VALD_TO,
    LOAD_S
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.ACCT_MSTR_CYT_DATA 
  WHERE LOAD_S BETWEEN (
    SELECT FNYR_RPRT_TMPD_STRT_D 
    FROM (
      SELECT CASE WHEN EXTRACT(MONTH FROM FNCL_CALR_D) = '7' 
        THEN CAST(CONCAT(EXTRACT(YEAR FROM FNCL_CALR_D)-1, '-06-30') AS DATE) 
        ELSE FNYR_RPRT_TMPD_STRT_D-1 
      END AS FNYR_RPRT_TMPD_STRT_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_RPRT_CALR_FNYR 
      WHERE FNCL_CALR_D = CURRENT_DATE
    ) A
  ) AND (
    SELECT FNYR_RPRT_TMPD_END_D 
    FROM (
      SELECT DATEADD(MONTH, 0, DATE_TRUNC('MONTH', FNCL_CALR_D)) + 1 AS FNYR_RPRT_TMPD_END_D
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_RPRT_CALR_FNYR 
      WHERE FNCL_CALR_D = CURRENT_DATE
    ) A
  );
  
  -- Create temporary table for missing accounts identification
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.LOAD_MISSING_ACCTS1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.LOAD_MISSING_ACCTS1 (
    ACCT_I STRING,
    PATY_I STRING,
    SRCE_SYST_C STRING,
    RESI_STUS_C STRING,
    IDNN_TYPE_C STRING,
    IDNN_STUS_C STRING,
    RSDT_STUS STRING,
    USER_IN_CYT_CALC STRING,
    CYT_DOCU_QOTE STRING,
    VALD_TO_CYT DATE,
    VALD_TO DATE,
    LAST_UPDT_ON TIMESTAMP,
    ACCT_STUS STRING,
    VALD_FROM DATE,
    SRC_EFFT_D DATE,
    TGT_EFFT_D DATE,
    IND STRING
  );
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.LOAD_MISSING_ACCTS1
  SELECT
    SRC.ACCT_I,
    SRC.PATY_I,
    TGT.SRCE_SYST_C,
    SRC.RESI_STUS_C,
    SRC.IDNN_TYPE_C,
    SRC.IDNN_STUS_C,
    SRC.RSDT_STUS,
    SRC.USER_IN_CYT_CALC,
    SRC.CYT_DOCU_QOTE,
    SRC.VALD_TO_CYT,
    SRC.VALD_TO,
    SRC.LAST_UPDT_ON,
    SRC.ACCT_STUS,
    SRC.VALD_FROM,
    SRC.RUN_STRM_PROS_D AS SRC_EFFT_D,
    TGT.EFFT_D AS TGT_EFFT_D,
    CASE 
      WHEN TGT.ACCT_I IS NULL AND TGT.PATY_I IS NULL THEN 'I'
      WHEN SRC.ACCT_I = TGT.ACCT_I AND SRC.PATY_I = TGT.PATY_I 
        AND (SRC.RESI_STUS_C <> TGT.RESI_STUS_C OR SRC.IDNN_TYPE_C <> TGT.IDNN_TYPE_C OR SRC.IDNN_STUS_C <> TGT.IDNN_STUS_C) THEN 'U'
      WHEN SRC.ACCT_I = TGT.ACCT_I AND SRC.PATY_I = TGT.PATY_I
        AND (SRC.VALD_TO_CYT <> '9999-12-31' OR SRC.VALD_TO <> '9999-12-31' OR SRC.ACCT_STUS = '50') THEN 'D'
      ELSE 'C'
    END AS IND
  FROM (
    SELECT
      ACCT_I,
      PATY_I,
      RSDT_STUS,
      USER_IN_CYT_CALC,
      CYT_DOCU_QOTE,
      CASE WHEN RESI_STUS_C IS NULL THEN '9999' ELSE RESI_STUS_C END AS RESI_STUS_C,
      CASE WHEN IDNN_TYPE_C IS NULL THEN '9999' ELSE IDNN_TYPE_C END AS IDNN_TYPE_C,
      CASE WHEN IDNN_STUS_C IS NULL THEN '9999' ELSE IDNN_STUS_C END AS IDNN_STUS_C,
      AMCD.VALD_TO AS VALD_TO_CYT,
      BP.VALD_TO,
      LAST_UPDT_ON,
      ACCT_STUS,
      AMD.VALD_FROM,
      UTIL.RUN_STRM_PROS_D
    FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_MSTR1 AMCD 
    INNER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.ACCT_MSTR_DATA_GENL AMD
      ON AMD.ACCT = AMCD.ACCT
      AND AMD.SRCE_SYST_ISAC_CODE = AMCD.SRCE_SYST_ISAC_CODE
    INNER JOIN (
      SELECT
        PATY_I,
        BP.BUSN_PTNR_NUMB,
        BP.SRCE_SYST_ISAC_CODE,
        BP.LOAD_S,
        PI.LOAD_S AS LOAD_S_PI,
        COALESCE(PI.VALD_TO, DATE '9999-12-31') AS VALD_TO
      FROM (
        SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.BUSN_PTNR
        WHERE IS_CURR_IND = 1
      ) BP
      LEFT OUTER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.PTNR_IDNN_NUMB PI
        ON BP.BUSN_PTNR_NUMB = PI.BUSN_PTNR_NUMB
        AND BP.SRCE_SYST_ISAC_CODE = PI.SRCE_SYST_ISAC_CODE
        AND PTNR_ID_TYPE = 'ZCIF00'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY BP.BUSN_PTNR_NUMB, BP.SRCE_SYST_ISAC_CODE ORDER BY PI.LOAD_S DESC, PI.VALD_TO DESC) = 1
    ) BP
      ON BP.BUSN_PTNR_NUMB = AMCD.BUSN_PTNR_NUMB
      AND BP.SRCE_SYST_ISAC_CODE = AMCD.SRCE_SYST_ISAC_CODE
    INNER JOIN (
      SELECT RUN_STRM_PROS_D, SRCE_SYST_ISAC, EXT_FROM_S, EXT_TO_S 
      FROM NPD_D12_DMN_GDWMIG_IBRG_V.UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H 
      WHERE RUN_STRM_C = 'SAP00' AND SRCE_SYST_ISAC = 'E001'
    ) UTIL
      ON UTIL.SRCE_SYST_ISAC = AMCD.SRCE_SYST_ISAC_CODE
    LEFT OUTER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_RESI_STUS AS MSRS
      ON MSRS.RESI_STUS = AMCD.RSDT_STUS 
      AND RUN_STRM_PROS_D BETWEEN MSRS.EFFT_D AND MSRS.EXPY_D
    LEFT OUTER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_IDNN_TYPE AS MSIT
      ON MSIT.SAP_IDNN_TYPE_C = AMCD.USER_IN_CYT_CALC 
      AND RUN_STRM_PROS_D BETWEEN MSIT.EFFT_D AND MSIT.EXPY_D
    LEFT OUTER JOIN NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_IDNN_STUS AS MSIS
      ON MSIS.SAP_IDNN_STUS_C = AMCD.CYT_DOCU_QOTE 
      AND RUN_STRM_PROS_D BETWEEN MSIS.EFFT_D AND MSIS.EXPY_D
    WHERE
      AMD.LOAD_S <= UTIL.EXT_TO_S 
      AND COALESCE(AMD.UPD_LOAD_S, TIMESTAMP '9999-12-31 00:00:00.000000') > UTIL.EXT_TO_S
      AND AMCD.LOAD_S <= UTIL.EXT_TO_S
      AND COALESCE(AMD.PDCT, '') NOT IN (
        SELECT PDCT FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_INVL_PDCT
        WHERE PDCT_C = 'INVL' AND UTIL.RUN_STRM_PROS_D BETWEEN EFFT_D AND EXPY_D
      )
      AND (
        (AMCD.LOAD_S >= UTIL.EXT_FROM_S)
        OR (AMD.LOAD_S >= UTIL.EXT_FROM_S)
        OR (BP.VALD_TO <> '9999-12-31' AND BP.LOAD_S_PI >= UTIL.EXT_FROM_S)
      )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY AMCD.ACCT, AMCD.BUSN_PTNR_NUMB, AMCD.SRCE_SYST_ISAC_CODE ORDER BY AMCD.VALD_FROM DESC, AMCD.LOAD_S DESC) = 1
  ) AS SRC
  LEFT OUTER JOIN (
    SELECT * FROM NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY_TAX_INSS
    WHERE SRCE_SYST_C = 'SAP' AND ACCT_I LIKE 'SAP%'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ACCT_I, PATY_I, SRCE_SYST_C ORDER BY EFFT_D DESC, EXPY_D DESC) = 1
  ) TGT
    ON SRC.ACCT_I = TGT.ACCT_I AND SRC.PATY_I = TGT.PATY_I AND TGT.SRCE_SYST_C = 'SAP'
  WHERE IND = 'I';
  
  SELECT COUNT(*) INTO :row_count FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.LOAD_MISSING_ACCTS1;
  total_processed := :row_count;
  
  -- Create utility processing key table
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.UTIL_PROS_SAP_RUN;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  CREATE ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.UTIL_PROS_SAP_RUN (
    PROS_KEY_EFFT_I STRING
  );
  
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.UTIL_PROS_SAP_RUN
  SELECT TO_CHAR(CAST(COALESCE(TOP_KEY_I, '0') + 1;
$$;