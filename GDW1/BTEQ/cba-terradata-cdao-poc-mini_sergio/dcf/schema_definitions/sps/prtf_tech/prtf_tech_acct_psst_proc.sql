CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_ACCT_PSST_PROC(
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_PSST A
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_PSST B
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
    INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
    ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D
    AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY DT1.ACCT_I, DT1.REL_C, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC, DT1.INT_GRUP_I DESC) = 1
  ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 7: Process Portfolio Account History Persist
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST PAIG
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
            ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST A
            INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST B
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  WHERE EXISTS (
    SELECT 1
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_HIST_PSST B
    WHERE
      DERV_PRTF_ACCT_PSST.ACCT_I = B.ACCT_I
      AND DERV_PRTF_ACCT_PSST.REL_C = B.REL_C
      AND DERV_PRTF_ACCT_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      AND DERV_PRTF_ACCT_PSST.JOIN_FROM_D <= B.JOIN_TO_D
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 9: Insert updated records with corrected VALD dates
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
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
  FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Portfolio account processing completed. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;