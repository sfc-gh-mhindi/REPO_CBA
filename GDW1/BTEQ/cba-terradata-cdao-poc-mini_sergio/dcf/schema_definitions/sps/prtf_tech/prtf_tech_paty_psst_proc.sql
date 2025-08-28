CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_PATY_PSST_PROC(
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_PSST A
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_PSST B
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
    INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST
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
      ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST C
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
          ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST A
          INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST B
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_PSST
  WHERE EXISTS (
    SELECT 1
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_HIST_PSST B
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
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_PSST
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
  FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Portfolio party interest group processing completed. Total rows processed: ' || :total_rows;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;