CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_INT_GRUP_ENHC_PSST_PROC(
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_PSST
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
    ps_gdw1_bteq.PVTECH.INT_GRUP A
    INNER JOIN ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE
      ON GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C
      AND GPTE.VALD_TO_D >= A.CRAT_D
      AND GPTE.VALD_FROM_D <= A.VALD_TO_D
      AND TRY_TO_NUMBER(COALESCE(A.PTCL_N, '0')) IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 2: Process INT_GRUP enhancement
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_ENHC_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_ENHC_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_PSST A
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_PSST B
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
        INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
          ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D
          AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) 
                                  AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY A.INT_GRUP_I, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC) = 1
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- STEP 3: Create history records
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_HIST_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_HIST_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST C
        LEFT JOIN (
          SELECT
             A.INT_GRUP_I,
             A.PTCL_N,
             A.REL_MNGE_I,
             A.PERD_D,
             A.ROW_N
          FROM
            ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST A
            INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST B
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