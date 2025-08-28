CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_PATY_INT_GRUP_PSST_PROC()
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

  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_PSST 
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
    ps_gdw1_bteq.PVTECH.PATY_INT_GRUP PIG

    INNER JOIN ps_gdw1_bteq.PVTECH.INT_GRUP IG
    ON PIG.INT_GRUP_I = IG.INT_GRUP_I
    AND PIG.VALD_FROM_D <= IG.VALD_TO_D AND PIG.VALD_TO_D >= IG.CRAT_D

    INNER JOIN ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
    ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
    AND GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= IG.CRAT_D
    AND GPTE.VALD_FROM_D <= PIG.VALD_TO_D AND GPTE.VALD_TO_D >= PIG.VALD_FROM_D
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  --<============================================>--
  --< STEP 2 - Daily PIG overlaps                >--
  --<============================================>--

  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST 
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_PSST A
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_PSST B
        ON A.PATY_I = B.PATY_I
        AND A.INT_GRUP_I = B.INT_GRUP_I
        AND A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D  
        AND (
          A.JOIN_FROM_D <> B.JOIN_FROM_D
          OR A.JOIN_TO_D <> B.JOIN_TO_D
        )

        INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN A.VALD_FROM_D AND A.VALD_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)

      QUALIFY ROW_NUMBER() OVER( PARTITION BY A.PATY_I, A.INT_GRUP_I, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC) = 1    
    ) DT2;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  --<============================================>--
  --< STEP 3 - History group                     >--
  --<============================================>--
  
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST 
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
      ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST C
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
          ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST A
          INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST B
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

  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_HIST_PSST B
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

  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_PSST
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
    ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_HIST_PSST;

  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  RETURN 'SUCCESS: Portfolio party interest group processing completed. Total operations: ' || :total_rows;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;