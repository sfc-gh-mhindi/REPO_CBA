CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_OWN_PSST_PROC()
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_PSST A
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_PSST B    
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
        INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY A.INT_GRUP_I, A.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC, A.ROLE_PLAY_I DESC) = 1
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  --<=========================================================================>--
  --< Step 6 : Hist view of Step 5
  --<=========================================================================>--
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST C
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
            ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST A
            INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST B
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_HIST_PSST B
    WHERE  
      DERV_PRTF_OWN_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_OWN_PSST.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X        
      AND DERV_PRTF_OWN_PSST.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C        
      AND (DERV_PRTF_OWN_PSST.JOIN_FROM_D <= B.JOIN_TO_D AND DERV_PRTF_OWN_PSST.JOIN_TO_D >= B.JOIN_FROM_D)
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST
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
    ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  --<=========================================================================>--
  --< Step 8 : When both Dept and Empl take Empl (Rule 5)
  --<=========================================================================>--
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_PSST A
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_PSST B    
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
        INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
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