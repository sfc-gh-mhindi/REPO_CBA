CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_INT_GRUP_OWN_PSST_PROC(
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST
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
    ps_gdw1_bteq.PVTECH.INT_GRUP_EMPL IGE
    INNER JOIN ps_gdw1_bteq.PVTECH.INT_GRUP IG
      ON IG.INT_GRUP_I = IGE.INT_GRUP_I
    INNER JOIN ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
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
    ps_gdw1_bteq.PVTECH.INT_GRUP_DEPT IGD
    INNER JOIN ps_gdw1_bteq.PVTECH.INT_GRUP IG
      ON IG.INT_GRUP_I = IGD.INT_GRUP_I
    INNER JOIN ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
      ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
      AND (GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= IG.CRAT_D)
      AND (GPTE.VALD_FROM_D <= IGD.VALD_TO_D AND GPTE.VALD_TO_D >= IGD.VALD_FROM_D)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  -- Step 2: Employee Business date deduplication
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST;

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
          ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_PSST A
          INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_PSST B
            ON A.INT_GRUP_I = B.INT_GRUP_I
            AND A.ROLE_PLAY_I = B.ROLE_PLAY_I
            AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
            AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
            AND A.DERV_PRTF_ROLE_C = 'OWNR'
            AND (A.JOIN_FROM_D <= B.JOIN_TO_D AND A.JOIN_TO_D >= B.JOIN_FROM_D)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
      ) DERV
      INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN DERV.JOIN_FROM_D AND DERV.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY DERV.INT_GRUP_I, DERV.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY DERV.EFFT_D DESC, DERV.ROLE_PLAY_I) = 1 
    ) DT2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  -- Step 3: History version of above
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST;

  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST 
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
            ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST C
            LEFT JOIN (
              SELECT
                 A.INT_GRUP_I
                ,A.ROLE_PLAY_I
                ,A.DERV_PRTF_ROLE_C
                ,A.ROW_N
                ,A.EFFT_D
                ,A.PERD_D
              FROM
                ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST A
                INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST B
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_HIST_PSST B
    WHERE DERV_PRTF_OWN_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_OWN_PSST.ROLE_PLAY_I = B.ROLE_PLAY_I
      AND DERV_PRTF_OWN_PSST.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
      AND DERV_PRTF_OWN_PSST.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
      AND DERV_PRTF_OWN_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      AND DERV_PRTF_OWN_PSST.JOIN_FROM_D <= B.JOIN_TO_D
  );
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST
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
  FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_OWN_HIST_PSST;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;

  RETURN 'SUCCESS: Portfolio ownership processing completed. Total rows processed: ' || :total_rows;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;