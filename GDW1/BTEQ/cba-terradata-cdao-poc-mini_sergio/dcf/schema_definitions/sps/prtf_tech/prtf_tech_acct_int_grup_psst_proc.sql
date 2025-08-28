CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_ACCT_INT_GRUP_PSST_PROC()
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
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
    ps_gdw1_bteq.PVTECH.ACCT_INT_GRUP AIG
    
    /* Add the GRD filter to reduce the data */
    INNER JOIN ps_gdw1_bteq.PVTECH.INT_GRUP IG
    ON IG.INT_GRUP_I = AIG.INT_GRUP_I
      
    INNER JOIN ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST
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
            ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_PSST A
            INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_PSST B
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
  
        INNER JOIN ps_gdw1_bteq.PVTECH.CALENDAR C
        ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D
        AND C.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, DATE_TRUNC('MONTH', CURRENT_DATE)) AND DATEADD(MONTH, 1, CURRENT_DATE)
  
      QUALIFY ROW_NUMBER() OVER(PARTITION BY DT1.ACCT_I, DT1.INT_GRUP_I, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC, DT1.REL_C DESC) = 1
  
    ) DT2;
    
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  -- STEP 3: Calculate correct history
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST;
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST
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
      ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST C
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
          ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST A
          INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST B
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
  WHERE EXISTS (
    SELECT 1 
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_HIST_PSST B
    WHERE  
      DERV_PRTF_ACCT_PSST.ACCT_I = B.ACCT_I
      AND DERV_PRTF_ACCT_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_ACCT_PSST.JOIN_TO_D >= B.JOIN_FROM_D
      AND DERV_PRTF_ACCT_PSST.JOIN_FROM_D <= B.JOIN_TO_D  
  );
  
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  -- STEP 5: Replace with updated records
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
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
    ps_gdw1_bteq.PVTECH.DERV_PRTF_ACCT_HIST_PSST;
    
  row_count := SQLROWCOUNT;
  total_processed := total_processed + row_count;
  
  RETURN 'SUCCESS: Portfolio account processing completed. Total records processed: ' || :total_processed;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;