CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_PATY_REL_PSST_PROC(
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
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_REL;
  
  delete_count := SQLROWCOUNT;
  
  -- Insert new records with portfolio party relationship data
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_REL
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
        ps_gdw1_bteq.PVTECH.DERV_PRTF_PATY_PSST PIG3
        INNER JOIN ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_PSST IG3
        ON IG3.INT_GRUP_I = PIG3.INT_GRUP_I
        AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D
        AND PIG3.JOIN_FROM_D <= IG3.JOIN_TO_D
    ) DT1
    INNER JOIN ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2
    ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C;
  
  insert_count := SQLROWCOUNT;
  row_count := delete_count + insert_count;
  
  RETURN 'SUCCESS: Deleted ' || :delete_count || ' records, Inserted ' || :insert_count || ' records. Total operations: ' || :row_count;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;