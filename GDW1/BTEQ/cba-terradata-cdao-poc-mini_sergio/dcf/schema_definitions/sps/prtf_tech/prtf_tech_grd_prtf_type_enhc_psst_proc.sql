CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_GRD_PRTF_TYPE_ENHC_PSST_PROC(
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
  DELETE FROM ps_gdw1_bteq.DGRDDB.GRD_PRTF_TYPE_ENHC_PSST;
  row_count := SQLROWCOUNT;
  total_rows := total_rows + :row_count;

  -- Insert into main persistent table from source
  INSERT INTO ps_gdw1_bteq.DGRDDB.GRD_PRTF_TYPE_ENHC_PSST
  SELECT
     GP.PERD_D
    ,GP.PRTF_TYPE_C
    ,GP.PRTF_TYPE_M
    ,GP.PRTF_CLAS_C
    ,GP.PRTF_CLAS_M
    ,GP.PRTF_CATG_C
    ,GP.PRTF_CATG_M
  FROM                
    ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC GP;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + :row_count;

  -- Populate the HISTORY version of the table
  DELETE FROM ps_gdw1_bteq.DGRDDB.GRD_PRTF_TYPE_ENHC_HIST_PSST;
  row_count := SQLROWCOUNT;

  INSERT INTO ps_gdw1_bteq.DGRDDB.GRD_PRTF_TYPE_ENHC_HIST_PSST
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
    ps_gdw1_bteq.PVTECH.GRD_PRTF_TYPE_ENHC_PSST G
  GROUP BY 1,2,3,4,5,6;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + :row_count;

  RETURN 'SUCCESS: Portfolio type enhancement data processed. Total rows affected: ' || :total_rows;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;