USE ROLE r_dev_npd_d12_gdwmig;
CREATE OR REPLACE PROCEDURE npd_d12_dmn_gdwmig.lcl.sp_bteq_DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY(
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
  UPDATE PS_CLD_RW.STARCADPRODDATA.UTIL_BTCH_ISAC           
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