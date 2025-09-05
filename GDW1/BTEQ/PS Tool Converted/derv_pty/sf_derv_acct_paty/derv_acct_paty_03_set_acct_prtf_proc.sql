USE ROLE r_dev_npd_d12_gdwmig;

CREATE OR REPLACE PROCEDURE npd_d12_dmn_gdwmig.lcl.sp_bteq_derv_acct_paty_03_set_acct_prtf(
  EXTR_D STRING DEFAULT '2023-01-01'
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
  -- Get accounts that are relationship managed and ONLY ONE
  -- of the parties on this account is relationship managed 
  -- by the same RM. This party will be a preferred party for
  -- such an account.
  
  -- Clear account portfolio staging table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG;
  row_count := SQLROWCOUNT;
  
  -- Load account portfolio details as per the extract date
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG
  SELECT ACCT_I,
         PRTF_CODE_X
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.pvtech.DERV_PRTF_ACCT 
  WHERE PERD_D = :EXTR_D
    AND TRIM(DERV_PRTF_CATG_C) = 'RM'
  GROUP BY 1,2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  -- Clear party portfolio staging table
  DELETE FROM NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG;
  row_count := SQLROWCOUNT;
  
  -- Load party portfolio details as per the extract date
  INSERT INTO NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG
  SELECT PATY_I,
         PRTF_CODE_X
  FROM NPD_D12_DMN_GDWMIG_IBRG_V.pvtech.DERV_PRTF_PATY 
  WHERE PERD_D = :EXTR_D
    AND TRIM(DERV_PRTF_CATG_C) = 'RM'
  GROUP BY 1,2;
  
  row_count := SQLROWCOUNT;
  total_rows := total_rows + row_count;
  
  RETURN 'SUCCESS: Processed ' || :total_rows || ' total records for extract date ' || :EXTR_D;
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;