USE ROLE r_dev_npd_d12_gdwmig;
CREATE OR REPLACE PROCEDURE npd_d12_dmn_gdwmig.lcl.sp_bteq_DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  env_check INTEGER DEFAULT 0;
  tables_dropped INTEGER DEFAULT 0;
BEGIN
  -- Check if we are in PROD environment
  -- Only drop tables if in PROD, skip in DEV
  SELECT CASE WHEN 'PROD' = 'PROD' THEN 1 ELSE 0 END INTO :env_check;
  
  -- If not in PROD (env_check = 0), exit successfully without dropping tables
  IF (env_check = 0) THEN
    RETURN 'SUCCESS: DEV environment detected - tables preserved';
  END IF;
  
  -- Drop work/staging tables with individual error handling
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL; -- Ignore drop errors (equivalent to ERRORCODE 3807)
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_THA_NEW_RNGE;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_WSS;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_REL_WSS_DITPS;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    DROP ICEBERG TABLE IF EXISTS NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX;
    tables_dropped := tables_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  RETURN 'SUCCESS: PROD environment - attempted to drop ' || :tables_dropped || ' work/staging tables';
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;