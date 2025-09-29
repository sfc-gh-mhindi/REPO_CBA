CREATE OR REPLACE PROCEDURE ps_gdw1_bteq.bteq_sps.ACCT_BALN_BKDT_DELT_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- =============================================================================
  -- Procedure: ACCT_BALN_BKDT_DELT_PROC
  -- Original Author: vajapes
  -- Original Date: 2012-02-28 09:08:57 +1100 (Tue, 28 Feb 2012)
  -- Description: Delete Accts from ACCT BALN so that the modified data can be inserted in next step
  -- =============================================================================
  
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  
BEGIN
  
  -- Delete records from ACCT_BALN_BKDT table that are modified as a result of applying adjustment
  -- These records will be reinserted from STG2 at next step
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.ACCT_BALN_BKDT
  WHERE EXISTS (
    SELECT 1 
    FROM PS_CLD_RW.PDDSTG.ACCT_BALN_BKDT_STG1 STG1
    WHERE 
      STG1.ACCT_I = ACCT_BALN_BKDT.ACCT_I    
      AND STG1.BALN_TYPE_C = ACCT_BALN_BKDT.BALN_TYPE_C                    
      AND STG1.CALC_FUNC_C = ACCT_BALN_BKDT.CALC_FUNC_C                   
      AND STG1.TIME_PERD_C = ACCT_BALN_BKDT.TIME_PERD_C                   
      AND STG1.BKDT_EFFT_D = ACCT_BALN_BKDT.BKDT_EFFT_D                        
      AND STG1.BKDT_EXPY_D = ACCT_BALN_BKDT.BKDT_EXPY_D                        
      AND STG1.BALN_A = ACCT_BALN_BKDT.BALN_A                        
      AND STG1.CALC_F = ACCT_BALN_BKDT.CALC_F                        
      AND COALESCE(STG1.PROS_KEY_EFFT_I,0) = COALESCE(ACCT_BALN_BKDT.PROS_KEY_EFFT_I,0)
      AND COALESCE(STG1.PROS_KEY_EXPY_I,0) = COALESCE(ACCT_BALN_BKDT.PROS_KEY_EXPY_I,0)
  );
  
  row_count := SQLROWCOUNT;
  
  RETURN 'SUCCESS: Deleted ' || row_count || ' records from ACCT_BALN_BKDT';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;