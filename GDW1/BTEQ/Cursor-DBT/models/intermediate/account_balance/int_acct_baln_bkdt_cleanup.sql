/*
  GDW1 Account Balance Backdate Cleanup Model
  
  Migrated from: ACCT_BALN_BKDT_DELT.sql
  
  Description: 
  Identifies accounts that need to be removed from ACCT_BALN_BKDT 
  so that modified data can be inserted in the next step.
  
  Original Logic:
  - Delete records from ACCT_BALN_BKDT that match staging data
  - These records are modified as a result of applying adjustments
  - They will be reinserted from STG2 in the next step
  
  DBT Approach:
  - Instead of DELETE, identify records that would be removed
  - Used in downstream models for MERGE/UPSERT operations
  - Maintains data lineage and audit trail
  
  DCF Pattern: Intermediate processing table
  Stream: GDW1_ACCT_BALN_PROCESSING
*/

{{ 
  intermediate_model_config(
    process_type='account_balance_cleanup',
    stream_name=var('account_balance_stream')
  )
}}

-- Identify records in production that need to be updated/replaced
SELECT DISTINCT
    BAL.ACCT_I,
    BAL.BALN_TYPE_C,
    BAL.CALC_FUNC_C,
    BAL.TIME_PERD_C,
    BAL.BKDT_EFFT_D,
    BAL.BKDT_EXPY_D,
    BAL.BALN_A,
    BAL.CALC_F,
    BAL.PROS_KEY_EFFT_I,
    BAL.PROS_KEY_EXPY_I,
    BAL.BKDT_PROS_KEY_I,
    
    -- Flag for action
    'DELETE' AS CLEANUP_ACTION,
    'MATCHED_STG1' AS CLEANUP_REASON,
    
    -- Add DCF audit columns
    {{ gdw1_audit_columns() }}

FROM {{ bteq_var('CAD_PROD_DATA') }}.ACCT_BALN_BKDT BAL
INNER JOIN {{ bteq_var('DDSTG') }}.ACCT_BALN_BKDT_STG1 STG1
    ON STG1.ACCT_I = BAL.ACCT_I    
    AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C                    
    AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C                   
    AND STG1.TIME_PERD_C = BAL.TIME_PERD_C                   
    AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D                        
    AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D                        
    AND STG1.BALN_A = BAL.BALN_A                        
    AND STG1.CALC_F = BAL.CALC_F                        
    AND COALESCE(STG1.PROS_KEY_EFFT_I, 0) = COALESCE(BAL.PROS_KEY_EFFT_I, 0)
    AND COALESCE(STG1.PROS_KEY_EXPY_I, 0) = COALESCE(BAL.PROS_KEY_EXPY_I, 0)

-- Add data quality filters
WHERE BAL.ACCT_I IS NOT NULL
  AND STG1.ACCT_I IS NOT NULL 