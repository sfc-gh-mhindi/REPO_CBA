/*
  GDW1 Account Balance Backdate Insert Model
  
  Migrated from: ACCT_BALN_BKDT_ISRT.sql
  
  Description: 
  Populate accounts into ACCT_BALN_BKDT with modified adjustments
  from the staging table ACCT_BALN_BKDT_STG2
  
  Original Logic:
  - Insert account balance data from staging to production table
  - Includes all balance calculations and adjustments
  - Part of the account balance processing workflow
  
  DCF Pattern: Intermediate processing table
  Stream: GDW1_ACCT_BALN_PROCESSING
*/

{{ 
  intermediate_model_config(
    process_type='account_balance_insert',
    stream_name=var('account_balance_stream')
  )
}}

SELECT
    ACCT_I,                        
    BALN_TYPE_C,                   
    CALC_FUNC_C,                   
    TIME_PERD_C,                   
    BALN_A,                        
    CALC_F,                        
    SRCE_SYST_C,                   
    ORIG_SRCE_SYST_C,              
    LOAD_D,                        
    BKDT_EFFT_D,                   
    BKDT_EXPY_D,                  
    PROS_KEY_EFFT_I,               
    PROS_KEY_EXPY_I,               
    BKDT_PROS_KEY_I,
    
    -- Add DCF audit columns
    {{ gdw1_audit_columns() }}
    
FROM {{ bteq_var('DDSTG') }}.ACCT_BALN_BKDT_STG2

-- Add data quality filters if needed
WHERE ACCT_I IS NOT NULL
  AND BALN_TYPE_C IS NOT NULL
  AND BKDT_EFFT_D IS NOT NULL 