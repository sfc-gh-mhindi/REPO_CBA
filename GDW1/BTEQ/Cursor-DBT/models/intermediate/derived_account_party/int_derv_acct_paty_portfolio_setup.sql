/*
  GDW1 Derived Account Party Portfolio Setup
  
  Migrated from: DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
  
  Description: 
  Get accounts that are relationship managed and ONLY ONE of the parties 
  on this account is relationship managed by the same RM. This party will 
  be a preferred party for such an account.
  
  Original Logic:
  - Delete and populate staging tables for account and party portfolios
  - Use extract date from file to filter data
  - Get relationship managed accounts and parties
  - Collect statistics for performance optimization
  
  DBT Approach:
  - Create combined staging view for both account and party portfolios
  - Use variables for extract date instead of file operations
  - Maintain data lineage and audit trail
  - Handle file operations via pre-hooks if needed
  
  DCF Pattern: Intermediate processing table
  Stream: GDW1_DERV_ACCT_PATY_PROCESSING
*/

{{ 
  intermediate_model_config(
    process_type='derived_party_portfolio_setup',
    stream_name=var('derived_party_stream')
  )
}}

WITH extract_date AS (
    -- Handle extract date - could be from variable or file stage
    SELECT {{ gdw1_business_date() }} AS EXTR_D
    -- Alternative: Read from file stage if needed
    -- SELECT EXTR_D FROM {{ file_stage_ref() }}/schedule/{{ var('STRM_C') }}_extr_date.txt
),

account_portfolio_staging AS (
    -- Account portfolio details as per extract date
    SELECT 
        ACCT_I,
        PRTF_CODE_X,
        'ACCOUNT' AS ENTITY_TYPE,
        
        -- Add process tracking
        {{ gdw1_business_date() }} AS PROCESS_DATE,
        {{ generate_process_key() }} AS PROCESS_KEY
        
    FROM {{ bteq_var('VTECH') }}.DERV_PRTF_ACCT ACCT
    CROSS JOIN extract_date ED
    
    WHERE ACCT.PERD_D = ED.EXTR_D
      AND ACCT.DERV_PRTF_CATG_C = 'RM'
      AND ACCT.ACCT_I IS NOT NULL
      AND ACCT.PRTF_CODE_X IS NOT NULL
    
    GROUP BY ACCT_I, PRTF_CODE_X
),

party_portfolio_staging AS (
    -- Party portfolio details as per extract date  
    SELECT 
        PATY_I,
        PRTF_CODE_X,
        'PARTY' AS ENTITY_TYPE,
        
        -- Add process tracking
        {{ gdw1_business_date() }} AS PROCESS_DATE,
        {{ generate_process_key() }} AS PROCESS_KEY
        
    FROM {{ bteq_var('VTECH') }}.DERV_PRTF_PATY PATY
    CROSS JOIN extract_date ED
    
    WHERE PATY.PERD_D = ED.EXTR_D
      AND PATY.DERV_PRTF_CATG_C = 'RM'
      AND PATY.PATY_I IS NOT NULL
      AND PATY.PRTF_CODE_X IS NOT NULL
    
    GROUP BY PATY_I, PRTF_CODE_X
)

-- Combined output for downstream processing
SELECT 
    -- Account portfolio data
    APS.ACCT_I,
    NULL AS PATY_I,
    APS.PRTF_CODE_X,
    APS.ENTITY_TYPE,
    APS.PROCESS_DATE,
    APS.PROCESS_KEY,
    
    -- Add DCF audit columns
    {{ gdw1_audit_columns() }}
    
FROM account_portfolio_staging APS

UNION ALL

SELECT 
    -- Party portfolio data
    NULL AS ACCT_I,
    PPS.PATY_I,
    PPS.PRTF_CODE_X,
    PPS.ENTITY_TYPE,
    PPS.PROCESS_DATE,
    PPS.PROCESS_KEY,
    
    -- Add DCF audit columns
    {{ gdw1_audit_columns() }}
    
FROM party_portfolio_staging PPS

-- Order for consistent output
ORDER BY ENTITY_TYPE, COALESCE(ACCT_I, PATY_I), PRTF_CODE_X 