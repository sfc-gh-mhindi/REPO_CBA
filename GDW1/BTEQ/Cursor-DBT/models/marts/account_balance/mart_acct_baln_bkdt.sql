/*
  GDW1 Account Balance Backdate Mart
  
  Final production table for account balance backdate processing
  
  Description: 
  Consolidated account balance data with backdated adjustments applied.
  This mart combines all intermediate processing steps into a final 
  production-ready table for consumption by downstream systems.
  
  Source Models:
  - int_acct_baln_bkdt_insert: Account balance insertions
  - int_acct_baln_bkdt_cleanup: Records to be updated/replaced
  
  Business Logic:
  - Apply all balance adjustments and calculations
  - Ensure data quality and completeness
  - Maintain effective and expiry date logic
  - Include process tracking and audit information
  
  DCF Pattern: Final mart table
  Stream: GDW1_ACCT_BALN_PROCESSING
*/

{{ 
  marts_model_config(
    business_area='account_balance',
    stream_name=var('account_balance_stream')
  )
}}

WITH current_balances AS (
    -- Get current balance data from intermediate processing
    SELECT DISTINCT
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
        CREATED_BY,
        CREATED_TS,
        UPDATED_BY,
        UPDATED_TS,
        SESSION_ID,
        WAREHOUSE_NAME
        
    FROM {{ ref('int_acct_baln_bkdt_insert') }}
    
    WHERE ACCT_I IS NOT NULL
      AND BALN_TYPE_C IS NOT NULL
      -- Add any additional business rules
),

cleanup_records AS (
    -- Get records that were identified for cleanup
    SELECT DISTINCT
        ACCT_I,
        BALN_TYPE_C,
        CALC_FUNC_C,
        TIME_PERD_C,
        BKDT_EFFT_D,
        BKDT_EXPY_D,
        CLEANUP_ACTION,
        CLEANUP_REASON
        
    FROM {{ ref('int_acct_baln_bkdt_cleanup') }}
    
    WHERE CLEANUP_ACTION = 'DELETE'
),

final_balances AS (
    -- Apply cleanup logic - include new records and exclude cleaned up ones
    SELECT 
        CB.*,
        
        -- Add business indicators
        CASE 
            WHEN CR.ACCT_I IS NOT NULL THEN 'UPDATED'
            ELSE 'NEW'
        END AS RECORD_STATUS,
        
        -- Add mart-specific columns
        {{ gdw1_business_date() }} AS MART_LOAD_DATE,
        '{{ var("account_balance_stream") }}' AS SOURCE_STREAM,
        
        -- Add data quality flags
        CASE 
            WHEN CB.BALN_A < 0 THEN 'NEGATIVE_BALANCE'
            WHEN CB.BKDT_EFFT_D > CB.BKDT_EXPY_D THEN 'INVALID_DATE_RANGE'
            ELSE 'VALID'
        END AS DATA_QUALITY_FLAG
        
    FROM current_balances CB
    LEFT JOIN cleanup_records CR 
        ON CB.ACCT_I = CR.ACCT_I
        AND CB.BALN_TYPE_C = CR.BALN_TYPE_C
        AND CB.CALC_FUNC_C = CR.CALC_FUNC_C
        AND CB.TIME_PERD_C = CR.TIME_PERD_C
        AND CB.BKDT_EFFT_D = CR.BKDT_EFFT_D
        AND CB.BKDT_EXPY_D = CR.BKDT_EXPY_D
)

SELECT
    -- Core account balance columns
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
    
    -- Process and audit columns
    RECORD_STATUS,
    DATA_QUALITY_FLAG,
    MART_LOAD_DATE,
    SOURCE_STREAM,
    
    -- Original audit columns
    CREATED_BY,
    CREATED_TS,
    UPDATED_BY,
    UPDATED_TS,
    SESSION_ID,
    WAREHOUSE_NAME,
    
    -- Add mart-level audit columns
    CURRENT_USER() AS MART_CREATED_BY,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS

FROM final_balances

-- Add final data quality filters
WHERE DATA_QUALITY_FLAG = 'VALID'
  OR (DATA_QUALITY_FLAG = 'NEGATIVE_BALANCE' AND BALN_TYPE_C IN ('OVERDRAFT', 'CREDIT_BALANCE'))

-- Order for consistent output
ORDER BY ACCT_I, BALN_TYPE_C, BKDT_EFFT_D 