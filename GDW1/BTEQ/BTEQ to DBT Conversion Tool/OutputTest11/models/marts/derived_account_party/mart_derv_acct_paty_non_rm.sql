-- =====================================================================
-- DBT Mart Model: DERV_ACCT_PATY_NON_RM
-- Target Table: DERV_ACCT_PATY_NON_RM
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql, DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql
-- Intermediate Models: int_derv_acct_paty_06_set_max_prfr_flag, int_derv_acct_paty_06_set_max_prfr_flag_chg0379808
-- Generated: 2025-08-21 15:50:26
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='DERV_ACCT_PATY_NON_RM'
) }}

-- Final DERV_ACCT_PATY_NON_RM mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
    SELECT 
        *,
        'int_derv_acct_paty_06_set_max_prfr_flag' AS SOURCE_MODEL
    FROM {{ ref('int_derv_acct_paty_06_set_max_prfr_flag') }}
    UNION ALL
    SELECT 
        *,
        'int_derv_acct_paty_06_set_max_prfr_flag_chg0379808' AS SOURCE_MODEL
    FROM {{ ref('int_derv_acct_paty_06_set_max_prfr_flag_chg0379808') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data