-- =====================================================================
-- DBT Mart Model: ACCT_PATY_DEDUP
-- Target Table: ACCT_PATY_DEDUP
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_04_POP_CURR_TABL.sql
-- Intermediate Models: int_derv_acct_paty_04_pop_curr_tabl
-- Generated: 2025-08-21 15:55:19
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='ACCT_PATY_DEDUP'
) }}

-- Final ACCT_PATY_DEDUP mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_derv_acct_paty_04_pop_curr_tabl') }}