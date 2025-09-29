-- =====================================================================
-- DBT Mart Model: DERV_ACCT_PATY_CURR
-- Target Table: DERV_ACCT_PATY_CURR
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_04_POP_CURR_TABL.sql
-- Intermediate Models: int_derv_acct_paty_04_pop_curr_tabl
-- Generated: 2025-08-21 13:57:48
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='DERV_ACCT_PATY_CURR'
) }}

-- Final DERV_ACCT_PATY_CURR mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_derv_acct_paty_04_pop_curr_tabl') }}