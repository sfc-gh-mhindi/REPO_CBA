-- =====================================================================
-- DBT Mart Model: DERV_ACCT_PATY_ADD
-- Target Table: DERV_ACCT_PATY_ADD
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_07_CRAT_DELTAS.sql
-- Intermediate Models: int_derv_acct_paty_07_crat_deltas
-- Generated: 2025-08-21 13:57:48
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='DERV_ACCT_PATY_ADD'
) }}

-- Final DERV_ACCT_PATY_ADD mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_derv_acct_paty_07_crat_deltas') }}