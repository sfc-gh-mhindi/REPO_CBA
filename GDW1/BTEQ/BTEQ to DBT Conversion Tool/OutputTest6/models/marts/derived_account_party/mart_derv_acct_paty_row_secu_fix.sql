-- =====================================================================
-- DBT Mart Model: DERV_ACCT_PATY_ROW_SECU_FIX
-- Target Table: DERV_ACCT_PATY_ROW_SECU_FIX
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_08_APPLY_DELTAS.sql
-- Intermediate Models: int_derv_acct_paty_08_apply_deltas
-- Generated: 2025-08-21 13:45:01
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='DERV_ACCT_PATY_ROW_SECU_FIX'
) }}

-- Final DERV_ACCT_PATY_ROW_SECU_FIX mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_derv_acct_paty_08_apply_deltas') }}