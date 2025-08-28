-- =====================================================================
-- DBT Mart Model: DERV_PRTF_ACCT_STAG
-- Target Table: DERV_PRTF_ACCT_STAG
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
-- Intermediate Models: int_derv_acct_paty_03_set_acct_prtf
-- Generated: 2025-08-21 13:45:01
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='DERV_PRTF_ACCT_STAG'
) }}

-- Final DERV_PRTF_ACCT_STAG mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_derv_acct_paty_03_set_acct_prtf') }}