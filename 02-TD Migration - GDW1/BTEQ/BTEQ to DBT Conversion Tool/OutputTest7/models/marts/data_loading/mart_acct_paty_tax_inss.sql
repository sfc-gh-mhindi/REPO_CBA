-- =====================================================================
-- DBT Mart Model: ACCT_PATY_TAX_INSS
-- Target Table: ACCT_PATY_TAX_INSS
-- Category: data_loading
-- Source Files: BTEQ_TAX_INSS_MNLY_LOAD.sql
-- Intermediate Models: int_bteq_tax_inss_mnly_load
-- Generated: 2025-08-21 13:48:49
-- =====================================================================

{{ marts_model_config(
    business_area='data_loading',
    target_table='ACCT_PATY_TAX_INSS'
) }}

-- Final ACCT_PATY_TAX_INSS mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_bteq_tax_inss_mnly_load') }}