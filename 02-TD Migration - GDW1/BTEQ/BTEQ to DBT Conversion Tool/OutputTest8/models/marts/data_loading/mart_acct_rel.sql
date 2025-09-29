-- =====================================================================
-- DBT Mart Model: ACCT_REL
-- Target Table: ACCT_REL
-- Category: data_loading
-- Source Files: BTEQ_SAP_EDO_WKLY_LOAD.sql
-- Intermediate Models: int_bteq_sap_edo_wkly_load
-- Generated: 2025-08-21 13:57:48
-- =====================================================================

{{ marts_model_config(
    business_area='data_loading',
    target_table='ACCT_REL'
) }}

-- Final ACCT_REL mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_bteq_sap_edo_wkly_load') }}