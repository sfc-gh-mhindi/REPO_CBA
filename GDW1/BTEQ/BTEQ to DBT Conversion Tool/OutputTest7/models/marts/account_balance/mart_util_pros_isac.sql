-- =====================================================================
-- DBT Mart Model: UTIL_PROS_ISAC
-- Target Table: UTIL_PROS_ISAC
-- Category: account_balance
-- Source Files: ACCT_BALN_BKDT_AUDT_GET_PROS_KEY.sql, ACCT_BALN_BKDT_GET_PROS_KEY.sql, BTEQ_SAP_EDO_WKLY_LOAD.sql, ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql, BTEQ_TAX_INSS_MNLY_LOAD.sql
-- Intermediate Models: int_acct_baln_bkdt_audt_get_pros_key, int_acct_baln_bkdt_get_pros_key, int_bteq_sap_edo_wkly_load, int_acct_baln_bkdt_recn_get_pros_key, int_bteq_tax_inss_mnly_load
-- Generated: 2025-08-21 13:48:49
-- =====================================================================

{{ marts_model_config(
    business_area='account_balance',
    target_table='UTIL_PROS_ISAC'
) }}

-- Final UTIL_PROS_ISAC mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
    SELECT 
        *,
        'int_acct_baln_bkdt_audt_get_pros_key' AS SOURCE_MODEL
    FROM {{ ref('int_acct_baln_bkdt_audt_get_pros_key') }}
    UNION ALL
    SELECT 
        *,
        'int_acct_baln_bkdt_get_pros_key' AS SOURCE_MODEL
    FROM {{ ref('int_acct_baln_bkdt_get_pros_key') }}
    UNION ALL
    SELECT 
        *,
        'int_bteq_sap_edo_wkly_load' AS SOURCE_MODEL
    FROM {{ ref('int_bteq_sap_edo_wkly_load') }}
    UNION ALL
    SELECT 
        *,
        'int_acct_baln_bkdt_recn_get_pros_key' AS SOURCE_MODEL
    FROM {{ ref('int_acct_baln_bkdt_recn_get_pros_key') }}
    UNION ALL
    SELECT 
        *,
        'int_bteq_tax_inss_mnly_load' AS SOURCE_MODEL
    FROM {{ ref('int_bteq_tax_inss_mnly_load') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data