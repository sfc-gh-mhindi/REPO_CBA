-- =====================================================================
-- DBT Mart Model: ACCT_BALN_BKDT_RECN
-- Target Table: ACCT_BALN_BKDT_RECN
-- Category: account_balance
-- Source Files: ACCT_BALN_BKDT_RECN_ISRT.sql
-- Intermediate Models: int_acct_baln_bkdt_recn_isrt
-- Generated: 2025-08-21 13:45:01
-- =====================================================================

{{ marts_model_config(
    business_area='account_balance',
    target_table='ACCT_BALN_BKDT_RECN'
) }}

-- Final ACCT_BALN_BKDT_RECN mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_acct_baln_bkdt_recn_isrt') }}