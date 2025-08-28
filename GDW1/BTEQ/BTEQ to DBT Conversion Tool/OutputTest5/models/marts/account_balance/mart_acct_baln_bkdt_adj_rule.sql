-- =====================================================================
-- DBT Mart Model: ACCT_BALN_BKDT_ADJ_RULE
-- Target Table: ACCT_BALN_BKDT_ADJ_RULE
-- Category: account_balance
-- Source Files: ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql
-- Intermediate Models: int_acct_baln_bkdt_adj_rule_isrt
-- Generated: 2025-08-21 13:42:27
-- =====================================================================

{{ marts_model_config(
    business_area='account_balance',
    target_table='ACCT_BALN_BKDT_ADJ_RULE'
) }}

-- Final ACCT_BALN_BKDT_ADJ_RULE mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_acct_baln_bkdt_adj_rule_isrt') }}