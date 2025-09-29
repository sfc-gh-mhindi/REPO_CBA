-- =====================================================================
-- DBT Mart Model: GRD_GNRC_MAP_DERV_PATY_HOLD
-- Target Table: GRD_GNRC_MAP_DERV_PATY_HOLD
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808.sql, DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql
-- Intermediate Models: int_derv_acct_paty_02_crat_work_tabl_chg0379808, int_derv_acct_paty_02_crat_work_tabl
-- Generated: 2025-08-21 13:42:27
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='GRD_GNRC_MAP_DERV_PATY_HOLD'
) }}

-- Final GRD_GNRC_MAP_DERV_PATY_HOLD mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
    SELECT 
        *,
        'int_derv_acct_paty_02_crat_work_tabl_chg0379808' AS SOURCE_MODEL
    FROM {{ ref('int_derv_acct_paty_02_crat_work_tabl_chg0379808') }}
    UNION ALL
    SELECT 
        *,
        'int_derv_acct_paty_02_crat_work_tabl' AS SOURCE_MODEL
    FROM {{ ref('int_derv_acct_paty_02_crat_work_tabl') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data