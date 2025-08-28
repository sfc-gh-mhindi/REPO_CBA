-- =====================================================================
-- DBT Mart Model: GRD_GNRC_MAP_BUSN_SEGM_PRTY
-- Target Table: GRD_GNRC_MAP_BUSN_SEGM_PRTY
-- Category: derived_account_party
-- Source Files: DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql
-- Intermediate Models: int_derv_acct_paty_02_crat_work_tabl
-- Generated: 2025-08-21 13:45:01
-- =====================================================================

{{ marts_model_config(
    business_area='derived_account_party',
    target_table='GRD_GNRC_MAP_BUSN_SEGM_PRTY'
) }}

-- Final GRD_GNRC_MAP_BUSN_SEGM_PRTY mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_derv_acct_paty_02_crat_work_tabl') }}