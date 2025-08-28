-- =====================================================================
-- DBT Mart Model: DERV_PRTF_PATY_REL
-- Target Table: DERV_PRTF_PATY_REL
-- Category: portfolio_technical
-- Source Files: prtf_tech_paty_rel_psst.sql
-- Intermediate Models: int_prtf_tech_paty_rel_psst
-- Generated: 2025-08-21 15:46:51
-- =====================================================================

{{ marts_model_config(
    business_area='portfolio_technical',
    target_table='DERV_PRTF_PATY_REL'
) }}

-- Final DERV_PRTF_PATY_REL mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_prtf_tech_paty_rel_psst') }}