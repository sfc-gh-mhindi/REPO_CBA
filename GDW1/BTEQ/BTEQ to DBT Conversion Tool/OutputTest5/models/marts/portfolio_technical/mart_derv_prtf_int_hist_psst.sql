-- =====================================================================
-- DBT Mart Model: DERV_PRTF_INT_HIST_PSST
-- Target Table: DERV_PRTF_INT_HIST_PSST
-- Category: portfolio_technical
-- Source Files: prtf_tech_int_grup_enhc_psst.sql
-- Intermediate Models: int_prtf_tech_int_grup_enhc_psst
-- Generated: 2025-08-21 13:42:27
-- =====================================================================

{{ marts_model_config(
    business_area='portfolio_technical',
    target_table='DERV_PRTF_INT_HIST_PSST'
) }}

-- Final DERV_PRTF_INT_HIST_PSST mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_prtf_tech_int_grup_enhc_psst') }}