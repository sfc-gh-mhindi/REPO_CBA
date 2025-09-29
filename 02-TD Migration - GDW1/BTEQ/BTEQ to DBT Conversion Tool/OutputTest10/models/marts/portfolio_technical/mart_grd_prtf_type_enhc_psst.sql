-- =====================================================================
-- DBT Mart Model: GRD_PRTF_TYPE_ENHC_PSST
-- Target Table: GRD_PRTF_TYPE_ENHC_PSST
-- Category: portfolio_technical
-- Source Files: prtf_tech_grd_prtf_type_enhc_psst.sql
-- Intermediate Models: int_prtf_tech_grd_prtf_type_enhc_psst
-- Generated: 2025-08-21 15:46:51
-- =====================================================================

{{ marts_model_config(
    business_area='portfolio_technical',
    target_table='GRD_PRTF_TYPE_ENHC_PSST'
) }}

-- Final GRD_PRTF_TYPE_ENHC_PSST mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{ ref('int_prtf_tech_grd_prtf_type_enhc_psst') }}