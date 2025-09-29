-- =====================================================================
-- DBT Mart Model: DERV_PRTF_INT_PSST
-- Target Table: DERV_PRTF_INT_PSST
-- Category: portfolio_technical
-- Source Files: prtf_tech_int_grup_enhc_psst.sql, prtf_tech_int_psst.sql
-- Intermediate Models: int_prtf_tech_int_grup_enhc_psst, int_prtf_tech_int_psst
-- Generated: 2025-08-21 14:21:55
-- =====================================================================

{{ marts_model_config(
    business_area='portfolio_technical',
    target_table='DERV_PRTF_INT_PSST'
) }}

-- Final DERV_PRTF_INT_PSST mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
    SELECT 
        *,
        'int_prtf_tech_int_grup_enhc_psst' AS SOURCE_MODEL
    FROM {{ ref('int_prtf_tech_int_grup_enhc_psst') }}
    UNION ALL
    SELECT 
        *,
        'int_prtf_tech_int_psst' AS SOURCE_MODEL
    FROM {{ ref('int_prtf_tech_int_psst') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data