-- =====================================================================
-- DBT Mart Model: DERV_PRTF_PATY_PSST
-- Target Table: DERV_PRTF_PATY_PSST
-- Category: portfolio_technical
-- Source Files: prtf_tech_paty_psst.sql, prtf_tech_paty_int_grup_psst.sql
-- Intermediate Models: int_prtf_tech_paty_psst, int_prtf_tech_paty_int_grup_psst
-- Generated: 2025-08-21 11:23:07
-- =====================================================================

{{ marts_model_config(
    business_area='portfolio_technical',
    target_table='DERV_PRTF_PATY_PSST'
) }}

-- Final DERV_PRTF_PATY_PSST mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
    SELECT 
        *,
        'int_prtf_tech_paty_psst' AS SOURCE_MODEL
    FROM {{ ref('int_prtf_tech_paty_psst') }}
    UNION ALL
    SELECT 
        *,
        'int_prtf_tech_paty_int_grup_psst' AS SOURCE_MODEL
    FROM {{ ref('int_prtf_tech_paty_int_grup_psst') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data