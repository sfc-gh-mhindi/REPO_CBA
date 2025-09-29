-- =====================================================================
-- DBT Mart Model: DERV_PRTF_OWN_PSST
-- Target Table: DERV_PRTF_OWN_PSST
-- Category: portfolio_technical
-- Source Files: prtf_tech_own_psst.sql, prtf_tech_int_grup_own_psst.sql
-- Intermediate Models: int_prtf_tech_own_psst, int_prtf_tech_int_grup_own_psst
-- Generated: 2025-08-21 15:55:19
-- =====================================================================

{{ marts_model_config(
    business_area='portfolio_technical',
    target_table='DERV_PRTF_OWN_PSST'
) }}

-- Final DERV_PRTF_OWN_PSST mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
    SELECT 
        *,
        'int_prtf_tech_own_psst' AS SOURCE_MODEL
    FROM {{ ref('int_prtf_tech_own_psst') }}
    UNION ALL
    SELECT 
        *,
        'int_prtf_tech_int_grup_own_psst' AS SOURCE_MODEL
    FROM {{ ref('int_prtf_tech_int_grup_own_psst') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data