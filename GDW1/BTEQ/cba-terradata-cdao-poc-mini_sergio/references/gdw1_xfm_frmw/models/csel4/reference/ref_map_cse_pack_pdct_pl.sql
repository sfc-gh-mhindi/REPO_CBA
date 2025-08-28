{{ config(
    materialized='table',
    unique_key='pl_pack_cat_id',
    tags=['reference_data', 'daily_refresh']
) }}

/*
    Reference table for Product Package mapping
    Equivalent to LdMAP_CSE_PACK_PDCT_PLLkp DataStage job
    
    This creates a lookup table with effective dating
*/

WITH source_data AS (
    SELECT 
        pl_pack_cat_id,
        pdct_n,
        efft_d,
        expy_d
    FROM {{ var('reference_database') }}.{{ var('reference_schema') }}.MAP_CSE_PACK_PDCT_PL
),

business_date AS (
    SELECT BUS_DT as current_business_date 
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
    WHERE STRM_NAME = '{{ var("run_stream") }}' 
      AND PROCESSING_FLAG = 1
    LIMIT 1
),

current_effective AS (
    SELECT 
        TRIM(s.pl_pack_cat_id) AS pl_pack_cat_id,
        TRIM(s.pdct_n) AS pdct_n,
        s.efft_d AS effective_date,
        s.expy_d AS expiry_date,
        
        -- Add effective dating flags
        CASE 
            WHEN bd.current_business_date BETWEEN s.efft_d AND s.expy_d 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS _loaded_at,
        TO_CHAR(bd.current_business_date, 'YYYYMMDD') AS _effective_as_of_date
        
    FROM source_data s
    CROSS JOIN business_date bd
    WHERE s.efft_d <= bd.current_business_date
      AND s.expy_d >= bd.current_business_date
)

SELECT * FROM current_effective
