{%- set process_name = 'ACCT_BALN_BKDT_STG_ISRT' -%}
{%- set stream_name = 'ACCT_BALN_BKDT' -%}

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    schema='pddstg',
    tags=['account_balance', 'backdated_adjustment', 'staging', 'sap_source', 'stream_acct_baln_bkdt'],
    pre_hook=[
        "{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_STG_ISRT
    Purpose: Stage account balance records for backdating adjustments with complex time period calculations
    Business Logic: 
    - Loads account balance records that need backdating adjustments into staging
    - Applies complex time period intersection logic between balances and adjustments
    - Calculates adjusted balance amounts using window functions
    - Creates time periods based on balance and adjustment date ranges
    - Handles overlapping time periods with proper date range logic
    Dependencies: 
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_BKDT
    - {{ var('ddstg_db') }}.{{ var('ddstg_db') }}.ACCT_BALN_BKDT_ADJ_RULE
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
*/

WITH stg1_candidates AS (
    -- Identify suitable candidates for processing to avoid pulling entire history
    SELECT
        ACCT_I, 
        MIN(BKDT_ADJ_FROM_D) AS BKDT_ADJ_FROM_D
    FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_ADJ_RULE
    GROUP BY ACCT_I
),

stg1_balance_data AS (
    -- Load current balances that need to be adjusted
    SELECT
        A.ACCT_I,
        A.BALN_TYPE_C,
        A.CALC_FUNC_C,                  
        A.TIME_PERD_C,                  
        A.BKDT_EFFT_D,
        A.BKDT_EXPY_D,
        A.BALN_A,
        A.CALC_F,
        A.PROS_KEY_EFFT_I,
        A.PROS_KEY_EXPY_I,
        A.BKDT_PROS_KEY_I,
        A.ORIG_SRCE_SYST_C,
        A.SRCE_SYST_C,
        A.LOAD_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_BKDT A
    INNER JOIN stg1_candidates B
        ON A.ACCT_I = B.ACCT_I
    WHERE A.BKDT_EXPY_D >= B.BKDT_ADJ_FROM_D
),

time_period_endpoints AS (
    -- Identify END points of ALL TIME periods of interest for EACH ACCT_I
    SELECT ACCT_I, BKDT_EXPY_D          
    FROM stg1_balance_data
    
    UNION
    
    SELECT ACCT_I, ADJ_TO_D            
    FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_ADJ_RULE
    
    UNION
    
    SELECT ACCT_I, DATEADD(DAY, -1, BKDT_EFFT_D)                
    FROM stg1_balance_data
    
    UNION
    
    SELECT ACCT_I, DATEADD(DAY, -1, BKDT_ADJ_FROM_D)             
    FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_ADJ_RULE
),

time_periods AS (
    -- Identify ALL TIME periods OF interest FOR EACH ACCT_I
    SELECT
        ACCT_I,
        -- Calculate start points of time periods based on end point of previous time period
        DATEADD(DAY, 1, 
            LAG(BKDT_EXPY_D) OVER (
                PARTITION BY ACCT_I 
                ORDER BY BKDT_EXPY_D
            )
        ) AS BKDT_EFFT_D,
        BKDT_EXPY_D
    FROM time_period_endpoints
    -- Ignore record where there is no start point
    QUALIFY BKDT_EFFT_D IS NOT NULL
),

prospective_key AS (
    -- Update the latest PROS_KEY_I into subsequent inserts
    SELECT MAX(PROS_KEY_I) AS BKDT_PROS_KEY_I
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC 
    WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT'
)

SELECT DISTINCT
    DT1.ACCT_I AS ACCT_I,
    -- The BALN_TYPE_C in ACCT_BALN_BKDT table is hardcoded to 'BDCL'
    COALESCE(B.BALN_TYPE_C, 'BDCL') AS BALN_TYPE_C,
    COALESCE(B.CALC_FUNC_C, 'SPOT') AS CALC_FUNC_C,              
    COALESCE(B.TIME_PERD_C, 'E') AS TIME_PERD_C,
    DT1.BKDT_EFFT_D AS BKDT_EFFT_D,
    DT1.BKDT_EXPY_D AS BKDT_EXPY_D,
    -- Calculate the adjusted balance value as the sum of all relevant adjustments plus the relevant balance value
    -- Note that MAX is used for BAL amount simply to identify the single balance valid during the time period
    MAX(COALESCE(B.BALN_A, 0.0)) OVER (
        PARTITION BY DT1.ACCT_I, DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D
    ) + SUM(COALESCE(A.ADJ_A, 0.0)) OVER (
        PARTITION BY DT1.ACCT_I, DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D
    ) AS BALN_A,
    COALESCE(B.CALC_F, 'N') AS CALC_F,
    B.PROS_KEY_EFFT_I,
    B.PROS_KEY_EXPY_I,
    PKEY.BKDT_PROS_KEY_I AS BKDT_PROS_KEY_I,
    A.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I,
    COALESCE(B.ORIG_SRCE_SYST_C, 'SAP') AS ORIG_SRCE_SYST_C,
    COALESCE(B.SRCE_SYST_C, 'GDW') AS SRCE_SYST_C,
    CURRENT_DATE() AS LOAD_D
FROM time_periods DT1
-- Join to balance table based on ACCT_I and intersection with time periods of interest
-- Note that there may be no balance amount related to a time period
LEFT OUTER JOIN stg1_balance_data B
    ON DT1.ACCT_I = B.ACCT_I
    AND (
        -- Handle overlaps condition with explicit date range checks
        (DT1.BKDT_EFFT_D <= B.BKDT_EXPY_D AND DT1.BKDT_EXPY_D >= B.BKDT_EFFT_D)
        -- as Overlaps does not include equality
        OR DT1.BKDT_EFFT_D = B.BKDT_EFFT_D
        OR DT1.BKDT_EXPY_D = B.BKDT_EXPY_D
    )
-- Join to adjustment table based on ACCT_I and intersection with time periods of interest
-- Note that there may be multiple adjustments or no adjustments related to a time period
LEFT OUTER JOIN {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_ADJ_RULE A
    ON DT1.ACCT_I = A.ACCT_I                   
    AND (
        -- Handle overlaps condition with explicit date range checks
        (DT1.BKDT_EFFT_D <= A.ADJ_TO_D AND DT1.BKDT_EXPY_D >= A.BKDT_ADJ_FROM_D)
        OR DT1.BKDT_EFFT_D = A.BKDT_ADJ_FROM_D
        OR DT1.BKDT_EXPY_D = A.ADJ_TO_D
    )
CROSS JOIN prospective_key PKEY