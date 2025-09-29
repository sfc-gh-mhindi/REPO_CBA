{%- set process_name = 'ACCT_BALN_BKDT' -%}
{%- set stream_name = 'ACCT_BALN_BKDT_ADJ_RULE' -%}

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    schema='pddstg',
    tags=['account_balance', 'backdated_adjustment', 'core_transform', 'sap_source', 'stream_acct_baln_bkdt'],
    pre_hook=[
        "{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_ADJ_RULE
    Purpose: Calculate the Backdated adjustment from ACCT BALN ADJ and apply it on ACCT BALN
    Business Logic: 
    - Processes SAP balance adjustments with backdating logic
    - Calculates business day 4 logic for adjustment timing
    - Aggregates similar adjustments for the same period
    Dependencies: 
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_ADJ
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
*/

WITH business_day_4 AS (
    -- Calculation of Business day 4 Logic
    SELECT	
        CALR_YEAR_N,
        CALR_MNTH_N,
        CALR_CALR_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR grc
    WHERE	
        CALR_WEEK_DAY_N NOT IN (1,7) 
        AND CALR_NON_WORK_DAY_F = 'N'
        AND CALR_CALR_D BETWEEN ADD_MONTHS(CURRENT_DATE(),-13) AND ADD_MONTHS(CURRENT_DATE(),+1)
    QUALIFY	ROW_NUMBER() OVER (
        PARTITION BY CALR_YEAR_N, CALR_MNTH_N 
        ORDER BY CALR_CALR_D
    ) = 4
),

adjustment_data AS (
    SELECT	
        ADJ.ACCT_I AS ACCT_I,
        ADJ.SRCE_SYST_C AS SRCE_SYST_C, 
        ADJ.BALN_TYPE_C AS BALN_TYPE_C,
        ADJ.CALC_FUNC_C AS CALC_FUNC_C,
        ADJ.TIME_PERD_C AS TIME_PERD_C,
        ADJ.ADJ_FROM_D AS ADJ_FROM_D,
        ADJ.ADJ_TO_D AS ADJ_TO_D,
        -- Adjustments impacting the current record need to be loaded on the next day to avoid changing the open balances
        CASE 
            WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D THEN DATEADD(day, 1, ADJ.EFFT_D)
            ELSE ADJ.EFFT_D 
        END AS EFFT_D,
        ADJ.GL_RECN_F,
        ADJ.ADJ_A,
        ADJ.PROS_KEY_EFFT_I
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_ADJ adj
    WHERE	
        ADJ.SRCE_SYST_C = 'SAP'
        AND ADJ.BALN_TYPE_C = 'BALN'
        AND ADJ.CALC_FUNC_C = 'SPOT' 
        AND ADJ.TIME_PERD_C = 'E' 
        -- Excluding the adjustments with $0 in value as this brings no change to the 
        -- $value in the ACCT BALN and had a negative impact on the last records in 
        -- ACCT BALN, so considerably important to eliminate
        AND ADJ.ADJ_A <> 0 
        -- Capturing delta adjustments
        AND ADJ.EFFT_D >= (
            SELECT MAX(BTCH_RUN_D) 
            FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC 
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
                AND SRCE_SYST_M = 'GDW'
                AND COMT_F = 'Y'  
                AND SUCC_F = 'Y'
        )
),

backdated_calculations AS (
    SELECT 
        dt1.ACCT_I,
        dt1.SRCE_SYST_C, 
        dt1.BALN_TYPE_C,
        dt1.CALC_FUNC_C,
        dt1.TIME_PERD_C,
        dt1.ADJ_FROM_D,
        CASE 
            -- When difference of months is 0
            WHEN DATEDIFF(month, dt1.ADJ_FROM_D, dt1.EFFT_D) = 0 
                THEN dt1.ADJ_FROM_D 
            -- Backdated logic calculation when difference of months is 1 
            -- and DT1.EFFT_D is between Business day 1 and Biz day 4
            WHEN DATEDIFF(month, dt1.ADJ_FROM_D, dt1.EFFT_D) = 1 
                AND dt1.EFFT_D <= bsdy_4.CALR_CALR_D 
                THEN dt1.ADJ_FROM_D
            -- Backdated logic calculation when difference of months is 1 
            -- and DT1.EFFT_D is NOT between Business day 1 and Biz day 4
            WHEN DATEDIFF(month, dt1.ADJ_FROM_D, dt1.EFFT_D) = 1 
                AND dt1.EFFT_D > bsdy_4.CALR_CALR_D  
                THEN DATE_TRUNC('month', dt1.EFFT_D)
            -- Backdated logic calculation when difference of months is greater than 1 
            -- and DT1.EFFT_D is between Business day 1 and Biz day 4
            WHEN DATEDIFF(month, dt1.ADJ_FROM_D, dt1.EFFT_D) > 1 
                AND dt1.EFFT_D <= bsdy_4.CALR_CALR_D 
                THEN DATEADD(month, -1, DATE_TRUNC('month', dt1.EFFT_D))
            -- Backdated logic calculation when difference of months is greater than 1 
            -- and DT1.EFFT_D is NOT between Business day 1 and Biz day 4
            WHEN DATEDIFF(month, dt1.ADJ_FROM_D, dt1.EFFT_D) > 1 
                AND dt1.EFFT_D > bsdy_4.CALR_CALR_D  
                THEN DATE_TRUNC('month', dt1.EFFT_D)
        END AS BKDT_ADJ_FROM_D,
        dt1.ADJ_TO_D,
        -- Similar adjustments for the same period are added
        SUM(dt1.ADJ_A) AS ADJ_A,
        dt1.EFFT_D,
        dt1.GL_RECN_F,
        dt1.PROS_KEY_EFFT_I
    FROM adjustment_data dt1
    INNER JOIN business_day_4 bsdy_4
        ON EXTRACT(YEAR FROM dt1.EFFT_D) = EXTRACT(YEAR FROM bsdy_4.CALR_CALR_D)
        AND EXTRACT(MONTH FROM dt1.EFFT_D) = EXTRACT(MONTH FROM bsdy_4.CALR_CALR_D)
    WHERE
        -- Including the adjustments that are excluded in the previous run for open record
        dt1.EFFT_D <= (
            SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
            FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
            WHERE TRGT_M = 'ACCT_BALN_ADJ' 
                AND SRCE_SYST_M = 'SAP'
                AND COMT_F = 'Y'  
                AND SUCC_F = 'Y'
        )
        -- To avoid any records that are processed in the previous runs
        AND dt1.EFFT_D > (
            SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
            FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
                AND SRCE_SYST_M = 'GDW'
                AND COMT_F = 'Y'  
                AND SUCC_F = 'Y'
        )
    GROUP BY 
        dt1.ACCT_I,
        dt1.SRCE_SYST_C, 
        dt1.BALN_TYPE_C,
        dt1.CALC_FUNC_C,
        dt1.TIME_PERD_C,
        dt1.ADJ_FROM_D,
        BKDT_ADJ_FROM_D,
        dt1.ADJ_TO_D,
        dt1.EFFT_D,
        dt1.GL_RECN_F, 
        dt1.PROS_KEY_EFFT_I
),

final AS (
    SELECT 
        ACCT_I, 
        SRCE_SYST_C,
        BALN_TYPE_C,
        CALC_FUNC_C,
        TIME_PERD_C,
        ADJ_FROM_D,
        BKDT_ADJ_FROM_D,
        ADJ_TO_D,
        ADJ_A,
        EFFT_D,
        GL_RECN_F,
        PROS_KEY_EFFT_I               
    FROM backdated_calculations
    WHERE
        -- To exclude any adjustments that fall in the period where the GL is closed
        BKDT_ADJ_FROM_D <= ADJ_TO_D
)

SELECT * FROM final