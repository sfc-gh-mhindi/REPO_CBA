{%- set process_name = 'ACCT_BALN_BKDT_ADJ_RULE' -%}
{%- set stream_name = 'ACCT_BALN_BKDT' -%}

{{
  config(
    materialized='table',
    database=var('target_database'),
    schema='ddstg',
    tags=['account_balance', 'backdated_adjustment', 'sap'],
    pre_hook=[
        "DELETE FROM {{ this }}",
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_ADJ_RULE process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_ADJ_RULE process ended') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_ADJ_RULE
    Purpose: Calculate the Backdated adjustment from ACCT BALN ADJ and apply it on ACCT BALN
    Business Logic: 
    - Processes SAP account balance adjustments with backdating logic
    - Calculates business day 4 logic for adjustment timing
    - Aggregates similar adjustments for the same period
    Dependencies: 
    - {{ ref('acct_baln_adj') }}
    - {{ ref('grd_rprt_calr_clyr') }}
    - {{ ref('util_pros_isac') }}
*/

WITH acct_baln_adj_filtered AS (
    SELECT	
        ADJ.ACCT_I AS ACCT_I,
        ADJ.SRCE_SYST_C AS SRCE_SYST_C, 
        ADJ.BALN_TYPE_C AS BALN_TYPE_C,
        ADJ.CALC_FUNC_C AS CALC_FUNC_C,
        ADJ.TIME_PERD_C AS TIME_PERD_C,
        ADJ.ADJ_FROM_D AS ADJ_FROM_D,
        ADJ.ADJ_TO_D,
        -- Adjustments impacting the current record need to be loaded on the next day to avoid changing the open balances
        CASE 
            WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D THEN ADJ.EFFT_D + 1
            ELSE ADJ.EFFT_D 
        END AS EFFT_D,
        ADJ.GL_RECN_F,
        ADJ.ADJ_A,
        ADJ.PROS_KEY_EFFT_I
    FROM {{ ref('acct_baln_adj') }} ADJ
    WHERE	
        ADJ.SRCE_SYST_C = 'SAP'
        AND ADJ.BALN_TYPE_C = 'BALN'
        AND ADJ.CALC_FUNC_C = 'SPOT' 
        AND ADJ.TIME_PERD_C = 'E' 
        -- Excluding the adjustments with $0 in value as this brings no change to the 
        -- $value in tha ACCT BALN and had a negative impact on the last records in 
        -- ACCT BALN, so considerably important to eliminate
        AND ADJ.ADJ_A <> 0 
        -- Capturing delta adjustments
        AND ADJ.EFFT_D >= (
            SELECT MAX(BTCH_RUN_D) 
            FROM {{ ref('util_pros_isac') }}
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
                AND SRCE_SYST_M = 'GDW'
                AND COMT_F = 'Y'  	
                AND SUCC_F = 'Y'
        )
),

business_day_4 AS (
    -- Calculation of Business day 4 Logic
    SELECT	
        CALR_YEAR_N,
        CALR_MNTH_N,
        CALR_CALR_D
    FROM {{ ref('grd_rprt_calr_clyr') }}
    WHERE	
        CALR_WEEK_DAY_N NOT IN (1, 7) 
        AND CALR_NON_WORK_DAY_F = 'N'
        AND CALR_CALR_D BETWEEN ADD_MONTHS(CURRENT_DATE, -13) AND ADD_MONTHS(CURRENT_DATE, 1)
    QUALIFY	ROW_NUMBER() OVER (
        PARTITION BY CALR_YEAR_N, CALR_MNTH_N 
        ORDER BY CALR_CALR_D
    ) = 4
),

dt1_with_business_day AS (
    SELECT 
        DT1.ACCT_I,
        DT1.SRCE_SYST_C, 
        DT1.BALN_TYPE_C,
        DT1.CALC_FUNC_C,
        DT1.TIME_PERD_C,
        DT1.ADJ_FROM_D,
        DT1.ADJ_TO_D,
        DT1.EFFT_D,
        DT1.GL_RECN_F,
        DT1.ADJ_A,
        DT1.PROS_KEY_EFFT_I,
        BSDY_4.CALR_CALR_D AS BUSINESS_DAY_4
    FROM acct_baln_adj_filtered DT1
    INNER JOIN business_day_4 BSDY_4
        ON EXTRACT(YEAR FROM DT1.EFFT_D) = EXTRACT(YEAR FROM BSDY_4.CALR_CALR_D)
        AND EXTRACT(MONTH FROM DT1.EFFT_D) = EXTRACT(MONTH FROM BSDY_4.CALR_CALR_D)
),

final AS (
    SELECT 
        DT1.ACCT_I,
        DT1.SRCE_SYST_C, 
        DT1.BALN_TYPE_C,
        DT1.CALC_FUNC_C,
        DT1.TIME_PERD_C,
        DT1.ADJ_FROM_D,
        CASE 
            WHEN DATEDIFF('month', DT1.ADJ_FROM_D, DT1.EFFT_D) = 0 
                THEN DT1.ADJ_FROM_D 
            -- Backdated logic calculation when difference of months is 1 
            -- and DT1.EFFT_D is between Business day 1 and Biz day 4
            WHEN DATEDIFF('month', DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                AND DT1.EFFT_D <= DT1.BUSINESS_DAY_4 
                THEN DT1.ADJ_FROM_D
            -- Backdated logic calculation when difference of months is 1 
            -- and DT1.EFFT_D is NOT between Business day 1 and Biz day 4
            WHEN DATEDIFF('month', DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
                AND DT1.EFFT_D > DT1.BUSINESS_DAY_4  
                THEN DATE_TRUNC('month', DT1.EFFT_D)
            -- Backdated logic calculation when difference of months is greater than 1 
            -- and DT1.EFFT_D is between Business day 1 and Biz day 4
            WHEN DATEDIFF('month', DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                AND DT1.EFFT_D <= DT1.BUSINESS_DAY_4 
                THEN DATEADD('month', -1, DATE_TRUNC('month', DT1.EFFT_D))
            -- Backdated logic calculation when difference of months is greater than 1 
            -- and DT1.EFFT_D is NOT between Business day 1 and Biz day 4
            WHEN DATEDIFF('month', DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
                AND DT1.EFFT_D > DT1.BUSINESS_DAY_4  
                THEN DATE_TRUNC('month', DT1.EFFT_D)
        END AS BKDT_ADJ_FROM_D,
        DT1.ADJ_TO_D,
        -- Similar adjustments for the same period are added
        SUM(DT1.ADJ_A) AS ADJ_A,
        DT1.EFFT_D,
        DT1.GL_RECN_F,
        DT1.PROS_KEY_EFFT_I
    FROM dt1_with_business_day DT1
    WHERE
        -- Including the adjustments that are excluded in the previous run for open record
        DT1.EFFT_D <= (
            SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
            FROM {{ ref('util_pros_isac') }}
            WHERE TRGT_M = 'ACCT_BALN_ADJ' 
                AND SRCE_SYST_M = 'SAP'
                AND COMT_F = 'Y'  
                AND SUCC_F = 'Y'
        )
        -- To avoid any records that are processed in the previous runs
        AND DT1.EFFT_D > (
            SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
            FROM {{ ref('util_pros_isac') }}
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
                AND SRCE_SYST_M = 'GDW'
                AND COMT_F = 'Y'  
                AND SUCC_F = 'Y'
        )
    GROUP BY 
        DT1.ACCT_I,
        DT1.SRCE_SYST_C, 
        DT1.BALN_TYPE_C,
        DT1.CALC_FUNC_C,
        DT1.TIME_PERD_C,
        DT1.ADJ_FROM_D,
        BKDT_ADJ_FROM_D,
        DT1.ADJ_TO_D,
        DT1.EFFT_D,
        DT1.GL_RECN_F, 
        DT1.PROS_KEY_EFFT_I
    HAVING 
        -- To exclude any adjustments that fall in the period where the GL is closed
        BKDT_ADJ_FROM_D <= ADJ_TO_D
)

SELECT * FROM final