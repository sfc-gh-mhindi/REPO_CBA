/*
=============================================================================
Model: ACCT_BALN_BKDT_ADJ_RULE
Purpose: Calculate backdated adjustments from ACCT_BALN_ADJ and apply to ACCT_BALN

Description: This model processes account balance adjustments by calculating
             backdated adjustment periods based on business day rules and applies
             them to the account balance backdated adjustment rule table.

Business Logic:
- Processes SAP account balance adjustments with specific criteria
- Calculates backdated adjustment periods using business day 4 logic
- Applies complex date logic for different month scenarios
- Aggregates similar adjustments for the same period
- Excludes zero-value adjustments and closed GL periods

Dependencies:
- {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_ADJ
- {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
- {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR

Migration Notes:
- Converted from Teradata BTEQ script 90_ISRT_ACCT_BALN_BKDT_ADJ_RULE
- QUALIFY clause preserved (supported in Snowflake)
- Teradata interval arithmetic converted to Snowflake DATEDIFF/DATEADD
- Complex CASE logic for backdated calculations preserved
- DELETE+INSERT pattern converted to truncate-load materialization

Version History:
1.0  22/07/2011 Suresh Vajapeyajula - Initial Teradata version
2.0  2025-01-XX Migration Team      - DBT conversion
=============================================================================
*/

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    schema='pddstg',
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    tags=['stream_acct_baln_bkdt', 'account_balance', 'backdated_adjustments', 'sap_source'],
    pre_hook="{{ log_process_start('ACCT_BALN_BKDT', 'ACCT_BALN_BKDT_ADJ_RULE') }}",
    post_hook="{{ log_process_success('ACCT_BALN_BKDT', 'ACCT_BALN_BKDT_ADJ_RULE') }}"
  )
}}

SELECT 
    DT1.ACCT_I,
    DT1.SRCE_SYST_C, 
    DT1.BALN_TYPE_C,
    DT1.CALC_FUNC_C,
    DT1.TIME_PERD_C,
    DT1.ADJ_FROM_D,
    -- Complex backdated logic calculation using Snowflake date functions
    CASE 
        -- Same month: no backdating needed
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 0 
        THEN DT1.ADJ_FROM_D 
        
        -- One month difference and within business day 4: use original date
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
             AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
        THEN DT1.ADJ_FROM_D
        
        -- One month difference and after business day 4: use first of current month
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
             AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
        THEN DATE_TRUNC('MONTH', DT1.EFFT_D)
        
        -- More than one month and within business day 4: use first of previous month
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
             AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
        THEN DATEADD(MONTH, -1, DATE_TRUNC('MONTH', DT1.EFFT_D))
        
        -- More than one month and after business day 4: use first of current month
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
             AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
        THEN DATE_TRUNC('MONTH', DT1.EFFT_D)
        
        ELSE DT1.ADJ_FROM_D
    END AS BKDT_ADJ_FROM_D,
    DT1.ADJ_TO_D,
    -- Aggregate similar adjustments for the same period
    SUM(DT1.ADJ_A) AS ADJ_A,
    DT1.EFFT_D,
    DT1.GL_RECN_F,
    DT1.PROS_KEY_EFFT_I
FROM (
    -- Source adjustment data with effective date logic
    SELECT	
        ADJ.ACCT_I,
        ADJ.SRCE_SYST_C, 
        ADJ.BALN_TYPE_C,
        ADJ.CALC_FUNC_C,
        ADJ.TIME_PERD_C,
        ADJ.ADJ_FROM_D,
        ADJ.ADJ_TO_D,
        -- Adjustments impacting current record loaded next day to avoid changing open balances
        CASE 
            WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D 
            THEN DATEADD(DAY, 1, ADJ.EFFT_D)
            ELSE ADJ.EFFT_D 
        END AS EFFT_D,
        ADJ.GL_RECN_F,
        ADJ.ADJ_A,
        ADJ.PROS_KEY_EFFT_I
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_ADJ ADJ
    WHERE	
        ADJ.SRCE_SYST_C = 'SAP'
        AND ADJ.BALN_TYPE_C = 'BALN'
        AND ADJ.CALC_FUNC_C = 'SPOT' 
        AND ADJ.TIME_PERD_C = 'E' 
        -- Exclude zero-value adjustments to avoid negative impact on last records
        AND ADJ.ADJ_A <> 0 
        -- Capture delta adjustments since last successful run
        AND ADJ.EFFT_D >= (
            SELECT MAX(BTCH_RUN_D) 
            FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC 
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
              AND SRCE_SYST_M = 'GDW'
              AND COMT_F = 'Y' 
              AND SUCC_F = 'Y'
        )
) DT1
INNER JOIN (
    -- Business day 4 calculation logic
    SELECT	
        YEAR(CALR_CALR_D) AS CALR_YEAR_N,
        MONTH(CALR_CALR_D) AS CALR_MNTH_N,
        CALR_CALR_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR
    WHERE	
        DAYOFWEEK(CALR_CALR_D) NOT IN (1, 7)  -- Exclude weekends
        AND CALR_NON_WORK_DAY_F = 'N'
        AND CALR_CALR_D BETWEEN DATEADD(MONTH, -13, CURRENT_DATE()) 
                            AND DATEADD(MONTH, 1, CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY YEAR(CALR_CALR_D), MONTH(CALR_CALR_D) 
        ORDER BY CALR_CALR_D
    ) = 4
) BSDY_4
ON YEAR(DT1.EFFT_D) = BSDY_4.CALR_YEAR_N
   AND MONTH(DT1.EFFT_D) = BSDY_4.CALR_MNTH_N
WHERE
    -- Include adjustments excluded in previous run for open records
    DT1.EFFT_D <= (
        SELECT MAX(BTCH_RUN_D) 
        FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
        WHERE TRGT_M = 'ACCT_BALN_ADJ' 
          AND SRCE_SYST_M = 'SAP'
          AND COMT_F = 'Y' 
          AND SUCC_F = 'Y'
    )
    -- Avoid records processed in previous runs
    AND DT1.EFFT_D > (
        SELECT MAX(BTCH_RUN_D) 
        FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
        WHERE TRGT_M = 'ACCT_BALN_BKDT' 
          AND SRCE_SYST_M = 'GDW'
          AND COMT_F = 'Y' 
          AND SUCC_F = 'Y'
    )
GROUP BY 
    DT1.ACCT_I, DT1.SRCE_SYST_C, DT1.BALN_TYPE_C, DT1.CALC_FUNC_C,
    DT1.TIME_PERD_C, DT1.ADJ_FROM_D, DT1.ADJ_TO_D, DT1.EFFT_D,
    DT1.GL_RECN_F, DT1.PROS_KEY_EFFT_I, BSDY_4.CALR_CALR_D
HAVING 
    -- Exclude adjustments that fall in closed GL periods
    CASE 
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 0 
        THEN DT1.ADJ_FROM_D 
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
             AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
        THEN DT1.ADJ_FROM_D
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) = 1 
             AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
        THEN DATE_TRUNC('MONTH', DT1.EFFT_D)
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
             AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
        THEN DATEADD(MONTH, -1, DATE_TRUNC('MONTH', DT1.EFFT_D))
        WHEN DATEDIFF(MONTH, DT1.ADJ_FROM_D, DT1.EFFT_D) > 1 
             AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  
        THEN DATE_TRUNC('MONTH', DT1.EFFT_D)
        ELSE DT1.ADJ_FROM_D
    END <= DT1.ADJ_TO_D