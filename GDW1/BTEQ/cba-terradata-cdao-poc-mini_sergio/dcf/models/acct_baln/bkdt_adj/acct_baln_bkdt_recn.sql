{%- set process_name = 'ACCT_BALN_BKDT_RECN' -%}
{%- set stream_name = 'ACCT_BALN_BKDT' -%}

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    schema='starcadproddata',
    tags=['account_balance', 'backdated_reconciliation', 'core_transform', 'reconciliation_process', 'stream_acct_baln_bkdt'],
    pre_hook=[
        "{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "UPDATE {{ var('cld_db') }}.{{ var('cadproddata') }}.UTIL_PROS_ISAC SET COMT_F = 'Y', SUCC_F = 'Y', COMT_S = CURRENT_TIMESTAMP(0), SYST_INS_Q = (SELECT COUNT(*) FROM {{ var('cld_db') }}.{{ var('cadproddata') }}.ACCT_BALN_BKDT_RECN) WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM {{ var('cld_db') }}.{{ var('cadproddata') }}.UTIL_PROS_ISAC WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_RECN')",
        "{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_RECN
    Purpose: Reconciliation process to identify discrepancies between staging and production account balance data
    Business Logic: 
    - Identifies accounts with balance discrepancies between staging (STG2) and production (ACCT_BALN/ACCT_BALN_BKDT)
    - Performs set difference operations (MINUS) to find mismatched records
    - Handles both BALN and BDCL balance types with SPOT calculations
    - Filters for current effective records using CURRENT_DATE between effective and expiry dates
    - Combines two reconciliation scenarios into a single result set
    Dependencies: 
    - {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_BKDT
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
*/

WITH qualifying_accounts AS (
    -- Accounts that are considered for applying adjustments as part of this run
    SELECT DISTINCT A.ACCT_I
    FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2 A
),

baln_discrepancies AS (
    -- First reconciliation: BALN records from ACCT_BALN minus BDCL records comparison
    SELECT 
        B.ACCT_I,
        B.BALN_A
    FROM qualifying_accounts A
    INNER JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN B
        ON A.ACCT_I = B.ACCT_I
    WHERE
        B.BALN_TYPE_C = 'BALN'
        AND B.CALC_FUNC_C = 'SPOT' 
        AND B.TIME_PERD_C = 'E' 
        AND CURRENT_DATE() BETWEEN B.EFFT_D AND B.EXPY_D
    
    EXCEPT
    
    SELECT 
        STG.ACCT_I,
        CASE 
            WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D THEN STG.BALN_A 
            ELSE BKDT.BALN_A 
        END AS BALN_A
    FROM (
        SELECT 
            A.ACCT_I,
            A.BKDT_EFFT_D,
            A.BKDT_EXPY_D,
            A.BALN_A
        FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2 A
        WHERE
            A.BALN_TYPE_C = 'BDCL'
            AND A.CALC_FUNC_C = 'SPOT' 
            AND A.TIME_PERD_C = 'E' 
            AND CURRENT_DATE() BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
    ) STG
    INNER JOIN (
        SELECT 
            B.ACCT_I,
            B.BKDT_EFFT_D,
            B.BKDT_EXPY_D,
            B.BALN_A
        FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2 A
        INNER JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_BKDT B
            ON A.ACCT_I = B.ACCT_I
        WHERE
            B.BALN_TYPE_C = 'BDCL'
            AND B.CALC_FUNC_C = 'SPOT' 
            AND B.TIME_PERD_C = 'E' 
            AND CURRENT_DATE() BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
    ) BKDT
        ON STG.ACCT_I = BKDT.ACCT_I
),

first_reconciliation AS (
    -- First INSERT equivalent: BALN discrepancies with BDCL effective dates
    SELECT 
        DT.ACCT_I,
        BAL.EFFT_D,
        BAL.EXPY_D,
        DT.BALN_A,
        NULL AS PROS_KEY_EFFT_I
    FROM baln_discrepancies DT
    INNER JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN BAL
        ON DT.ACCT_I = BAL.ACCT_I
    WHERE
        BAL.BALN_TYPE_C = 'BDCL'
        AND BAL.CALC_FUNC_C = 'SPOT' 
        AND BAL.TIME_PERD_C = 'E' 
        AND CURRENT_DATE() BETWEEN BAL.EFFT_D AND BAL.EXPY_D
),

bdcl_discrepancies AS (
    -- Second reconciliation: BDCL records comparison minus BALN records
    SELECT 
        STG.ACCT_I,
        CASE 
            WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D THEN STG.BALN_A 
            ELSE BKDT.BALN_A 
        END AS BALN_A,
        NULL AS PROS_KEY_EFFT_I
    FROM (
        SELECT 
            A.ACCT_I,
            A.BKDT_EFFT_D,
            A.BKDT_EXPY_D,
            A.BALN_A
        FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2 A
        WHERE
            A.BALN_TYPE_C = 'BDCL'
            AND A.CALC_FUNC_C = 'SPOT' 
            AND A.TIME_PERD_C = 'E' 
            AND CURRENT_DATE() BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
    ) STG
    INNER JOIN (
        SELECT 
            B.ACCT_I,
            B.BKDT_EFFT_D,
            B.BKDT_EXPY_D,
            B.BALN_A
        FROM {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2 A
        INNER JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_BKDT B
            ON A.ACCT_I = B.ACCT_I
        WHERE
            B.BALN_TYPE_C = 'BDCL'
            AND B.CALC_FUNC_C = 'SPOT' 
            AND B.TIME_PERD_C = 'E' 
            AND CURRENT_DATE() BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
    ) BKDT
        ON STG.ACCT_I = BKDT.ACCT_I
    
    EXCEPT
    
    SELECT 
        B.ACCT_I,
        B.BALN_A,
        NULL AS PROS_KEY_EFFT_I
    FROM qualifying_accounts A
    INNER JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN B
        ON A.ACCT_I = B.ACCT_I
    WHERE
        B.BALN_TYPE_C = 'BALN'
        AND B.CALC_FUNC_C = 'SPOT' 
        AND B.TIME_PERD_C = 'E' 
        AND CURRENT_DATE() BETWEEN B.EFFT_D AND B.EXPY_D
),

second_reconciliation AS (
    -- Second INSERT equivalent: BDCL discrepancies with staging dates
    SELECT 
        DT1.ACCT_I,
        STG.BKDT_EFFT_D AS EFFT_D,
        STG.BKDT_EXPY_D AS EXPY_D,
        STG.BALN_A,
        NULL AS PROS_KEY_EFFT_I
    FROM bdcl_discrepancies DT1
    INNER JOIN {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2 STG
        ON DT1.ACCT_I = STG.ACCT_I
    WHERE
        CURRENT_DATE() BETWEEN STG.BKDT_EFFT_D AND STG.BKDT_EXPY_D
)

-- Combine both reconciliation results
SELECT 
    ACCT_I,
    EFFT_D BKDT_EFFT_D,
    EXPY_D BKDT_EXPY_D,
    BALN_A,
    PROS_KEY_EFFT_I
FROM first_reconciliation

UNION ALL

SELECT 
    ACCT_I,
    EFFT_D,
    EXPY_D,
    BALN_A,
    PROS_KEY_EFFT_I
FROM second_reconciliation