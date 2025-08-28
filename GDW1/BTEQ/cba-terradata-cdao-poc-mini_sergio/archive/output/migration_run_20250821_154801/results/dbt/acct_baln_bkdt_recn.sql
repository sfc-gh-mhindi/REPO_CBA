{%- set process_name = 'ACCT_BALN_BKDT_RECN_ISRT' -%}
{%- set stream_name = 'CAD_X01_ACCT_BALN_BKDT_RECN' -%}

{{
  config(
    materialized='table',
    database=var('cad_prod_data_db'),
    schema=var('cad_prod_data_schema'),
    tags=['reconciliation', 'account_balance', 'backdate'],
    pre_hook=[
        "DELETE FROM {{ this }} WHERE 1=1",
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_RECN process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_RECN process ended') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_RECN
    Purpose: Reconciliation process for account balance backdate records
    Business Logic: Identifies discrepancies between staging and production balance data
    Dependencies: ACCT_BALN_BKDT_STG2, ACCT_BALN, ACCT_BALN_BKDT
*/

WITH qualifying_accounts AS (
    SELECT DISTINCT ACCT_I
    FROM {{ ref('acct_baln_bkdt_stg2') }}
),

current_balances AS (
    SELECT 
        B.ACCT_I,
        B.BALN_A
    FROM qualifying_accounts A
    INNER JOIN {{ source('vtech', 'acct_baln') }} B
        ON A.ACCT_I = B.ACCT_I
    WHERE B.BALN_TYPE_C = 'BALN'
        AND B.CALC_FUNC_C = 'SPOT' 
        AND B.TIME_PERD_C = 'E' 
        AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D
),

staging_bdcl_records AS (
    SELECT 
        A.ACCT_I,
        A.BKDT_EFFT_D,
        A.BKDT_EXPY_D,
        A.BALN_A
    FROM {{ ref('acct_baln_bkdt_stg2') }} A
    WHERE A.BALN_TYPE_C = 'BDCL'
        AND A.CALC_FUNC_C = 'SPOT' 
        AND A.TIME_PERD_C = 'E' 
        AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
),

production_bdcl_records AS (
    SELECT 
        B.ACCT_I,
        B.BKDT_EFFT_D,
        B.BKDT_EXPY_D,
        B.BALN_A
    FROM qualifying_accounts A
    INNER JOIN {{ source('vtech', 'acct_baln_bkdt') }} B
        ON A.ACCT_I = B.ACCT_I
    WHERE B.BALN_TYPE_C = 'BDCL'
        AND B.CALC_FUNC_C = 'SPOT' 
        AND B.TIME_PERD_C = 'E' 
        AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
),

staging_vs_production_bdcl AS (
    SELECT 
        STG.ACCT_I,
        CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  
             THEN STG.BALN_A 
             ELSE BKDT.BALN_A 
        END AS BALN_A
    FROM staging_bdcl_records STG
    INNER JOIN production_bdcl_records BKDT
        ON STG.ACCT_I = BKDT.ACCT_I
),

first_insert_data AS (
    SELECT 
        DT.ACCT_I,
        BAL.EFFT_D,
        BAL.EXPY_D,
        DT.BALN_A,
        NULL AS PROS_KEY_EFFT_I
    FROM (
        SELECT ACCT_I, BALN_A FROM current_balances
        EXCEPT
        SELECT ACCT_I, BALN_A FROM staging_vs_production_bdcl
    ) DT
    INNER JOIN {{ source('vtech', 'acct_baln') }} BAL
        ON DT.ACCT_I = BAL.ACCT_I
    WHERE BAL.BALN_TYPE_C = 'BDCL'
        AND BAL.CALC_FUNC_C = 'SPOT' 
        AND BAL.TIME_PERD_C = 'E' 
        AND CURRENT_DATE BETWEEN BAL.EFFT_D AND BAL.EXPY_D
),

second_insert_data AS (
    SELECT 
        DT1.ACCT_I,
        STG.BKDT_EFFT_D AS EFFT_D,
        STG.BKDT_EXPY_D AS EXPY_D,
        STG.BALN_A,
        NULL AS PROS_KEY_EFFT_I
    FROM (
        SELECT 
            ACCT_I, 
            BALN_A,
            NULL AS PROS_KEY_EFFT_I
        FROM staging_vs_production_bdcl
        EXCEPT
        SELECT 
            B.ACCT_I,
            B.BALN_A,
            NULL AS PROS_KEY_EFFT_I
        FROM qualifying_accounts A
        INNER JOIN {{ source('vtech', 'acct_baln') }} B
            ON A.ACCT_I = B.ACCT_I
        WHERE B.BALN_TYPE_C = 'BALN'
            AND B.CALC_FUNC_C = 'SPOT' 
            AND B.TIME_PERD_C = 'E' 
            AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D
    ) DT1
    INNER JOIN {{ ref('acct_baln_bkdt_stg2') }} STG
        ON DT1.ACCT_I = STG.ACCT_I
    WHERE CURRENT_DATE BETWEEN STG.BKDT_EFFT_D AND STG.BKDT_EXPY_D
),

final AS (
    SELECT * FROM first_insert_data
    UNION ALL
    SELECT * FROM second_insert_data
)

SELECT * FROM final