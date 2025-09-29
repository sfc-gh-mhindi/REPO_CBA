{%- set process_name = 'ACCT_BALN_BKDT_UTIL_PROS_UPDT' -%}
{%- set stream_name = 'CAD_X01_ACCT_BALN_BKDT' -%}

{{
  config(
    materialized='table',
    database=var('cad_prod_data_db'),
    schema=var('cad_prod_data_schema'),
    tags=['utility', 'process_control', 'isac'],
    pre_hook=[
        "{{ log_dcf_exec_msg('UTIL_PROS_ISAC update process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('UTIL_PROS_ISAC update process completed') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_UTIL_PROS_UPDT
    Purpose: Update UTIL_PROS_ISAC with completion status and record counts
    Business Logic: Updates process control table with success flags and counts from staging tables
    Dependencies: ACCT_BALN_BKDT_STG1, ACCT_BALN_BKDT_STG2, UTIL_PROS_ISAC
*/

WITH stage2_counts AS (
    SELECT COUNT(*) AS ins_cnt
    FROM {{ var('ddstg_db') }}.{{ var('ddstg_schema') }}.ACCT_BALN_BKDT_STG2
),

stage1_counts AS (
    SELECT COUNT(*) AS del_cnt
    FROM {{ var('ddstg_db') }}.{{ var('ddstg_schema') }}.ACCT_BALN_BKDT_STG1
),

max_process_key AS (
    SELECT MAX(PROS_KEY_I) AS max_pros_key_i
    FROM {{ var('vtech_db') }}.{{ var('vtech_schema') }}.UTIL_PROS_ISAC 
    WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT'
),

updated_records AS (
    SELECT 
        u.PROS_KEY_I,
        u.CONV_M,
        'Y' AS COMT_F,
        'Y' AS SUCC_F,
        CURRENT_TIMESTAMP() AS COMT_S,
        s2.ins_cnt AS SYST_INS_Q,
        s1.del_cnt AS SYST_DEL_Q,
        u.PROS_S,
        u.PROS_E,
        u.SYST_UPD_Q,
        u.SYST_ERR_Q,
        u.SYST_REJ_Q
    FROM {{ var('cad_prod_data_db') }}.{{ var('cad_prod_data_schema') }}.UTIL_PROS_ISAC u
    CROSS JOIN stage2_counts s2
    CROSS JOIN stage1_counts s1
    CROSS JOIN max_process_key mpk
    WHERE u.CONV_M = 'CAD_X01_ACCT_BALN_BKDT'
      AND u.PROS_KEY_I = mpk.max_pros_key_i
),

final AS (
    SELECT 
        PROS_KEY_I,
        CONV_M,
        COMT_F,
        SUCC_F,
        COMT_S,
        SYST_INS_Q,
        SYST_DEL_Q,
        PROS_S,
        PROS_E,
        SYST_UPD_Q,
        SYST_ERR_Q,
        SYST_REJ_Q
    FROM updated_records
)

SELECT * FROM final