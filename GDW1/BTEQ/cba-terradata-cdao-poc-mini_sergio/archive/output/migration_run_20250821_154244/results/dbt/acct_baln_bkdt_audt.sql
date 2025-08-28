{%- set process_name = 'ACCT_BALN_BKDT_AUDT_ISRT' -%}
{%- set stream_name = 'CAD_X01_ACCT_BALN_BKDT_AUDT' -%}

{{
  config(
    materialized='table',
    database=var('cad_prod_data_db'),
    schema='cad_prod_data',
    tags=['audit', 'account_balance', 'backdate'],
    pre_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_AUDT_ISRT Process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_AUDT_ISRT Process ended') }}",
        "UPDATE {{ var('vtech_db') }}.UTIL_PROS_ISAC 
         SET COMT_F = 'Y', SUCC_F = 'Y', COMT_S = CURRENT_TIMESTAMP(), 
             SYST_INS_Q = (SELECT COUNT(*) FROM {{ ref('acct_baln_bkdt_stg1') }})
         WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM {{ var('vtech_db') }}.UTIL_PROS_ISAC 
                             WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT')"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_AUDT
    Purpose: Populate the AUDT table for future reference as the records 
             are going to be deleted from ACCT BALN BKDT and this acts as a driver for the 
             ADJ RULE view
    Business Logic: Loading records from ACCT_BALN_BKDT_STG1 to ACCT_BALN_BKDT_AUDT.
                   This table holds data that is going to be deleted from ACCT_BALN_BKDT.
    Dependencies: acct_baln_bkdt_stg1, acct_baln_bkdt_adj_rule, acct_baln_bkdt_stg2
*/

WITH stg1_data AS (
    SELECT 
        ACCT_I,
        BALN_TYPE_C,
        CALC_FUNC_C,
        TIME_PERD_C,
        BALN_A,
        CALC_F,
        SRCE_SYST_C,
        ORIG_SRCE_SYST_C,
        LOAD_D,
        BKDT_EFFT_D,
        BKDT_EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I
    FROM {{ ref('acct_baln_bkdt_stg1') }}
),

-- Capturing the maximum pros_key_efft_i in the event of multiple pros keys for one account 
-- and populating for Auditing purposes
adj_max_pros_key AS (
    SELECT 
        ACCT_I, 
        MAX(PROS_KEY_EFFT_I) AS PROS_KEY_EFFT_I 
    FROM {{ ref('acct_baln_bkdt_adj_rule') }}
    GROUP BY ACCT_I
),

-- Capture the latest pros key from the parent process and update the audt table
stg2_max_bkdt_pros_key AS (
    SELECT MAX(BKDT_PROS_KEY_I) AS BKDT_PROS_KEY_I
    FROM {{ ref('acct_baln_bkdt_stg2') }}
),

-- Capture the latest pros key from the Auditing process and update the audt table
util_max_pros_key AS (
    SELECT MAX(PROS_KEY_I) AS PROS_KEY_EFFT_I
    FROM {{ var('vtech_db') }}.UTIL_PROS_ISAC 
    WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT'
),

final AS (
    SELECT 
        STG1.ACCT_I,                        
        STG1.BALN_TYPE_C,
        STG1.CALC_FUNC_C,                  
        STG1.TIME_PERD_C,                  
        STG1.BALN_A,
        STG1.CALC_F,
        STG1.SRCE_SYST_C,
        STG1.ORIG_SRCE_SYST_C,
        STG1.LOAD_D,
        STG1.BKDT_EFFT_D,
        STG1.BKDT_EXPY_D,
        PKEY.PROS_KEY_EFFT_I,
        STG1.PROS_KEY_EFFT_I AS ABAL_PROS_KEY_EFFT_I,
        STG1.PROS_KEY_EXPY_I AS ABAL_PROS_KEY_EXPY_I,
        STG2.BKDT_PROS_KEY_I AS ABAL_BKDT_PROS_KEY_I,
        ADJ.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I
    FROM stg1_data STG1
    INNER JOIN adj_max_pros_key ADJ
        ON STG1.ACCT_I = ADJ.ACCT_I
    CROSS JOIN stg2_max_bkdt_pros_key STG2
    CROSS JOIN util_max_pros_key PKEY
)

SELECT * FROM final