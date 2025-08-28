{%- set process_name = 'ACCT_BALN_BKDT_AUDT' -%}
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
    tags=['account_balance', 'backdated_adjustment', 'audit', 'core_transform'],
    pre_hook=[
        "{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}",
        "UPDATE {{ var('cld_db') }}.{{ var('cadproddata') }}.UTIL_PROS_ISAC
         SET COMT_F = 'Y',
             SUCC_F = 'Y',
             COMT_S = CURRENT_TIMESTAMP(0),
             SYST_INS_Q = (SELECT COUNT(*) FROM {{ this }})
         WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC 
                             WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT')"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_AUDT_ISRT
    Purpose: Populate the AUDT table for future reference as the records are going to be deleted from ACCT BALN BKDT
    Business Logic: 
    - Loads records from ACCT_BALN_BKDT_STG1 to ACCT_BALN_BKDT_AUDT
    - Captures audit trail for records being deleted from ACCT_BALN_BKDT
    - Establishes logical relationship between tables for rollback purposes
    - Captures maximum pros keys from various sources for auditing
    Dependencies: 
    - {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG1
    - {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_ADJ_RULE
    - {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
*/

WITH max_adj_pros_keys AS (
    -- Capturing the maximum pros_key_efft_i in the event of multiple pros keys for one account
    -- and populating for Auditing purposes
    SELECT 
        ACCT_I, 
        MAX(PROS_KEY_EFFT_I) AS PROS_KEY_EFFT_I 
    FROM {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_ADJ_RULE
    GROUP BY ACCT_I
),

max_stg2_pros_key AS (
    -- Capture the latest pros key from the parent process and update the audt table
    SELECT MAX(BKDT_PROS_KEY_I) AS BKDT_PROS_KEY_I
    FROM {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2
),

max_audit_pros_key AS (
    -- Capture the latest pros key from the Auditing process and update the audt table
    SELECT MAX(PROS_KEY_I) AS PROS_KEY_EFFT_I
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC 
    WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT'
)

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
FROM {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG1 STG1
INNER JOIN max_adj_pros_keys ADJ
    ON STG1.ACCT_I = ADJ.ACCT_I
CROSS JOIN max_stg2_pros_key STG2
CROSS JOIN max_audit_pros_key PKEY