{%- set process_name = 'ACCT_BALN_BKDT_AUDT_GET_PROS_KEY' -%}
{%- set stream_name = 'UTIL_PROS_ISAC' -%}

{{
  config(
    materialized='table',
    database=var('cad_prod_data'),
    schema='default',
    tags=['util', 'pros_key', 'audit'],
    pre_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_AUDT_GET_PROS_KEY Process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_AUDT_GET_PROS_KEY Process ended') }}",
        "UPDATE {{ var('cad_prod_data') }}.UTIL_PARM SET PARM_LTRL_N = PARM_LTRL_N + 1 WHERE PARM_M='PROS_KEY'"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_AUDT_GET_PROS_KEY
    Purpose: Populate the AUDT table for future reference as the records 
             are going to be deleted from ACCT BALN BKDT and this acts as a driver for the 
             ADJ RULE view
    Business Logic: 
        - Capture the Latest Pros Key from UTIL PARM table and update UTIL PROS ISAC
        - Insert records for each batch run date between last successful and current batch runs
        - Increment PROS_KEY by 1 in UTIL_PARM table
    Dependencies: UTIL_PARM, UTIL_PROS_ISAC, GRD_RPRT_CALR_CLYR
*/

WITH util_parm AS (
    SELECT 
        PARM_LTRL_N
    FROM {{ var('vtech') }}.UTIL_PARM
    WHERE PARM_M = 'PROS_KEY'
),

bkdt_prev AS (
    -- Capture last Successful Batch run date relating to the Backdated adjustment solution
    SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D 
    FROM {{ var('vtech') }}.UTIL_PROS_ISAC
    WHERE TRGT_M = 'ACCT_BALN_BKDT' 
        AND SRCE_SYST_M = 'GDW'
        AND COMT_F = 'Y'  
        AND SUCC_F = 'Y'
),

bkdt_curr AS (
    -- Capture latest Batch run date relating to the Backdated adjustment solution into ACCT_BALN_BKDT
    SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D 
    FROM {{ var('vtech') }}.UTIL_PROS_ISAC
    WHERE TRGT_M = 'ACCT_BALN_BKDT' 
        AND SRCE_SYST_M = 'GDW'
),

calendar_dates AS (
    -- Get calendar dates between previous and current batch run dates
    SELECT CAL.CALR_CALR_D
    FROM {{ var('vtech') }}.GRD_RPRT_CALR_CLYR CAL
    CROSS JOIN bkdt_prev
    CROSS JOIN bkdt_curr
    WHERE CAL.CALR_CALR_D > bkdt_prev.BTCH_RUN_D 
        AND CAL.CALR_CALR_D <= bkdt_curr.BTCH_RUN_D
),

final AS (
    SELECT 
        PARM.PARM_LTRL_N + 1 AS PROS_KEY_I,
        'CAD_X01_ACCT_BALN_BKDT_AUDT' AS CONV_M,
        'TD' AS CONV_TYPE_M,
        CURRENT_TIMESTAMP(0) AS PROS_RQST_S,
        CURRENT_TIMESTAMP(0) AS PROS_LAST_RQST_S,
        1 AS PROS_RQST_Q,
        DT.CALR_CALR_D AS BTCH_RUN_D,
        NULL AS BTCH_KEY_I,
        'GDW' AS SRCE_SYST_M,
        'ACCT_BALN_BKDT' AS SRCE_M,
        'ACCT_BALN_BKDT_AUDT' AS TRGT_M,
        'N' AS SUCC_F,
        'N' AS COMT_F,
        NULL AS COMT_S,
        NULL AS MLTI_LOAD_EFFT_D,
        NULL AS SYST_S,
        NULL AS MLTI_LOAD_COMT_S,
        NULL AS SYST_ET_Q,
        NULL AS SYST_UV_Q,
        NULL AS SYST_INS_Q,
        NULL AS SYST_UPD_Q,
        NULL AS SYST_DEL_Q,
        NULL AS SYST_ET_TABL_M,
        NULL AS SYST_UV_TABL_M,
        NULL AS SYST_HEAD_ET_TABL_M,
        NULL AS SYST_HEAD_UV_TABL_M,
        NULL AS SYST_TRLR_ET_TABL_M,
        NULL AS SYST_TRLR_UV_TABL_M,
        NULL AS PREV_PROS_KEY_I,
        NULL AS HEAD_RECD_TYPE_C,
        NULL AS HEAD_FILE_M,
        NULL AS HEAD_BTCH_RUN_D,
        NULL AS HEAD_FILE_CRAT_S,
        NULL AS HEAD_GENR_PRGM_M,
        NULL AS HEAD_BTCH_KEY_I,
        NULL AS HEAD_PROS_KEY_I,
        NULL AS HEAD_PROS_PREV_KEY_I,
        NULL AS TRLR_RECD_TYPE_C,
        NULL AS TRLR_RECD_Q,
        NULL AS TRLR_HASH_TOTL_A,
        NULL AS TRLR_COLM_HASH_TOTL_M,
        NULL AS TRLR_EROR_RECD_Q,
        NULL AS TRLR_FILE_COMT_S,
        NULL AS TRLR_RECD_ISRT_Q,
        NULL AS TRLR_RECD_UPDT_Q,
        NULL AS TRLR_RECD_DELT_Q
    FROM util_parm PARM
    CROSS JOIN calendar_dates DT
)

SELECT * FROM final