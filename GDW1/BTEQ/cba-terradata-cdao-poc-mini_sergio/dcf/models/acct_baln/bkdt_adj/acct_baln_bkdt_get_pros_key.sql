{%- set process_name = 'CAD_X01_ACCT_BALN_BKDT' -%}
{%- set stream_name = 'UTIL_PROS_ISAC' -%}

{{
  config(
    materialized='view',
    tags=['util_pros_isac', 'pros_key_management', 'core_transform', 'gdw_source', 'stream_util_pros_isac'],
    pre_hook=[
        "{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "INSERT INTO {{ var('cld_db') }}.{{ var('cadproddata') }}.UTIL_PROS_ISAC SELECT * FROM {{ this }}",
        "UPDATE {{ var('cld_db') }}.{{ var('cadproddata') }}.UTIL_PARM SET PARM_LTRL_N = PARM_LTRL_N + 1 WHERE PARM_M='PROS_KEY'",
        "{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    Model: UTIL_PROS_ISAC_GET_PROS_KEY
    Purpose: Capture the PROS Key and update in UTIL PROS ISAC for CAD_X01_ACCT_BALN_BKDT conversion
    Business Logic: 
    - Captures the latest PROS Key from UTIL_PARM table and inserts into UTIL_PROS_ISAC
    - Creates records for batch runs between latest ACCT_BALN_BKDT and ACCT_BALN_ADJ processing dates
    - Handles conversion tracking for CAD_X01_ACCT_BALN_BKDT process
    - Increments PROS_KEY in UTIL_PARM table via post_hook
    Dependencies: 
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PARM
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR
*/

WITH latest_batch_dates AS (
    SELECT 
        MAX(CASE WHEN TRGT_M='ACCT_BALN_ADJ' 
                      AND SRCE_SYST_M='SAP'
                      AND COMT_F = 'Y'  
                      AND SUCC_F='Y' 
                 THEN BTCH_RUN_D END) AS BALN_BTCH_RUN_D,
        MAX(CASE WHEN TRGT_M='ACCT_BALN_BKDT' 
                      AND SRCE_SYST_M='GDW'
                      AND COMT_F = 'Y'  
                      AND SUCC_F='Y' 
                 THEN BTCH_RUN_D END) AS BKDT_BTCH_RUN_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
),

batch_run_dates AS (
    SELECT  
        CAL.CALR_CALR_D
    FROM latest_batch_dates LBD
    CROSS JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR CAL
    WHERE CAL.CALR_CALR_D > LBD.BKDT_BTCH_RUN_D 
        AND CAL.CALR_CALR_D <= LBD.BALN_BTCH_RUN_D
),

pros_key_data AS (
    SELECT 
        PARM_LTRL_N + 1 AS PROS_KEY_I
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PARM
    WHERE PARM_M = 'PROS_KEY'
)

SELECT 
    PKD.PROS_KEY_I,
    'CAD_X01_ACCT_BALN_BKDT' AS CONV_M,
    'TD' AS CONV_TYPE_M,
    CURRENT_TIMESTAMP(0) AS PROS_RQST_S,
    CURRENT_TIMESTAMP(0) AS PROS_LAST_RQST_S,
    1 AS PROS_RQST_Q,
    BRD.CALR_CALR_D AS BTCH_RUN_D,
    NULL AS BTCH_KEY_I,
    'GDW' AS SRCE_SYST_M,
    'ACCT_BALN_BKDT_ADJ' AS SRCE_M,
    'ACCT_BALN_BKDT' AS TRGT_M,
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
    NULL AS HEAD_BTCH_KEY_I,
    NULL AS HEAD_PROS_KEY_I,
    NULL AS HEAD_PROS_PREV_KEY_I,
    NULL AS TRLR_RECD_TYPE_C,
    NULL AS TRLR_RECD_Q,
    NULL AS TRLR_HASH_TOTL_A,
    NULL AS TRLR_COLM_HASH_TOTL_M,
    NULL AS TRLR_EROR_RECD_Q,
    NULL AS TRLR_FILE_COMT_S,
    NULL AS TRLR_RECD_UPDT_Q,
    NULL AS TRLR_RECD_DELT_Q
FROM pros_key_data PKD
CROSS JOIN batch_run_dates BRD