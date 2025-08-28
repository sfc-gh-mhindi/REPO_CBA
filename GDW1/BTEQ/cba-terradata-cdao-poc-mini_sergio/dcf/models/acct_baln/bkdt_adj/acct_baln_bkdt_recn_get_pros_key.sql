{%- set process_name = 'CAD_X01_ACCT_BALN_BKDT_RECN' -%}
{%- set stream_name = 'ACCT_BALN_BKDT_RECN' -%}

{{
  config(
    materialized='view',
    tags=['reconciliation', 'process_tracking', 'util_pros_isac', 'stream_acct_baln_bkdt_recn'],
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
    Model: UTIL_PROS_ISAC_RECN_INSERT
    Purpose: Reconciliation process - Insert tracking records into UTIL_PROS_ISAC for account balance backdated reconciliation
    Business Logic: 
    - Captures the latest PROS_KEY from UTIL_PARM table and creates new tracking record
    - Inserts multiple records for each batch run in the event of any delays or failures
    - Tracks reconciliation process for ACCT_BALN_BKDT_RECN with GDW as source system
    - Uses calendar dates between last successful run and current batch run
    Dependencies: 
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PARM
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR
*/

WITH parm_data AS (
    SELECT PARM_LTRL_N
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PARM
    WHERE PARM_M = 'PROS_KEY'
),

batch_date_range AS (
    -- Capture last successful batch run date relating to the backdated adjustment solution
    SELECT MAX(BTCH_RUN_D) AS PREV_BTCH_RUN_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
    WHERE TRGT_M = 'ACCT_BALN_BKDT'
        AND SRCE_SYST_M = 'GDW'
        AND COMT_F = 'Y'
        AND SUCC_F = 'Y'
),

current_batch_date AS (
    -- Capture latest batch run date relating to the backdated adjustment solution
    SELECT MAX(BTCH_RUN_D) AS CURR_BTCH_RUN_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
    WHERE TRGT_M = 'ACCT_BALN_BKDT'
        AND SRCE_SYST_M = 'GDW'
),

calendar_dates AS (
    -- Get calendar dates between previous and current batch runs
    SELECT CAL.CALR_CALR_D
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR CAL
    CROSS JOIN batch_date_range BKDT_PREV
    CROSS JOIN current_batch_date BKDT_CURR
    WHERE CAL.CALR_CALR_D > BKDT_PREV.PREV_BTCH_RUN_D
        AND CAL.CALR_CALR_D <= BKDT_CURR.CURR_BTCH_RUN_D
)

SELECT 
    PARM.PARM_LTRL_N + 1 AS PROS_KEY_I,
    'CAD_X01_ACCT_BALN_BKDT_RECN' AS CONV_M,
    'TD' AS CONV_TYPE_M,
    CURRENT_TIMESTAMP(0) AS PROS_RQST_S,
    CURRENT_TIMESTAMP(0) AS PROS_LAST_RQST_S,
    1 AS PROS_RQST_Q,
    -- Inserts multiple records for each batch run in the event of any delays or failures
    DT.CALR_CALR_D AS BTCH_RUN_D,
    -- As this solution is not part of any batch [eg, SAP], the Batch Key is populated as null
    NULL AS BTCH_KEY_I,
    -- Sourcing from the GDW itself and so the source system name is GDW
    'GDW' AS SRCE_SYST_M,
    -- Applying the adjustments from ACCT_BALN on ACCT_BALN_BKDT_RECN
    'ACCT_BALN_BKDT' AS SRCE_M,
    'ACCT_BALN_BKDT_RECN' AS TRGT_M,
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
FROM parm_data PARM
CROSS JOIN calendar_dates DT