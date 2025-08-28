{%- set process_name = 'ACCT_BALN_BKDT_AUDT_GET_PROS_KEY' -%}
{%- set stream_name = 'ACCT_BALN_BKDT' -%}

{{
  config(
    materialized='view',
    tags=['account_balance', 'backdated_adjustment', 'audit', 'process_key', 'stream_acct_baln_bkdt'],
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
    Model: ACCT_BALN_BKDT_AUDT_GET_PROS_KEY
    Purpose: Populate the AUDT table for future reference as the records are going to be deleted from ACCT BALN BKDT and this acts as a driver for the ADJ RULE view
    
    Business Logic: 
    - Creates a view with audit records for each batch run date between last successful run and current run
    - Post-hook automatically inserts view data into UTIL_PROS_ISAC target table
    - Increments PROS_KEY counter in UTIL_PARM for tracking purposes
    - Handles multiple records for each batch run in the event of any delays or failures
    - Provides comprehensive DCF logging and validation
    
    Execution Flow:
    1. View materialization: Captures latest PROS_KEY and generates audit records (44 columns to match target table)
    2. Post-hook 1: Insert audit data from view into UTIL_PROS_ISAC table
    3. Post-hook 2: Increment PROS_KEY counter in UTIL_PARM table  
    4. Post-hook 3: Log successful completion to DCF framework
    
    Column Alignment: View excludes HEAD_GENR_PRGM_M and TRLR_RECD_ISRT_Q to match UTIL_PROS_ISAC table structure
    
    Dependencies: 
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PARM (source for PROS_KEY)
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC (target for audit data)
    - {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR (calendar data)
    - {{ var('cld_db') }}.{{ var('cadproddata') }}.UTIL_PROS_ISAC (target table)
    - DCF framework macros (logging, audit data loading)
*/

WITH pros_key_data AS (
    SELECT 
        PARM_LTRL_N + 1 AS NEXT_PROS_KEY
    FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PARM
    WHERE PARM_M = 'PROS_KEY'
),

batch_date_range AS (
    SELECT  
        CAL.CALR_CALR_D
    FROM
        -- Capture last Successful Batch run date relating to the Backdated adjustment solution
        (
            SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D 
            FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
                AND SRCE_SYST_M = 'GDW'
                AND COMT_F = 'Y'  
                AND SUCC_F = 'Y'
        ) BKDT_PREV
    CROSS JOIN 
        -- Capture latest Batch run date relating to the Backdated adjustment solution into ACCT_BALN_BKDT
        (
            SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D 
            FROM {{ var('vtech_db') }}.{{ var('vtech_sch') }}.UTIL_PROS_ISAC
            WHERE TRGT_M = 'ACCT_BALN_BKDT' 
                AND SRCE_SYST_M = 'GDW'
        ) BKDT_CURR
    CROSS JOIN {{ var('vtech_db') }}.{{ var('vtech_sch') }}.GRD_RPRT_CALR_CLYR CAL
    WHERE CAL.CALR_CALR_D > BKDT_PREV.BTCH_RUN_D 
        AND CAL.CALR_CALR_D <= BKDT_CURR.BTCH_RUN_D
)

SELECT 
    PK.NEXT_PROS_KEY AS PROS_KEY_I,
    'CAD_X01_ACCT_BALN_BKDT_AUDT' AS CONV_M,
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
    -- Applying the adjustments from ACCT_BALN on ACCT_BALN_BKDT _AUDT
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
FROM pros_key_data PK
CROSS JOIN batch_date_range DT