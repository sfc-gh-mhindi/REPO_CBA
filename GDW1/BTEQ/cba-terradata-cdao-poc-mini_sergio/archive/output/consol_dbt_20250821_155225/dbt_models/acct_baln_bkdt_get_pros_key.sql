{%- set process_name = 'ACCT_BALN_BKDT_GET_PROS_KEY' -%}
{%- set stream_name = 'ACCT_BALN_BKDT' -%}

{{
  config(
    materialized='table',
    database=var('cad_prod_data_db'),
    schema='util',
    tags=['pros_key', 'util', 'account_balance'],
    pre_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_GET_PROS_KEY Process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('ACCT_BALN_BKDT_GET_PROS_KEY Process ended') }}",
        "UPDATE {{ var('cad_prod_data_db') }}.util.util_parm SET parm_ltrl_n = parm_ltrl_n + 1 WHERE parm_m = 'PROS_KEY'"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_GET_PROS_KEY
    Purpose: Capture the PROS Key and update in UTIL PROS ISAC
    Business Logic: 
        - Captures the Latest Pros Key from UTIL PARM table and updates UTIL PROS ISAC
        - Inserts records for batch runs between ACCT_BALN_BKDT and ACCT_BALN_ADJ processing dates
        - Increments PROS_KEY in UTIL_PARM table via post-hook
    Dependencies: 
        - util_parm (PROS_KEY parameter)
        - util_pros_isac (process tracking)
        - grd_rprt_calr_clyr (calendar)
*/

WITH util_parm_cte AS (
    SELECT parm_ltrl_n
    FROM {{ source('vtech', 'util_parm') }}
    WHERE parm_m = 'PROS_KEY'
),

baln_max_batch AS (
    SELECT MAX(btch_run_d) AS btch_run_d 
    FROM {{ source('vtech', 'util_pros_isac') }}
    WHERE trgt_m = 'ACCT_BALN_ADJ' 
        AND srce_syst_m = 'SAP'
        AND comt_f = 'Y'  
        AND succ_f = 'Y'
),

bkdt_max_batch AS (
    SELECT MAX(btch_run_d) AS btch_run_d 
    FROM {{ source('vtech', 'util_pros_isac') }}
    WHERE trgt_m = 'ACCT_BALN_BKDT' 
        AND srce_syst_m = 'GDW'
        AND comt_f = 'Y'  
        AND succ_f = 'Y'
),

calendar_dates AS (
    SELECT cal.calr_calr_d
    FROM {{ source('vtech', 'grd_rprt_calr_clyr') }} cal
    CROSS JOIN baln_max_batch baln
    CROSS JOIN bkdt_max_batch bkdt
    WHERE cal.calr_calr_d > bkdt.btch_run_d 
        AND cal.calr_calr_d <= baln.btch_run_d
),

final AS (
    SELECT 
        parm.parm_ltrl_n + 1 AS pros_key_i,
        'CAD_X01_ACCT_BALN_BKDT' AS conv_m,
        'TD' AS conv_type_m,
        CURRENT_TIMESTAMP() AS pros_rqst_s,
        CURRENT_TIMESTAMP() AS pros_last_rqst_s,
        1 AS pros_rqst_q,
        dt.calr_calr_d AS btch_run_d,
        NULL AS btch_key_i,
        'GDW' AS srce_syst_m,
        'ACCT_BALN_BKDT_ADJ' AS srce_m,
        'ACCT_BALN_BKDT' AS trgt_m,
        'N' AS succ_f,
        'N' AS comt_f,
        NULL AS comt_s,
        NULL AS mlti_load_efft_d,
        NULL AS syst_s,
        NULL AS mlti_load_comt_s,
        NULL AS syst_et_q,
        NULL AS syst_uv_q,
        NULL AS syst_ins_q,
        NULL AS syst_upd_q,
        NULL AS syst_del_q,
        NULL AS syst_et_tabl_m,
        NULL AS syst_uv_tabl_m,
        NULL AS syst_head_et_tabl_m,
        NULL AS syst_head_uv_tabl_m,
        NULL AS syst_trlr_et_tabl_m,
        NULL AS syst_trlr_uv_tabl_m,
        NULL AS prev_pros_key_i,
        NULL AS head_recd_type_c,
        NULL AS head_file_m,
        NULL AS head_btch_run_d,
        NULL AS head_file_crat_s,
        NULL AS head_genr_prgm_m,
        NULL AS head_btch_key_i,
        NULL AS head_pros_key_i,
        NULL AS head_pros_prev_key_i,
        NULL AS trlr_recd_type_c,
        NULL AS trlr_recd_q,
        NULL AS trlr_hash_totl_a,
        NULL AS trlr_colm_hash_totl_m,
        NULL AS trlr_eror_recd_q,
        NULL AS trlr_file_comt_s,
        NULL AS trlr_recd_isrt_q,
        NULL AS trlr_recd_updt_q,
        NULL AS trlr_recd_delt_q
    FROM util_parm_cte parm
    CROSS JOIN calendar_dates dt
)

SELECT * FROM final