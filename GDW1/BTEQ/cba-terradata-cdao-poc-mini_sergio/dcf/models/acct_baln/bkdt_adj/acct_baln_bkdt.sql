{%- set process_name = 'ACCT_BALN_BKDT_ISRT' -%}
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
    tags=['account_balance', 'backdated_adjustment', 'core_transform', 'sap_source', 'stream_acct_baln_bkdt'],
    pre_hook=[
        "{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    Model: ACCT_BALN_BKDT_ISRT
    Purpose: Insert account balance backdated records from staging table to production table
    Business Logic: 
    - Direct insert of processed backdated account balance records
    - Transfers all columns from staging to production table
    - Maintains data integrity for backdated balance adjustments
    Dependencies: 
    - {{ var('ddstg_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2
*/

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
    PROS_KEY_EXPY_I,               
    BKDT_PROS_KEY_I,
    null CNCY_C,
    null ROW_SECU_ACCS_C
FROM {{ var('cld_db') }}.{{ var('ddstg_sch') }}.ACCT_BALN_BKDT_STG2