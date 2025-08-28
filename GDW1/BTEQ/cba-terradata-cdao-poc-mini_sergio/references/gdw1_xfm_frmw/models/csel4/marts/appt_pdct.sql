{%- set process_name = 'CSEL4_APPT_PDCT_TRANSFORM' -%}
{%- set stream_name = 'CSEL4_CPL_BUS_APP' -%}

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='scd_type2',
    unique_key=['appt_i'],
    database=var('target_database'),
    schema='starcadproddata',
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    tags=['stream_csel4', 'process_csel4_appt_pdct_transform', 'marts_layer', 'target_table', 'appt_pdct'],
    pre_hook=[
        "{{ validate_single_open_business_date('" ~ stream_name ~ "') }}",
        "{{ register_process_instance('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ],
    post_hook=[
        "{{ mark_process_completed('" ~ process_name ~ "', '" ~ stream_name ~ "') }}"
    ]
  )
}}

/*
    Target APPT_PDCT table - Type 2 SCD Implementation
    
    Purpose: Transform CSEL4 APPT_PDCT records to final target structure
    - Implements Type 2 SCD pattern (maintains history with effective/expiry dates)
    - Uses iceberg catalog with incremental Type 2 strategy
    - Matches DataStage transformation logic from mapping document
    
    Architecture: Validation → Intermediate Transform → Target Table (this model)
    Type 2 SCD logic handled by incremental strategy macro
*/

-- Source data from intermediate transformation
SELECT 
    -- Business Key columns (for SCD logic)
    appt_pdct_i,
    appt_i,
    
    -- Temporal columns (for SCD Type 2)
    efft_d,
    expy_d,
    
    -- Data columns
    appt_qlfy_c,
    acqr_type_c,
    acqr_adhc_x,
    acqr_srce_c,
    pdct_n,
    srce_syst_c,
    srce_syst_appt_pdct_i,
    loan_fndd_meth_c,
    new_acct_f,
    brok_paty_i,
    copy_from_othr_appt_f,
    appt_pdct_catg_c,
    ases_d,
    appt_pdct_durt_c,
    
    -- Process tracking columns
    pros_key_efft_i,
    pros_key_expy_i,
    eror_seqn_i,
    
    -- Additional columns (not populated by DataStage but exist in target)
    job_comm_catg_c,
    debt_abn_x,
    debt_busn_m,
    smpl_appt_f
    
FROM {{ ref('int_tmp_appt_pdct') }}
