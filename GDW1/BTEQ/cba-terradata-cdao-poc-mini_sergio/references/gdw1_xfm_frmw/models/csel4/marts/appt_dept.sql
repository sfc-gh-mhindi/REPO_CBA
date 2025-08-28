{%- set process_name = 'CSEL4_APPT_DEPT_TRANSFORM' -%}
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
    tags=['stream_csel4', 'process_csel4_appt_dept_transform', 'marts_layer', 'target_table', 'appt_dept'],
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
    Target APPT_DEPT table - Type 2 SCD Implementation
    
    Purpose: Transform CSEL4 APPT_DEPT records to final target structure
    - Implements Type 2 SCD pattern (maintains history with effective/expiry dates)
    - Uses iceberg catalog with incremental Type 2 strategy
    - Matches DataStage transformation logic from mapping document
    
    Architecture: Validation → Intermediate Transform → Target Table (this model)
    Type 2 SCD logic handled by incremental strategy macro
*/

-- Source data from intermediate transformation
SELECT 
    -- Business Key columns (for SCD logic)
    appt_i,
    dept_role_c,
    
    -- Temporal columns (for SCD Type 2)
    efft_d,
    expy_d,
    
    -- Data columns
    dept_i,
    
    -- Process tracking columns
    pros_key_efft_i,
    pros_key_expy_i,
    eror_seqn_i,
    brch_n,
    srce_syst_c,
    chng_reas_c
    
FROM {{ ref('int_tmp_appt_dept') }}
