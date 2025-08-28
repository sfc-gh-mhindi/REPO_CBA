{%- set process_name = 'CSEL4_DEPT_APPT_TRANSFORM' -%}
{%- set stream_name = 'CSEL4_CPL_BUS_APP' -%}

{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='scd_type2',
    unique_key=['dept_i', 'appt_i'],
    database=var('target_database'),
    schema='starcadproddata',
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    tags=['stream_csel4', 'process_csel4_dept_appt_transform', 'marts_layer', 'target_table', 'dept_appt'],
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
    Target DEPT_APPT table - Type 2 SCD Implementation
    
    Purpose: Transform CSEL4 DEPT_APPT records to final target structure
    - Implements Type 2 SCD pattern (maintains history with effective/expiry dates)
    - Uses iceberg catalog with incremental Type 2 strategy
    - Matches DataStage transformation logic from mapping document
    
    Architecture: Validation → Intermediate Transform → Target Table (this model)
    Type 2 SCD logic handled by incremental strategy macro
    
    Note: DEPT_APPT is a relationship table between appointments and departments
*/

-- Source data from intermediate transformation
SELECT 
    -- Business Key columns (for SCD logic)
    dept_i,
    appt_i,
    dept_role_c,
    
    -- Temporal columns (for SCD Type 2)
    efft_d,
    expy_d,
    
    -- Process tracking columns
    pros_key_efft_i,
    pros_key_expy_i,
    eror_seqn_i,
    
    -- Additional columns (not populated by DataStage but exist in target)
    brch_n,
    srce_syst_c,
    chng_reas_c
    
FROM {{ ref('int_tmp_dept_appt') }}
