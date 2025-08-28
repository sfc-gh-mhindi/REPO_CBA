/*
    CSEL4 Data Validation Model - Direct to XFM_ERR_DTL Table
    
    Purpose: Validate CSEL4 data and INSERT error records into XFM_ERR_DTL
    - Reads and validates key fields: PL_APP_ID, NOMINATED_BRANCH_ID, MOD_TIMESTAMP
    - Validates field formats, nullability, and business rules
    - Inserts error records directly into DCF XFM_ERR_DTL table
    - Only error records are materialized (WHERE has_error = TRUE)
    - Uses incremental append strategy with pre-hook cleanup
    
    Equivalent to DataStage validation logic with centralized error handling.
*/

{%- set process_name = 'CSEL4_VALIDATION' -%}
{%- set stream_name = 'CSEL4_CPL_BUS_APP' -%}

{{
  config(
    materialized='view',
    pre_hook=[
        "{{ validate_single_open_business_date('" ~ stream_name ~ "') }}",
        "{{ validate_header_trailer('" ~ stream_name ~ "', '" ~ var('staging_database') ~ "." ~ var('staging_schema') ~ ".cse_cpl_bus_app') }}",
        "{{ register_process_instance('" ~ process_name ~ "', '" ~ stream_name ~ "') }}",
        "{{ err_tbl_reset('" ~ stream_name ~ "', '" ~ process_name ~ "') }}"
    ],
    post_hook=[
        "{{ load_errors_to_central_table(this, '" ~ stream_name ~ "', '" ~ process_name ~ "') }}",
        "{{ check_error_and_end_prcs('" ~ stream_name ~ "', '" ~ process_name ~ "') }}"
    ],
    tags=['stream_csel4', 'process_csel4_validation', 'intermediate_layer', 'data_validation', 'error_detection']
  )
}}

WITH source_validation AS (
    SELECT 
        -- Primary key components for error tracking
        pl_app_id,
        ROW_NUMBER() OVER (ORDER BY pl_app_id) as source_row_num,
        
        -- Get business date for processing
        (SELECT BUS_DT FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
         WHERE STRM_NAME = '{{ stream_name }}' AND PROCESSING_FLAG = 1 
         LIMIT 1) as business_date,
        
        -- Get process instance
        (SELECT PRCS_INST_ID FROM {{ dcf_database_ref() }}.DCF_T_PRCS_INST 
         WHERE STRM_NAME = '{{ stream_name }}' AND PRCS_NAME = '{{ process_name }}' 
         ORDER BY PRCS_START_TS DESC LIMIT 1) as batch_id,
        
        -- Field validations - return TRUE for errors (opposite of BCFINSG)
        CASE 
            WHEN pl_app_id IS NULL THEN TRUE
            WHEN TRIM(pl_app_id) = '' THEN TRUE
            WHEN NOT REGEXP_LIKE(TRIM(pl_app_id), '^[0-9]+$') THEN TRUE
            WHEN LENGTH(TRIM(pl_app_id)) > 18 THEN TRUE
            ELSE FALSE
        END as pl_app_id_error,
        
        CASE 
            WHEN nominated_branch_id IS NULL THEN TRUE
            WHEN TRIM(nominated_branch_id) = '' THEN TRUE
            ELSE FALSE
        END as nominated_branch_id_error,
        
        CASE 
            WHEN mod_timestamp IS NULL THEN TRUE
            WHEN TRIM(mod_timestamp) = '' THEN TRUE
            WHEN NOT {{ dcf_database_ref() }}.fn_is_valid_dt(mod_timestamp) THEN TRUE
            ELSE FALSE
        END as mod_timestamp_error,
        
        -- Store original values for error reporting
        pl_app_id as orig_pl_app_id,
        nominated_branch_id as orig_nominated_branch_id,
        mod_timestamp as orig_mod_timestamp
        
    FROM {{ var('staging_database') }}.{{ var('staging_schema') }}.cse_cpl_bus_app
),

error_details AS (
    SELECT 
        pl_app_id,
        source_row_num,
        business_date,
        batch_id,
        
        -- Individual field errors
        pl_app_id_error,
        nominated_branch_id_error,
        mod_timestamp_error,
        
        -- Build primary key for error tracking
        COALESCE(TRIM(pl_app_id), 'NULL_PL_APP_ID') as rec_pk,
        
        -- Check if record has any errors
        (pl_app_id_error OR nominated_branch_id_error OR mod_timestamp_error) as has_error,
        
        -- Build error details JSON
        OBJECT_CONSTRUCT(
            'pl_app_id_error', pl_app_id_error,
            'nominated_branch_id_error', nominated_branch_id_error,
            'mod_timestamp_error', mod_timestamp_error,
            'orig_pl_app_id', orig_pl_app_id,
            'orig_nominated_branch_id', orig_nominated_branch_id,
            'orig_mod_timestamp', orig_mod_timestamp,
            'validation_timestamp', CURRENT_TIMESTAMP()
        ) as error_details_json
        
    FROM source_validation
)

-- Final output - only error records, formatted for XFM_ERR_DTL table
SELECT 
    '{{ stream_name }}' as STRM_NM,
    '{{ process_name }}' as PRCS_NM,
    business_date as PRCS_DT,
    rec_pk::VARCHAR(100) as SRCE_KEY_NM,
    'CSE_CPL_BUS_APP_DATA_FILE' as SRCE_FILE_NM,
    source_row_num::NUMBER(10,0) as SRCE_ROW_NUM,
    PARSE_JSON(TO_JSON(error_details_json)) as ERR_DTLS_JSON,
    batch_id as PRCS_INST_ID,
    CURRENT_TIMESTAMP() as REC_INS_TS,
    CURRENT_USER() as INS_USR_NM
FROM error_details
WHERE has_error = TRUE  -- ONLY ERROR RECORDS