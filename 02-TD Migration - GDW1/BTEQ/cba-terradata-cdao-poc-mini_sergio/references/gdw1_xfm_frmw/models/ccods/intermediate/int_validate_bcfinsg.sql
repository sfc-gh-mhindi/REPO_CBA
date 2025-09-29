{%- set process_name = 'BCFINSG_VALIDATION' -%}
{%- set stream_name = 'BCFINSG' -%}

{{
  config(
    materialized='view',
    pre_hook=[
        "{{ validate_single_open_business_date('" ~ stream_name ~ "') }}",
        "{{ validate_header('" ~ stream_name ~ "') }}",
        "{{ register_process_instance('" ~ process_name ~ "', '" ~ stream_name ~ "') }}",
        "{{ err_tbl_reset('" ~ stream_name ~ "', '" ~ process_name ~ "') }}"
    ],
    post_hook=[
        "{{ load_errors_to_central_table(this, '" ~ stream_name ~ "', '" ~ process_name ~ "') }}",
        "{{ check_error_and_end_prcs('" ~ stream_name ~ "', '" ~ process_name ~ "') }}"
    ],
    tags=['stream_bcfinsg', 'process_bcfinsg_validation', 'intermediate_layer', 'data_validation', 'error_detection']
  )
}}

/*
    BCFINSG Data Validation Model - Direct to XFM_ERR_DTL Table
    
    Purpose: Validate BCFINSG data and INSERT error records into XFM_ERR_DTL
    - Reads only required columns: Primary key + 17 date fields for validation
    - Validates using fn_is_valid_dt UDF (same as DataStage)  
    - Inserts error records directly into DCF XFM_ERR_DTL table
    - Only error records are materialized (WHERE has_error = TRUE)
    - Uses incremental append strategy with pre-hook cleanup
    
    Equivalent to DataStage validation logic with centralized error handling.
*/

WITH source_validation AS (
    SELECT 
        -- Primary key components for error tracking
        BCF_CORP as corp_id,
        BCF_ACCOUNT_NO1 as account_number, 
        BCF_PLAN_ID as plan_id,
        
        -- Only the 17 date fields that need validation (from actual source table)
        BCF_DT_SPEC_TERMS_END, BCF_DT_INTEREST_DEFER, BCF_DT_PAYMENT_DEFER, BCF_DT_FIRST_TRANS,
        BCF_DT_PAID_OFF, BCF_DT_LAST_PAYMENT, BCF_DT_LAST_MAINT, BCF_PLAN_DUE_DATE,
        BCF_DATE_END_INTEREST_FREE, BCF_DT_MIGRATE, BCF_FL_LAST_INSTALLMENT_DT, BCF_SCHED_PAYOFF_DT,
        BCF_ACTUAL_PAYOFF_DT, BCF_DT_INSTALL_TERM_CHG, BCF_DT_INSTALL_PAID, BCF_PROJECTED_PAYOFF_DT,
        BCF_DISPUTE_OLD_DATE,
        
        -- Date validation for all 17 fields using DataStage UDF
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_SPEC_TERMS_END) as dt_spec_terms_end_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_INTEREST_DEFER) as dt_interest_defer_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_PAYMENT_DEFER) as dt_payment_defer_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_FIRST_TRANS) as dt_first_trans_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_PAID_OFF) as dt_paid_off_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_LAST_PAYMENT) as dt_last_payment_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_LAST_MAINT) as dt_last_maint_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_PLAN_DUE_DATE) as plan_due_date_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DATE_END_INTEREST_FREE) as dt_end_interest_free_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_MIGRATE) as dt_migrate_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_FL_LAST_INSTALLMENT_DT) as fl_last_installment_dt_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_SCHED_PAYOFF_DT) as sched_payoff_dt_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_ACTUAL_PAYOFF_DT) as actual_payoff_dt_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_INSTALL_TERM_CHG) as dt_install_term_chg_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_INSTALL_PAID) as dt_install_paid_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_PROJECTED_PAYOFF_DT) as projected_payoff_dt_error,
        NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DISPUTE_OLD_DATE) as dispute_old_date_error,
        
        -- Create JSON primary key for error tracking
        OBJECT_CONSTRUCT(
            'CORP_IDNN', TRIM(BCF_CORP),
            'ACCT_I', TRIM(BCF_ACCOUNT_NO1),
            'PLAN_IDNN', TRIM(BCF_PLAN_ID)
        )::VARIANT as rec_pk,
        
        -- Processing metadata
        '{{ var("ods_batch_id", "BATCH_" ~ run_started_at.strftime("%Y%m%d_%H%M%S")) }}' as batch_id,
        
        -- Row numbering for error table
        ROW_NUMBER() OVER (ORDER BY BCF_CORP, BCF_ACCOUNT_NO1, BCF_PLAN_ID) as source_row_num,
        
        -- Business date from DCF framework
        bd.business_date
        
    FROM {{ var('staging_database') }}.{{ var('staging_schema') }}.bcfinsg s
    CROSS JOIN (
        SELECT BUS_DT as business_date 
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
        WHERE STRM_NAME = '{{ stream_name }}' 
          AND PROCESSING_FLAG = 1
        LIMIT 1
    ) bd
),

error_details AS (
    SELECT *,
        -- Check if record has ANY date errors (all 17 fields)
        (dt_spec_terms_end_error OR dt_interest_defer_error OR dt_payment_defer_error OR 
         dt_first_trans_error OR dt_paid_off_error OR dt_last_payment_error OR 
         dt_last_maint_error OR plan_due_date_error OR dt_end_interest_free_error OR
         dt_migrate_error OR fl_last_installment_dt_error OR sched_payoff_dt_error OR
         actual_payoff_dt_error OR dt_install_term_chg_error OR dt_install_paid_error OR
         projected_payoff_dt_error OR dispute_old_date_error) as has_error,
        
        -- Create JSON error details array (only include errors that occurred)
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN dt_spec_terms_end_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_SPEC_TERMS_END', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_SPEC_TERMS_END::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_interest_defer_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_INTEREST_DEFER', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_INTEREST_DEFER::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_payment_defer_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_PAYMENT_DEFER', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_PAYMENT_DEFER::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_first_trans_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_FIRST_TRANS', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_FIRST_TRANS::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_paid_off_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_PAID_OFF', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_PAID_OFF::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_last_payment_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_LAST_PAYMENT', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_LAST_PAYMENT::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_last_maint_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_LAST_MAINT', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_LAST_MAINT::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN plan_due_date_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_PLAN_DUE_DATE', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_PLAN_DUE_DATE::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_end_interest_free_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DATE_END_INTEREST_FREE', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DATE_END_INTEREST_FREE::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_migrate_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_MIGRATE', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_MIGRATE::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN fl_last_installment_dt_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_FL_LAST_INSTALLMENT_DT', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_FL_LAST_INSTALLMENT_DT::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN sched_payoff_dt_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_SCHED_PAYOFF_DT', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_SCHED_PAYOFF_DT::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN actual_payoff_dt_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_ACTUAL_PAYOFF_DT', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_ACTUAL_PAYOFF_DT::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_install_term_chg_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_INSTALL_TERM_CHG', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_INSTALL_TERM_CHG::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dt_install_paid_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DT_INSTALL_PAID', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DT_INSTALL_PAID::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN projected_payoff_dt_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_PROJECTED_PAYOFF_DT', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_PROJECTED_PAYOFF_DT::VARCHAR,
                'validation_rule', 'date_conversion_validation') END,
            CASE WHEN dispute_old_date_error THEN OBJECT_CONSTRUCT(
                'column_name', 'BCF_DISPUTE_OLD_DATE', 'error_type', 'DATE_FORMAT_ERROR',
                'error_message', 'Invalid record due to date', 'original_value', BCF_DISPUTE_OLD_DATE::VARCHAR,
                'validation_rule', 'date_conversion_validation') END
        ) as error_details_json
        
    FROM source_validation
)

-- Output ONLY error records in XFM_ERR_DTL structure
SELECT 
    '{{ stream_name }}' as STRM_NM,
    '{{ process_name }}' as PRCS_NM,
    business_date as PRCS_DT,
    rec_pk::VARCHAR(100) as SRCE_KEY_NM,
    'BCFINSG_DATA_FILE' as SRCE_FILE_NM,
    source_row_num::NUMBER(10,0) as SRCE_ROW_NUM,
    PARSE_JSON(TO_JSON(error_details_json)) as ERR_DTLS_JSON,
    batch_id as PRCS_INST_ID,
    CURRENT_TIMESTAMP() as REC_INS_TS,
    CURRENT_USER() as INS_USR_NM

FROM error_details
WHERE (dt_spec_terms_end_error OR dt_interest_defer_error OR dt_payment_defer_error OR 
       dt_first_trans_error OR dt_paid_off_error OR dt_last_payment_error OR 
       dt_last_maint_error OR plan_due_date_error OR dt_end_interest_free_error OR
       dt_migrate_error OR fl_last_installment_dt_error OR sched_payoff_dt_error OR
       actual_payoff_dt_error OR dt_install_term_chg_error OR dt_install_paid_error OR
       projected_payoff_dt_error OR dispute_old_date_error) = TRUE  -- ONLY ERROR RECORDS