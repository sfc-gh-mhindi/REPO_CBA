/*
    DCF Audit Data Loader Macros
    
    Macros for loading audit data from DBT views into target tables
    with comprehensive DCF framework integration including process logging,
    error handling, and transaction management.
*/

{%- macro load_audit_data_simple(view_table, target_table) -%}
    /*
        Simplified Load Audit Data - Post-hook safe version
        
        Parameters:
        - view_table: Fully qualified view/table name 
        - target_table: Fully qualified target table name 
        
        This macro performs a direct INSERT operation without complex validation
        that can cause recursion issues in post-hook context.
    */
    
    INSERT INTO {{ target_table }} (
        PROS_KEY_I,
        CONV_M,
        CONV_TYPE_M,
        PROS_RQST_S,
        PROS_LAST_RQST_S,
        PROS_RQST_Q,
        BTCH_RUN_D,
        BTCH_KEY_I,
        SRCE_SYST_M,
        SRCE_M,
        TRGT_M,
        SUCC_F,
        COMT_F,
        COMT_S,
        MLTI_LOAD_EFFT_D,
        SYST_S,
        MLTI_LOAD_COMT_S,
        SYST_ET_Q,
        SYST_UV_Q,
        SYST_INS_Q,
        SYST_UPD_Q,
        SYST_DEL_Q,
        SYST_ET_TABL_M,
        SYST_UV_TABL_M,
        SYST_HEAD_ET_TABL_M,
        SYST_HEAD_UV_TABL_M,
        SYST_TRLR_ET_TABL_M,
        SYST_TRLR_UV_TABL_M,
        PREV_PROS_KEY_I,
        HEAD_RECD_TYPE_C,
        HEAD_FILE_M,
        HEAD_BTCH_RUN_D,
        HEAD_FILE_CRAT_S,
        HEAD_GENR_PRGM_M,
        HEAD_BTCH_KEY_I,
        HEAD_PROS_KEY_I,
        HEAD_PROS_PREV_KEY_I,
        TRLR_RECD_TYPE_C,
        TRLR_RECD_Q,
        TRLR_HASH_TOTL_A,
        TRLR_COLM_HASH_TOTL_M,
        TRLR_EROR_RECD_Q,
        TRLR_FILE_COMT_S,
        TRLR_RECD_ISRT_Q,
        TRLR_RECD_UPDT_Q,
        TRLR_RECD_DELT_Q
    )
    SELECT 
        PROS_KEY_I,
        CONV_M,
        CONV_TYPE_M,
        PROS_RQST_S,
        PROS_LAST_RQST_S,
        PROS_RQST_Q,
        BTCH_RUN_D,
        BTCH_KEY_I,
        SRCE_SYST_M,
        SRCE_M,
        TRGT_M,
        SUCC_F,
        COMT_F,
        COMT_S,
        MLTI_LOAD_EFFT_D,
        SYST_S,
        MLTI_LOAD_COMT_S,
        SYST_ET_Q,
        SYST_UV_Q,
        SYST_INS_Q,
        SYST_UPD_Q,
        SYST_DEL_Q,
        SYST_ET_TABL_M,
        SYST_UV_TABL_M,
        SYST_HEAD_ET_TABL_M,
        SYST_HEAD_UV_TABL_M,
        SYST_TRLR_ET_TABL_M,
        SYST_TRLR_UV_TABL_M,
        PREV_PROS_KEY_I,
        HEAD_RECD_TYPE_C,
        HEAD_FILE_M,
        HEAD_BTCH_RUN_D,
        HEAD_FILE_CRAT_S,
        HEAD_GENR_PRGM_M,
        HEAD_BTCH_KEY_I,
        HEAD_PROS_KEY_I,
        HEAD_PROS_PREV_KEY_I,
        TRLR_RECD_TYPE_C,
        TRLR_RECD_Q,
        TRLR_HASH_TOTL_A,
        TRLR_COLM_HASH_TOTL_M,
        TRLR_EROR_RECD_Q,
        TRLR_FILE_COMT_S,
        TRLR_RECD_ISRT_Q,
        TRLR_RECD_UPDT_Q,
        TRLR_RECD_DELT_Q
    FROM {{ view_table }}
{%- endmacro -%}

{%- macro load_audit_data_from_view(view_ref, target_table, process_name, stream_name) -%}
    /*
        Load Audit Data from View to Target Table
        
        Parameters:
        - view_ref: DBT reference to the source view
        - target_table: Fully qualified target table name 
        - process_name: Name of the process for logging
        - stream_name: Stream name for DCF tracking
        
        Returns: Number of records inserted
        
        Usage:
        {{ load_audit_data_from_view(
            ref('acct_baln_bkdt_audt_get_pros_key'),
            var('cld_db') ~ '.' ~ var('cadproddata') ~ '.UTIL_PROS_ISAC',
            'ACCT_BALN_BKDT_AUDT_GET_PROS_KEY',
            'ACCT_BALN_BKDT'
        ) }}
    */
    
    {%- set log_msg = "Loading audit data from view to target table: " ~ target_table -%}
    {{ log(log_msg) }}
    
    {% if execute %}
        {# Register process start #}
        {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Starting audit data load from view: ' ~ view_ref) }}
        
        {# Get record count from source view #}
        {% set count_query %}
            SELECT COUNT(*) as record_count 
            FROM {{ view_ref }}
        {% endset %}
        
        {% set source_count_result = run_query(count_query) %}
        {% set source_record_count = source_count_result[0][0] %}
        
        {{ log("Source view contains " ~ source_record_count ~ " records") }}
        
        {# Perform the insert operation #}
        {% set insert_sql %}
            INSERT INTO {{ target_table }} (
                PROS_KEY_I,
                CONV_M,
                CONV_TYPE_M,
                PROS_RQST_S,
                PROS_LAST_RQST_S,
                PROS_RQST_Q,
                BTCH_RUN_D,
                BTCH_KEY_I,
                SRCE_SYST_M,
                SRCE_M,
                TRGT_M,
                SUCC_F,
                COMT_F,
                COMT_S,
                MLTI_LOAD_EFFT_D,
                SYST_S,
                MLTI_LOAD_COMT_S,
                SYST_ET_Q,
                SYST_UV_Q,
                SYST_INS_Q,
                SYST_UPD_Q,
                SYST_DEL_Q,
                SYST_ET_TABL_M,
                SYST_UV_TABL_M,
                SYST_HEAD_ET_TABL_M,
                SYST_HEAD_UV_TABL_M,
                SYST_TRLR_ET_TABL_M,
                SYST_TRLR_UV_TABL_M,
                PREV_PROS_KEY_I,
                HEAD_RECD_TYPE_C,
                HEAD_FILE_M,
                HEAD_BTCH_RUN_D,
                HEAD_FILE_CRAT_S,
                HEAD_GENR_PRGM_M,
                HEAD_BTCH_KEY_I,
                HEAD_PROS_KEY_I,
                HEAD_PROS_PREV_KEY_I,
                TRLR_RECD_TYPE_C,
                TRLR_RECD_Q,
                TRLR_HASH_TOTL_A,
                TRLR_COLM_HASH_TOTL_M,
                TRLR_EROR_RECD_Q,
                TRLR_FILE_COMT_S,
                TRLR_RECD_ISRT_Q,
                TRLR_RECD_UPDT_Q,
                TRLR_RECD_DELT_Q
            )
            SELECT 
                PROS_KEY_I,
                CONV_M,
                CONV_TYPE_M,
                PROS_RQST_S,
                PROS_LAST_RQST_S,
                PROS_RQST_Q,
                BTCH_RUN_D,
                BTCH_KEY_I,
                SRCE_SYST_M,
                SRCE_M,
                TRGT_M,
                SUCC_F,
                COMT_F,
                COMT_S,
                MLTI_LOAD_EFFT_D,
                SYST_S,
                MLTI_LOAD_COMT_S,
                SYST_ET_Q,
                SYST_UV_Q,
                SYST_INS_Q,
                SYST_UPD_Q,
                SYST_DEL_Q,
                SYST_ET_TABL_M,
                SYST_UV_TABL_M,
                SYST_HEAD_ET_TABL_M,
                SYST_HEAD_UV_TABL_M,
                SYST_TRLR_ET_TABL_M,
                SYST_TRLR_UV_TABL_M,
                PREV_PROS_KEY_I,
                HEAD_RECD_TYPE_C,
                HEAD_FILE_M,
                HEAD_BTCH_RUN_D,
                HEAD_FILE_CRAT_S,
                HEAD_GENR_PRGM_M,
                HEAD_BTCH_KEY_I,
                HEAD_PROS_KEY_I,
                HEAD_PROS_PREV_KEY_I,
                TRLR_RECD_TYPE_C,
                TRLR_RECD_Q,
                TRLR_HASH_TOTL_A,
                TRLR_COLM_HASH_TOTL_M,
                TRLR_EROR_RECD_Q,
                TRLR_FILE_COMT_S,
                TRLR_RECD_ISRT_Q,
                TRLR_RECD_UPDT_Q,
                TRLR_RECD_DELT_Q
            FROM {{ view_ref }}
        {% endset %}
        
        {# Execute the insert #}
        {% do run_query(insert_sql) %}
        
        {# Verify the insert by checking target table count #}
        {% set verify_query %}
            SELECT COUNT(*) as inserted_count
            FROM {{ target_table }}
            WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT'
              AND TRGT_M = 'ACCT_BALN_BKDT_AUDT'
              AND PROS_RQST_S >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
        {% endset %}
        
        {% set verify_result = run_query(verify_query) %}
        {% set inserted_count = verify_result[0][0] %}
        
        {# Log completion #}
        {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Successfully inserted ' ~ inserted_count ~ ' records from view to ' ~ target_table) }}
        
        {% if inserted_count != source_record_count %}
            {{ log_dcf_exec_msg(process_name, stream_name, 30, 'Warning: Source count (' ~ source_record_count ~ ') does not match inserted count (' ~ inserted_count ~ ')') }}
        {% endif %}
        
        {{ log("✅ Audit data load completed: " ~ inserted_count ~ " records inserted into " ~ target_table) }}
        
        {{ return(inserted_count) }}
    {% else %}
        {{ return(0) }}
    {% endif %}
{%- endmacro -%}

{%- macro load_acct_baln_bkdt_audt_pros_key_data() -%}
    /*
        Specific macro for loading ACCT_BALN_BKDT audit PROS_KEY data
        
        This macro encapsulates the specific logic for loading audit data
        from the acct_baln_bkdt_audt_get_pros_key view into UTIL_PROS_ISAC table.
        
        Usage: 
        {{ load_acct_baln_bkdt_audt_pros_key_data() }}
        
        Or as a run-operation:
        dbt run-operation load_acct_baln_bkdt_audt_pros_key_data
    */
    
    {%- set process_name = 'ACCT_BALN_BKDT_AUDT_PROS_KEY_LOAD' -%}
    {%- set stream_name = 'ACCT_BALN_BKDT' -%}
    {%- set target_table = var('cld_db') ~ '.' ~ var('cadproddata') ~ '.UTIL_PROS_ISAC' -%}
    {%- set view_ref = ref('acct_baln_bkdt_audt_get_pros_key') -%}
    
    {{ log("🚀 Starting ACCT_BALN_BKDT audit PROS_KEY data load") }}
    {{ log("   Source View: " ~ view_ref) }}
    {{ log("   Target Table: " ~ target_table) }}
    {{ log("   Process: " ~ process_name) }}
    {{ log("   Stream: " ~ stream_name) }}
    
    {% if execute %}
        {% set inserted_records = load_audit_data_from_view(
            view_ref,
            target_table,
            process_name,
            stream_name
        ) %}
        
        {{ print("✅ ACCT_BALN_BKDT audit data load completed successfully") }}
        {{ print("📊 Records processed: " ~ inserted_records) }}
        {{ print("🎯 Target table: " ~ target_table) }}
    {% else %}
        {{ log("Skipping execution during parsing phase") }}
    {% endif %}
{%- endmacro -%}

{%- macro validate_audit_data_load(target_table, process_name, stream_name, expected_min_records=1) -%}
    /*
        Validate Audit Data Load
        
        Validates that the audit data load completed successfully by checking:
        - Record count in target table
        - Data freshness (recent PROS_RQST_S timestamps)
        - Data integrity (required fields populated)
        
        Parameters:
        - target_table: Target table to validate
        - process_name: Process name for logging
        - stream_name: Stream name for logging
        - expected_min_records: Minimum expected record count (default 1)
    */
    
    {% if execute %}
        {# Check record count and freshness #}
        {% set validation_query %}
            SELECT 
                COUNT(*) as total_recent_records,
                COUNT(CASE WHEN PROS_KEY_I IS NOT NULL THEN 1 END) as records_with_pros_key,
                COUNT(CASE WHEN CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT' THEN 1 END) as matching_conv_records,
                MIN(PROS_RQST_S) as earliest_request_ts,
                MAX(PROS_RQST_S) as latest_request_ts
            FROM {{ target_table }}
            WHERE PROS_RQST_S >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
              AND TRGT_M = 'ACCT_BALN_BKDT_AUDT'
        {% endset %}
        
        {% set validation_result = run_query(validation_query) %}
        {% set total_records = validation_result[0][0] %}
        {% set records_with_key = validation_result[0][1] %}
        {% set matching_conv = validation_result[0][2] %}
        {% set earliest_ts = validation_result[0][3] %}
        {% set latest_ts = validation_result[0][4] %}
        
        {{ log("🔍 Validation Results for " ~ target_table ~ ":") }}
        {{ log("   Total recent records: " ~ total_records) }}
        {{ log("   Records with PROS_KEY: " ~ records_with_key) }}
        {{ log("   Matching CONV_M records: " ~ matching_conv) }}
        {{ log("   Time range: " ~ earliest_ts ~ " to " ~ latest_ts) }}
        
        {# Log validation results #}
        {% if total_records >= expected_min_records %}
            {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Validation passed: ' ~ total_records ~ ' records found in target table') }}
            {{ log("✅ Validation passed: Expected minimum " ~ expected_min_records ~ ", found " ~ total_records ~ " records") }}
        {% else %}
            {{ log_dcf_exec_msg(process_name, stream_name, 40, 'Validation failed: Only ' ~ total_records ~ ' records found, expected minimum ' ~ expected_min_records) }}
            {{ exceptions.raise_compiler_error("Validation failed: Only " ~ total_records ~ " records found in " ~ target_table ~ ", expected minimum " ~ expected_min_records) }}
        {% endif %}
        
        {# Validate data integrity #}
        {% if records_with_key != total_records %}
            {{ log_dcf_exec_msg(process_name, stream_name, 30, 'Warning: Some records missing PROS_KEY_I values') }}
        {% endif %}
        
        {% if matching_conv != total_records %}
            {{ log_dcf_exec_msg(process_name, stream_name, 30, 'Warning: Some records have unexpected CONV_M values') }}
        {% endif %}
        
    {% endif %}
{%- endmacro -%}

{%- macro cleanup_old_audit_data(target_table, process_name, stream_name, retention_days=30) -%}
    /*
        Cleanup Old Audit Data
        
        Optional cleanup macro to remove old audit records based on retention policy.
        This helps manage table size for frequently running audit processes.
        
        Parameters:
        - target_table: Target table to clean up
        - process_name: Process name for logging  
        - stream_name: Stream name for logging
        - retention_days: Number of days to retain (default 30)
    */
    
    {{ log("🧹 Starting cleanup of old audit data older than " ~ retention_days ~ " days") }}
    
    {% if execute %}
        {# Check how many records would be deleted #}
        {% set count_query %}
            SELECT COUNT(*) as old_record_count
            FROM {{ target_table }}
            WHERE PROS_RQST_S < DATEADD(day, -{{ retention_days }}, CURRENT_TIMESTAMP())
              AND TRGT_M = 'ACCT_BALN_BKDT_AUDT'
        {% endset %}
        
        {% set count_result = run_query(count_query) %}
        {% set old_record_count = count_result[0][0] %}
        
        {{ log("Found " ~ old_record_count ~ " old records to cleanup") }}
        
        {% if old_record_count > 0 %}
            {# Perform the cleanup #}
            {% set cleanup_sql %}
                DELETE FROM {{ target_table }}
                WHERE PROS_RQST_S < DATEADD(day, -{{ retention_days }}, CURRENT_TIMESTAMP())
                  AND TRGT_M = 'ACCT_BALN_BKDT_AUDT'
            {% endset %}
            
            {% do run_query(cleanup_sql) %}
            
            {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Cleaned up ' ~ old_record_count ~ ' old audit records older than ' ~ retention_days ~ ' days') }}
            {{ log("✅ Cleanup completed: Removed " ~ old_record_count ~ " old records") }}
        {% else %}
            {{ log("No old records found for cleanup") }}
        {% endif %}
    {% endif %}
{%- endmacro -%}
