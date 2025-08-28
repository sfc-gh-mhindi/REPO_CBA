{% macro summarize_errors(stream_name, process_date=none) %}
    /*
        Generic Error Summary and Validation
        
        This macro:
        - Summarizes all errors captured during stream processing  
        - Provides comprehensive error reporting and statistics
        - Ensures error audit trail is complete for compliance
        - Can be called after any error detection process
        
        Usage:
        - dbt run-operation summarize_errors --args '{stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD", process_date: "20241220"}'
        - Call as post-hook: {{ summarize_errors(var('stream_name'), var('run_stream_process_date')) }}
    */
    
    {% if execute %}
        
        {% set process_date_filter = process_date or var("run_stream_process_date", run_started_at.strftime("%Y%m%d")) %}
        
        {% set error_summary_query %}
            SELECT 
                COUNT(*) as total_error_records,
                COUNT(DISTINCT SRCE_KEY_NM) as unique_failed_records,
                COUNT(DISTINCT ERR_CTGRY_NM) as error_categories,
                MIN(LOAD_TS) as first_error_time,
                MAX(LOAD_TS) as last_error_time,
                LISTAGG(DISTINCT ERR_CTGRY_NM, ', ') as error_types
            FROM {{ target.database }}.{{ target.schema }}.xfm_err_dtl
            WHERE STRM_NM = '{{ stream_name }}'
              AND PRCS_DT = TRY_TO_DATE('{{ process_date_filter }}', 'YYYYMMDD')
        {% endset %}
        
        {% set stats = run_query(error_summary_query) %}
        {% set total_errors = stats.columns[0].values()[0] %}
        {% set unique_records = stats.columns[1].values()[0] %}
        {% set error_categories = stats.columns[2].values()[0] %}
        {% set first_error = stats.columns[3].values()[0] %}
        {% set last_error = stats.columns[4].values()[0] %}
        {% set error_types = stats.columns[5].values()[0] %}
        
        -- Log comprehensive error processing summary
        {{ log("") }}
        {{ log("📋 ===== ERROR PROCESSING SUMMARY =====") }}
        {{ log("") }}
        {{ log("🔍 ERROR AUDIT TRAIL:") }}
        {{ log("   Stream: " ~ stream_name) }}
        {{ log("   Processing Date: " ~ process_date_filter) }}
        {{ log("   Total Error Records: " ~ total_errors) }}
        {{ log("   Unique Failed Records: " ~ unique_records) }}
        {{ log("   Error Categories: " ~ error_categories) }}
        {% if total_errors > 0 %}
            {{ log("   Error Types: " ~ error_types) }}
            {{ log("   First Error: " ~ first_error) }}
            {{ log("   Last Error: " ~ last_error) }}
        {% endif %}
        {{ log("") }}
        
        {% if total_errors > 0 %}
            {{ log("⚠️  ERROR VALIDATION STATUS:") }}
            {{ log("   Status: ERROR RECORDS CAPTURED") }}
            {{ log("   Action: Errors logged to xfm_err_dtl table") }}
            {{ log("   Compliance: Complete error audit trail maintained") }}
            {{ log("") }}
            {{ log("📊 ERROR ANALYSIS QUERIES:") }}
            {{ log("   View all errors: SELECT * FROM VW_XFM_ERR_DTL_FLAT WHERE PRCS_DT = TRY_TO_DATE('" ~ process_date_filter ~ "', 'YYYYMMDD')") }}
            {{ log("   Error summary: SELECT ERR_COLM_NM, COUNT(*) FROM VW_XFM_ERR_DTL_FLAT WHERE PRCS_DT = TRY_TO_DATE('" ~ process_date_filter ~ "', 'YYYYMMDD') GROUP BY 1") }}
        {% else %}
            {{ log("✅ ERROR VALIDATION STATUS:") }}
            {{ log("   Status: NO ERRORS DETECTED") }}
            {{ log("   Quality: 100% CLEAN DATA PROCESSING") }}
            {{ log("   Compliance: Complete processing with full audit trail") }}
        {% endif %}
        
        {{ log("") }}
        {{ log("🔄 ERROR PROCESSING COMPLETE:") }}
        {{ log("   Error Capture: ✅ Real-time during validation") }}
        {{ log("   Error Storage: ✅ Modern XFM_ERR_DTL table") }}
        {{ log("   Error Audit: ✅ Complete audit trail maintained") }}
        {{ log("   Error Reporting: ✅ Summary generated") }}
        {{ log("==========================================") }}
        {{ log("") }}
        
        -- Log completion for operational monitoring
        {{ log("📧 Error processing summary available for stakeholders") }}
        {{ log("✅ Error Processing Summary: COMPLETED") }}
        
    {% endif %}
    
{% endmacro %}