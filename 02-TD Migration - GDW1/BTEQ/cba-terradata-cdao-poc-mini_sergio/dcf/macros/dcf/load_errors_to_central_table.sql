{% macro load_errors_to_central_table(source_view, stream_name, process_name) %}
    {%- set log_prefix = '[DCF][ERROR_LOAD]' -%}
    
    {{ log(log_prefix ~ " Loading error records from " ~ source_view ~ " to central XFM_ERR_DTL table") }}
    
    {%- set insert_sql -%}
        INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.XFM_ERR_DTL 
        SELECT * FROM {{ source_view }}
        WHERE STRM_NM = '{{ stream_name }}' AND PRCS_NM = '{{ process_name }}'
    {%- endset -%}
    
    {%- if execute -%}
        {%- set results = run_query(insert_sql) -%}
        {{ log(log_prefix ~ " Successfully loaded error records for stream: " ~ stream_name ~ ", process: " ~ process_name) }}
        {{ log_dcf_exec_msg('ERROR_LOAD', stream_name, 10, 'Error records loaded to central XFM_ERR_DTL table from ' ~ source_view) }}
    {%- endif -%}
    
    {{ return(insert_sql) }}
{% endmacro %}

{% macro load_errors_and_end_process(source_view, stream_name, process_name) %}
    {%- set load_sql = load_errors_to_central_table(source_view, stream_name, process_name) -%}
    {%- set end_sql = check_error_and_end_prcs(stream_name, process_name) -%}
    
    {{ return([load_sql, end_sql]) }}
{% endmacro %}
