{% macro err_tbl_reset(stream_name, process_name) %}
  {#- 
    Macro to reset error table for a specific stream and process on current business date
    Returns SQL that dbt will execute in the pre-hook context
  -#}
  
  {# Log to DCF_T_EXEC_LOG table #}
  {% if execute %}
    {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Error table reset started for stream ' ~ stream_name ~ ' process ' ~ process_name) }}
  {% endif %}
  
  {{ log('INFO', 'ERR_TBL_RESET: Generating delete statement for stream ' ~ stream_name ~ ' process ' ~ process_name) }}
  
  DELETE FROM {{ dcf_database_ref() }}.XFM_ERR_DTL 
  WHERE STRM_NM = '{{ stream_name }}'
    AND PRCS_NM = '{{ process_name }}'
    AND PRCS_DT = (
      SELECT BUS_DT 
      FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
      WHERE STRM_NAME = '{{ stream_name }}' 
        AND PROCESSING_FLAG = 1 
      LIMIT 1
    );

  {# Log successful completion to DCF_T_EXEC_LOG table #}
  {% if execute %}
    {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Error table successfully deleted for stream ' ~ stream_name ~ ' process ' ~ process_name) }}
  {% endif %}

{% endmacro %}