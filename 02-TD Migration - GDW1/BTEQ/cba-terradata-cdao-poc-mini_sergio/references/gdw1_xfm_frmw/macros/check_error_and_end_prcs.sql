{% macro check_error_and_end_prcs(stream_name, process_name) %}
  {#- 
    Macro to check for error records and update process status accordingly
    - If errors found: Update process status to FAILED and fail the dbt pipeline
    - If no errors: Update process status to COMPLETED
    Uses proper dbt exception handling for clean failure
  -#}
  
  {% if execute %}
    {% set error_count_query %}
      SELECT 
        COUNT(e.STRM_NM) as error_count,
        bd.BUS_DT as current_business_date
      FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT bd
      LEFT JOIN {{ dcf_database_ref() }}.XFM_ERR_DTL e
        ON e.PRCS_DT = bd.BUS_DT
        AND e.STRM_NM = '{{ stream_name }}'
        AND e.PRCS_NM = '{{ process_name }}'
      WHERE bd.STRM_NAME = '{{ stream_name }}' 
        AND bd.PROCESSING_FLAG = 1
      GROUP BY bd.BUS_DT
      LIMIT 1
    {% endset %}
    
    {% set results = run_query(error_count_query) %}
    
    {% if results and results.columns[0].values() | length > 0 %}
      {% set error_count = results.columns[0].values()[0] %}
      {% set business_date = results.columns[1].values()[0] %}
      
      {% if error_count > 0 %}
        {#- Update process status to FAILED before raising error -#}
        {% set update_failed_sql %}
          UPDATE {{ dcf_database_ref() }}.DCF_T_PRCS_INST 
          SET PRCS_STATUS = 'FAILED', 
              PRCS_END_TS = CURRENT_TIMESTAMP(), 
              UPDT_TS = CURRENT_TIMESTAMP()
          WHERE PRCS_NAME = '{{ process_name }}' 
            AND STRM_NAME = '{{ stream_name }}' 
            AND PRCS_STATUS = 'RUNNING' 
            AND PRCS_BUS_DT = '{{ business_date }}'
        {% endset %}
        
        {% do run_query(update_failed_sql) %}
        
        {# Log failure to DCF_T_EXEC_LOG #}
        {{ log_dcf_exec_msg(process_name, stream_name, 11, 'Validation failed with ' ~ error_count ~ ' errors') }}
        {{ log('ERROR', 'PROCESS FAILED: ' ~ error_count ~ ' error records found for stream ' ~ stream_name ~ ' process ' ~ process_name ~ ' on business date ' ~ business_date ~ '. Process marked as FAILED.') }}
        {{ exceptions.raise_compiler_error('PIPELINE FAILED: ' ~ error_count ~ ' error records found for stream ' ~ stream_name ~ ' process ' ~ process_name ~ ' on business date ' ~ business_date ~ '. Check XFM_ERR_DTL table for detailed error information.') }}
      {% else %}
        {# Log success to DCF_T_EXEC_LOG #}
        {{ log_dcf_exec_msg(process_name, stream_name, 10, 'Process completed successfully with no errors') }}
        {{ log('INFO', 'VALIDATION SUCCESS: No error records found for stream ' ~ stream_name ~ ' process ' ~ process_name ~ '. Marking process as COMPLETED.') }}
      {% endif %}
    {% else %}
      {{ log('WARNING', 'Could not determine error count for stream ' ~ stream_name ~ ' process ' ~ process_name) }}
    {% endif %}
  {% endif %}
  
  -- Return SQL to mark process as COMPLETED if no errors
  UPDATE {{ dcf_database_ref() }}.DCF_T_PRCS_INST 
  SET PRCS_STATUS = 'COMPLETED', 
      PRCS_END_TS = CURRENT_TIMESTAMP(), 
      UPDT_TS = CURRENT_TIMESTAMP()
  WHERE PRCS_NAME = '{{ process_name }}' 
    AND STRM_NAME = '{{ stream_name }}' 
    AND PRCS_STATUS = 'RUNNING' 
    AND PRCS_BUS_DT = (
      SELECT BUS_DT 
      FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
      WHERE STRM_NAME = '{{ stream_name }}' 
        AND PROCESSING_FLAG = 1 
      LIMIT 1
    )

{% endmacro %}