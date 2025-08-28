{% macro mark_process_completed(process_name, stream_name) %}
  {#- 
    Macro to mark a process as completed in DCF_T_PRCS_INST
    This runs in the post-hook when the model executes successfully
  -#}
  
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