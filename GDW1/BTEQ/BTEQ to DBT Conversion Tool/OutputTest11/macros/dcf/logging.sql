-- DCF Framework Integration Macros
-- Adapted for BTEQ to DBT conversion

{% macro register_process_instance(process_name, stream_code='DEFAULT') %}
  -- Register process instance in DCF framework
  INSERT INTO DCF_T_PRCS_INST (
    PRCS_NAME,
    STRM_C,
    STRT_TS,
    STAT_C,
    CREATED_TS
  ) VALUES (
    '{{ process_name }}',
    '{{ stream_code }}', 
    CURRENT_TIMESTAMP(),
    'RUNNING',
    CURRENT_TIMESTAMP()
  )
{% endmacro %}

{% macro update_process_status(status, message='') %}
  -- Update process status in DCF framework
  UPDATE DCF_T_PRCS_INST 
  SET 
    STAT_C = '{{ status }}',
    END_TS = CURRENT_TIMESTAMP(),
    UPDT_TS = CURRENT_TIMESTAMP()
    {% if message %}
    , MSG_T = '{{ message }}'
    {% endif %}
  WHERE PRCS_NAME = '{{ this.name }}'
    AND STRT_TS = (SELECT MAX(STRT_TS) FROM DCF_T_PRCS_INST WHERE PRCS_NAME = '{{ this.name }}')
{% endmacro %}

{% macro log_exec_msg(message_type, message_text) %}
  -- Log execution message
  INSERT INTO DCF_T_EXEC_LOG (
    PRCS_NAME,
    MSG_TYPE_C,
    MSG_T,
    CREATED_TS
  ) VALUES (
    '{{ this.name }}',
    '{{ message_type }}',
    '{{ message_text }}',
    CURRENT_TIMESTAMP()
  )
{% endmacro %}