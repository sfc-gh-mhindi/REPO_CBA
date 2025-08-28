/*
  DCF Logging Macros
  
  Handles logging of model execution and status
*/

/*
  DCF Execution Message Logging Macro
  
  Logs execution messages to dcf_t_exec_log
  Message Types:
    10 = Info
    11 = Error 
    12 = Warning
    20 = Debug
    30 = Trace
*/

{%- macro log_dcf_exec_msg(process_name, stream_name, message_type, message_text, sql_text=none) -%}
    {% set sql %}
    insert into {{ dcf_database_ref() }}.DCF_T_EXEC_LOG (
        PRCS_NAME,
        STRM_NAME,
        STRM_ID,
        BUSINESS_DATE,
        STEP_STATUS,
        MESSAGE_TYPE,
        MESSAGE_TEXT,
        SQL_TEXT,
        CREATED_BY,
        CREATED_TS,
        SESSION_ID,
        WAREHOUSE_NAME
    )
    select
        '{{ process_name }}',
        '{{ stream_name }}',
        s.STRM_ID,
        s.BUS_DT,
        '-',
        {{ message_type }},
        '{{ message_text }}',
        {% if sql_text is none %}null{% else %}'{{ sql_text }}'{% endif %},
        current_user(),
        current_timestamp(),
        current_session(),
        current_warehouse()
    from {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT s
    where s.STRM_NAME = '{{ stream_name }}'
    order by s.BUS_DT desc
    limit 1
    {% endset %}
    
    {% if execute %}
        {% do run_query(sql) %}
    {% endif %}
{%- endmacro -%}

/*
  Simplified DCF Logging Macros
  
  These provide easy-to-use abstractions for common logging patterns
*/

{%- macro log_process_start(process_name, stream_name=none) -%}
    {{ log_dcf_exec_msg(
        process_name=process_name,
        stream_name=stream_name or var('stream_name', 'BCFINSG'),
        message_type=10,
        message_text=process_name ~ ' processing started'
    ) }}
{%- endmacro -%}

{%- macro log_process_success(process_name, stream_name=none) -%}
    {{ log_dcf_exec_msg(
        process_name=process_name,
        stream_name=stream_name or var('stream_name', 'BCFINSG'),
        message_type=10,
        message_text=process_name ~ ' processing completed successfully'
    ) }}
{%- endmacro -%}

{%- macro log_process_error(process_name, error_message, stream_name=none) -%}
    {{ log_dcf_exec_msg(
        process_name=process_name,
        stream_name=stream_name or var('stream_name', 'BCFINSG'),
        message_type=11,
        message_text=process_name ~ ' processing failed: ' ~ error_message
    ) }}
{%- endmacro -%}

{%- macro log_model_execution(model_name, stream_name=none) -%}
    {{ log_dcf_exec_msg(
        process_name=model_name,
        stream_name=stream_name or var('stream_name', 'BCFINSG'),
        message_type=10,
        message_text=model_name ~ ' model execution completed',
        sql_text=this.schema ~ '.' ~ this.name
    ) }}
{%- endmacro -%} 