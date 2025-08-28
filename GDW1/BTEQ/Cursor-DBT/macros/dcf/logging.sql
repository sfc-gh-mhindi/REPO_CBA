/*
  GDW1 DCF Logging Macros
  
  Adapted from the DCF framework for BTEQ migration
  Handles logging of model execution and status for GDW1 processes
*/

/*
  DCF Database Reference Macro
  
  Returns the proper database reference for DCF operations
*/
{%- macro dcf_database_ref() -%}
    {{ var('dcf_database') }}.{{ var('dcf_schema') }}
{%- endmacro -%}

/*
  GDW1 Execution Message Logging Macro
  
  Logs execution messages to DCF_T_EXEC_LOG for GDW1 processes
  Message Types:
    10 = Info
    11 = Error 
    12 = Warning
    20 = Debug
    30 = Trace
*/
{%- macro log_gdw1_exec_msg(process_name, stream_name, message_type, message_text, sql_text=none) -%}
    {% set sql %}
    INSERT INTO {{ dcf_database_ref() }}.DCF_T_EXEC_LOG (
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
    SELECT
        '{{ process_name }}',
        '{{ stream_name }}',
        'GDW1_{{ this.name if this is defined else "MIGRATION" }}',
        CURRENT_DATE(),
        'RUNNING',
        {{ message_type }},
        '{{ message_text }}',
        {% if sql_text is none %}NULL{% else %}'{{ sql_text }}'{% endif %},
        CURRENT_USER(),
        CURRENT_TIMESTAMP(),
        CURRENT_SESSION(),
        CURRENT_WAREHOUSE()
    {% endset %}
    
    {% do run_query(sql) %}
{%- endmacro -%}

/*
  GDW1 Process Instance Registration
  
  Registers a process instance in DCF_T_PRCS_INST for tracking
*/
{%- macro register_gdw1_process_instance(model_name, stream_name, process_name=none) -%}
    {% set actual_process_name = process_name or model_name %}
    {% set sql %}
    INSERT INTO {{ dcf_database_ref() }}.DCF_T_PRCS_INST (
        PROCESS_NAME,
        STREAM_NAME,
        MODEL_NAME,
        PROCESS_STATUS,
        START_TIMESTAMP,
        STATUS_MESSAGE,
        CREATED_BY,
        CREATED_TS
    )
    SELECT
        '{{ actual_process_name }}',
        '{{ stream_name }}',
        '{{ model_name }}',
        'RUNNING',
        CURRENT_TIMESTAMP(),
        'GDW1 process {{ actual_process_name }} started via dbt model {{ model_name }}',
        CURRENT_USER(),
        CURRENT_TIMESTAMP()
    {% endset %}
    
    {% do run_query(sql) %}
{%- endmacro -%}

/*
  GDW1 Process Status Update
  
  Updates process status in DCF_T_PRCS_INST
*/
{%- macro update_gdw1_process_status(model_name, stream_name, status, message, process_name=none) -%}
    {% set actual_process_name = process_name or model_name %}
    {% set sql %}
    UPDATE {{ dcf_database_ref() }}.DCF_T_PRCS_INST 
    SET 
        PROCESS_STATUS = '{{ status }}',
        END_TIMESTAMP = CASE WHEN '{{ status }}' IN ('COMPLETED', 'FAILED') THEN CURRENT_TIMESTAMP() ELSE END_TIMESTAMP END,
        STATUS_MESSAGE = '{{ message }}',
        UPDATED_BY = CURRENT_USER(),
        UPDATED_TS = CURRENT_TIMESTAMP()
    WHERE PROCESS_NAME = '{{ actual_process_name }}'
        AND STREAM_NAME = '{{ stream_name }}'
        AND MODEL_NAME = '{{ model_name }}'
        AND PROCESS_STATUS = 'RUNNING'
    {% endset %}
    
    {% do run_query(sql) %}
{%- endmacro -%}

/*
  GDW1 Audit Columns Macro
  
  Returns standard audit columns for GDW1 models
*/
{%- macro gdw1_audit_columns() -%}
    CURRENT_USER() AS CREATED_BY,
    CURRENT_TIMESTAMP() AS CREATED_TS,
    CURRENT_USER() AS UPDATED_BY,
    CURRENT_TIMESTAMP() AS UPDATED_TS,
    CURRENT_SESSION() AS SESSION_ID,
    CURRENT_WAREHOUSE() AS WAREHOUSE_NAME
{%- endmacro -%}

/*
  GDW1 Business Date Processing Macro
  
  Returns current business date for processing
*/
{%- macro gdw1_business_date() -%}
    CURRENT_DATE()
{%- endmacro -%}

/*
  GDW1 Stream Validation Macro
  
  Validates that a stream is available for processing
*/
{%- macro validate_gdw1_stream(stream_name) -%}
    {% set sql %}
    SELECT COUNT(*) as stream_count
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
    WHERE STRM_NAME = '{{ stream_name }}'
        AND BUS_DT = {{ gdw1_business_date() }}
    {% endset %}
    
    {% set results = run_query(sql) %}
    {% if execute and results.columns[0].values()[0] == 0 %}
        {% do log.error("Stream " ~ stream_name ~ " not found for business date " ~ gdw1_business_date()) %}
        {{ exceptions.raise_compiler_error("Stream validation failed for " ~ stream_name) }}
    {% endif %}
{%- endmacro -%}

/*
  GDW1 Error Logging Macro
  
  Logs errors for GDW1 processes with automatic process status update
*/
{%- macro log_gdw1_error(process_name, stream_name, error_message, sql_text=none) -%}
    {% do log_gdw1_exec_msg(process_name, stream_name, 11, error_message, sql_text) %}
    {% do update_gdw1_process_status(this.name, stream_name, 'FAILED', error_message, process_name) %}
{%- endmacro -%} 