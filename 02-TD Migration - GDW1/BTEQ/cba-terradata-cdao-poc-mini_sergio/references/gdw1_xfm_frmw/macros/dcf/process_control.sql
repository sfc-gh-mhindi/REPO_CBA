/*
  DCF Process Control Macros
  Enterprise process control and validation macros for DCF streams
*/

{%- macro register_process_instance(process_name, stream_name) -%}
    {% set sql %}
    insert into {{ dcf_database_ref() }}.DCF_T_PRCS_INST (
        PRCS_NAME,
        STRM_NAME,
        PRCS_STATUS,
        PRCS_START_TS,
        PRCS_END_TS,
        PRCS_BUS_DT,
        PRCS_RETRY_CNT,
        PRCS_PARMS,
        INST_TS,
        UPDT_TS,
        CREATED_BY,
        STRM_ID
    )
    select
        '{{ process_name }}',
        '{{ stream_name }}',
        'RUNNING',
        current_timestamp(),
        current_timestamp(),
        s.BUS_DT,
        0,
        null,
        current_timestamp(),
        current_timestamp(),
        current_user(),
        s.STRM_ID
    from {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT s
    where s.STRM_NAME = '{{ stream_name }}'
    order by s.BUS_DT desc
    limit 1
    {% endset %}
    
    {% if execute %}
        {% do run_query(sql) %}
        {# Log process registration to DCF_T_EXEC_LOG #}
        {{ log_dcf_exec_msg(process_name, stream_name, 10, process_name ~ ' processing started') }}
    {% endif %}
{%- endmacro -%}

{% macro validate_stream_status(stream_name, expected_status='READY') %}
  -- ====================================================================
  -- Validate Stream Status - Hook-safe version for pre-hooks
  -- Used in pre-hook to validate stream readiness before process execution
  -- ====================================================================
  {% set sql %}
    DECLARE
      v_stream_status VARCHAR;
    BEGIN
      SELECT 
        CASE 
          WHEN PROCESSING_FLAG = 0 THEN 'READY'
          WHEN PROCESSING_FLAG = 1 THEN 'PROCESSING' 
          WHEN PROCESSING_FLAG = 2 THEN 'ERROR'
          ELSE 'UNKNOWN'
        END INTO v_stream_status
      FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
      WHERE STRM_NAME = '{{ stream_name }}'
      ORDER BY BUS_DT DESC
      LIMIT 1;
      
      IF v_stream_status != '{{ expected_status }}' THEN
        RAISE USING MESSAGE = 'Stream ' || '{{ stream_name }}' || ' validation failed. Expected: {{ expected_status }}, Actual: ' || v_stream_status;
      END IF;
    END;
  {% endset %}
  
  {% do adapter.execute(sql, auto_begin=False) %}
{% endmacro %}

{% macro validate_stream_status_full(stream_name, expected_status='READY') %}
  -- ====================================================================
  -- Validate Stream Status - Full version with database queries
  -- Use this version outside of hooks when you need actual validation
  -- ====================================================================
  {% set query %}
    SELECT 
      STRM_ID,
      STRM_NAME,
      PROCESSING_FLAG,
      BUS_DT,
      CASE 
        WHEN PROCESSING_FLAG = 0 THEN 'READY'
        WHEN PROCESSING_FLAG = 1 THEN 'PROCESSING' 
        WHEN PROCESSING_FLAG = 2 THEN 'ERROR'
        ELSE 'UNKNOWN'
      END as stream_status
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
    WHERE STRM_NAME = '{{ stream_name }}'
    ORDER BY BUS_DT DESC
    LIMIT 1
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% if results and results.rows %}
      {% set current_status = results.columns[4].values()[0] %}
      {% set business_date = results.columns[3].values()[0] %}
      
      {% if current_status == expected_status %}
        {{ log("✅ Stream " ~ stream_name ~ " validation passed. Status: " ~ current_status ~ ", Business Date: " ~ business_date) }}
      {% else %}
        {{ log("❌ Stream " ~ stream_name ~ " validation failed. Expected: " ~ expected_status ~ ", Actual: " ~ current_status) }}
        {% if current_status == 'ERROR' %}
          {{ exceptions.raise_compiler_error("Stream " ~ stream_name ~ " is in ERROR status. Please resolve before continuing.") }}
        {% endif %}
      {% endif %}
    {% else %}
      {{ log("⚠️  Stream " ~ stream_name ~ " not found in DCF control tables") }}
    {% endif %}
  {% endif %}
  -- Stream validation completed
{% endmacro %}

{% macro register_process_completion(process_name, stream_name) %}
  -- ====================================================================
  -- Register Process Completion - Hook-safe version for post-hooks
  -- Used in post_hook to log process completion and transformation statistics
  -- ====================================================================
  {% set sql %}
    INSERT INTO {{ dcf_database_ref() }}.DCF_T_EXEC_LOG (
      PRCS_NAME,
      STRM_NAME,
      STRM_ID,
      BUSINESS_DATE,
      STEP_STATUS,
      MESSAGE_TYPE,
      MESSAGE_TEXT,
      CREATED_BY,
      CREATED_TS,
      SESSION_ID,
      WAREHOUSE_NAME
    ) 
    SELECT
      '{{ process_name }}',
      '{{ stream_name }}',
      s.STRM_ID,
      s.BUS_DT,
      'COMPLETED',
      10, -- Info
      'Process completed successfully',
      CURRENT_USER(),
      CURRENT_TIMESTAMP(),
      CURRENT_SESSION(),
      CURRENT_WAREHOUSE()
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT s
    WHERE s.STRM_NAME = '{{ stream_name }}'
    ORDER BY s.BUS_DT DESC
    LIMIT 1
  {% endset %}
  
  {% do adapter.execute(sql, auto_begin=False) %}
{% endmacro %}

{% macro validate_process_completion(process_name, stream_name, required=false) %}
  -- ====================================================================
  -- Validate Process Completion - Hook-safe version
  -- Used to enforce process dependencies in complex transformation pipelines
  -- ====================================================================
  {% set query %}
    SELECT COUNT(*) as completion_count
    FROM {{ dcf_database_ref() }}.DCF_T_EXEC_LOG e
    JOIN {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT s
      ON e.STRM_ID = s.STRM_ID
    WHERE e.PRCS_NAME = '{{ process_name }}'
      AND s.STRM_NAME = '{{ stream_name }}'
      AND e.STEP_STATUS = 'COMPLETED'
      AND e.BUSINESS_DATE = s.BUS_DT
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% set completion_count = results.columns[0].values()[0] %}
    
    {% if completion_count == 0 and required %}
      {{ exceptions.raise_compiler_error("Required process dependency not met: " ~ process_name ~ " has not completed") }}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro check_dcf_dependencies(stream_name) %}
  -- ====================================================================
  -- Check DCF Dependencies - Hook-safe version for pre-hooks
  -- Validates all DCF prerequisites before stream execution
  -- ====================================================================
  
  -- Check DCF control tables are accessible
  {% set check_sql %}
    SELECT COUNT(*) 
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
    WHERE STRM_NAME = '{{ stream_name }}'
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(check_sql) %}
    {% if results.columns[0].values()[0] == 0 %}
      {{ exceptions.raise_compiler_error("Stream " ~ stream_name ~ " not found in DCF control tables") }}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro register_process_completion_full(process_name, stream_name) %}
  -- ====================================================================
  -- Register Process Completion - Full version with database insert
  -- Use this version outside of hooks when you need actual process logging
  -- ====================================================================
  {% set query %}
    INSERT INTO {{ ref('dcf_t_process_log') }} (
      stream_name,
      process_name,
      start_timestamp,
      end_timestamp,
      status,
      business_date,
      created_timestamp
    ) VALUES (
      '{{ stream_name }}',
      '{{ process_name }}',
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP(),
      'COMPLETED',
      CURRENT_DATE(),
      CURRENT_TIMESTAMP()
    )
  {% endset %}
  
  {% if execute %}
    {% do run_query(query) %}
    {{ log("✅ Process completion registered for " ~ process_name) }}
  {% endif %}
  -- Process completion registered
{% endmacro %}

{% macro validate_process_completion_full(process_name, stream_name, required=false) %}
  -- ====================================================================
  -- Validate Process Completion - Full version with database queries
  -- Checks if dependent processes have completed
  -- ====================================================================
  {% set query %}
    SELECT COUNT(*) as completion_count
    FROM {{ ref('dcf_t_process_log') }}
    WHERE process_name = '{{ process_name }}'
      AND stream_name = '{{ stream_name }}'
      AND status = 'COMPLETED'
      AND business_date = CURRENT_DATE()
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% set completion_count = results.columns[0].values()[0] %}
    
    {% if completion_count > 0 %}
      {{ log("✅ Process dependency satisfied: " ~ process_name ~ " completed") }}
    {% elif required %}
      {{ exceptions.raise_compiler_error("Required process dependency not met: " ~ process_name ~ " has not completed") }}
    {% else %}
      {{ log("⚠️  Optional process dependency not met: " ~ process_name ~ " has not completed") }}
    {% endif %}
  {% endif %}
  -- Process dependency validation completed
{% endmacro %}

{% macro check_dcf_dependencies_full(stream_name) %}
  -- ====================================================================
  -- Check DCF Dependencies - Full version with comprehensive validation
  -- Validates all DCF prerequisites before stream execution
  -- ====================================================================
  {{ log("🔍 Checking DCF dependencies for Stream " ~ stream_name ~ "...") }}
  
  -- Validate stream status
  {{ validate_stream_status_full(stream_name, 'READY') }}
  
  -- Check DCF control tables are accessible
  {% set control_check %}
    SELECT 
      COUNT(*) as busdate_count,
      COUNT(DISTINCT stream_name) as stream_count
    FROM {{ ref('dcf_t_stream_busdate') }}
    WHERE stream_name = '{{ stream_name }}'
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(control_check) %}
    {% if results.rows and results.columns[0].values()[0] > 0 %}
      {{ log("✅ DCF control tables accessible and populated") }}
    {% else %}
      {{ exceptions.raise_compiler_error("DCF control tables not accessible or missing data for Stream " ~ stream_name) }}
    {% endif %}
  {% endif %}
  
  {{ log("✅ All DCF dependencies validated successfully") }}
  -- DCF dependencies validation completed
{% endmacro %}

{% macro get_stream_processing_window(stream_name) %}
  -- ====================================================================
  -- Get Stream Processing Window - Advanced processing window calculation
  -- Returns optimized processing window based on stream characteristics
  -- ====================================================================
  {% set query %}
    SELECT 
      bd.business_date,
      bd.prev_business_date,
      si.business_date_cycle_start_ts,
      si.business_date_cycle_end_ts,
      bd.cycle_freq_code,
      CASE 
        WHEN bd.cycle_freq_code = 1 THEN 1  -- Daily
        WHEN bd.cycle_freq_code = 7 THEN 7  -- Weekly  
        WHEN bd.cycle_freq_code = 30 THEN 30 -- Monthly
        ELSE 1
      END as processing_window_days
    FROM {{ ref('dcf_t_stream_busdate') }} bd
    INNER JOIN {{ ref('dcf_t_stream_id') }} si
      ON bd.stream_name = si.stream_name
      AND bd.business_date = si.business_date
    WHERE bd.stream_name = '{{ stream_name }}'
      AND bd.processing_flag = 0
    ORDER BY bd.business_date DESC
    LIMIT 1
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% if results.rows %}
      {% set window_info = {
        'business_date': results.columns[0].values()[0],
        'prev_business_date': results.columns[1].values()[0],
        'cycle_start_ts': results.columns[2].values()[0],
        'cycle_end_ts': results.columns[3].values()[0],
        'cycle_freq_code': results.columns[4].values()[0],
        'processing_window_days': results.columns[5].values()[0]
      } %}
      {{ return(window_info) }}
    {% else %}
      {{ return({'business_date': '2024-01-01', 'processing_window_days': 1}) }}
    {% endif %}
  {% else %}
    {{ return({'business_date': '2024-01-01', 'processing_window_days': 1}) }}
  {% endif %}
{% endmacro %}

{% macro set_stream_processing_flag(stream_name, flag_value=1) %}
  -- ====================================================================
  -- Set Stream Processing Flag - Stream state management
  -- Updates processing flag to control stream execution state
  -- ====================================================================
  UPDATE {{ ref('dcf_t_stream_busdate') }}
  SET 
    processing_flag = {{ flag_value }},
    update_ts = CURRENT_TIMESTAMP(),
    update_user = '{{ target.user }}'
  WHERE stream_name = '{{ stream_name }}'
    AND business_date = CURRENT_DATE()
    
  {{ log("INFO: Set processing flag to " ~ flag_value ~ " for Stream " ~ stream_name) }}
{% endmacro %}

{% macro get_stream_execution_metrics(stream_name, lookback_days=7) %}
  -- ====================================================================
  -- Get Stream Execution Metrics - Performance and execution analytics
  -- Returns comprehensive execution metrics for monitoring and optimization
  -- ====================================================================
  {% set query %}
    SELECT 
      COUNT(*) as total_executions,
      COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_executions,
      COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as failed_executions,
      AVG(DATEDIFF(second, start_timestamp, end_timestamp)) as avg_execution_seconds,
      MAX(DATEDIFF(second, start_timestamp, end_timestamp)) as max_execution_seconds,
      MIN(DATEDIFF(second, start_timestamp, end_timestamp)) as min_execution_seconds
    FROM {{ ref('dcf_t_process_log') }}
    WHERE stream_name = '{{ stream_name }}'
      AND business_date >= CURRENT_DATE() - {{ lookback_days }}
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% if results.rows %}
      {% set metrics = {
        'total_executions': results.columns[0].values()[0],
        'successful_executions': results.columns[1].values()[0],
        'failed_executions': results.columns[2].values()[0],
        'avg_execution_seconds': results.columns[3].values()[0],
        'max_execution_seconds': results.columns[4].values()[0],
        'min_execution_seconds': results.columns[5].values()[0]
      } %}
      {{ return(metrics) }}
    {% else %}
      {{ return({'total_executions': 0, 'successful_executions': 0, 'failed_executions': 0}) }}
    {% endif %}
  {% else %}
    {{ return({'total_executions': 0, 'successful_executions': 0, 'failed_executions': 0}) }}
  {% endif %}
{% endmacro %}

{% macro log(message, stream_name) %}
  -- {{ message }} for Stream {{ stream_name }}
{% endmacro %}

{% macro dcf_model_wrapper(model_sql, process_name, stream_name) %}
  -- ====================================================================
  -- DCF Model Wrapper - Comprehensive model execution wrapper
  -- Provides full DCF capabilities around any model execution
  -- ====================================================================
  
  -- Pre-execution validation
  {{ check_dcf_dependencies_full(stream_name) }}
  {{ validate_stream_status_full(stream_name, 'READY') }}
  
  -- Execute the model
  {{ model_sql }}
  
  -- Post-execution logging and validation
  {{ register_process_completion_full(process_name, stream_name) }}
  {{ validate_row_counts_full(this, process_name, 0) }}
  {{ log_transformation_stats_full(this, stream_name) }}
  
{% endmacro %}

{%- macro register_dcf_process(process_name, stream_name) -%}
    INSERT INTO {{ ref('dcf_t_process_log') }} (
        process_id,
        stream_name,
        process_name,
        process_type,
        process_status,
        start_ts,
        created_by,
        created_ts
    )
    SELECT
        MD5('{{ process_name }}' || TO_CHAR(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF')) as process_id,
        '{{ stream_name }}' as stream_name,
        '{{ process_name }}' as process_name,
        'DBT_MODEL' as process_type,
        'STARTED' as process_status,
        current_timestamp() as start_ts,
        '{{ target.user }}' as created_by,
        current_timestamp() as created_ts;
{%- endmacro -%}

{%- macro update_dcf_process_status(process_name, stream_name, status, error_message='') -%}
    UPDATE {{ ref('dcf_t_process_log') }}
    SET process_status = '{{ status }}',
        end_ts = current_timestamp(),
        error_message = '{{ error_message }}'
    WHERE process_name = '{{ process_name }}'
      AND stream_name = '{{ stream_name }}'
      AND end_ts IS NULL;
{%- endmacro -%}

{%- macro get_dcf_process_status(process_name, stream_name) -%}
    SELECT process_status
    FROM {{ ref('dcf_t_process_log') }}
    WHERE process_name = '{{ process_name }}'
      AND stream_name = '{{ stream_name }}'
    ORDER BY start_ts DESC
    LIMIT 1;
{%- endmacro -%}

{%- macro update_process_status(process_name, stream_name, status, message='') -%}
    {% set sql %}
    update {{ dcf_database_ref() }}.DCF_T_PRCS_INST
    set 
        PRCS_STATUS = '{{ status }}',
        PRCS_END_TS = current_timestamp(),
        UPDT_TS = current_timestamp(),
        PRCS_PARMS = '{{ message }}'
    where PRCS_NAME = '{{ process_name }}'
      and STRM_NAME = '{{ stream_name }}'
      and PRCS_STATUS = 'RUNNING'
      and PRCS_BUS_DT = (
        select BUS_DT
        from {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        where STRM_NAME = '{{ stream_name }}'
        order by BUS_DT desc
        limit 1
      )
    {% endset %}
    
    {% do run_query(sql) %}
{%- endmacro -%}

{% macro process_hook() %}
    {# Generic process hook that handles both pre and post execution #}
    {% set config = model['config'] %}
    {% set stream_name = config.get('stream_name', 'BV_PDS_TRAN') %}  -- Default to BV_PDS_TRAN stream
    {% set model_name = model.name %}

    {% if execute %}
        {% if model.has_node_target %}
            {# Pre-execution #}
            {{ register_process_instance(model_name, stream_name) }}
        {% else %}
            {# Post-execution #}
            {% if model.status == 'success' %}
                {{ update_process_status(model_name, stream_name, 'COMPLETED') }}
            {% else %}
                {{ update_process_status(model_name, stream_name, 'FAILED', model.message) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %} 