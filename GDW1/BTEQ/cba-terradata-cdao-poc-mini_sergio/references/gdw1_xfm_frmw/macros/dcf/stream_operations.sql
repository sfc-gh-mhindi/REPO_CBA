/*
    Consolidated Stream Operations for DCF Framework with Automatic Intraday Cycle Detection
    
    This file consolidates stream_operations.sql and stream_control.sql into one comprehensive
    stream management system with enhanced intraday processing capabilities.
    
    ESSENTIAL STREAM COMMANDS (from README):
    - dbt run-operation get_stream_status_op --args '{stream_name: "BV_PDS_TRAN"}'
    - dbt run-operation start_stream_op --args '{stream_name: "BV_PDS_TRAN", business_date: "YYYY-MM-DD"}'
    - dbt run --select tag:stream_1475
    - dbt run-operation end_stream_op --args '{stream_name: "BV_PDS_TRAN"}'
    - dbt run-operation get_stream_history_op --args '{stream_name: "BV_PDS_TRAN"}'
    - dbt run-operation reset_stream_op --args '{stream_name: "BV_PDS_TRAN", business_date: "YYYY-MM-DD"}'
    
    AUTOMATIC INTRADAY CYCLE DETECTION:
    - First run for business date: Automatically starts as Cycle 1
    - Subsequent runs: Automatically detects next cycle (2, 3, 4...) based on completed cycles
    - No manual cycle_num parameter needed - system auto-detects based on completion history
    - Example: start_stream_op with same business_date will auto-increment cycle after previous completes
    
    Enhanced Features (requires DCF_T_STRM table):
    - Stream configuration validation from DCF_T_STRM
    - Automatic intraday cycle management with MAX_CYCLES_PER_DAY validation
    - Running state validation (fails if stream already running)
    - Automatic cycle start timestamp setting
    - DAILY vs INTRADAY stream type enforcement
    
    Note: Each business date creates a new historical record. No advance_business_date needed.
*/

{# ========================================
   STREAM CONFIGURATION & MAPPING
   ======================================== #}

{# Stream ID lookup from DCF_T_STRM table #}
{% macro get_stream_id_by_name(stream_name) %}
    {% if execute %}
        {% set stream_query %}
            SELECT STRM_ID
            FROM {{ dcf_database_ref() }}.DCF_T_STRM
            WHERE STRM_NAME = '{{ stream_name }}'
              AND STRM_STATUS = 'ACTIVE'
        {% endset %}
        
        {% set stream_results = run_query(stream_query) %}
        
        {% if stream_results|length == 0 %}
            {{ exceptions.raise_compiler_error("Stream '" ~ stream_name ~ "' not found in DCF_T_STRM table or is not ACTIVE. Please check stream configuration.") }}
        {% elif stream_results|length > 1 %}
            {{ exceptions.raise_compiler_error("Multiple active streams found with name '" ~ stream_name ~ "' in DCF_T_STRM table. Stream names must be unique.") }}
        {% else %}
            {% set stream_id = stream_results[0][0] %}
            {{ return(stream_id) }}
        {% endif %}
    {% else %}
        {# During parsing phase, return a placeholder #}
        {{ return(0) }}
    {% endif %}
{% endmacro %}

{# Get stream configuration from DCF_T_STRM table (if exists) #}
{% macro get_stream_config_safe(stream_name, stream_id) %}
    {% set config_query %}
        SELECT 
            STRM_ID,
            STRM_NAME,
            STRM_DESC,
            STRM_TYPE,
            CYCLE_FREQ_CODE,
            MAX_CYCLES_PER_DAY,
            STRM_STATUS,
            ALLOW_MULTIPLE_CYCLES,
            BUSINESS_DOMAIN,
            TARGET_SCHEMA,
            TARGET_TABLE,
            DBT_TAG
        FROM {{ dcf_database_ref() }}.DCF_T_STRM
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
          AND STRM_STATUS = 'ACTIVE'
    {% endset %}
    
    {% set config_results = run_query(config_query) %}
    
    {% if config_results|length == 0 %}
        {# Return default config if DCF_T_STRM doesn't exist or no config found #}
        {% set config = {
            'strm_id': stream_id,
            'strm_name': stream_name,
            'strm_desc': 'Legacy Stream (No DCF_T_STRM config)',
            'strm_type': 'DAILY',
            'cycle_freq_code': 1,
            'max_cycles_per_day': 1,
            'strm_status': 'ACTIVE',
            'allow_multiple_cycles': false,
            'business_domain': 'UNKNOWN',
            'target_schema': 'UNKNOWN',
            'target_table': 'UNKNOWN',
            'dbt_tag': 'tag:stream_' ~ stream_id,
            'has_config': false
        } %}
    {% else %}
        {% set config_row = config_results[0] %}
        {% set config = {
            'strm_id': config_row[0],
            'strm_name': config_row[1],
            'strm_desc': config_row[2],
            'strm_type': config_row[3],
            'cycle_freq_code': config_row[4],
            'max_cycles_per_day': config_row[5],
            'strm_status': config_row[6],
            'allow_multiple_cycles': config_row[7],
            'business_domain': config_row[8],
            'target_schema': config_row[9],
            'target_table': config_row[10],
            'dbt_tag': config_row[11],
            'has_config': true
        } %}
    {% endif %}
    
    {{ return(config) }}
{% endmacro %}

{# ========================================
   STREAM VALIDATION LOGIC
   ======================================== #}

{# Auto-detect next cycle number for business date #}
{% macro get_next_cycle_number(stream_name, stream_id, business_date, config) %}
    {# For legacy streams without config, always return cycle 1 #}
    {% if not config.has_config %}
        {{ return(1) }}
    {% endif %}
    
    {# Check if stream is already running for this business date #}
    {% set running_check_query %}
        SELECT COUNT(*) as running_count
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
          AND BUS_DT = '{{ business_date }}'::DATE
          AND PROCESSING_FLAG = 1  -- 1 = RUNNING
    {% endset %}
    
    {% set running_results = run_query(running_check_query) %}
    {% set running_count = running_results[0][0] %}
    
    {% if running_count > 0 %}
        {{ exceptions.raise_compiler_error("❌ Stream " ~ stream_name ~ " is already RUNNING for business date " ~ business_date ~ ". Cannot start new cycle until current cycle completes.") }}
    {% endif %}
    
    {# Get the highest completed cycle number for this business date #}
    {% set max_cycle_query %}
        SELECT COALESCE(MAX(BUSINESS_DATE_CYCLE_NUM), 0) as max_cycle
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
          AND BUS_DT = '{{ business_date }}'::DATE
          AND PROCESSING_FLAG = 2  -- 2 = COMPLETED
    {% endset %}
    
    {% set max_cycle_results = run_query(max_cycle_query) %}
    {% set max_completed_cycle = max_cycle_results[0][0] %}
    {% set next_cycle = max_completed_cycle + 1 %}
    
    {# Validate next cycle number against stream configuration #}
    {% if config.strm_type == 'INTRADAY' %}
        {% if next_cycle > config.max_cycles_per_day %}
            {{ exceptions.raise_compiler_error("❌ Next cycle number " ~ next_cycle ~ " would exceed maximum cycles per day (" ~ config.max_cycles_per_day ~ ") for intraday stream " ~ stream_name) }}
        {% endif %}
        
        {# For DAILY streams, only allow cycle 1 #}
    {% elif config.strm_type == 'DAILY' and next_cycle > 1 %}
        {{ exceptions.raise_compiler_error("❌ DAILY stream " ~ stream_name ~ " can only run once per business date. Business date " ~ business_date ~ " already has completed cycles.") }}
    {% endif %}
    
    {# Check if multiple cycles are allowed #}
    {% if next_cycle > 1 and not config.allow_multiple_cycles %}
        {{ exceptions.raise_compiler_error("❌ Multiple cycles not allowed for stream " ~ stream_name ~ ". Business date " ~ business_date ~ " already has completed cycle(s).") }}
    {% endif %}
    
    {{ return(next_cycle) }}
{% endmacro %}

{# ========================================
   CORE STREAM CONTROL FUNCTIONS
   ======================================== #}

{# Start stream - consolidated from stream_control.sql with intraday support #}
{% macro start_stream(stream_name, business_date, stream_id, cycle_num=1) %}
    {%- set log_msg = "Starting stream " ~ stream_name ~ " for business date " ~ business_date ~ " (Cycle: " ~ cycle_num ~ ")" -%}
    {{ log(log_msg) }}

    -- Set business date cycle start timestamp to current time for intraday, beginning of day for daily
    {%- set cycle_start_ts = "CURRENT_TIMESTAMP()" -%}
    {%- set next_bus_dt = "DATEADD(day, 1, '" ~ business_date ~ "'::DATE)" -%}
    {%- set prev_bus_dt = "DATEADD(day, -1, '" ~ business_date ~ "'::DATE)" -%}

    -- Check if this specific business date and cycle already exists for this stream
    {% set check_existing_query %}
        SELECT COUNT(*) as record_count
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
          AND BUS_DT = '{{ business_date }}'::DATE
          AND BUSINESS_DATE_CYCLE_NUM = {{ cycle_num }}
    {% endset %}
    
    {% set existing_check = run_query(check_existing_query) %}
    {% if existing_check[0][0] > 0 %}
        {{ exceptions.raise_compiler_error("Business date " ~ business_date ~ " cycle " ~ cycle_num ~ " already exists for stream " ~ stream_name ~ ". This should not happen with auto-cycle detection.") }}
    {% endif %}

    -- Insert new business date record (HISTORICAL - always new row)
    {% set insert_query %}
        INSERT INTO {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT (
            STRM_ID, STRM_NAME, BUS_DT, BUSINESS_DATE_CYCLE_START_TS, BUSINESS_DATE_CYCLE_NUM,
            NEXT_BUS_DT, PREV_BUS_DT, PROCESSING_FLAG, STREAM_STATUS, CREATED_BY, INST_TS, UPDT_TS
        )
        VALUES (
            {{ stream_id }},
            '{{ stream_name }}',
            '{{ business_date }}'::DATE,
            {{ cycle_start_ts }},
            {{ cycle_num }},
            {{ next_bus_dt }},
            {{ prev_bus_dt }},
            1,  -- 1 = RUNNING
            'RUNNING',
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    {% endset %}
    
    {% do run_query(insert_query) %}
    
    -- Log stream start
    {{ log("✅ Stream " ~ stream_name ~ " started successfully for business date " ~ business_date ~ " (Cycle: " ~ cycle_num ~ ")") }}
{% endmacro %}

{# End stream - consolidated from stream_control.sql #}
{% macro end_stream(stream_name, stream_id) %}
    {%- set log_msg = "Ending stream " ~ stream_name -%}
    {{ log(log_msg) }}
    
    -- Update the most recent running stream to completed
    {% set update_query %}
        UPDATE {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        SET 
            PROCESSING_FLAG = 2,  -- 2 = COMPLETED
            STREAM_STATUS = 'COMPLETED',
            UPDT_TS = CURRENT_TIMESTAMP()
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
          AND PROCESSING_FLAG = 1  -- Only update if currently RUNNING
    {% endset %}
    
    {% do run_query(update_query) %}
    
    {{ log("✅ Stream " ~ stream_name ~ " ended successfully") }}
{% endmacro %}

{# Get stream status - consolidated from stream_control.sql #}
{% macro get_stream_status(stream_name, stream_id) %}
    {%- set status_query -%}
        SELECT 
            BUS_DT,
            BUSINESS_DATE_CYCLE_START_TS,
            BUSINESS_DATE_CYCLE_NUM,
            PROCESSING_FLAG,
            STREAM_STATUS,
            INST_TS,
            UPDT_TS
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
        ORDER BY PROCESSING_FLAG ASC, BUS_DT DESC, BUSINESS_DATE_CYCLE_NUM DESC, UPDT_TS DESC
        LIMIT 1
    {%- endset -%}

    {%- set results = run_query(status_query) -%}
    {%- if results|length == 0 -%}
        {%- do return({
            'exists': false,
            'message': 'No stream record found for ' ~ stream_name
        }) -%}
    {%- endif -%}

    {%- set row = results[0] -%}
    {%- do return({
        'exists': true,
        'business_date': row[0],
        'cycle_start_ts': row[1],
        'cycle_num': row[2],
        'processing_flag': row[3],
        'stream_status': row[4],
        'inst_ts': row[5],
        'updt_ts': row[6]
    }) -%}
{% endmacro %}

{# ========================================
   ENHANCED STREAM OPERATIONS (PUBLIC API)
   ======================================== #}

{% macro start_stream_op(stream_name, business_date) %}
    {%- if not stream_name -%}
        {{ exceptions.raise_compiler_error("stream_name parameter is required. Usage: dbt run-operation start_stream_op --args '{stream_name: \"BV_PDS_TRAN\", business_date: \"2024-12-17\"}'") }}
    {%- endif -%}
    {%- if not business_date -%}
        {{ exceptions.raise_compiler_error("business_date parameter is required. Usage: dbt run-operation start_stream_op --args '{stream_name: \"BV_PDS_TRAN\", business_date: \"2024-12-17\"}'") }}
    {%- endif -%}
    
    {% if execute %}
    {%- set stream_id = get_stream_id_by_name(stream_name) -%}
    {%- set config = get_stream_config_safe(stream_name, stream_id) -%}
    
    {# Auto-detect the next cycle number #}
    {%- set cycle_num = get_next_cycle_number(stream_name, stream_id, business_date, config) -%}
    
    {% if config.has_config %}
        {{ print("🔍 Enhanced Stream Configuration Loaded:") }}
        {{ print("   Stream: " ~ config.strm_name ~ " (" ~ config.strm_desc ~ ")") }}
        {{ print("   Type: " ~ config.strm_type ~ " (Freq Code: " ~ config.cycle_freq_code ~ ")") }}
        {{ print("   Max Cycles/Day: " ~ config.max_cycles_per_day) }}
        {{ print("   Multiple Cycles: " ~ config.allow_multiple_cycles) }}
        {{ print("   Business Domain: " ~ config.business_domain) }}
        {{ print("   Target: " ~ config.target_schema ~ "." ~ config.target_table) }}
        
        {% if cycle_num == 1 %}
            {{ print("🚀 Starting " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") for business date: " ~ business_date ~ " (First run - Cycle: " ~ cycle_num ~ "/" ~ config.max_cycles_per_day ~ ")") }}
        {% else %}
            {{ print("🚀 Starting " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") for business date: " ~ business_date ~ " (Intraday run - Cycle: " ~ cycle_num ~ "/" ~ config.max_cycles_per_day ~ ")") }}
        {% endif %}
    {% else %}
        {% if cycle_num == 1 %}
            {{ print("🚀 Starting " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") for business date: " ~ business_date ~ " (Legacy Mode - First run)") }}
        {% else %}
            {{ print("🚀 Starting " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") for business date: " ~ business_date ~ " (Legacy Mode - Cycle: " ~ cycle_num ~ ")") }}
        {% endif %}
    {% endif %}
    
    {# Use consolidated start_stream function #}
    {{ start_stream(stream_name, business_date, stream_id, cycle_num) }}
    
    -- Display current stream status
    {% set status = get_stream_status(stream_name, stream_id) %}
    {{ print("📊 Stream Status:") }}
    {{ print("   Stream Name: " ~ stream_name ~ " (" ~ config.strm_type ~ ")") }}
    {{ print("   Stream ID: " ~ stream_id) }}
    {{ print("   Business Date: " ~ status.business_date) }}
    {{ print("   Cycle Start: " ~ status.cycle_start_ts) }}
    {{ print("   Cycle Number: " ~ status.cycle_num ~ "/" ~ config.max_cycles_per_day) }}
    {{ print("   Status: " ~ status.stream_status) }}
    {{ print("   Processing Flag: " ~ status.processing_flag ~ " (0=Ready, 1=Running, 2=Completed, 3=Error)") }}
    {% if config.has_config %}
        {{ print("   DBT Tag: " ~ config.dbt_tag) }}
    {% endif %}
    {% endif %}
{% endmacro %}

{% macro end_stream_op(stream_name) %}
    {%- if not stream_name -%}
        {{ exceptions.raise_compiler_error("stream_name parameter is required. Usage: dbt run-operation end_stream_op --args '{stream_name: \"BV_PDS_TRAN\"}'") }}
    {%- endif -%}
    
    {%- set stream_id = get_stream_id_by_name(stream_name) -%}
    {%- set config = get_stream_config_safe(stream_name, stream_id) -%}
    
    {{ print("🏁 Ending " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ")") }}
    
    {# Use consolidated end_stream function #}
    {{ end_stream(stream_name, stream_id) }}
    
    -- Display final stream status
    {% set status = get_stream_status(stream_name, stream_id) %}
    {{ print("📊 Final Stream Status:") }}
    {{ print("   Stream Name: " ~ stream_name ~ " (" ~ config.strm_type ~ ")") }}
    {{ print("   Business Date: " ~ status.business_date) }}
    {{ print("   Cycle Number: " ~ status.cycle_num ~ "/" ~ config.max_cycles_per_day) }}
    {{ print("   Status: " ~ status.stream_status) }}
    {{ print("   Completed At: " ~ status.updt_ts) }}
{% endmacro %}

{% macro get_stream_status_op(stream_name) %}
    {%- if not stream_name -%}
        {{ exceptions.raise_compiler_error("stream_name parameter is required. Usage: dbt run-operation get_stream_status_op --args '{stream_name: \"BV_PDS_TRAN\"}'") }}
    {%- endif -%}
    
    {%- set stream_id = get_stream_id_by_name(stream_name) -%}
    {%- set config = get_stream_config_safe(stream_name, stream_id) -%}
    
    {{ print("📊 " ~ stream_name ~ " Stream Status") }}
    
    {# Use consolidated get_stream_status function #}
    {% set status = get_stream_status(stream_name, stream_id) %}
    
    {% if status.exists %}
        {{ print("   Stream Name: " ~ stream_name ~ " (" ~ config.strm_type ~ ")") }}
        {{ print("   Stream ID: " ~ stream_id) }}
        {{ print("   Business Date: " ~ status.business_date) }}
        {{ print("   Cycle Start: " ~ status.cycle_start_ts) }}
        {{ print("   Cycle Number: " ~ status.cycle_num ~ "/" ~ config.max_cycles_per_day) }}
        {{ print("   Status: " ~ status.stream_status) }}
        {{ print("   Processing Flag: " ~ status.processing_flag ~ " (0=Ready, 1=Running, 2=Completed, 3=Error)") }}
        {{ print("   Created: " ~ status.inst_ts) }}
        {{ print("   Last Updated: " ~ status.updt_ts) }}
        {% if config.has_config %}
            {{ print("   DBT Tag: " ~ config.dbt_tag) }}
            {{ print("   Multiple Cycles Allowed: " ~ config.allow_multiple_cycles) }}
            {{ print("   Configuration: Enhanced (" ~ config.strm_desc ~ ")") }}
        {% else %}
            {{ print("   Configuration: Legacy (No DCF_T_STRM config)") }}
        {% endif %}
    {% else %}
        {{ print("   ❌ No stream record found for " ~ stream_name) }}
        {{ print("   💡 Use 'dbt run-operation start_stream_op --args \"{stream_name: \"" ~ stream_name ~ "\", business_date: \"2024-12-17\"}\"' to initialize") }}
    {% endif %}
{% endmacro %}

{% macro reset_stream_op(stream_name, business_date=none, cycle_num=none) %}
    {%- if not stream_name -%}
        {{ exceptions.raise_compiler_error("stream_name parameter is required. Usage: dbt run-operation reset_stream_op --args '{stream_name: \"BV_PDS_TRAN\", business_date: \"2024-12-17\"}'") }}
    {%- endif -%}
    
    {%- set stream_id = get_stream_id_by_name(stream_name) -%}
    {%- set config = get_stream_config_safe(stream_name, stream_id) -%}
    
    {% if business_date and cycle_num %}
        {{ print("🔄 Resetting " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") for business date " ~ business_date ~ " cycle " ~ cycle_num ~ " to READY status") }}
        
        {% set reset_query %}
            UPDATE {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
            SET 
                PROCESSING_FLAG = 0,  -- 0 = READY
                STREAM_STATUS = 'READY',
                UPDT_TS = CURRENT_TIMESTAMP()
            WHERE STRM_ID = {{ stream_id }}
              AND STRM_NAME = '{{ stream_name }}'
              AND BUS_DT = '{{ business_date }}'::DATE
              AND BUSINESS_DATE_CYCLE_NUM = {{ cycle_num }}
        {% endset %}
    {% elif business_date %}
        {{ print("🔄 Resetting " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") for business date " ~ business_date ~ " (all cycles) to READY status") }}
        
        {% set reset_query %}
            UPDATE {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
            SET 
                PROCESSING_FLAG = 0,  -- 0 = READY
                STREAM_STATUS = 'READY',
                UPDT_TS = CURRENT_TIMESTAMP()
            WHERE STRM_ID = {{ stream_id }}
              AND STRM_NAME = '{{ stream_name }}'
              AND BUS_DT = '{{ business_date }}'::DATE
        {% endset %}
    {% else %}
        {{ print("🔄 Resetting ALL business dates for " ~ stream_name ~ " stream (ID: " ~ stream_id ~ ") to READY status") }}
        
        {% set reset_query %}
            UPDATE {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
            SET 
                PROCESSING_FLAG = 0,  -- 0 = READY
                STREAM_STATUS = 'READY',
                UPDT_TS = CURRENT_TIMESTAMP()
            WHERE STRM_ID = {{ stream_id }}
              AND STRM_NAME = '{{ stream_name }}'
        {% endset %}
    {% endif %}
    
    {% do run_query(reset_query) %}
    
    {{ print("✅ Stream reset to READY status") }}
    
    -- Display updated status
    {% set status = get_stream_status(stream_name, stream_id) %}
    {{ print("📊 Latest Stream Status: " ~ status.stream_status ~ " for business date " ~ status.business_date ~ " (Cycle: " ~ status.cycle_num ~ "/" ~ config.max_cycles_per_day ~ ")") }}
{% endmacro %}

{% macro get_stream_history_op(stream_name, limit_days=10) %}
    {%- if not stream_name -%}
        {{ exceptions.raise_compiler_error("stream_name parameter is required. Usage: dbt run-operation get_stream_history_op --args '{stream_name: \"BV_PDS_TRAN\"}'") }}
    {%- endif -%}
    
    {%- set stream_id = get_stream_id_by_name(stream_name) -%}
    {%- set config = get_stream_config_safe(stream_name, stream_id) -%}
    
    {{ print("📅 " ~ stream_name ~ " Stream History (Last " ~ limit_days ~ " records) - " ~ config.strm_type ~ " Stream") }}
    
    {% set history_query %}
        SELECT 
            BUS_DT,
            BUSINESS_DATE_CYCLE_NUM,
            BUSINESS_DATE_CYCLE_START_TS,
            PROCESSING_FLAG,
            STREAM_STATUS,
            INST_TS,
            UPDT_TS
        FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
        WHERE STRM_ID = {{ stream_id }}
          AND STRM_NAME = '{{ stream_name }}'
        ORDER BY BUS_DT DESC, BUSINESS_DATE_CYCLE_NUM DESC, UPDT_TS DESC
        LIMIT {{ limit_days }}
    {% endset %}
    
    {% set history_results = run_query(history_query) %}
    
    {% if history_results|length == 0 %}
        {{ print("   ❌ No historical records found for " ~ stream_name) }}
    {% else %}
        {{ print("   📊 Historical Records (Max " ~ config.max_cycles_per_day ~ " cycles/day):") }}
        {{ print("   Business Date | Cycle | Status     | Flag | Cycle Start         | Created             | Updated") }}
        {{ print("   " ~ "-" * 100) }}
        
        {% for row in history_results %}
            {%- set bus_dt = row[0] -%}
            {%- set cycle_num = row[1] -%}
            {%- set cycle_start = row[2] -%}
            {%- set flag = row[3] -%}
            {%- set status = row[4] -%}
            {%- set inst_ts = row[5] -%}
            {%- set updt_ts = row[6] -%}
            {{ print("   " ~ bus_dt ~ " | " ~ cycle_num ~ "     | " ~ status ~ " | " ~ flag ~ "    | " ~ cycle_start ~ " | " ~ inst_ts ~ " | " ~ updt_ts) }}
        {% endfor %}
        
        {{ print("   ") }}
        {{ print("   Processing Flags: 0=Ready, 1=Running, 2=Completed, 3=Error") }}
        {{ print("   Stream Type: " ~ config.strm_type ~ " (Max " ~ config.max_cycles_per_day ~ " cycles/day)") }}
        {% if config.has_config %}
            {{ print("   Multiple Cycles Allowed: " ~ config.allow_multiple_cycles) }}
            {{ print("   Configuration: Enhanced (" ~ config.strm_desc ~ ")") }}
        {% else %}
            {{ print("   Configuration: Legacy (No DCF_T_STRM config)") }}
        {% endif %}
    {% endif %}
{% endmacro %} 