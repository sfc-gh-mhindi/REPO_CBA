/*
  GDW1 Common Utility Macros
  
  Utility functions for BTEQ migration and data processing
*/

/*
  File Stage Reference Macro
  
  Returns the proper stage reference for file operations
*/
{%- macro file_stage_ref() -%}
    @{{ var('file_stage_name') }}
{%- endmacro -%}

/*
  Get Current Process Key Macro
  
  Generates or retrieves current process key for batch processing
*/
{%- macro get_current_process_key(stream_name) -%}
    {% set sql %}
    SELECT COALESCE(MAX(PROS_KEY_I), 0) + 1 as next_key
    FROM {{ var('CAD_PROD_MACRO') }}.PROCESS_CONTROL
    WHERE STRM_NAME = '{{ stream_name }}'
        AND BUS_DT = {{ gdw1_business_date() }}
    {% endset %}
    
    {% set results = run_query(sql) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% else %}
        {{ return(1) }}
    {% endif %}
{%- endmacro -%}

/*
  Get Current Batch Key Macro
  
  Generates or retrieves current batch key for processing
*/
{%- macro get_current_batch_key(stream_name) -%}
    {% set sql %}
    SELECT COALESCE(MAX(BTCH_KEY_I), 0) + 1 as next_key
    FROM {{ var('CAD_PROD_MACRO') }}.BATCH_CONTROL
    WHERE STRM_NAME = '{{ stream_name }}'
        AND BUS_DT = {{ gdw1_business_date() }}
    {% endset %}
    
    {% set results = run_query(sql) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% else %}
        {{ return(1) }}
    {% endif %}
{%- endmacro -%}

/*
  BTEQ Variable Mapping Macro
  
  Maps original BTEQ variables to dbt variables
*/
{%- macro bteq_var(variable_name) -%}
    {%- if variable_name == 'CAD_PROD_DATA' -%}
        {{ var('CAD_PROD_DATA') }}
    {%- elif variable_name == 'CAD_PROD_MACRO' -%}
        {{ var('CAD_PROD_MACRO') }}
    {%- elif variable_name == 'DDSTG' -%}
        {{ var('DDSTG') }}
    {%- elif variable_name == 'VTECH' -%}
        {{ var('VTECH') }}
    {%- elif variable_name == 'VCBODS' -%}
        {{ var('VCBODS') }}
    {%- elif variable_name == 'VEXTR' -%}
        {{ var('VEXTR') }}
    {%- elif variable_name == 'STARMACRDB' -%}
        {{ var('STARMACRDB') }}
    {%- elif variable_name == 'ENV_C' -%}
        {{ var('ENV_C') }}
    {%- elif variable_name == 'STRM_C' -%}
        {{ var('STRM_C') }}
    {%- elif variable_name == 'GDW_USER' -%}
        {{ var('GDW_USER') }}
    {%- elif variable_name == 'TBSHORT' -%}
        {{ var('TBSHORT') }}
    {%- else -%}
        {{ var(variable_name) }}
    {%- endif -%}
{%- endmacro -%}

/*
  Create Temporary Table Macro
  
  Creates temporary tables replacing BTEQ volatile tables
*/
{%- macro create_temp_table(table_name, select_sql) -%}
    CREATE OR REPLACE TEMPORARY TABLE {{ table_name }} AS
    {{ select_sql }}
{%- endmacro -%}

/*
  Drop Temporary Table Macro
  
  Safely drops temporary tables
*/
{%- macro drop_temp_table(table_name) -%}
    DROP TABLE IF EXISTS {{ table_name }}
{%- endmacro -%}

/*
  File Copy Macro
  
  Handles file operations using Snowflake stages
*/
{%- macro copy_from_stage(stage_path, target_table, file_format='CSV') -%}
    COPY INTO {{ target_table }}
    FROM {{ file_stage_ref() }}/{{ stage_path }}
    FILE_FORMAT = (TYPE = '{{ file_format }}')
{%- endmacro -%}

{%- macro copy_to_stage(source_table, stage_path, file_format='CSV') -%}
    COPY INTO {{ file_stage_ref() }}/{{ stage_path }}
    FROM {{ source_table }}
    FILE_FORMAT = (TYPE = '{{ file_format }}' HEADER = FALSE)
{%- endmacro -%}

/*
  Date Function Conversions
  
  Converts Teradata date functions to Snowflake equivalents
*/
{%- macro add_months(date_expr, months) -%}
    DATEADD(MONTH, {{ months }}, {{ date_expr }})
{%- endmacro -%}

{%- macro current_date_minus(days) -%}
    DATEADD(DAY, -{{ days }}, CURRENT_DATE())
{%- endmacro -%}

{%- macro current_date_plus(days) -%}
    DATEADD(DAY, {{ days }}, CURRENT_DATE())
{%- endmacro -%}

/*
  Process Key Generation Macros
  
  Handles process and batch key generation for tracking
*/
{%- macro generate_process_key() -%}
    {{ get_current_process_key(var('default_stream_name')) }}
{%- endmacro -%}

{%- macro generate_batch_key() -%}
    {{ get_current_batch_key(var('default_stream_name')) }}
{%- endmacro -%}

/*
  Error Handling Macro
  
  Provides consistent error handling across models
*/
{%- macro handle_model_error(error_message) -%}
    {% do log_gdw1_error(this.name, var('default_stream_name'), error_message) %}
    {{ exceptions.raise_compiler_error("Model " ~ this.name ~ " failed: " ~ error_message) }}
{%- endmacro -%}

/*
  Model Configuration Helper
  
  Provides standard configuration for different model types
*/
{%- macro staging_model_config(source_table, stream_name=none) -%}
    {{
        config(
            materialized='view',
            tags=['staging', 'gdw1_migration'],
            pre_hook=[
                "{{ log_gdw1_exec_msg(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 10, 'Starting staging model processing for " ~ source_table ~ "') }}"
            ]
        )
    }}
{%- endmacro -%}

{%- macro intermediate_model_config(process_type, stream_name=none) -%}
    {{
        config(
            materialized='table',
            tags=['intermediate', 'gdw1_migration', process_type],
            pre_hook=[
                "{{ register_gdw1_process_instance(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "') }}",
                "{{ log_gdw1_exec_msg(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 10, 'Starting intermediate processing for " ~ process_type ~ "') }}"
            ],
            post_hook=[
                "{{ log_gdw1_exec_msg(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 10, 'Completed intermediate processing for " ~ process_type ~ "') }}",
                "{{ update_gdw1_process_status(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 'COMPLETED', 'Model executed successfully') }}"
            ]
        )
    }}
{%- endmacro -%}

{%- macro marts_model_config(business_area, stream_name=none) -%}
    {{
        config(
            materialized='table',
            tags=['marts', 'gdw1_migration', business_area],
            pre_hook=[
                "{{ register_gdw1_process_instance(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "') }}",
                "{{ log_gdw1_exec_msg(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 10, 'Starting marts processing for " ~ business_area ~ "') }}"
            ],
            post_hook=[
                "{{ log_gdw1_exec_msg(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 10, 'Completed marts processing for " ~ business_area ~ "') }}",
                "{{ update_gdw1_process_status(this.name, '" ~ (stream_name or var('default_stream_name')) ~ "', 'COMPLETED', 'Model executed successfully') }}"
            ]
        )
    }}
{%- endmacro -%} 