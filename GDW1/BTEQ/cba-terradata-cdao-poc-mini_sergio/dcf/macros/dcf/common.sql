{% macro get_current_timestamp() %}
    current_timestamp()::timestamp_ntz(6)
{% endmacro %}

{% macro get_current_date() %}
{# 🚧 FUTURE USE: Current date utility - not currently used in active models #}
    current_date()::date
{% endmacro %}

{% macro get_max_timestamp() %}
    '9999-12-31 23:59:59'::timestamp_ntz(6)
{% endmacro %}

{% macro get_min_timestamp() %}
{# 🚧 FUTURE USE: Minimum timestamp utility - not currently used in active models #}
    '1900-01-01 00:00:00'::timestamp_ntz(6)
{% endmacro %}

{% macro get_max_date() %}
{# 🚧 FUTURE USE: Maximum date utility - not currently used in active models #}
    '9999-12-31'::date
{% endmacro %}

{% macro get_min_date() %}
{# 🚧 FUTURE USE: Minimum date utility - not currently used in active models #}
    '1900-01-01'::date
{% endmacro %}

{% macro dcf_database_ref() %}
  {{ var('dcf_database') }}.{{ var('dcf_schema') }}
{% endmacro %}

{# ============================================================================ #}
{# INTERMEDIATE TABLE CONFIGURATION MACROS                                     #}
{# ============================================================================ #}

{% macro intermediate_table_config() %}
  {#- 
    Macro to provide consistent database and schema configuration for intermediate tables
    Intermediate tables are created in the DCF database with alphanumeric schema names
    to work with catalog-linked database restrictions
  -#}
  
  database='{{ var('intermediate_database') }}',
  schema='{{ var('intermediate_schema') }}'
  
{% endmacro %}

{% macro intermediate_database() %}
  {#- Return just the database name for intermediate tables -#}
  {{ var('intermediate_database') }}
{% endmacro %}

{% macro intermediate_schema() %}
  {#- Return just the schema name for intermediate tables -#}
  {{ var('intermediate_schema') }}
{% endmacro %}

{% macro intermediate_ref(table_name) %}
  {#- Reference an intermediate table with proper database and schema -#}
  {{ intermediate_database() }}.{{ intermediate_schema() }}.{{ table_name }}
{% endmacro %}

{% macro get_stream_id(stream_name) %}
{# Get stream ID from DCF_T_STRM table for given stream name #}
(SELECT STRM_ID FROM {{ dcf_database_ref() }}.DCF_T_STRM WHERE STRM_NAME = '{{ stream_name }}' AND STRM_STATUS = 'ACTIVE')
{% endmacro %}

{% macro get_ctl_id_by_stream_name(stream_name) %}
{# Get control ID by stream name from DCF_T_STRM #}
(SELECT CTL_ID FROM {{ dcf_database_ref() }}.DCF_T_STRM WHERE STRM_NAME = '{{ stream_name }}' AND STRM_STATUS = 'ACTIVE')
{% endmacro %}

{% macro get_process_id() %}
{# Generate new process ID using timestamp hash instead of sequence for catalog-linked database compatibility #}
ABS(HASH(CONCAT(CURRENT_TIMESTAMP()::VARCHAR, '{{ invocation_id }}', RANDOM()))) % 2147483647
{% endmacro %}

{% macro get_current_process_instance_id(process_name, stream_name) %}
{# Get the current process instance ID that was created by register_process_instance #}
(SELECT PRCS_INST_ID 
 FROM {{ dcf_database_ref() }}.DCF_T_PRCS_INST 
 WHERE PRCS_NAME = '{{ process_name }}' 
   AND STRM_NAME = '{{ stream_name }}' 
   AND PRCS_STATUS = 'RUNNING'
   AND PRCS_BUS_DT = (SELECT BUS_DT FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
                      WHERE STRM_NAME = '{{ stream_name }}' 
                      ORDER BY BUS_DT DESC LIMIT 1)
 ORDER BY PRCS_START_TS DESC 
 LIMIT 1)
{% endmacro %}

{% macro get_business_date(stream_name) %}
{# 🚧 FUTURE USE: Simple business date lookup - not currently used in active models #}
{# Get current business date for the stream from DCF_T_STRM_BUS_DT #}
(SELECT BUS_DT FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
 WHERE STRM_NAME = '{{ stream_name }}')
{% endmacro %}

{% macro get_business_date_cycle_start(stream_name) %}
{# 🚧 FUTURE USE: Business date cycle start lookup - not currently used in active models #}
{# Get business date cycle start timestamp #}
(SELECT BUSINESS_DATE_CYCLE_START_TS FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT
 WHERE STRM_NAME = '{{ stream_name }}')
{% endmacro %}

{# ============================================================================ #}
{# AUDIT COLUMNS MACROS                                                         #}
{# ============================================================================ #}

{% macro dcf_audit_columns(process_name, stream_name) %}
    {{ get_current_timestamp() }} as EFFT_TS,
    {{ get_max_timestamp() }} as EXPY_TS,
    cast(RPAD('{{ process_name }}', 30, ' ') as varchar(30)) as PRCS_NAME,
    cast('{{ stream_name }}' as varchar(30)) as STRM_NAME,
    cast(0 as integer) as REC_DEL_FLG,
    {{ get_current_timestamp() }} as INST_TS,
    {{ get_current_timestamp() }} as UPDT_TS,
    cast('{{ invocation_id }}' as varchar(36)) as SESSION_ID,
    cast('{{ target.warehouse }}' as varchar(30)) as WAREHOUSE_NAME,
    cast('{{ target.user }}' as varchar(30)) as CREATED_BY
{% endmacro %}

{%- macro dcf_audit_columns_extended(process_name, stream_name) -%}
    -- DCF Audit Columns with proper business logic
    -- Based on original CTLFW_Tfm_Insert_Append procedure
    
    -- EFFT_D: Current active business date from DCF_T_STRM_BUS_DT
    (SELECT BUS_DT FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
     WHERE STRM_NAME = '{{ stream_name }}' AND PROCESSING_FLAG = 1) AS EFFT_D,
    
    -- EXPY_D: High date (9999-12-31)
    DATE '9999-12-31' AS EXPY_D,
    
    -- EFFT_TS: Business_Date_Cycle_Start_Ts for intraday cycles (NOT NULL to match target)
    COALESCE(
        (SELECT BUSINESS_DATE_CYCLE_START_TS FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT 
         WHERE STRM_NAME = '{{ stream_name }}' AND PROCESSING_FLAG = 1),
        CURRENT_TIMESTAMP()
    )::TIMESTAMP_NTZ(6) AS EFFT_TS,
    
    -- EXPY_TS: High date with end of day timestamp (9999-12-31 23:59:59.999999)
    TIMESTAMP '9999-12-31 23:59:59.999999'::TIMESTAMP_NTZ(6) AS EXPY_TS,
    
    -- PROCESS_ID: Generated from sequence for each new process instance
    {{ get_process_id() }} AS PROCESS_ID,
    
    -- PROCESS_NAME: Fixed process name with RPAD to match Teradata CHAR space-padding
    RPAD(COALESCE('{{ process_name }}', 'UNKNOWN_PROCESS'), 30, ' ') AS PROCESS_NAME,
    
    -- UPDATE_PROCESS_ID: NULL for Insert Append pattern (cast to NUMBER to match target)
    CAST(NULL AS NUMBER) AS UPDATE_PROCESS_ID,
    
    -- UPDATE_PROCESS_NAME: NULL for Insert Append pattern  
    NULL AS UPDATE_PROCESS_NAME,
    
    -- RECORD_DELETED_FLAG: Always 0 for active records
    0 AS RECORD_DELETED_FLAG,
    
    -- CTL_ID: Stream control ID (dynamic lookup from DCF_T_STRM)
    {{ get_ctl_id_by_stream_name(stream_name) }} AS CTL_ID

{%- endmacro -%}

{% macro dcf_standard_columns(process_name, stream_name) %}
    -- DCF Standard Columns for NBC POC models
    CURRENT_DATE() AS EFFT_D,
    DATE('9999-12-31') AS EXPY_D,
    0 AS RECORD_DELETED_FLAG,
    {{ get_ctl_id_by_stream_name(stream_name) }} AS CTL_ID,
    '{{ process_name }}' AS PROCESS_NAME,
    {{ get_process_id() }} AS PROCESS_ID,
    '{{ process_name }}' AS UPDATE_PROCESS_NAME,
    {{ get_process_id() }} AS UPDATE_PROCESS_ID,
    CURRENT_TIMESTAMP() AS EFFT_TS,
    TO_TIMESTAMP_LTZ('9999-12-31 23:59:59.999999') AS EXPY_TS,
    CURRENT_TIMESTAMP() AS RECD_ISRT_DTTM
{% endmacro %}

{# ============================================================================ #}
{# COMPREHENSIVE DATABASE & SCHEMA REFERENCE MACROS                           #}
{# ============================================================================ #}

{# Complete database/schema reference macros to eliminate hardcoding #}
{# ⚠️ NOTE: Many of these macros are built for future use but not currently used in active models #}

{% macro raw_database_ref() %}
{# 🚧 FUTURE USE: Raw layer database reference - not currently used in active models #}
  {{ var('dcf_view_database') }}.{{ var('raw_schema') }}
{% endmacro %}

{% macro bus_database_ref() %}
{# 🚧 FUTURE USE: Business layer database reference - not currently used in active models #}
  {{ var('dcf_view_database') }}.{{ var('bus_schema') }}
{% endmacro %}

{% macro ref_database_ref() %}
{# 🚧 FUTURE USE: Reference layer database reference - not currently used in active models #}
  {{ var('dcf_view_database') }}.{{ var('ref_schema') }}
{% endmacro %}

{% macro cox_database_ref() %}
  {{ var('dcf_view_database') }}.{{ var('cox_schema') }}
{% endmacro %}

{% macro inp_database_ref() %}
  {{ var('dcf_view_database') }}.{{ var('inp_schema') }}
{% endmacro %}

{% macro out_database_ref() %}
  {{ var('dcf_view_database') }}.{{ var('out_schema') }}
{% endmacro %}

{% macro bal_database_ref() %}
  {{ var('dcf_view_database') }}.{{ var('bal_schema') }}
{% endmacro %}

{% macro target_database_ref() %}
  {{ var('target_database') }}.{{ var('target_bal_schema') }}
{% endmacro %}

{# Source reference macros for use in models #}
{% macro source_ref(source_name, table_name) %}
{# 🚧 FUTURE USE: Generic source reference - not currently used in active models #}
  {#- Generic source reference that uses proper database/schema variables -#}
  {% if source_name == 'raw' or source_name == 'raw_pds' %}
    {{ raw_database_ref() }}.{{ table_name }}
  {% elif source_name == 'bus_layer' %}
    {{ bus_database_ref() }}.{{ table_name }}
  {% elif source_name == 'ref_layer' %}
    {{ ref_database_ref() }}.{{ table_name }}
  {% elif source_name == 'cox_data' %}
    {{ cox_database_ref() }}.{{ table_name }}
  {% else %}
    {{ exceptions.raise_compiler_error("Unknown source: " ~ source_name) }}
  {% endif %}
{% endmacro %}

{# Model configuration macros for different layers #}
{# 🚧 FUTURE USE: Model configuration helpers - not currently used in active models #}
{% macro raw_model_config() %}
  database='{{ var("dcf_view_database") }}',
  schema='{{ var("raw_schema") }}'
{% endmacro %}

{% macro staging_model_config() %}
  database='{{ var("intermediate_database") }}',
  schema='{{ var("intermediate_schema") }}'
{% endmacro %}

{% macro intermediate_model_config() %}
  database='{{ var("intermediate_database") }}',
  schema='{{ var("intermediate_schema") }}'
{% endmacro %}

{% macro marts_model_config() %}
  database='{{ var("intermediate_database") }}',
  schema='{{ var("intermediate_schema") }}'
{% endmacro %}

{% macro target_model_config() %}
  database='{{ var("target_database") }}',
  schema='{{ var("target_bal_schema") }}'
{% endmacro %}

{# Schema initialization reference for DCF tables #}
{% macro dcf_catalog_ref() %}
  {{ var('target_database') }}.{{ var('target_bal_schema') }}
{% endmacro %}

{# Helper macro to get database name only #}
{% macro get_database(layer) %}
{# 🚧 FUTURE USE: Database name helper - not currently used in active models #}
  {% if layer in ['raw', 'bus', 'ref', 'cox', 'inp', 'out', 'bal'] %}
    {{ var('dcf_view_database') }}
  {% elif layer in ['staging', 'intermediate', 'marts', 'dcf'] %}
    {{ var('intermediate_database') }}
  {% elif layer == 'target' %}
    {{ var('target_database') }}
  {% else %}
    {{ exceptions.raise_compiler_error("Unknown layer: " ~ layer) }}
  {% endif %}
{% endmacro %}

{# Helper macro to get schema name only #}
{% macro get_schema(layer) %}
{# 🚧 FUTURE USE: Schema name helper - not currently used in active models #}
  {% if layer == 'raw' %}
    {{ var('raw_schema') }}
  {% elif layer == 'bus' %}
    {{ var('bus_schema') }}
  {% elif layer == 'ref' %}
    {{ var('ref_schema') }}
  {% elif layer == 'cox' %}
    {{ var('cox_schema') }}
  {% elif layer == 'inp' %}
    {{ var('inp_schema') }}
  {% elif layer == 'out' %}
    {{ var('out_schema') }}
  {% elif layer == 'bal' %}
    {{ var('bal_schema') }}
  {% elif layer in ['staging', 'intermediate', 'dcf'] %}
    {{ var('intermediate_schema') }}
  {% elif layer == 'target' %}
    {{ var('target_bal_schema') }}
  {% else %}
    {{ exceptions.raise_compiler_error("Unknown layer: " ~ layer) }}
  {% endif %}
{% endmacro %}