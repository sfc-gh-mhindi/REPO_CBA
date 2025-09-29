-- Common BTEQ to DBT Conversion Utilities

{% macro bteq_var(var_name) %}
  -- Convert BTEQ variable reference to DBT variable
  {{ var(var_name) }}
{% endmacro %}

{% macro volatile_to_cte(table_name, sql) %}
  -- Convert BTEQ volatile table to CTE
  WITH {{ table_name }} AS (
    {{ sql }}
  )
{% endmacro %}

{% macro copy_from_stage(stage_path, target_table) %}
  -- Convert BTEQ .IMPORT to Snowflake COPY
  COPY INTO {{ target_table }}
  FROM @{{ stage_path }}
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
{% endmacro %}

{% macro copy_to_stage(source_table, stage_path) %}
  -- Convert BTEQ .EXPORT to Snowflake COPY
  COPY INTO @{{ stage_path }}
  FROM {{ source_table }}
  FILE_FORMAT = (TYPE = 'CSV', HEADER = TRUE)
{% endmacro %}

{% macro intermediate_model_config() %}
  -- Standard configuration for intermediate models
  {{
    config(
      materialized='table',
      pre_hook="{{ register_process_instance(this.name) }}",
      post_hook="{{ update_process_status('SUCCESS') }}"
    )
  }}
{% endmacro %}

{% macro marts_model_config() %}
  -- Standard configuration for marts models  
  {{
    config(
      materialized='table',
      docs={'show': true}
    )
  }}
{% endmacro %}