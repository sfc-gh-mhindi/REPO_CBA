{% macro handle_null(column_name, default_value='') -%}
    {#- 
        Emulates DataStage null handling behavior
        Usage: {{ handle_null('column_name', '0') }}
    -#}
    COALESCE(NULLIF(TRIM({{ column_name }}), ''), '{{ default_value }}')
{%- endmacro %}

{% macro null_to_empty(column_name) -%}
    {#- 
        Converts NULL to empty string (DataStage NullToEmpty function)
        Usage: {{ null_to_empty('column_name') }}
    -#}
    COALESCE({{ column_name }}, '')
{%- endmacro %}

{% macro is_not_null_or_empty(column_name) -%}
    {#- 
        Checks if a column is not null and not empty
        Usage: WHERE {{ is_not_null_or_empty('column_name') }}
    -#}
    ({{ column_name }} IS NOT NULL AND TRIM({{ column_name }}) != '')
{%- endmacro %}
