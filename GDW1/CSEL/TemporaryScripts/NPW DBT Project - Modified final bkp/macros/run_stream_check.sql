{% macro run_stream_check(model_name) %}
  {% if execute %}
    {% set query %}
      select writeerrortolog
      from {{ ref(model_name) }} 
      where nullif(writeerrortolog,'') is not null
    {% endset %}
    
    {% set results = run_query(query) %}
    {% if results|length > 0 %}
      {% set error_msg %}
        {{ results.columns[0].values()[0] }}
      {% endset %}
      {{ exceptions.warn(error_msg) }}
    {% endif %}
  {% endif %}
{% endmacro %}
