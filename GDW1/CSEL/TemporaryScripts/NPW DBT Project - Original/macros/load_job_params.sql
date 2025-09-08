{% macro cvar(var_name) %}
  {% set model_path = model.path.replace('\\', '/').split('/')[-2] %}
  {% set all_job_vars = var("job_params") %}
  {% set param_dict = all_job_vars.get(model_path, {}) %}
  {%- if var(var_name, none) is not none -%}
    {% set return_val = var(var_name) %}
  {%- else -%}
    {%- if var_name in param_dict -%}
        {% set value = param_dict[var_name] %}
        {%- if value == '$PROJDEF' -%}
            {% set return_val = var(var_name, var('envs').get(var_name.replace('$', 'env_'))) %}
        {%- else -%}
            {% set return_val = var(var_name, value) %}
        {%- endif -%}
    {%- else -%}
	    {{ exceptions.raise_compiler_error("Variable '" ~ var_name ~ "' not found in the job parameter!!!") }}
    {%- endif -%}
  {%- endif -%}
  {{ return(return_val) }}
{% endmacro %}
