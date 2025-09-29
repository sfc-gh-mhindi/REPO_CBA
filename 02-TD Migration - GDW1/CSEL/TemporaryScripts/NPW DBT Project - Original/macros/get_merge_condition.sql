{% macro cmerge_condition(tgt_table) %}
    {% if tgt_table == 'appt_dept' %}
        {{ return('tgt."appt_i" = src.APPT_I') }}
    {% elif tgt_table == 'dept_appt' %}
        {{ return('tgt.appt_i = src.APPT_I AND tgt.dept_i = src.DEPT_I') }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown tgt_table value: " ~ tgt_table) }}
    {% endif %}
{% endmacro %}