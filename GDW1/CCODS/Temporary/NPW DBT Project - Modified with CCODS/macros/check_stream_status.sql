{% macro check_stream_status(run_stream=none, app_release=none, ctl_schema=none) %}
    {% do print("============================================") %}
    {% do print("Checking stream status for stream: " ~ run_stream) %}
    {% do print("Application release: " ~ app_release) %}
    {% do print("Control schema: " ~ ctl_schema) %}
    {% do print("============================================") %}
    
    {% if run_stream is none or app_release is none or ctl_schema is none %}
        {{ exceptions.raise_compiler_error("Missing required parameters. Please provide run_stream, app_release, and ctl_schema.") }}
    {% endif %}
    
    {# Check if run stream exists #}
    {% set run_stream_exists_query %}
        SELECT COUNT(*) - 1 AS RUN_STRM_COUNT
        FROM {{ source(ctl_schema, 'run_strm_tmpl') }}
        WHERE RUN_STRM_C = '{{ run_stream }}'
        AND SYST_C = '{{ app_release }}'
    {% endset %}
    {% set run_stream_count = run_query(run_stream_exists_query).columns[0].values()[0] %}

    {# Check run stream flags #}
    {% if run_stream_count != -1 %}
        {% set check_flags_query %}
            SELECT 
                RUN_STRM_C,
                RUN_STRM_ABRT_F,
                RUN_STRM_ACTV_F,
                CASE WHEN TRIM(RUN_STRM_ABRT_F) <> 'Y' THEN 'N' ELSE 'Y' END AS StreamAborted,
                CASE WHEN TRIM(RUN_STRM_ACTV_F) = 'I' THEN 'N' ELSE 'Y' END AS StreamActive
            FROM {{ source(ctl_schema, 'run_strm_tmpl') }}
            WHERE RUN_STRM_C = '{{ run_stream }}'
            AND SYST_C = '{{ app_release }}'
        {% endset %}
        {% set flags = run_query(check_flags_query) %}
        
        {% if flags|length > 0 %}
            {% set stream_aborted = flags.columns[3].values()[0] %}
            {% set stream_active = flags.columns[4].values()[0] %}
            
            {% if stream_aborted == 'Y' %}
                {{ exceptions.raise_compiler_error('Run stream ' ~ run_stream ~ ' has aborted flag set to Y in RUN_STRM_TMPL.') }}
            {% endif %}
            
            {% if stream_active == 'Y' %}
                {{ exceptions.raise_compiler_error('Run stream ' ~ run_stream ~ ' has active flag set to Y in RUN_STRM_TMPL.') }}
            {% endif %}
            
            {# If no errors, proceed with update #}
            {% if stream_aborted == 'N' and stream_active == 'N' %}
                {% set update_query %}
                    UPDATE {{ source(ctl_schema, 'run_strm_tmpl') }}
                    SET RUN_STRM_ABRT_F = 'N',
                        RUN_STRM_ACTV_F = 'A',
                        RECD_CRAT_S = CURRENT_TIMESTAMP
                    WHERE RUN_STRM_C = '{{ run_stream }}'
                    AND SYST_C = '{{ app_release }}'
                {% endset %}
                {% do run_query(update_query) %}
                {% do print("Stream status updated successfully!") %}
                {% do print("- RUN_STRM_ABRT_F set to: N") %}
                {% do print("- RUN_STRM_ACTV_F set to: A") %}
            {% endif %}
        {% endif %}
    {% else %}
        {{ exceptions.raise_compiler_error('Run stream ' ~ run_stream ~ ' does not exist in control table RUN_STRM_TMPL. Add run stream entry in table and re-run process.') }}
    {% endif %}
{% endmacro %}
