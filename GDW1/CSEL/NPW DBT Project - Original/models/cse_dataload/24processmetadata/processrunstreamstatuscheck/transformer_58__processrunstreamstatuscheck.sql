with transformer_58 as (
    select run_strm_count
    from {{ ref("oracle_oci_57__processrunstreamstatuscheck") }}
)
select
    run_strm_count,
    case
        when run_strm_count = -1
        then
            'Run stream '
            || '{{ cvar("run_stream") }}'
            || ' does not exist in control table RUN_STRM_TMPL. Add run stream entry in table and re-run process.'
        else ''
    end as writeerrortolog
from transformer_58
