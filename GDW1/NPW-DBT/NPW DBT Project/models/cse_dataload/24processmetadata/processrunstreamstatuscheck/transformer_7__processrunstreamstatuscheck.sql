with
    dslink11 as (
        select run_strm_c, run_strm_abrt_f, run_strm_actv_f
        from {{ ref("run_strm_tmpl__processrunstreamstatuscheck") }}
    ),
    dslink63 as (
        select run_strm_count
        from {{ ref("hashed_file_61__processrunstreamstatuscheck") }}
    )
select
    run_strm_c,
    run_strm_abrt_f,
    run_strm_actv_f,
    case when trim(run_strm_abrt_f) <> 'Y' then 'N' else 'Y' end as streamaborted,
    case when trim(run_strm_actv_f) = 'I' then 'N' else 'Y' end as streamactive,
    case
        when streamaborted = 'Y'
        then 'Run stream has aborted flag set to Y in RUN_STRM_TMPL.'
        when streamactive = 'Y'
        then 'Run stream has active flag set to Y in RUN_STRM_TMPL.'
        else ''
    end as writeerrortolog
from dslink11
