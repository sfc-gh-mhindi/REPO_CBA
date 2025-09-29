with
    dslink11 as (
        select run_strm_c
        from {{ ref("transformer_7__processrunstreamstatuscheck") }}
        where streamaborted = 'N' and streamactive = 'N'
    )

select
    run_strm_c,
    'N' as run_strm_abrt_f,
    'A' as run_strm_actv_f,
    current_timestamp as recd_crat_s
from dslink11
