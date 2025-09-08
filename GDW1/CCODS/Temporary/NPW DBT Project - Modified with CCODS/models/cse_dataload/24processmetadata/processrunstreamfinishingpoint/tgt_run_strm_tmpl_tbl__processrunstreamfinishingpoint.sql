{{
    config(
        post_hook=[
            'UPDATE '~ cvar("stg_ctl_db") ~'.'~ cvar("ctl_schema") ~ '.run_strm_tmpl tgt SET tgt.RUN_STRM_ABRT_F = src.RUN_STRM_ABRT_F, tgt.RUN_STRM_ACTV_F = src.RUN_STRM_ACTV_F, tgt.RECD_CRAT_S = src.RECD_CRAT_S FROM {{ this }} src WHERE src.RUN_STRM_C = tgt.RUN_STRM_C'
        ]
    )
}}

select 
    '{{ cvar("run_stream") }}' as RUN_STRM_C,
    'N' as RUN_STRM_ABRT_F,
    'I' as RUN_STRM_ACTV_F,
    current_timestamp as RECD_CRAT_S
from {{ ref("xfm_rs_finish__processrunstreamfinishingpoint") }}
