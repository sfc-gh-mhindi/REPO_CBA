with
    to_err_handling_xfm as (
        select etl_d
        from {{ ref("processrunstreamerrorhandler__src_run_strm_etl_d_tbl") }}
    )

select

    SUBSTR('{{ cvar("err_msg") }}', POSITION(' in job ' IN '{{ cvar("err_msg") }}') + LENGTH(' in job '), LENGTH('{{ cvar("err_msg") }}')) as jobname
    
from to_err_handling_xfm
