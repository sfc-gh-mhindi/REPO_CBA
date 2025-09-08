with xfm_err_handling as (
    select etl_d
    from {{ ref("src_run_strm_etl_d_tbl__processrunstreamerrorhandler") }}
)

select SUBSTR('{{ cvar("err_msg") }}', POSITION(' in job ' IN '{{ cvar("err_msg") }}') + LENGTH(' in job '), LENGTH('{{ cvar("err_msg") }}')) as jobname
from xfm_err_handling
