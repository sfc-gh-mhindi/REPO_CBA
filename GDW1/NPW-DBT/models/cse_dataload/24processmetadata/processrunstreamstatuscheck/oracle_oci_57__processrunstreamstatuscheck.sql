select count(*) - 1 as run_strm_count
from {{ cvar("stg_ctl_db") }}.{{ cvar("ctl_schema") }}.run_strm_tmpl
where run_strm_c = '{{ cvar("run_stream") }}' and syst_c = '{{ cvar("app_release") }}'
