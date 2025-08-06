select run_strm_c, run_strm_abrt_f, run_strm_actv_f, syst_c
from {{ cvar('stg_ctl_db') }}.{{ cvar("ctl_schema") }}.run_strm_tmpl
where run_strm_c = '{{ cvar("run_stream") }}' and syst_c = '{{ cvar("app_release") }}'
