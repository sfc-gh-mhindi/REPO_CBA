select CAST(ETL_D as varchar) as etl_d
from {{ cvar('stg_ctl_db') }}.{{ cvar("ctl_schema") }}.run_strm_etl_d 
where rs_m = '{{ cvar("run_stream") }}'


