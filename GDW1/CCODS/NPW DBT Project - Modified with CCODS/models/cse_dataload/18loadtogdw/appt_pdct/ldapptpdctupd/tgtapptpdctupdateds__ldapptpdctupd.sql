select
    appt_pdct_i,
    appt_i,
    efft_d,
    expy_d,
    pros_key_expy_i 
from {{ cvar("intermediate_db") }}.{{ cvar('datasets_schema')}}.{{ cvar("base_dir") }}__dataset__{{ cvar("tgt_table_tbl") }}_U_{{ cvar("run_stream") }}_{{ cvar("etl_process_dt") }}__DS
