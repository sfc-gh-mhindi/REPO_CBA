SELECT MAP_CSE_PACK_PDCT_PL."pl_pack_cat_id" as pl_pack_cat_id,
	MAP_CSE_PACK_PDCT_PL."pdct_n" as pdct_n,
	MAP_CSE_PACK_PDCT_PL."efft_d" as efft_d,
	MAP_CSE_PACK_PDCT_PL."expy_d" as expy_d

FROM {{ cvar('stg_ctl_db') }}.{{ cvar('gdw_acct_vw') }}.MAP_CSE_PACK_PDCT_PL AS MAP_CSE_PACK_PDCT_PL

WHERE TO_CHAR(MAP_CSE_PACK_PDCT_PL."efft_d", 'YYYYMMDD') <= '{{ cvar("etl_process_dt") }}'
	AND TO_CHAR(MAP_CSE_PACK_PDCT_PL."expy_d", 'YYYYMMDD') >= '{{ cvar("etl_process_dt") }}'

