WITH cte AS (
	SELECT dummy
	FROM {{ ref('before__ldmap_cse_pack_pdct_pllkp') }}
	WHERE 1=2
),
srcmap_cse_pack_pdct_pltera as(
SELECT pl_pack_cat_id as pl_pack_cat_id,
	pdct_n as pdct_n,
	efft_d as efft_d,
	expy_d as expy_d

FROM {{ cvar('stg_ctl_db') }}.{{ cvar('gdw_acct_vw') }}.MAP_CSE_PACK_PDCT_PL

WHERE TO_CHAR(efft_d, 'YYYYMMDD') <= '{{ cvar("etl_process_dt") }}'
	AND TO_CHAR(expy_d, 'YYYYMMDD') >= '{{ cvar("etl_process_dt") }}'

)

SELECT
pl_pack_cat_id,
pdct_n,
efft_d,
expy_d
FROM srcmap_cse_pack_pdct_pltera