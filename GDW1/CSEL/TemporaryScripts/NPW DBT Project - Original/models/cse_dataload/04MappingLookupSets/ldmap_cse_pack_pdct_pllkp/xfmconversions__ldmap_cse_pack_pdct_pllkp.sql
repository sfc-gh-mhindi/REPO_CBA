SELECT
	TRIM(PL_PACK_CAT_ID) AS PL_PACKAGE_CAT_ID,
	TRIM(PDCT_N) AS PDCT_N

FROM {{ ref('srcmap_cse_pack_pdct_pltera__ldmap_cse_pack_pdct_pllkp') }}
