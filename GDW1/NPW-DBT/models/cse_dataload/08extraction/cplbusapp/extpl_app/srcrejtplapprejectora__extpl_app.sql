SELECT PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	TO_CHAR(ORIG_ETL_D, 'YYYYMMDD') AS ORIG_ETL_D

FROM {{ cvar("stg_ctl_db") }}.{{ cvar("stg_schema") }}.rejt_cpl_bus_app

WHERE EROR_C LIKE 'RPR%'

