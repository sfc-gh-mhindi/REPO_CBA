WITH CpyRename AS (
	SELECT
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ORIG_ETL_D
	FROM {{ ref('srcplapppremapds__xfmpl_appfrmext') }}
)

SELECT * FROM CpyRename