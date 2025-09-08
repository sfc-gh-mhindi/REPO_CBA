WITH cpyrejtplapprejectora AS (
	SELECT PL_APP_ID,
		NOMINATED_BRANCH_ID AS NOMINATED_BRANCH_ID_R,
		PL_PACKAGE_CAT_ID AS PL_PACKAGE_CAT_ID_R,
		ORIG_ETL_D AS ORIG_ETL_D_R,
		ROW_NUMBER() OVER (
			PARTITION BY PL_APP_ID
			ORDER BY PL_APP_ID COLLATE 'en' ASC NULLS FIRST
		) AS sorting_order
	
	FROM {{ ref('srcrejtplapprejectora__extpl_app') }}
)

SELECT
	PL_APP_ID,
	NOMINATED_BRANCH_ID_R,
	PL_PACKAGE_CAT_ID_R,
	ORIG_ETL_D_R

FROM cpyrejtplapprejectora