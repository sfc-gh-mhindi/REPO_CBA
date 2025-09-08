WITH outcheckplappidnullssorted
AS (
	SELECT PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ROW_NUMBER() OVER (
			PARTITION BY PL_APP_ID
			ORDER BY PL_APP_ID COLLATE 'en' ASC NULLS FIRST
		) AS sorting_order
	
	FROM {{ ref('xfmcheckplappidnulls__extpl_app') }}
	
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
	)

SELECT 
	outcheckplappidnullssorted.PL_APP_ID,
	outcheckplappidnullssorted.NOMINATED_BRANCH_ID,
	outcheckplappidnullssorted.PL_PACKAGE_CAT_ID,
	CpyRejtPlAppRejectOra.PL_APP_ID AS PL_APP_ID_R,
	CpyRejtPlAppRejectOra.NOMINATED_BRANCH_ID_R,
	CpyRejtPlAppRejectOra.PL_PACKAGE_CAT_ID_R,
	CpyRejtPlAppRejectOra.ORIG_ETL_D_R

FROM outcheckplappidnullssorted

FULL JOIN {{ ref('cpyrejtplapprejectora__extpl_app') }} CpyRejtPlAppRejectOra
	ON outcheckplappidnullssorted.PL_APP_ID = CpyRejtPlAppRejectOra.PL_APP_ID

