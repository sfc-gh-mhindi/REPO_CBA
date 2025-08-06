WITH insourcerec
AS (
	SELECT PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		'{{ cvar("etl_process_dt") }}' AS ORIG_ETL_D
	
	FROM {{ ref('xfmseparaterejectwithoutsourceandtherest__extpl_app') }}
	
	WHERE DeltaFlag = 'S'
	),
inrejectwithoutsourcerec
AS (
	SELECT PL_APP_ID_R AS PL_APP_ID,
		NOMINATED_BRANCH_ID_R AS NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID_R AS PL_PACKAGE_CAT_ID,
		ORIG_ETL_D_R AS ORIG_ETL_D
	
	FROM {{ ref('xfmseparaterejectwithoutsourceandtherest__extpl_app') }}
	
	WHERE DeltaFlag = 'R'
	)

SELECT PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	ORIG_ETL_D

FROM insourcerec


UNION ALL

SELECT PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	ORIG_ETL_D

FROM inrejectwithoutsourcerec

