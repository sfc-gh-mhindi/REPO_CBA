SELECT 
	CASE 
		WHEN LEN(TRIM(CASE 
						WHEN PL_APP_ID IS NOT NULL
							THEN PL_APP_ID
						ELSE ''
					END)) = 0
			THEN 'R'
		ELSE 'S'
	END AS DeltaFlag,
	PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	PL_APP_ID_R,
	NOMINATED_BRANCH_ID_R,
	PL_PACKAGE_CAT_ID_R,
	ORIG_ETL_D_R

FROM {{ ref('joinsrcsortreject__extpl_app') }}

