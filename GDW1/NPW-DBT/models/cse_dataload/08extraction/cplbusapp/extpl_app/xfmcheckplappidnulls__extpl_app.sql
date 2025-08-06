SELECT 
	CASE 
		WHEN LEN(TRIM(CASE 
						WHEN PL_APP_ID IS NOT NULL
							THEN PL_APP_ID
						ELSE ''
					END)) = 0
			THEN 'REJ2005'
		ELSE ''
	END AS ErrorCode,
	PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID

FROM {{ ref('cpyplappseq__extpl_app') }}

