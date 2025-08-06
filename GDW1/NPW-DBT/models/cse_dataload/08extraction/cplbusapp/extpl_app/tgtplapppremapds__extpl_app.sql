{{
  config(
    post_hook=[
      "INSERT OVERWRITE INTO " ~ cvar("datasets_schema") ~ "."~ cvar("base_dir") ~ "__dataset__"~ cvar("run_stream") ~"_PREMAP__DS SELECT * FROM {{ this}}"
    ]
  )
}}

with TgtPlAppPremapDS as (
	SELECT PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ORIG_ETL_D,
		ROW_NUMBER() OVER (
			PARTITION BY PL_APP_ID
			ORDER BY PL_APP_ID COLLATE 'en' ASC NULLS FIRST
		) AS sorting_order

	FROM {{ ref('funmergejournal__extpl_app') }}
)

SELECT PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	ORIG_ETL_D
FROM TgtPlAppPremapDS

