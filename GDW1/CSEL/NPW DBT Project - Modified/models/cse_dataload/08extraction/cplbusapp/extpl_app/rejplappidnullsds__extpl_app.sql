{{
  config(
    post_hook=[
      'INSERT OVERWRITE INTO '~ cvar("intermediate_db") ~ '.'~cvar("datasets_schema") ~ '.'~ cvar("base_dir") ~ '__dataset__'~ cvar("run_stream") ~'_PlApp_Nulls_Rejects__DS SELECT * FROM {{ this }}'
    ]
  )
}}

SELECT PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	'{{ cvar("etl_process_dt") }}' AS ETL_D,
	'{{ cvar("etl_process_dt") }}' AS ORIG_ETL_D,
	ErrorCode AS EROR_C

FROM {{ ref('xfmcheckplappidnulls__extpl_app') }}

WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'

