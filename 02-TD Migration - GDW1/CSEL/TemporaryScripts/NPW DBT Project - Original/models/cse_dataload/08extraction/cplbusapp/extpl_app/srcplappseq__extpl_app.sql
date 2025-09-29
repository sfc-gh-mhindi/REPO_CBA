WITH 
before_cte as(
	SELECT
	dummy
	from {{ ref('before__extpl_app') }}
	where 1=2
),
srcplappseq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		DUMMY
	
	FROM {{ cvar("intermediate_db") }}.{{ cvar("files_schema") }}.{{ cvar("base_dir") }}__inprocess__CSE_CPL_BUS_APP_{{ cvar("run_stream") }}_{{ cvar("etl_process_dt") }}__DLY
)

SELECT 
	RECORD_TYPE,
	MOD_TIMESTAMP,
	PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	DUMMY
FROM srcplappseq