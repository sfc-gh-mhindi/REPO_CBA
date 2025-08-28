WITH 
before_cte as(
  SELECT
  dummy
  FROM {{ ref('before__xfmpl_appfrmext') }}
  where 1=2
),

srcplapppremapds AS (
	SELECT 
    PL_APP_ID,
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    ORIG_ETL_D
  from {{ cvar("datasets_schema") }}.{{ cvar("base_dir") }}__dataset__{{ cvar("run_stream") }}_premap__ds
)

SELECT * FROM srcplapppremapds