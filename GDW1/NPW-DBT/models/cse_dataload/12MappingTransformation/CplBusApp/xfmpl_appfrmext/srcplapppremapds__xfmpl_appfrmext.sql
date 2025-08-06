WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__premap AS (
	SELECT 
    PL_APP_ID,
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    ORIG_ETL_D
    from {{ cvar("datasets_schema") }}.{{ cvar("base_dir") }}__dataset__{{ cvar("run_stream") }}_premap__ds
),
SrcPlAppPremapDS AS (
SELECT
    PL_APP_ID,
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    ORIG_ETL_D
    from _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__premap
)

SELECT * FROM SrcPlAppPremapDS