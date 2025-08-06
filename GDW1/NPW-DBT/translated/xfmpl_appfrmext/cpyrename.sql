{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH CpyRename AS (
	SELECT
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcPlAppPremapDS') }}
)

SELECT * FROM CpyRename