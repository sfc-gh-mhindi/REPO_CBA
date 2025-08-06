{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PDCT_N: int32 = handle_null (PDCT_N, 800999)
	PL_APP_ID, NOMINATED_BRANCH_ID, PL_PACKAGE_CAT_ID, ORIG_ETL_D, PDCT_N 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling