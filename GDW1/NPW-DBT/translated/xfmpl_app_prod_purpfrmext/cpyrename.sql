{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH CpyRename AS (
	SELECT
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcPlAppProdPurpPremapDS') }}
)

SELECT * FROM CpyRename