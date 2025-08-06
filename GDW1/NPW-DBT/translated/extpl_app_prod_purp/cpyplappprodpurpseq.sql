{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH CpyPlAppProdPurpSeq AS (
	SELECT
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID
	FROM {{ ref('SrcPlAppProdPurpSeq') }}
)

SELECT * FROM CpyPlAppProdPurpSeq