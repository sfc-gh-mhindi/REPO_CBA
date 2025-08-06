{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH FunMergeJournal AS (
	SELECT
		PL_APP_PROD_PURP_ID as PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID as PL_APP_PROD_PURP_CAT_ID,
		AMT as AMT,
		PL_APP_PROD_ID as PL_APP_PROD_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal