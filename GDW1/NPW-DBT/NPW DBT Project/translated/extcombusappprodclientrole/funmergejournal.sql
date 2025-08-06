{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH FunMergeJournal AS (
	SELECT
		APP_PROD_CLIENT_ROLE_ID as APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID as ROLE_CAT_ID,
		CIF_CODE as CIF_CODE,
		APP_PROD_ID as APP_PROD_ID,
		SUBTYPE_CODE as SUBTYPE_CODE,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal