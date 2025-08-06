{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH FunMergeSourceAndRejt AS (
	SELECT
		APP_PROD_ID as APP_PROD_ID,
		COM_SUBTYPE_CODE as COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID as CAMPAIGN_CAT_ID,
		COM_APP_ID as COM_APP_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeSourceAndRejt