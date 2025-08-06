{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH FunMergeJournal AS (
	SELECT
		APP_ID as APP_ID,
		HLM_ACCOUNT_ID as HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER as ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID as CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID as HLM_APP_TYPE_CAT_ID,
		DCHG_REAS_ID as DCHG_REAS_ID,
		HL_APP_PROD_ID as HL_APP_PROD_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		DCHG_REAS_ID,
		HL_APP_PROD_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal