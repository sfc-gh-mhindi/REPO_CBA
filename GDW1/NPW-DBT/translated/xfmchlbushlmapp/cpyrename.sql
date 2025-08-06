{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH CpyRename AS (
	SELECT
		APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		DCHG_REAS_ID,
		HL_APP_PROD_ID,
		ORIG_ETL_D
	FROM {{ ref('TgtHlmAppPremapDS') }}
)

SELECT * FROM CpyRename