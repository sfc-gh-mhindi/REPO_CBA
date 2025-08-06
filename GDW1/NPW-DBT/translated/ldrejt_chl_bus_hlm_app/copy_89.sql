{{ config(materialized='view', tags=['LdREJT_CHL_BUS_HLM_APP']) }}

WITH Copy_89 AS (
	SELECT
		HLM_APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		DISCHARGE_REASON_ID,
		HL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM {{ ref('SrcChlBusHlmAppRejectsDS') }}
)

SELECT * FROM Copy_89