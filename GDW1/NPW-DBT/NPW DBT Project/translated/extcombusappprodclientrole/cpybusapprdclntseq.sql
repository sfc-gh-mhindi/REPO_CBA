{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH CpyBusApPrdClntSeq AS (
	SELECT
		RECORD_TYPE,
		MOD_TIMESTAMP,
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		DUMMY
	FROM {{ ref('RemoveDuplicates') }}
)

SELECT * FROM CpyBusApPrdClntSeq