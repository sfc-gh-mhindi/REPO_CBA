{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckBusApPrdClntIdNulls.APP_PROD_CLIENT_ROLE_ID)) THEN (XfmCheckBusApPrdClntIdNulls.APP_PROD_CLIENT_ROLE_ID) ELSE ""))) = 0) Then 'REJ5005' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyBusApPrdClntSeq') }}.APP_PROD_CLIENT_ROLE_ID IS NOT NULL, {{ ref('CpyBusApPrdClntSeq') }}.APP_PROD_CLIENT_ROLE_ID, ''))) = 0, 'REJ5005', '') AS ErrorCode,
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE
	FROM {{ ref('CpyBusApPrdClntSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckBusApPrdClntIdNulls__OutCheckBusApPrdClntIdNullsSorted