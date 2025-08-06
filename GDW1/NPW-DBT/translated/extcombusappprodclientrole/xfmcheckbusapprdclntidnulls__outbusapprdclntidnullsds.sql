{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH XfmCheckBusApPrdClntIdNulls__OutBusApPrdClntIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckBusApPrdClntIdNulls.APP_PROD_CLIENT_ROLE_ID)) THEN (XfmCheckBusApPrdClntIdNulls.APP_PROD_CLIENT_ROLE_ID) ELSE ""))) = 0) Then 'REJ5005' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyBusApPrdClntSeq') }}.APP_PROD_CLIENT_ROLE_ID IS NOT NULL, {{ ref('CpyBusApPrdClntSeq') }}.APP_PROD_CLIENT_ROLE_ID, ''))) = 0, 'REJ5005', '') AS ErrorCode,
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyBusApPrdClntSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckBusApPrdClntIdNulls__OutBusApPrdClntIdNullsDS