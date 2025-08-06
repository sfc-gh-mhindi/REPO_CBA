{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH XfmCheckPlAppProdPurpIdNulls__OutPlAppProdPurpIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckPlAppProdPurpIdNulls.PL_APP_PROD_PURP_ID)) THEN (XfmCheckPlAppProdPurpIdNulls.PL_APP_PROD_PURP_ID) ELSE ""))) = 0) Then 'REJ2007' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyPlAppProdPurpSeq') }}.PL_APP_PROD_PURP_ID IS NOT NULL, {{ ref('CpyPlAppProdPurpSeq') }}.PL_APP_PROD_PURP_ID, ''))) = 0, 'REJ2007', '') AS ErrorCode,
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyPlAppProdPurpSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckPlAppProdPurpIdNulls__OutPlAppProdPurpIdNullsDS