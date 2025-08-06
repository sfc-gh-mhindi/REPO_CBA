{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH XfmCheckPlAppIdNulls__OutPlAppIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckPlAppIdNulls.PL_APP_ID)) THEN (XfmCheckPlAppIdNulls.PL_APP_ID) ELSE ""))) = 0) Then 'REJ2005' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyPlAppSeq') }}.PL_APP_ID IS NOT NULL, {{ ref('CpyPlAppSeq') }}.PL_APP_ID, ''))) = 0, 'REJ2005', '') AS ErrorCode,
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyPlAppSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckPlAppIdNulls__OutPlAppIdNullsDS