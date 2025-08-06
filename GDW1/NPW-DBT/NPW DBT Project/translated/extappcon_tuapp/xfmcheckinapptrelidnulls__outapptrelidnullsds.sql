{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH XfmCheckInApptRelIdNulls__OutApptRelIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckInApptRelIdNulls.HL_APP_PROD_ID)) THEN (XfmCheckInApptRelIdNulls.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'REJ2009' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyInApptRelSeqSeq') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('CpyInApptRelSeqSeq') }}.HL_APP_PROD_ID, ''))) = 0, 'REJ2009', '') AS ErrorCode,
		SUBTYPE_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_DATE,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyInApptRelSeqSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckInApptRelIdNulls__OutApptRelIdNullsDS