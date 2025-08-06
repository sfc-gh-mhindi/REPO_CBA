{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH XfmCheckInApptRelIdNulls__OutApptRelIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckInApptRelIdNulls.HL_APP_ID)) THEN (XfmCheckInApptRelIdNulls.HL_APP_ID) ELSE ""))) = 0) Then 'REJ2009' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyInApptRelSeqSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyInApptRelSeqSeq') }}.HL_APP_ID, ''))) = 0, 'REJ2009', '') AS ErrorCode,
		CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyInApptRelSeqSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckInApptRelIdNulls__OutApptRelIdNullsDS