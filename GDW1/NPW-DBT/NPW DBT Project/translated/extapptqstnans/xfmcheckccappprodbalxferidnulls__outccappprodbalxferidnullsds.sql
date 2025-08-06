{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH XfmCheckCCAppProdBalXferIdNulls__OutCCAppProdBalXferIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckCCAppProdBalXferIdNulls.QA_QUESTION_ID)) THEN (XfmCheckCCAppProdBalXferIdNulls.QA_QUESTION_ID) ELSE ""))) = 0) Then 'REJ795' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyCCAppProdBalXferSeq') }}.QA_QUESTION_ID IS NOT NULL, {{ ref('CpyCCAppProdBalXferSeq') }}.QA_QUESTION_ID, ''))) = 0, 'REJ795', '') AS ErrorCode,
		APP_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		'REJ790' AS EROR_C
	FROM {{ ref('CpyCCAppProdBalXferSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckCCAppProdBalXferIdNulls__OutCCAppProdBalXferIdNullsDS