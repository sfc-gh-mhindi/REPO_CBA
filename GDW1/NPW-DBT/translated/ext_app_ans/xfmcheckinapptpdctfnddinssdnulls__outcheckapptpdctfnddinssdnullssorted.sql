{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.HL_APP_ID)) THEN (XfmCheckInApptPdctFnddInssdNulls.HL_APP_ID) ELSE ""))) = 0 Then 'REJ786' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID, ''))) = 0, 'REJ786', '') AS ErrorCode,
		MOD_TIMESTAMP,
		HL_APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SUBTYPE_CODE
	FROM {{ ref('CpyInApptPdctFnddInssSeqSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted