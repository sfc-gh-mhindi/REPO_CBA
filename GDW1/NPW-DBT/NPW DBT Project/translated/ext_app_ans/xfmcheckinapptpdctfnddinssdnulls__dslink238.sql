{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH XfmCheckInApptPdctFnddInssdNulls__DSLink238 AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.HL_APP_ID)) THEN (XfmCheckInApptPdctFnddInssdNulls.HL_APP_ID) ELSE ""))) = 0 Then 'REJ786' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID, ''))) = 0, 'REJ786', '') AS ErrorCode,
		-- *SRC*: 'CSEHL' : XfmCheckInApptPdctFnddInssdNulls.HL_APP_ID,
		CONCAT('CSEHL', {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID) AS APPT_I,
		{{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_QUESTION_ID AS QSTN_ID,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_ANSWER_ID AS ANS_ID,
		-- *SRC*: \(20)If IsNull(XfmCheckInApptPdctFnddInssdNulls.TEXT_ANSWER) Or XfmCheckInApptPdctFnddInssdNulls.TEXT_ANSWER = '0' Then SetNull() Else XfmCheckInApptPdctFnddInssdNulls.TEXT_ANSWER,
		IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TEXT_ANSWER IS NULL OR {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TEXT_ANSWER = '0', SETNULL(), {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TEXT_ANSWER) AS RESP_CMMT_X,
		-- *SRC*: StringToDate('99991231', "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		-- *SRC*: 'CIFPT+' : Str('0', 10 - Len(Trim(XfmCheckInApptPdctFnddInssdNulls.CIF_CODE))) : Trim(XfmCheckInApptPdctFnddInssdNulls.CIF_CODE),
		CONCAT(CONCAT('CIFPT+', STR('0', 10 - LEN(TRIM({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.CIF_CODE)))), TRIM({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.CIF_CODE)) AS PATY_I,
		0 AS ROW_SECU_ACCS_C,
		'1' AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM,
		-- *SRC*: \(20)If Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.YN_FLAG_ANSWER)) THEN (XfmCheckInApptPdctFnddInssdNulls.YN_FLAG_ANSWER) ELSE "")) = '' Then SetNull() Else Trim(XfmCheckInApptPdctFnddInssdNulls.YN_FLAG_ANSWER),
		IFF(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.YN_FLAG_ANSWER IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.YN_FLAG_ANSWER, '')) = '', SETNULL(), TRIM({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.YN_FLAG_ANSWER)) AS YN_FLAG_ANSWER
	FROM {{ ref('CpyInApptPdctFnddInssSeqSeq') }}
	WHERE {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.SUBTYPE_CODE = 'HL' OR {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.SUBTYPE_CODE = 'TU' AND SUBSTRING(ErrorCode, 1, 3) <> 'REJ' AND {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_QUESTION_ID IS NOT NULL AND IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID, 0) <> 0 AND TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_ID, '')) <> '' AND IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_QUESTION_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_QUESTION_ID, 0) <> 0 AND TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_QUESTION_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.QA_QUESTION_ID, '')) <> ''
)

SELECT * FROM XfmCheckInApptPdctFnddInssdNulls__DSLink238