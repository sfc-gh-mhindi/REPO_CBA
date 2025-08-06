{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH Xfm__ToErrQstnC AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTransfrmr.TEXT_ANSWER) Then 'Y' Else  If Trim(ToTransfrmr.TEXT_ANSWER) = '' Then 'Y' Else  if Trim(ToTransfrmr.TEXT_ANSWER) = '0' Then 'Y' Else 'N',
		IFF({{ ref('Fnnl') }}.TEXT_ANSWER IS NULL, 'Y', IFF(TRIM({{ ref('Fnnl') }}.TEXT_ANSWER) = '', 'Y', IFF(TRIM({{ ref('Fnnl') }}.TEXT_ANSWER) = '0', 'Y', 'N'))) AS svIsNullTextAnswer,
		-- *SRC*: \(20)If IsNull(ToTransfrmr.APPT_QLFY_C) Then 'Y' Else  If Trim(ToTransfrmr.APPT_QLFY_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('Fnnl') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM({{ ref('Fnnl') }}.APPT_QLFY_C) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: \(20)If IsNull(ToTransfrmr.QSTN_C) Then 'Y' Else  If Trim(ToTransfrmr.QSTN_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('Fnnl') }}.QSTN_C IS NULL, 'Y', IFF(TRIM({{ ref('Fnnl') }}.QSTN_C) = '', 'Y', 'N')) AS svIsNullQstnC,
		-- *SRC*: \(20)If IsNull(ToTransfrmr.RESP_C) Then 'Y' Else  If Trim(ToTransfrmr.RESP_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('Fnnl') }}.RESP_C IS NULL, 'Y', IFF(TRIM({{ ref('Fnnl') }}.RESP_C) = '', 'Y', 'N')) AS svIsNullRespC,
		-- *SRC*: \(20)If IsNull(ToTransfrmr.QA_ANSWER_ID) Then 'Y' Else  If Trim(ToTransfrmr.QA_ANSWER_ID) = '' Then 'Y' Else  If Trim(ToTransfrmr.QA_ANSWER_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Fnnl') }}.QA_ANSWER_ID IS NULL, 'Y', IFF(TRIM({{ ref('Fnnl') }}.QA_ANSWER_ID) = '', 'Y', IFF(TRIM({{ ref('Fnnl') }}.QA_ANSWER_ID) = 0, 'Y', 'N'))) AS svlsNullAnswrId,
		-- *SRC*: TRIM(ToTransfrmr.APP_ID),
		TRIM({{ ref('Fnnl') }}.APP_ID) AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_QSTN' AS CONV_MAP_RULE_M,
		'APPT_QSTN' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		-- *SRC*: TRIM(ToTransfrmr.QA_QUESTION_ID),
		TRIM({{ ref('Fnnl') }}.QA_QUESTION_ID) AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'QSTN_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_COM_BUS_APP_NCPR_ANS' AS SRCE_FILE_M,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: \(20)if svIsNullApptQlfyC = 'Y' Then "CSE" : '9999' : Trim(ToTransfrmr.APP_ID) else "CSE" : ToTransfrmr.APPT_QLFY_C : Trim(ToTransfrmr.APP_ID),
		IFF(svIsNullApptQlfyC = 'Y', CONCAT(CONCAT('CSE', '9999'), TRIM({{ ref('Fnnl') }}.APP_ID)), CONCAT(CONCAT('CSE', {{ ref('Fnnl') }}.APPT_QLFY_C), TRIM({{ ref('Fnnl') }}.APP_ID))) AS TRSF_KEY_I
	FROM {{ ref('Fnnl') }}
	WHERE svIsNullQstnC = 'Y'
)

SELECT * FROM Xfm__ToErrQstnC