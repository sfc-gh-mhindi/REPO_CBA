{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH Xfm__Xfm_to_Tgt AS (
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
		-- *SRC*: \(20)if svIsNullApptQlfyC = 'Y' Then "CSE" : '9999' : Trim(ToTransfrmr.APP_ID) else "CSE" : ToTransfrmr.APPT_QLFY_C : Trim(ToTransfrmr.APP_ID),
		IFF(svIsNullApptQlfyC = 'Y', CONCAT(CONCAT('CSE', '9999'), TRIM({{ ref('Fnnl') }}.APP_ID)), CONCAT(CONCAT('CSE', {{ ref('Fnnl') }}.APPT_QLFY_C), TRIM({{ ref('Fnnl') }}.APP_ID))) AS APPT_I,
		-- *SRC*: \(20)if svIsNullQstnC = 'Y' Then '9999' Else ToTransfrmr.QSTN_C,
		IFF(svIsNullQstnC = 'Y', '9999', {{ ref('Fnnl') }}.QSTN_C) AS QSTN_C,
		-- *SRC*: \(20)If svlsNullAnswrId = 'Y' Then SetNull() Else  if svIsNullRespC = 'Y' Then '9999' Else ToTransfrmr.RESP_C,
		IFF(svlsNullAnswrId = 'Y', SETNULL(), IFF(svIsNullRespC = 'Y', '9999', {{ ref('Fnnl') }}.RESP_C)) AS RESP_C,
		-- *SRC*: \(20)If svIsNullTextAnswer = 'Y' Then SetNull() else ToTransfrmr.TEXT_ANSWER,
		IFF(svIsNullTextAnswer = 'Y', SETNULL(), {{ ref('Fnnl') }}.TEXT_ANSWER) AS RESP_CMMT_X,
		-- *SRC*: 'CIFPT+' : Right('0000000000' : ToTransfrmr.CIF_CODE, 10),
		CONCAT('CIFPT+', RIGHT(CONCAT('0000000000', {{ ref('Fnnl') }}.CIF_CODE), 10)) AS PATY_I,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		'0' AS ROW_SECU_ACCS_C,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Fnnl') }}
	WHERE 
)

SELECT * FROM Xfm__Xfm_to_Tgt