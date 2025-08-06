{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH Transformer AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.CIF_CODE) Then 'Y' Else  If Trim(FrmSrc.CIF_CODE) = '' Then 'Y' Else  If Trim(FrmSrc.CIF_CODE) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.CIF_CODE IS NULL, 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.CIF_CODE) = '', 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.CIF_CODE) = 0, 'Y', 'N'))) AS svIsNullCifCode,
		-- *SRC*: \(20)If IsNull(FrmSrc.QA_QUESTION_ID) Then 'Y' Else  If Trim(FrmSrc.QA_QUESTION_ID) = '' Then 'Y' Else  If Trim(FrmSrc.QA_QUESTION_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.QA_QUESTION_ID IS NULL, 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.QA_QUESTION_ID) = '', 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.QA_QUESTION_ID) = 0, 'Y', 'N'))) AS svIsNullQaQuestionId,
		-- *SRC*: \(20)If IsNull(FrmSrc.APP_ID) Then 'Y' Else  If Trim(FrmSrc.APP_ID) = '' Then 'Y' Else  If Trim(FrmSrc.APP_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.APP_ID IS NULL, 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.APP_ID) = '', 'Y', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.APP_ID) = 0, 'Y', 'N'))) AS svIsNullAppId,
		-- *SRC*: \(20)If IsNull(FrmSrc.QA_ANSWER_ID) Then 'N' Else  If Trim(FrmSrc.QA_ANSWER_ID) = '' Then 'N' Else  If Trim(FrmSrc.QA_ANSWER_ID) = 0 Then 'N' Else 'Y',
		IFF({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.QA_ANSWER_ID IS NULL, 'N', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.QA_ANSWER_ID) = '', 'N', IFF(TRIM({{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}.QA_ANSWER_ID) = 0, 'N', 'Y'))) AS svlsNullQaAnswerId,
		APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		SUBTYPE_CODE,
		svlsNullQaAnswerId AS RESP_C,
		svlsNullQaAnswerId AS QA_ANSWER_ID_CHK
	FROM {{ ref('Src_CSE_COM_BUS_APP_NCPR_ANS') }}
	WHERE svIsNullAppId = 'N' AND svIsNullCifCode = 'N' AND svIsNullQaQuestionId = 'N'
)

SELECT * FROM Transformer