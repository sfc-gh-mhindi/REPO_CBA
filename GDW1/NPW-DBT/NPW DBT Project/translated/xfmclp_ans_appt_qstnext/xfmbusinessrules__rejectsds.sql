{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH XfmBusinessRules__RejectsDS AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.ROW_S = '9' then 'REJ903' else 'L',
		IFF({{ ref('Modify_226') }}.ROW_S = '9', 'REJ903', 'L') AS svErrorCode,
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' then 'REJ902' else 'L',
		IFF({{ ref('Modify_226') }}.APPT_QLFY_C = '99', 'REJ902', 'L') AS svErrorCode1,
		-- *SRC*: \(20)If InXfmBusinessRules.QA_ANSWER_ID = '9' then 'REJ904' else 'L',
		IFF({{ ref('Modify_226') }}.QA_ANSWER_ID = '9', 'REJ904', 'L') AS svErrorCode2,
		-- *SRC*: \(20)If InXfmBusinessRules.ROW_S = 'Y' then '4' else '0',
		IFF({{ ref('Modify_226') }}.ROW_S = 'Y', '4', '0') AS svSecCode,
		-- *SRC*: \(20)If svErrorCode = "L" AND svErrorCode1 = "L" AND svErrorCode2 = "L" Then 'Y' Else 'N',
		IFF(svErrorCode = 'L' AND svErrorCode1 = 'L' AND svErrorCode2 = 'L', 'Y', 'N') AS svRejectFlag,
		APP_ID,
		{{ ref('Modify_226') }}.APPT_QLFY_C AS SUBTYPE_CODE,
		{{ ref('Modify_226') }}.QSTN_ID AS QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		ETL_PROCESS_DT AS ETL_D,
		-- *SRC*: StringToDate('99991231', "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS ORIG_ETL_D,
		'REJ902903' AS EROR_C
	FROM {{ ref('Modify_226') }}
	WHERE svRejectFlag = 'N'
)

SELECT * FROM XfmBusinessRules__RejectsDS