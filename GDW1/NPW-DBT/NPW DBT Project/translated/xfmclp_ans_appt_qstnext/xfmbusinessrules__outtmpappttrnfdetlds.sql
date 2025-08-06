{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH XfmBusinessRules__OutTmpApptTrnfDetlDS AS (
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
		-- *SRC*: 'CSE' : InXfmBusinessRules.APPT_QLFY_C : InXfmBusinessRules.APP_ID,
		CONCAT(CONCAT('CSE', {{ ref('Modify_226') }}.APPT_QLFY_C), {{ ref('Modify_226') }}.APP_ID) AS APPT_I,
		{{ ref('Modify_226') }}.QSTN_ID AS QSTN_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('Modify_226') }}.QA_ANSWER_ID AS RESP_C,
		{{ ref('Modify_226') }}.TEXT_ANSWER AS RESP_CMMT_X,
		-- *SRC*: StringToDate('99991231', "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		{{ ref('Modify_226') }}.CIF_CODE AS PATY_I,
		svSecCode AS ROW_SECU_ACCS_C,
		'1' AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('Modify_226') }}
	WHERE svRejectFlag = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptTrnfDetlDS