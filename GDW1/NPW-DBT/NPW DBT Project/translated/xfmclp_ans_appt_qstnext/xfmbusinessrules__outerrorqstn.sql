{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH XfmBusinessRules__OutErrorQstn AS (
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
		{{ ref('Modify_226') }}.APP_ID AS SRCE_KEY_I,
		'CSE_CLP_BUS_APPT_QSTN' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_QSTN' AS TRSF_TABL_M,
		{{ ref('Modify_226') }}.QA_ANSWER_ID AS VALU_CHNG_BFOR_X,
		{{ ref('Modify_226') }}.QA_ANSWER_ID AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'QSTN_ID' AS TRSF_COLM_M
	FROM {{ ref('Modify_226') }}
	WHERE svErrorCode = 'REJ903'
)

SELECT * FROM XfmBusinessRules__OutErrorQstn