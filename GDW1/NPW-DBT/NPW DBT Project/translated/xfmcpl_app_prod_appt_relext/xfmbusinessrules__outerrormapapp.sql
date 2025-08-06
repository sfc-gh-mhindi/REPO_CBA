{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH XfmBusinessRules__OutErrorMapApp AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' then 'REJ787' else 'L',
		IFF({{ ref('Modify_226') }}.APPT_QLFY_C = '99', 'REJ787', 'L') AS svErrorCode,
		-- *SRC*: \(20)If InXfmBusinessRules.LOAN_APPT_QLFY_C = '99' then 'REJ788' else 'L',
		IFF({{ ref('Modify_226') }}.LOAN_APPT_QLFY_C = '99', 'REJ788', 'L') AS svErrorCode1,
		-- *SRC*: \(20)If svErrorCode = "L" AND svErrorCode1 = "L" Then 'Y' Else 'N',
		IFF(svErrorCode = 'L' AND svErrorCode1 = 'L', 'Y', 'N') AS svRejectFlag,
		{{ ref('Modify_226') }}.APPT_I AS SRCE_KEY_I,
		'CSE_CLP_BUS_APPT_REL' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_QLFY' AS TRSF_TABL_M,
		{{ ref('Modify_226') }}.APPT_QLFY_C AS VALU_CHNG_BFOR_X,
		{{ ref('Modify_226') }}.APPT_QLFY_C AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'SBTY_CODE' AS TRSF_COLM_M
	FROM {{ ref('Modify_226') }}
	WHERE svErrorCode = 'REJ787'
)

SELECT * FROM XfmBusinessRules__OutErrorMapApp