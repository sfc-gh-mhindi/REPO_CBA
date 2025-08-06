{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH XfmBusinessRules__OutTmpApptTrnfDetlDS AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' then 'REJ787' else 'L',
		IFF({{ ref('Modify_226') }}.APPT_QLFY_C = '99', 'REJ787', 'L') AS svErrorCode,
		-- *SRC*: \(20)If InXfmBusinessRules.LOAN_APPT_QLFY_C = '99' then 'REJ788' else 'L',
		IFF({{ ref('Modify_226') }}.LOAN_APPT_QLFY_C = '99', 'REJ788', 'L') AS svErrorCode1,
		-- *SRC*: \(20)If svErrorCode = "L" AND svErrorCode1 = "L" Then 'Y' Else 'N',
		IFF(svErrorCode = 'L' AND svErrorCode1 = 'L', 'Y', 'N') AS svRejectFlag,
		-- *SRC*: "CSE" : InXfmBusinessRules.LOAN_APPT_QLFY_C : InXfmBusinessRules.APPT_I,
		CONCAT(CONCAT('CSE', {{ ref('Modify_226') }}.LOAN_APPT_QLFY_C), {{ ref('Modify_226') }}.APPT_I) AS APPT_I,
		-- *SRC*: "CSE" : InXfmBusinessRules.APPT_QLFY_C : InXfmBusinessRules.RELD_APPT_I,
		CONCAT(CONCAT('CSE', {{ ref('Modify_226') }}.APPT_QLFY_C), {{ ref('Modify_226') }}.RELD_APPT_I) AS RELD_APPT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: InXfmBusinessRules.LOAN_APPT_QLFY_C : InXfmBusinessRules.APPT_QLFY_C,
		CONCAT({{ ref('Modify_226') }}.LOAN_APPT_QLFY_C, {{ ref('Modify_226') }}.APPT_QLFY_C) AS REL_TYPE_C,
		-- *SRC*: StringToDate('99991231', "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('Modify_226') }}
	WHERE svRejectFlag = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptTrnfDetlDS