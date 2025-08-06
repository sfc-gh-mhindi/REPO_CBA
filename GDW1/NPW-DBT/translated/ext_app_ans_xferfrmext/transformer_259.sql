{{ config(materialized='view', tags=['Ext_APP_ANS_XFERFrmExt']) }}

WITH Transformer_259 AS (
	SELECT
		-- *SRC*: \(20)If valid.APPT_QLFY_C = 'PL' then 'Y' ELSE  IF valid.APPT_QLFY_C = 'HL' THEN 'Y' ELSE 'N',
		IFF({{ ref('LkpReferences') }}.APPT_QLFY_C = 'PL', 'Y', IFF({{ ref('LkpReferences') }}.APPT_QLFY_C = 'HL', 'Y', 'N')) AS COND,
		-- *SRC*: \(20)IF trim(Len(valid.QA_QUESTION_ID)) = '0' Then 'DL' else 'L',
		IFF(TRIM(LEN({{ ref('LkpReferences') }}.QA_QUESTION_ID)) = '0', 'DL', 'L') AS COND1,
		-- *SRC*: \(20)If Len(( IF IsNotNull((valid.CIF_CODE)) THEN (valid.CIF_CODE) ELSE "")) > '0' then 'Y' ELSE 'N',
		IFF(LEN(IFF({{ ref('LkpReferences') }}.CIF_CODE IS NOT NULL, {{ ref('LkpReferences') }}.CIF_CODE, '')) > '0', 'Y', 'N') AS COND2,
		-- *SRC*: \(20)If valid.APPT_QLFY_C = 'HL' Then '3134' Else '3135',
		IFF({{ ref('LkpReferences') }}.APPT_QLFY_C = 'HL', '3134', '3135') AS SUBCODE,
		-- *SRC*: trim("CSE" : "C3" : valid.HL_APP_ID),
		TRIM(CONCAT(CONCAT('CSE', 'C3'), {{ ref('LkpReferences') }}.HL_APP_ID)) AS EVNT_I,
		{{ ref('LkpReferences') }}.QA_QUESTION_ID AS QSTN_C,
		{{ ref('LkpReferences') }}.ORIG_ETL_D AS EFFT_D,
		{{ ref('LkpReferences') }}.QA_ANSWER_ID AS RESP_C,
		{{ ref('LkpReferences') }}.TEXT_ANSWER AS RESP_CMMT_X,
		-- *SRC*: SetNull(),
		SETNULL() AS EXPY_D,
		'0' AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		{{ ref('LkpReferences') }}.HL_APP_ID AS SRCE_SYST_EVNT_I,
		'CSE' AS SRCE_SYST_C,
		'3098' AS EVNT_ACTV_TYPE_C_XS,
		SUBCODE AS EVNT_ACTV_TYPE_C,
		'INIT' AS DEPT_ROLE_C,
		{{ ref('LkpReferences') }}.LODGEMENT_BRANCH_ID AS DEPT_I,
		{{ ref('LkpReferences') }}.CBA_STAFF_NUMBER AS EMPL_I,
		'PACT' AS EVNT_PATY_ROLE_TYPE_C_EE,
		'RESP' AS EVNT_PATY_ROLE_TYPE_C,
		{{ ref('LkpReferences') }}.CIF_CODE AS SRCE_SYST_PATY_I,
		{{ ref('LkpReferences') }}.CIF_CODE AS PATY_I,
		-- *SRC*: trim("CSE" : "XS" : valid.HL_APP_ID),
		TRIM(CONCAT(CONCAT('CSE', 'XS'), {{ ref('LkpReferences') }}.HL_APP_ID)) AS RELD_EVNT_I,
		'XSEL' AS EVNT_REL_TYPE_C,
		APPT_QLFY_C,
		MOD_TIMESTAMP,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('LkpReferences') }}
	WHERE COND = 'Y' AND COND1 = 'L' AND COND2 = 'Y'
)

SELECT * FROM Transformer_259