{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH XfmBusinessRules__OutCCAppProdBalXferRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' then 'REJ787' else 'L',
		IFF({{ ref('Modify_226') }}.APPT_QLFY_C = '99', 'REJ787', 'L') AS svErrorCode,
		-- *SRC*: \(20)If InXfmBusinessRules.LOAN_APPT_QLFY_C = '99' then 'REJ788' else 'L',
		IFF({{ ref('Modify_226') }}.LOAN_APPT_QLFY_C = '99', 'REJ788', 'L') AS svErrorCode1,
		-- *SRC*: \(20)If svErrorCode = "L" AND svErrorCode1 = "L" Then 'Y' Else 'N',
		IFF(svErrorCode = 'L' AND svErrorCode1 = 'L', 'Y', 'N') AS svRejectFlag,
		APPT_I,
		RELD_APPT_I,
		{{ ref('Modify_226') }}.APPT_QLFY_C AS REL_TYPE_C,
		{{ ref('Modify_226') }}.LOAN_APPT_QLFY_C AS LOAN_SUBTYPE_CODE,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		'RPR787788' AS EROR_C
	FROM {{ ref('Modify_226') }}
	WHERE svRejectFlag = 'N'
)

SELECT * FROM XfmBusinessRules__OutCCAppProdBalXferRejectsDS