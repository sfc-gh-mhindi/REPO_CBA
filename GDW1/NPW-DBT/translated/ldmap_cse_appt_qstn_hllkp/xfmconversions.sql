{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QSTN_HLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: AsInteger(InMAP_CSE_APPT_QSTN_HLTera.QA_QUESTION_ID),
		ASINTEGER({{ ref('SrcMAP_CSE_APPT_QSTN_HLTera') }}.QA_QUESTION_ID) AS QA_QUESTION_ID,
		QSTN_C
	FROM {{ ref('SrcMAP_CSE_APPT_QSTN_HLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions