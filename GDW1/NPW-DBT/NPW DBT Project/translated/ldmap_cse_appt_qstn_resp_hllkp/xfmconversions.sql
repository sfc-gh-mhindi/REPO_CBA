{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QSTN_RESP_HLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(( IF IsNotNull((InMAP_CSE_APPT_QSTN_HLTera.QA_ANSWER_ID)) THEN (InMAP_CSE_APPT_QSTN_HLTera.QA_ANSWER_ID) ELSE "")),
		TRIM(IFF({{ ref('SrcMAP_CSE_APPT_QSTN_HLTera') }}.QA_ANSWER_ID IS NOT NULL, {{ ref('SrcMAP_CSE_APPT_QSTN_HLTera') }}.QA_ANSWER_ID, '')) AS QA_ANSWER_ID,
		RESP_C
	FROM {{ ref('SrcMAP_CSE_APPT_QSTN_HLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions