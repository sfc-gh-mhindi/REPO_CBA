{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH Copy_248 AS (
	SELECT
		{{ ref('LkpMAP_CSE_APPT_QSTN_HLLks') }}.QA_ANSWER_ID AS QA_ANSWER_ID_1,
		RESP_C,
		{{ ref('LkpMAP_CSE_APPT_QSTN_HLLks') }}.QA_ANSWER_ID AS QA_ANSWER_ID_2
	FROM {{ ref('LkpMAP_CSE_APPT_QSTN_HLLks') }}
)

SELECT * FROM Copy_248