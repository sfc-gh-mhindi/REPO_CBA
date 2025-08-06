{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH LKP AS (
	SELECT
		{{ ref('Filter_AnswrId') }}.APP_ID,
		{{ ref('Filter_AnswrId') }}.QA_QUESTION_ID,
		{{ ref('Filter_AnswrId') }}.QA_ANSWER_ID,
		{{ ref('Filter_AnswrId') }}.TEXT_ANSWER,
		{{ ref('Filter_AnswrId') }}.CIF_CODE,
		{{ ref('Filter_AnswrId') }}.SUBTYPE_CODE,
		{{ ref('Filter_AnswrId') }}.APPT_QLFY_C,
		{{ ref('Filter_AnswrId') }}.QSTN_C,
		{{ ref('MAP_CSE_APPT_QSTN_RESP_CN') }}.RESP_C
	FROM {{ ref('Filter_AnswrId') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_QSTN_RESP_CN') }} ON {{ ref('Filter_AnswrId') }}.QA_ANSWER_ID = {{ ref('MAP_CSE_APPT_QSTN_RESP_CN') }}.QA_ANSWER_ID
	WHERE QA_ANSWER_ID_CHK = 'Y'
)

SELECT * FROM LKP