{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH LkpQlfyCQstnC AS (
	SELECT
		{{ ref('Transformer') }}.APP_ID,
		{{ ref('Transformer') }}.QA_QUESTION_ID,
		{{ ref('Transformer') }}.QA_ANSWER_ID,
		{{ ref('Transformer') }}.TEXT_ANSWER,
		{{ ref('Transformer') }}.CIF_CODE,
		{{ ref('Transformer') }}.SUBTYPE_CODE,
		{{ ref('MAP_CSE_APPT_QSTN_COMN') }}.QSTN_C,
		{{ ref('Transformer') }}.RESP_C,
		{{ ref('Transformer') }}.QA_ANSWER_ID_CHK,
		{{ ref('MAP_CSE_APPT_QLFY') }}.APPT_QLFY_C
	FROM {{ ref('Transformer') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_QLFY') }} ON {{ ref('Transformer') }}.SUBTYPE_CODE = {{ ref('MAP_CSE_APPT_QLFY') }}.SBTY_CODE
	LEFT JOIN {{ ref('MAP_CSE_APPT_QSTN_COMN') }} ON {{ ref('Transformer') }}.QA_QUESTION_ID = {{ ref('MAP_CSE_APPT_QSTN_COMN') }}.QA_QUESTION_ID
)

SELECT * FROM LkpQlfyCQstnC