{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH Filter_AnswrId AS (
	SELECT
		APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		SUBTYPE_CODE,
		APPT_QLFY_C,
		QSTN_C,
		RESP_C,
		APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		SUBTYPE_CODE,
		APPT_QLFY_C,
		QSTN_C,
		RESP_C
	FROM {{ ref('LkpQlfyCQstnC') }}
)

SELECT * FROM Filter_AnswrId