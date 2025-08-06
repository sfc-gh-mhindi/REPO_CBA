{{ config(materialized='view', tags=['XfmAppt_Qstn']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20110110 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20110110")  }})
Src_CSE_COM_BUS_APP_NCPR_ANS AS (
	SELECT RECORD_TYPE,
		APP_ANSWER_ID,
		APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		GL_DEPT_NUMBER,
		SUBTYPE_CODE
	FROM _cba__app_csel4_dev_inprocess_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20110110
)

SELECT * FROM Src_CSE_COM_BUS_APP_NCPR_ANS