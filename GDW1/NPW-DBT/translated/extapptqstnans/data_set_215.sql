{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_cse__clp__appt__qstn__fun__cse__clp__bus__appt__qstn__20080212 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_cse__clp__appt__qstn__fun__cse__clp__bus__appt__qstn__20080212")  }})
Data_Set_215 AS (
	SELECT APP_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE
	FROM _cba__app_lpxs_lpxs__dev_dataset_cse__clp__appt__qstn__fun__cse__clp__bus__appt__qstn__20080212
)

SELECT * FROM Data_Set_215