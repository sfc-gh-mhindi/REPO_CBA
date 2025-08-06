{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH 
_cba__app_csel4_dev_dataset_map__cse__appt__qstn__hl__qa__question__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_map__cse__appt__qstn__hl__qa__question__id")  }})
LkpMAP_CSE_APPT_QSTN_RESP_HLLks AS (
	SELECT QA_QUESTION_ID,
		QSTN_C
	FROM _cba__app_csel4_dev_dataset_map__cse__appt__qstn__hl__qa__question__id
)

SELECT * FROM LkpMAP_CSE_APPT_QSTN_RESP_HLLks