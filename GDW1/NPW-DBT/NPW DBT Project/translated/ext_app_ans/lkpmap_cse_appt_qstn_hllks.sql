{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH 
_cba__app_csel4_dev_dataset_map__cse__appt__qstn__resp__hl__qa__answer__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_map__cse__appt__qstn__resp__hl__qa__answer__id")  }})
LkpMAP_CSE_APPT_QSTN_HLLks AS (
	SELECT QA_ANSWER_ID,
		RESP_C
	FROM _cba__app_csel4_dev_dataset_map__cse__appt__qstn__resp__hl__qa__answer__id
)

SELECT * FROM LkpMAP_CSE_APPT_QSTN_HLLks