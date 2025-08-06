{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_map__cse__appt__qstn__resp__hl__qa__answer__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_QSTN_RESP_HLLkp']) }}

SELECT
	QA_ANSWER_ID,
	RESP_C 
FROM {{ ref('XfmConversions') }}