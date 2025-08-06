{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_map__cse__appt__qstn__hl__qa__question__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_QSTN_HLLkp']) }}

SELECT
	QA_QUESTION_ID,
	QSTN_C 
FROM {{ ref('XfmConversions') }}