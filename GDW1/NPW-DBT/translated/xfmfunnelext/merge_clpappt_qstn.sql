{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_dataset_cse__clp__appt__qstn__fun__cse__clp__bus__appt__qstn__20080215', incremental_strategy='insert_overwrite', tags=['XfmFunnelExt']) }}

SELECT
	APP_ID,
	SUBTYPE_CODE,
	QA_QUESTION_ID,
	QA_ANSWER_ID,
	TEXT_ANSWER,
	CIF_CODE 
FROM {{ ref('Transformer_58') }}