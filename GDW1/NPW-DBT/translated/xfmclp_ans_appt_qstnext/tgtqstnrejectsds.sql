{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_cse__clp__bus__appt__qstn__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

SELECT
	APP_ID,
	SUBTYPE_CODE,
	QA_QUESTION_ID,
	QA_ANSWER_ID,
	TEXT_ANSWER,
	CIF_CODE,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__RejectsDS') }}