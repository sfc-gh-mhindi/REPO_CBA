{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__clnt__ans__cse__clp__bus__appt__qstn__20080114', incremental_strategy='insert_overwrite', tags=['XfmClpCntAnsExt']) }}

SELECT
	APP_ID,
	APP_PROD_ID,
	SUBTYPE_CODE,
	QA_QUESTION_ID,
	QA_ANSWER_ID,
	TEXT_ANSWER,
	CIF_CODE 
FROM {{ ref('Identify_IDs_ComPlAppProd') }}