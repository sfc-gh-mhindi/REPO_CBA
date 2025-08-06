{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_cse__clp__bus__appt__qstn__nulls__rejects20080212', incremental_strategy='insert_overwrite', tags=['ExtApptQstnAns']) }}

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
FROM {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCCAppProdBalXferIdNullsDS') }}