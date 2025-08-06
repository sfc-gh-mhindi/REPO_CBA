{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_chl__bus__hl__app__ans__hl__20071119', incremental_strategy='insert_overwrite', tags=['Ext_Hl_File_Ds']) }}

SELECT
	RECORD_TYPE,
	MOD_TIMESTAMP,
	HL_APP_ID,
	QA_QUESTION_ID,
	QA_ANSWER_ID,
	TEXT_ANSWER,
	CIF_CODE,
	CBA_STAFF_NUMBER,
	LODGEMENT_BRANCH_ID,
	SUBTYPE_CODE,
	YN_FLAG_ANSWER,
	Dummy 
FROM {{ ref('Sequential_File_0') }}