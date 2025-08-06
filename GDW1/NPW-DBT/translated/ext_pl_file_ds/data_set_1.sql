{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_cpl__bus__pl__app__ans__pl__20071119', incremental_strategy='insert_overwrite', tags=['Ext_Pl_File_Ds']) }}

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
	Dummy 
FROM {{ ref('Sequential_File_0') }}