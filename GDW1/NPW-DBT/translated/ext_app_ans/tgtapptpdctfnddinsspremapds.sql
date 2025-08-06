{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__xs__chl__cpl__bus__app__ans__premap', incremental_strategy='insert_overwrite', tags=['Ext_APP_ANS']) }}

SELECT
	HL_APP_ID,
	QA_QUESTION_ID,
	QA_ANSWER_ID,
	TEXT_ANSWER,
	CIF_CODE,
	CBA_STAFF_NUMBER,
	LODGEMENT_BRANCH_ID,
	SUBTYPE_CODE,
	MOD_TIMESTAMP,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}