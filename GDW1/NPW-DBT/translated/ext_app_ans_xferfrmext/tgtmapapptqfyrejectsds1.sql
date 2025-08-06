{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__map__rej__xs', incremental_strategy='insert_overwrite', tags=['Ext_APP_ANS_XFERFrmExt']) }}

SELECT
	APP_ID,
	QA_QUESTION_ID,
	QA_ANSWER_ID,
	TEXT_ANSWER,
	CIF_CODE,
	CBA_STAFF_NUMBER,
	LODGEMENT_BRANCH_ID,
	SBTY_CODE,
	MOD_TIMESTAMP,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('OutRejApptQfy__rej_qlf') }}