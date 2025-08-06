{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__premap', incremental_strategy='insert_overwrite', tags=['ExtCCL_HL_APP']) }}

SELECT
	CCL_HL_APP_ID,
	CCL_APP_ID,
	HL_APP_ID,
	LMI_AMT,
	HL_PACKAGE_CAT_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}