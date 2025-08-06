{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__premap', incremental_strategy='insert_overwrite', tags=['ExtHL_APP_PROD_PURPOSE']) }}

SELECT
	HL_APP_PROD_PURPOSE_ID,
	HL_APP_PROD_ID,
	HL_LOAN_PURPOSE_CAT_ID,
	AMOUNT,
	MAIN_PURPOSE,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}