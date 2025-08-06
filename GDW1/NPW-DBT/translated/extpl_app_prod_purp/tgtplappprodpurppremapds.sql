{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__premap', incremental_strategy='insert_overwrite', tags=['ExtPL_APP_PROD_PURP']) }}

SELECT
	PL_APP_PROD_PURP_ID,
	PL_APP_PROD_PURP_CAT_ID,
	AMT,
	PL_APP_PROD_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}