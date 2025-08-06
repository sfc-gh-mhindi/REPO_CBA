{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__premap', incremental_strategy='insert_overwrite', tags=['ExtAppCon_TuApp']) }}

SELECT
	SUBTYPE_CODE,
	HL_APP_PROD_ID,
	TU_APP_CONDITION_ID,
	TU_APP_CONDITION_CAT_ID,
	CONDITION_MET_DATE,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}