{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__state__premap', incremental_strategy='insert_overwrite', tags=['ExtSmCaseState']) }}

SELECT
	SM_CASE_STATE_ID,
	SM_CASE_ID,
	SM_STATE_CAT_ID,
	START_DATE,
	END_DATE,
	CREATED_BY_STAFF_NUMBER,
	STATE_CAUSED_BY_ACTION_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeSmCaseState') }}