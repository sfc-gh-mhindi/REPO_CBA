{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__premap', incremental_strategy='insert_overwrite', tags=['ExtComBusSmCase']) }}

SELECT
	SM_CASE_ID,
	CREATED_TIMESTAMP,
	WIM_PROCESS_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}