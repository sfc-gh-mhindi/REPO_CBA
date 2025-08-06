{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__sm__case__idnull__rejects', incremental_strategy='insert_overwrite', tags=['ExtComBusSmCase']) }}

SELECT
	SM_CASE_ID,
	CREATED_TIMESTAMP,
	WIM_PROCESS_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckIdNull__OutIdNullDS') }}