{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__nulls__rejects', incremental_strategy='insert_overwrite', tags=['ExtFaUndertaking']) }}

SELECT
	FA_UNDERTAKING_ID,
	PLANNING_GROUP_NAME,
	COIN_ADVICE_GROUP_ID,
	ADVICE_GROUP_CORRELATION_ID,
	CREATED_DATE,
	CREATED_BY_STAFF_NUMBER,
	SM_CASE_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckIdNulls__OutIdNullsDS') }}