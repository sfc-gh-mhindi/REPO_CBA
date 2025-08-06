{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__coi__bus__envi__evnt__premap', incremental_strategy='insert_overwrite', tags=['ExtFaEnvisionEvent']) }}

SELECT
	FA_ENVISION_EVENT_ID,
	FA_UNDERTAKING_ID,
	FA_ENVISION_EVENT_CAT_ID,
	CREATED_DATE,
	CREATED_BY_STAFF_NUMBER,
	COIN_REQUEST_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeSourceAndRejt') }}